"""Telegram Mini App «Выставить счёт» клиенту.

Роли: admin и shop_manager. Заказ и резерв не создаём.
"""
from __future__ import annotations

import os
import re
import tempfile
from html import escape as html_escape
from decimal import Decimal, InvalidOperation
from functools import wraps
from pathlib import Path

from flask import (
    Blueprint, current_app, jsonify, request, render_template, make_response,
)
from sqlalchemy import func
from werkzeug.utils import secure_filename

from app.models import (
    db, User, Client, Plant, Size, StockBalance,
    SaleCompany, SaleInvoice, SaleInvoiceLine,
)
from app.tg_pay import resolve_user
from app.tg_sale_parse import parse_buyer_file
from app.utils import msk_now, build_pdf_bytes
from app.telegram import send_chat_document, send_message as tg_send_message, default_miniapp_url
from app.stock_helpers import get_reserved_map
from app.shop_catalog import _price_history_map

bp = Blueprint('tg_sale', __name__, url_prefix='/tg/sale')

_ALLOWED_EXT = {'.pdf', '.doc', '.docx', '.jpg', '.jpeg', '.png', '.webp', '.bmp'}
_DEV_COOKIE = 'tg_sale_as'


def public_sale_url() -> str:
    env = (os.environ.get('TG_SALE_URL') or os.environ.get('TG_MINIAPP_URL') or '').strip()
    if env:
        base = env.rstrip('/')
        if base.endswith('/tg/pay'):
            return base[:-7] + '/tg/sale'
        if base.endswith('/tg/sale'):
            return base
        return base.rstrip('/') + '/tg/sale'
    hardcoded = default_miniapp_url()
    if hardcoded.startswith('https://'):
        if hardcoded.endswith('/tg/pay'):
            return hardcoded[:-7] + '/tg/sale'
        return hardcoded.rsplit('/', 1)[0] + '/tg/sale' if hardcoded else ''
    try:
        host = (request.host_url or '').rstrip('/')
        if host.startswith('https://'):
            return host + '/tg/sale'
    except RuntimeError:
        pass
    return ''


def _can_sale(user: User) -> bool:
    return (user.role or '') in ('admin', 'shop_manager')


def _can_firms(user: User) -> bool:
    return (user.role or '') == 'admin'


def require_sale(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        user, _dev, pending = resolve_user()
        if not user:
            if pending:
                return jsonify({
                    'error': 'not_linked',
                    'telegram_id': pending.get('id'),
                    'username': (pending.get('username') or ''),
                }), 403
            return jsonify({'error': 'unauthorized'}), 401
        if not _can_sale(user):
            return jsonify({'error': 'forbidden', 'hint': 'Только admin или активный менеджер продаж'}), 403
        return fn(user, *args, **kwargs)
    return wrapped


def _money(value) -> Decimal:
    return Decimal(str(value or 0).replace(',', '.').replace(' ', '').replace('\xa0', '') or 0)


def _serialize_company(c: SaleCompany) -> dict:
    return {
        'id': c.id,
        'short_name': c.short_name,
        'legal_name': c.legal_name or '',
        'inn': c.inn or '',
        'kpp': c.kpp or '',
        'ogrn': c.ogrn or '',
        'legal_address': c.legal_address or '',
        'fact_address': c.fact_address or '',
        'bank_name': c.bank_name or '',
        'bik': c.bik or '',
        'rs': c.rs or '',
        'ks': c.ks or '',
        'director': c.director or '',
        'vat_mode': c.vat_mode or 'none',
        'is_active': bool(c.is_active),
        'sort_order': c.sort_order or 0,
    }


def _line_sum(lines) -> Decimal:
    total = Decimal('0')
    for ln in lines:
        total += Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0))
    return total.quantize(Decimal('0.01'))


def _serialize_invoice(inv: SaleInvoice, *, detail: bool = False) -> dict:
    data = {
        'id': inv.id,
        'status': inv.status,
        'amount': float(inv.amount or 0),
        'created_at': inv.created_at.isoformat() if inv.created_at else None,
        'approved_at': inv.approved_at.isoformat() if inv.approved_at else None,
        'company_id': inv.company_id,
        'company': _serialize_company(inv.company) if inv.company else None,
        'company_name': (inv.company.short_name if inv.company else '') or '',
        'buyer_name': inv.buyer_name or '',
        'buyer_inn': inv.buyer_inn or '',
        'comment': inv.comment or '',
        'has_file': bool(inv.file_blob),
        'author': inv.user.username if inv.user else '',
        'lines_count': len(inv.lines or []),
    }
    if detail:
        free_map = _free_pairs()
        data.update({
            'buyer_kpp': inv.buyer_kpp or '',
            'buyer_address': inv.buyer_address or '',
            'buyer_bank': inv.buyer_bank or '',
            'buyer_rs': inv.buyer_rs or '',
            'buyer_bik': inv.buyer_bik or '',
            'buyer_ks': inv.buyer_ks or '',
            'client_id': inv.client_id,
            'lines': [
                {
                    'id': ln.id,
                    'plant_id': ln.plant_id,
                    'size_id': ln.size_id,
                    'plant_name': ln.plant_name,
                    'size_name': ln.size_name,
                    'qty': ln.qty,
                    'price': float(ln.price or 0),
                    'sum': float(Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0))),
                    'free_qty': (
                        free_map.get((int(ln.plant_id), int(ln.size_id)), 0)
                        if ln.plant_id and ln.size_id else 0
                    ),
                }
                for ln in inv.lines
            ],
        })
    return data


def _apply_buyer(inv: SaleInvoice, body: dict):
    mapping = {
        'buyer_name': 'buyer_name', 'name': 'buyer_name',
        'buyer_inn': 'buyer_inn', 'inn': 'buyer_inn',
        'buyer_kpp': 'buyer_kpp', 'kpp': 'buyer_kpp',
        'buyer_address': 'buyer_address', 'address': 'buyer_address',
        'buyer_bank': 'buyer_bank', 'bank': 'buyer_bank',
        'buyer_rs': 'buyer_rs', 'rs': 'buyer_rs',
        'buyer_bik': 'buyer_bik', 'bik': 'buyer_bik',
        'buyer_ks': 'buyer_ks', 'ks': 'buyer_ks',
    }
    for src, dest in mapping.items():
        if src in body and body.get(src) is not None:
            setattr(inv, dest, str(body.get(src) or '').strip()[:500])
    inn = (inv.buyer_inn or '').strip()
    if inn:
        found = Client.query.filter_by(inn=inn).first()
        if found:
            inv.client_id = found.id
            if not (inv.buyer_name or '').strip():
                inv.buyer_name = found.name


def _sync_client(inv: SaleInvoice):
    name = (inv.buyer_name or '').strip()
    inn = (inv.buyer_inn or '').strip()
    if not name and not inn:
        return
    client = None
    if inn:
        client = Client.query.filter_by(inn=inn).first()
    if not client and name:
        client = Client.query.filter(func.lower(Client.name) == name.lower()).first()
    if not client:
        client = Client(name=name or inn)
        db.session.add(client)
        db.session.flush()
    if name:
        client.name = name[:200]
    if inn:
        client.inn = inn[:20]
    if inv.buyer_kpp:
        client.kpp = inv.buyer_kpp[:20]
    if inv.buyer_address:
        client.address = inv.buyer_address[:500]
    if inv.buyer_bank:
        client.bank_name = inv.buyer_bank[:200]
    if inv.buyer_rs:
        client.rs = inv.buyer_rs[:40]
    if inv.buyer_bik:
        client.bik = inv.buyer_bik[:20]
    if inv.buyer_ks:
        client.ks = inv.buyer_ks[:40]
    inv.client_id = client.id


def _replace_lines(inv: SaleInvoice, rows: list):
    inv.lines.clear()
    db.session.flush()
    free_map = _free_pairs()
    for row in rows or []:
        try:
            pid = int(row.get('plant_id')) if row.get('plant_id') else None
            sid = int(row.get('size_id')) if row.get('size_id') else None
            qty = int(row.get('qty') or 0)
            price = _money(row.get('price'))
        except (TypeError, ValueError, InvalidOperation):
            continue
        if qty <= 0:
            continue
        if pid and sid:
            free = int(free_map.get((pid, sid), 0))
            if qty > free:
                qty = free
            if qty <= 0:
                continue
        plant = Plant.query.get(pid) if pid else None
        size = Size.query.get(sid) if sid else None
        db.session.add(SaleInvoiceLine(
            invoice=inv,
            plant_id=pid,
            size_id=sid,
            plant_name=(row.get('plant_name') or (plant.name if plant else ''))[:200],
            size_name=(row.get('size_name') or (size.name if size else ''))[:120],
            qty=qty,
            price=price,
        ))
    db.session.flush()
    inv.amount = _line_sum(inv.lines)


def _free_pairs() -> dict[tuple[int, int], int]:
    rmap = get_reserved_map()
    reserved = {}
    for (pid, sid, _f, _y), qty in rmap.items():
        reserved[(pid, sid)] = reserved.get((pid, sid), 0) + int(qty or 0)
    rows = (
        db.session.query(
            StockBalance.plant_id,
            StockBalance.size_id,
            func.coalesce(func.sum(StockBalance.quantity), 0),
        )
        .group_by(StockBalance.plant_id, StockBalance.size_id)
        .all()
    )
    out = {}
    for pid, sid, qty in rows:
        free = int(qty or 0) - reserved.get((pid, sid), 0)
        if free > 0:
            out[(int(pid), int(sid))] = free
    return out


def _logo_uri() -> str:
    path = Path(current_app.root_path) / 'static' / 'tg_sale' / 'mark.png'
    if path.is_file():
        return path.resolve().as_uri()
    return ''


def _qr_payload(company: SaleCompany, amount, purpose: str) -> str:
    parts = ['ST00012']

    def add(key, val):
        v = re.sub(r'[|\n\r]+', ' ', str(val or '')).strip()
        if v:
            parts.append(f'{key}={v}')

    add('Name', company.legal_name or company.short_name)
    add('PersonalAcc', company.rs)
    add('BankName', company.bank_name)
    add('BIC', company.bik)
    add('CorrespAcc', company.ks)
    add('PayeeINN', company.inn)
    add('KPP', company.kpp)
    kop = int(round(float(amount or 0) * 100))
    if kop > 0:
        add('Sum', str(kop))
    add('Purpose', purpose[:210])
    return '|'.join(parts)


def _qr_temp_png(payload: str) -> tuple[str, str]:
    """PNG на диск для xhtml2pdf. Возвращает (file_uri, path)."""
    try:
        import qrcode
        qr = qrcode.QRCode(
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=4,
            border=2,
        )
        qr.add_data(payload)
        qr.make(fit=True)
        img = qr.make_image(fill_color='black', back_color='white')
        fd, path = tempfile.mkstemp(suffix='.png')
        os.close(fd)
        img.save(path)
        return Path(path).resolve().as_uri(), path
    except Exception:
        current_app.logger.exception('sale qr failed')
        return '', ''


def render_sale_pdf(inv: SaleInvoice) -> bytes | None:
    company = inv.company
    vat_mode = (company.vat_mode if company else 'none') or 'none'
    amount = Decimal(str(inv.amount or 0))
    vat = Decimal('0')
    if vat_mode == 'included_20' and amount > 0:
        vat = (amount * Decimal('20') / Decimal('120')).quantize(Decimal('0.01'))
    purpose = f'Оплата по счёту №{inv.id} от {inv.created_at.strftime("%d.%m.%Y") if inv.created_at else ""}'
    qr_uri, qr_path = '', ''
    try:
        if company:
            qr_uri, qr_path = _qr_temp_png(_qr_payload(company, amount, purpose))
        html = render_template(
            'tg_sale/invoice_pdf.html',
            inv=inv,
            company=company,
            lines=inv.lines,
            amount=amount,
            vat=vat,
            vat_mode=vat_mode,
            purpose=purpose,
            logo_uri=_logo_uri(),
            qr_uri=qr_uri,
            doc_date=inv.created_at or msk_now(),
        )
        return build_pdf_bytes(html, page_margin='12mm')
    finally:
        if qr_path:
            try:
                os.remove(qr_path)
            except OSError:
                pass


def _store_pdf(inv: SaleInvoice) -> bytes | None:
    blob = render_sale_pdf(inv)
    if not blob:
        return None
    inv.file_blob = blob
    inv.file_name = f'schet_{inv.id}.pdf'
    return blob


# ---------------------------------------------------------------------------
# Pages / API
# ---------------------------------------------------------------------------

@bp.route('')
@bp.route('/')
def index():
    user, is_dev, _pending = resolve_user()
    html = render_template('tg_sale/index.html', has_user=bool(user and _can_sale(user)))
    resp = make_response(html)
    as_role = request.args.get('as')
    if is_dev and as_role in ('admin', 'shop_manager'):
        resp.set_cookie(_DEV_COOKIE, as_role, samesite='Lax')
    return resp


@bp.route('/api/me')
@require_sale
def api_me(user: User):
    return jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'can_firms': _can_firms(user),
        'can_edit_firms': _can_firms(user),
    })


@bp.route('/api/companies')
@require_sale
def api_companies(_user: User):
    rows = SaleCompany.query.filter_by(is_active=True).order_by(
        SaleCompany.sort_order, SaleCompany.id
    ).all()
    return jsonify({'companies': [_serialize_company(c) for c in rows]})


@bp.route('/api/companies/<int:cid>', methods=['POST'])
@require_sale
def api_company_save(user: User, cid: int):
    if not _can_firms(user):
        return jsonify({'error': 'forbidden'}), 403
    c = SaleCompany.query.get_or_404(cid)
    body = request.get_json(silent=True) or {}
    for field in (
        'short_name', 'legal_name', 'inn', 'kpp', 'ogrn', 'legal_address',
        'fact_address', 'bank_name', 'bik', 'rs', 'ks', 'director',
    ):
        if field in body:
            setattr(c, field, str(body.get(field) or '').strip()[:500])
    if body.get('vat_mode') in ('none', 'included_20'):
        c.vat_mode = body['vat_mode']
    db.session.commit()
    return jsonify(_serialize_company(c))


@bp.route('/api/stock')
@require_sale
def api_stock(_user: User):
    q = (request.args.get('q') or '').strip().lower()
    prices = _price_history_map()
    pairs = _free_pairs()
    items = []
    plants = {p.id: p.name for p in Plant.query.all()}
    sizes = {s.id: s.name for s in Size.query.all()}
    for (pid, sid), free in pairs.items():
        pname = plants.get(pid) or ''
        sname = sizes.get(sid) or ''
        hay = f'{pname} {sname}'.lower()
        if q and q not in hay:
            continue
        items.append({
            'plant_id': pid,
            'size_id': sid,
            'plant_name': pname,
            'size_name': sname,
            'free': free,
            'free_qty': free,
            'price': float(prices.get((pid, sid)) or 0),
        })
    items.sort(key=lambda x: (x['plant_name'].lower(), x['size_name'].lower()))
    return jsonify({'items': items[:80]})


@bp.route('/api/parse-buyer', methods=['POST'])
@require_sale
def api_parse_buyer(_user: User):
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({'error': 'no_file'}), 400
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in _ALLOWED_EXT:
        return jsonify({'error': 'bad_type'}), 400
    data = file.read()
    if not data:
        return jsonify({'error': 'empty'}), 400
    suffix = ext or '.bin'
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        os.write(fd, data)
        os.close(fd)
        parsed = parse_buyer_file(path, file.filename, data=data)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    fields = parsed.get('fields') or {}
    match = None
    inn = (fields.get('inn') or '').strip()
    if inn:
        found = Client.query.filter_by(inn=inn).first()
        if found:
            match = {
                'id': found.id,
                'name': found.name,
                'inn': found.inn or inn,
                'kpp': found.kpp or fields.get('kpp') or '',
                'address': found.address or fields.get('address') or '',
                'bank': found.bank_name or fields.get('bank') or '',
                'rs': found.rs or fields.get('rs') or '',
                'bik': found.bik or fields.get('bik') or '',
                'ks': found.ks or fields.get('ks') or '',
            }
            fields = {
                'name': found.name or fields.get('name') or '',
                'inn': found.inn or inn,
                'kpp': match['kpp'],
                'address': match['address'],
                'bank': match['bank'],
                'rs': match['rs'],
                'bik': match['bik'],
                'ks': match['ks'],
            }
    return jsonify({'fields': fields, 'buyer': fields, 'match': match, 'error': parsed.get('error')})


@bp.route('/api/invoices')
@require_sale
def api_invoices(_user: User):
    rows = (
        SaleInvoice.query
        .filter(SaleInvoice.status != 'discarded')
        .order_by(SaleInvoice.id.desc())
        .limit(80)
        .all()
    )
    return jsonify({'invoices': [_serialize_invoice(r) for r in rows]})


@bp.route('/api/invoices', methods=['POST'])
@require_sale
def api_create(user: User):
    body = request.get_json(silent=True) or {}
    cid = body.get('company_id')
    company = SaleCompany.query.get(int(cid)) if cid else SaleCompany.query.order_by(SaleCompany.sort_order).first()
    if not company:
        return jsonify({'error': 'no_company', 'hint': 'Сначала заполните фирмы'}), 400
    inv = SaleInvoice(
        company_id=company.id,
        user_id=user.id,
        status='draft',
        buyer_name='',
    )
    _apply_buyer(inv, body)
    db.session.add(inv)
    db.session.flush()
    _replace_lines(inv, body.get('lines') or [])
    db.session.commit()
    return jsonify(_serialize_invoice(inv, detail=True))


@bp.route('/api/invoices/<int:inv_id>')
@require_sale
def api_get(_user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    if inv.status == 'discarded':
        return jsonify({'error': 'not_found'}), 404
    return jsonify(_serialize_invoice(inv, detail=True))


@bp.route('/api/invoices/<int:inv_id>', methods=['POST'])
@require_sale
def api_save(_user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    if inv.status != 'draft':
        return jsonify({'error': 'locked'}), 400
    body = request.get_json(silent=True) or {}
    if body.get('company_id'):
        company = SaleCompany.query.get(int(body['company_id']))
        if company:
            inv.company_id = company.id
    if 'comment' in body:
        inv.comment = str(body.get('comment') or '')[:500]
    _apply_buyer(inv, body)
    if 'lines' in body:
        _replace_lines(inv, body.get('lines') or [])
    else:
        inv.amount = _line_sum(inv.lines)
    db.session.commit()
    return jsonify(_serialize_invoice(inv, detail=True))


@bp.route('/api/invoices/<int:inv_id>/discard', methods=['POST'])
@require_sale
def api_discard(_user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    if inv.status == 'approved':
        return jsonify({'error': 'locked'}), 400
    inv.status = 'discarded'
    db.session.commit()
    return jsonify({'ok': True})


@bp.route('/api/invoices/<int:inv_id>/pdf')
@require_sale
def api_pdf(_user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    blob = inv.file_blob if inv.status == 'approved' and inv.file_blob else _store_pdf(inv)
    if inv.status == 'draft':
        db.session.commit()
    if not blob:
        return jsonify({'error': 'pdf_failed'}), 500
    resp = make_response(bytes(blob))
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = f'inline; filename=schet_{inv.id}.pdf'
    return resp


@bp.route('/api/invoices/<int:inv_id>/send-pdf', methods=['POST'])
@require_sale
def api_send_pdf(user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    blob = inv.file_blob if inv.file_blob else _store_pdf(inv)
    if inv.status == 'draft':
        db.session.commit()
    if not blob:
        return jsonify({'ok': False, 'error': 'pdf_failed'}), 500
    if not user.telegram_id:
        return jsonify({'ok': False, 'error': 'no_telegram_id'})
    ok, err = send_chat_document(
        user.telegram_id,
        filename=inv.file_name or f'schet_{inv.id}.pdf',
        caption=f'Счёт №{inv.id} · {inv.buyer_name or "клиент"} · {inv.amount} ₽',
        file_bytes=bytes(blob),
    )
    return jsonify({'ok': bool(ok), 'error': err if not ok else None})


@bp.route('/api/invoices/<int:inv_id>/approve', methods=['POST'])
@require_sale
def api_approve(user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    if inv.status != 'draft':
        return jsonify({'error': 'locked'}), 400
    if not inv.lines:
        return jsonify({'error': 'no_lines'}), 400
    if not (inv.buyer_name or '').strip():
        return jsonify({'error': 'need_buyer'}), 400
    inv.amount = _line_sum(inv.lines)
    _sync_client(inv)
    blob = _store_pdf(inv)
    if not blob:
        return jsonify({'error': 'pdf_failed'}), 500
    inv.status = 'approved'
    inv.approved_at = msk_now()
    db.session.commit()
    company = html_escape(inv.company.short_name if inv.company else '')
    vat = 'НДС' if inv.company and inv.company.vat_mode == 'included_20' else 'без НДС'
    text = (
        f'Согласован счёт №{inv.id}\n'
        f'{html_escape(inv.buyer_name or "")}\n'
        f'{company} · {vat}\n'
        f'{inv.amount} ₽ · {len(inv.lines)} поз.\n'
        f'Автор: {html_escape(user.username)}'
    )
    try:
        tg_send_message(text, chat_type='orders')
    except Exception:
        current_app.logger.exception('sale invoice orders chat')
    return jsonify(_serialize_invoice(inv, detail=True))
