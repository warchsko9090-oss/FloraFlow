"""Telegram Mini App «Выставить счёт» клиенту.

Роли: admin и shop_manager. Заказ и резерв не создаём.
"""
from __future__ import annotations

import os
import re
import tempfile
from datetime import timedelta
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
from app.tg_pay import resolve_user, _auth_fail_hint, set_mini_cookie, log_mini_auth_fail
from app.tg_sale_parse import parse_buyer_file
from app.utils import msk_now, build_pdf_bytes, size_natural_key
from app.telegram import send_chat_document, send_message as tg_send_message, default_miniapp_url
from app.stock_helpers import get_reserved_map
from app.shop_catalog import _price_history_map
from app.seedlings import is_seedling_size_name, is_excluded_from_product_stock
from app.inn_lookup import lookup_requisites

bp = Blueprint('tg_sale', __name__, url_prefix='/tg/sale')

_ALLOWED_EXT = {'.pdf', '.doc', '.docx', '.jpg', '.jpeg', '.png', '.webp', '.bmp'}
_DEV_COOKIE = 'tg_sale_as'
_VAT_INCLUDED = ('included_20', 'included_22')
_VAT_RATE = Decimal('22')
_VAT_BASE = Decimal('122')
_MONTHS_GEN = (
    'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
    'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря',
)
_ONES = (
    ('', 'один', 'два', 'три', 'четыре', 'пять', 'шесть', 'семь', 'восемь', 'девять'),
    ('', 'одна', 'две', 'три', 'четыре', 'пять', 'шесть', 'семь', 'восемь', 'девять'),
)
_TEENS = (
    'десять', 'одиннадцать', 'двенадцать', 'тринадцать', 'четырнадцать',
    'пятнадцать', 'шестнадцать', 'семнадцать', 'восемнадцать', 'девятнадцать',
)
_TENS = (
    '', '', 'двадцать', 'тридцать', 'сорок', 'пятьдесят',
    'шестьдесят', 'семьдесят', 'восемьдесят', 'девяносто',
)
_HUNDREDS = (
    '', 'сто', 'двести', 'триста', 'четыреста', 'пятьсот',
    'шестьсот', 'семьсот', 'восемьсот', 'девятьсот',
)


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
            return jsonify({'error': 'unauthorized', 'hint': _auth_fail_hint()}), 401
        if not _can_sale(user):
            return jsonify({'error': 'forbidden', 'hint': 'Только admin или активный менеджер продаж'}), 403
        return fn(user, *args, **kwargs)
    return wrapped


def _money(value) -> Decimal:
    return Decimal(str(value or 0).replace(',', '.').replace(' ', '').replace('\xa0', '') or 0)


def _vat_included(mode: str | None) -> bool:
    return (mode or '') in _VAT_INCLUDED


def _vat_mode_norm(mode: str | None) -> str:
    if _vat_included(mode):
        return 'included_22'
    return (mode or 'none') or 'none'


def _vat_amount(amount: Decimal, mode: str | None) -> Decimal:
    if _vat_included(mode) and amount > 0:
        return (amount * _VAT_RATE / _VAT_BASE).quantize(Decimal('0.01'))
    return Decimal('0')


def _inn_digits(value: str | None) -> str:
    return re.sub(r'\D+', '', str(value or ''))[:12]


def _digits(value: str | None, n: int | None = None) -> str:
    s = re.sub(r'\D+', '', str(value or ''))
    return s[:n] if n else s


def _find_client_by_inn(inn: str | None) -> Client | None:
    digits = _inn_digits(inn)
    if len(digits) not in (10, 12):
        return None
    for client in Client.query.filter(Client.inn.isnot(None)).all():
        if _inn_digits(client.inn) == digits:
            return client
    return None


def _company_ready(c: SaleCompany) -> bool:
    name = (c.legal_name or c.short_name or '').strip()
    inn = _inn_digits(c.inn)
    rs = _digits(c.rs, 20)
    bik = _digits(c.bik, 9)
    bank = (c.bank_name or '').strip()
    return bool(name and bank and len(inn) in (10, 12) and len(rs) == 20 and len(bik) == 9)


def _is_container_size(sname: str) -> bool:
    if is_seedling_size_name(sname):
        return True
    n = (sname or '').strip()
    if re.search(r'(?i)контейнер', n):
        return True
    if re.search(r'(?i)(^|[^A-Za-zА-Яа-я])[CС]\s*\d', n) and not re.search(r'\d{2,3}\s*-\s*\d{2,3}', n):
        return True
    return False


def _fmt_money(value) -> str:
    q = Decimal(str(value or 0)).quantize(Decimal('0.01'))
    sign = '-' if q < 0 else ''
    q = abs(q)
    whole, frac = f'{q:.2f}'.split('.')
    grouped = f'{int(whole):,}'.replace(',', ' ')
    return f'{sign}{grouped},{frac}'


def _date_long(dt) -> str:
    if not dt:
        return ''
    return f'{dt.day} {_MONTHS_GEN[dt.month - 1]} {dt.year} г.'


def _plural_ru(n: int, forms: tuple[str, str, str]) -> str:
    n = abs(int(n)) % 100
    if 10 < n < 20:
        return forms[2]
    n = n % 10
    if n == 1:
        return forms[0]
    if 2 <= n <= 4:
        return forms[1]
    return forms[2]


def _triad_words(n: int, feminine: bool = False) -> str:
    n = int(n)
    if n <= 0:
        return ''
    h, rest = divmod(n, 100)
    parts = []
    if h:
        parts.append(_HUNDREDS[h])
    if 10 <= rest <= 19:
        parts.append(_TEENS[rest - 10])
    else:
        tens, ones = divmod(rest, 10)
        if tens:
            parts.append(_TENS[tens])
        if ones:
            parts.append(_ONES[1 if feminine else 0][ones])
    return ' '.join(parts)


def rubles_in_words(amount) -> str:
    q = Decimal(str(amount or 0)).quantize(Decimal('0.01'))
    rub = int(q)
    kop = int((q - Decimal(rub)) * 100)
    if kop < 0:
        kop = 0
    if rub == 0:
        words = 'ноль'
    else:
        millions = rub // 1_000_000
        thousands = (rub % 1_000_000) // 1000
        rest = rub % 1000
        chunks = []
        if millions:
            chunks.append(
                f'{_triad_words(millions, False)} '
                f'{_plural_ru(millions, ("миллион", "миллиона", "миллионов"))}'.strip()
            )
        if thousands:
            chunks.append(
                f'{_triad_words(thousands, True)} '
                f'{_plural_ru(thousands, ("тысяча", "тысячи", "тысяч"))}'.strip()
            )
        if rest:
            chunks.append(_triad_words(rest, False))
        words = ' '.join(x for x in chunks if x)
    if words:
        words = words[0].upper() + words[1:]
    rub_w = _plural_ru(rub, ('рубль', 'рубля', 'рублей'))
    kop_w = _plural_ru(kop, ('копейка', 'копейки', 'копеек'))
    return f'{words} {rub_w} {kop:02d} {kop_w}'


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
        'phone': c.phone or '',
        'director': c.director or '',
        'vat_mode': _vat_mode_norm(c.vat_mode),
        'is_active': bool(c.is_active),
        'sort_order': c.sort_order or 0,
        'filled': _company_ready(c),
    }


def _line_sum(lines) -> Decimal:
    total = Decimal('0')
    for ln in lines:
        total += Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0))
    return total.quantize(Decimal('0.01'))


def _fmt_money_ru(value) -> str:
    q = Decimal(str(value or 0)).quantize(Decimal('0.01'))
    sign = '-' if q < 0 else ''
    q = abs(q)
    whole = int(q)
    frac = int((q - Decimal(whole)) * 100)
    grouped = f'{whole:,}'.replace(',', ' ')
    if frac:
        return f'{sign}{grouped},{frac:02d} ₽'
    return f'{sign}{grouped} ₽'


def _approved_orders_text(inv: SaleInvoice) -> str:
    lines = list(inv.lines or [])
    npos = len(lines)
    pos_word = 'позиция' if npos == 1 else 'поз.'
    buyer = html_escape((inv.buyer_name or 'Без клиента').strip())
    shown = lines[:25]
    items = []
    for ln in shown:
        plant = html_escape(ln.plant_name or 'Растение')
        size = html_escape(ln.size_name or '')
        qty = int(ln.qty or 0)
        price = _fmt_money_ru(ln.price)
        total = _fmt_money_ru(Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0)))
        head = f'{plant} · {size}' if size else plant
        items.append(
            f'• {head}\n'
            f'<b>{qty} шт</b> по цене <b>{price}</b>\n'
            f'{total}'
        )
    extra = npos - len(shown)
    if extra > 0:
        items.append(f'• … и ещё {extra} {pos_word}')
    body = '\n'.join(items) if items else '• нет позиций'
    text = '\n'.join((
        f'✅ <b>Согласован на выкопку</b> счёт №{inv.id}',
        '',
        f'👤 {buyer}',
        f'💰 ИТОГО: {_fmt_money_ru(inv.amount)} · {npos} {pos_word}',
        '',
        f'📦 Позиции:',
        body,
    ))
    return text[:3500]


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
            'buyer_ogrn': inv.buyer_ogrn or '',
            'buyer_phone': inv.buyer_phone or '',
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
        'buyer_ogrn': 'buyer_ogrn', 'ogrn': 'buyer_ogrn',
        'buyer_phone': 'buyer_phone', 'phone': 'buyer_phone',
    }
    for src, dest in mapping.items():
        if src in body and body.get(src) is not None:
            setattr(inv, dest, str(body.get(src) or '').strip()[:500])
    inn = _inn_digits(inv.buyer_inn)
    if inn:
        inv.buyer_inn = inn
        found = _find_client_by_inn(inn)
        if found:
            inv.client_id = found.id
            if not (inv.buyer_name or '').strip():
                inv.buyer_name = found.name


def _sync_client(inv: SaleInvoice):
    """При согласовании: найти клиента по ИНН или создать карточку и заполнить реквизиты."""
    name = (inv.buyer_name or '').strip()
    inn = _inn_digits(inv.buyer_inn)
    if inn:
        inv.buyer_inn = inn
    if not name and not inn:
        return
    client = _find_client_by_inn(inn) if inn else None
    if not client:
        client = Client(name=(name or inn)[:200])
        db.session.add(client)
        db.session.flush()
    if name:
        client.name = name[:200]
    if inn:
        client.inn = inn[:20]
    if inv.buyer_kpp:
        client.kpp = _digits(inv.buyer_kpp, 9)[:20]
    if getattr(inv, 'buyer_ogrn', None):
        client.ogrn = _digits(inv.buyer_ogrn, 15)[:20]
    if inv.buyer_address:
        client.address = inv.buyer_address[:500]
    if getattr(inv, 'buyer_phone', None):
        client.phone = str(inv.buyer_phone)[:40]
    if inv.buyer_bank:
        client.bank_name = inv.buyer_bank[:200]
    if inv.buyer_rs:
        client.rs = _digits(inv.buyer_rs, 20)[:40]
    if inv.buyer_bik:
        client.bik = _digits(inv.buyer_bik, 9)[:20]
    if inv.buyer_ks:
        client.ks = _digits(inv.buyer_ks, 20)[:40]
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
    """ГОСТ Р 56042 ST00012 (UTF-8). Пустая строка, если нет обязательных полей."""
    name = re.sub(r'[|\n\r]+', ' ', (company.legal_name or company.short_name or '')).strip()[:160]
    bank = re.sub(r'[|\n\r]+', ' ', (company.bank_name or '')).strip()[:160]
    rs = _digits(company.rs, 20)
    bik = _digits(company.bik, 9)
    inn = _inn_digits(company.inn)
    if not (name and bank and len(rs) == 20 and len(bik) == 9 and len(inn) in (10, 12)):
        return ''
    parts = ['ST00012']

    def add(key, val):
        v = re.sub(r'[|\n\r]+', ' ', str(val or '')).strip()
        if v:
            parts.append(f'{key}={v}')

    add('Name', name)
    add('PersonalAcc', rs)
    add('BankName', bank)
    add('BIC', bik)
    add('CorrespAcc', _digits(company.ks, 20))
    add('PayeeINN', inn)
    kpp = _digits(company.kpp, 9)
    if len(inn) == 10 and len(kpp) == 9:
        add('KPP', kpp)
    kop = int(round(float(amount or 0) * 100))
    if kop > 0:
        add('Sum', str(kop))
    clean_purpose = purpose.replace('№', 'N').replace('ё', 'е').replace('Ё', 'Е')
    add('Purpose', clean_purpose[:210])
    return '|'.join(parts)


def _qr_temp_png(payload: str) -> tuple[str, str]:
    """PNG на диск для xhtml2pdf. Возвращает (file_uri, path)."""
    if not payload:
        return '', ''
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


def _supplier_line(company: SaleCompany | None) -> str:
    if not company:
        return ''
    parts = [(company.legal_name or company.short_name or '').strip()]
    if company.inn:
        parts.append(f'ИНН {company.inn}')
    if company.kpp:
        parts.append(f'КПП {company.kpp}')
    addr = (company.legal_address or company.fact_address or '').strip()
    if addr:
        parts.append(addr)
    if company.phone:
        parts.append(f'тел.: {company.phone}')
    return ', '.join(p for p in parts if p)


def _buyer_line(inv: SaleInvoice) -> str:
    parts = [(inv.buyer_name or '').strip()]
    inn = _inn_digits(inv.buyer_inn)
    if inn:
        chunk = f'ИНН {inn}'
        kpp = (inv.buyer_kpp or '').strip()
        if kpp:
            chunk += f', КПП {kpp}'
        parts.append(chunk)
    ogrn = _digits(inv.buyer_ogrn)
    if ogrn:
        parts.append(('ОГРНИП ' if len(ogrn) == 15 else 'ОГРН ') + ogrn)
    addr = (inv.buyer_address or '').strip()
    if addr:
        parts.append(addr)
    phone = (inv.buyer_phone or '').strip()
    if phone:
        parts.append(f'тел.: {phone}')
    bank_bits = []
    bank = (inv.buyer_bank or '').strip()
    if bank:
        bank_bits.append(bank)
    rs = _digits(inv.buyer_rs, 20)
    if len(rs) == 20:
        bank_bits.append(f'р/с {rs}')
    bik = _digits(inv.buyer_bik, 9)
    if len(bik) == 9:
        bank_bits.append(f'БИК {bik}')
    ks = _digits(inv.buyer_ks, 20)
    if len(ks) == 20:
        bank_bits.append(f'к/с {ks}')
    if bank_bits:
        parts.append(', '.join(bank_bits))
    return ', '.join(p for p in parts if p)


def _sign_line(company: SaleCompany | None) -> str:
    if not company:
        return ''
    director = (company.director or '').strip()
    if director:
        return director
    name = (company.legal_name or company.short_name or '').strip()
    if len(_inn_digits(company.inn)) == 12:
        return f'Предприниматель {name}'
    return name


def render_sale_pdf(inv: SaleInvoice) -> bytes | None:
    company = inv.company
    if not company:
        return None
    vat_mode = _vat_mode_norm(company.vat_mode)
    amount = Decimal(str(inv.amount or 0))
    vat = _vat_amount(amount, vat_mode)
    doc_date = inv.created_at or msk_now()
    purpose = f'Оплата по счету N {inv.id} от {doc_date.strftime("%d.%m.%Y")}'
    pay_until = (doc_date + timedelta(days=3)).strftime('%d.%m.%Y')
    qr_uri, qr_path = '', ''
    pdf_lines = []
    for i, ln in enumerate(inv.lines or [], 1):
        sm = Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0))
        title = f'{ln.plant_name} {ln.size_name}'.strip()
        pdf_lines.append({
            'n': i,
            'name': title,
            'qty': ln.qty,
            'price': _fmt_money(ln.price),
            'sum': _fmt_money(sm),
        })
    try:
        payload = _qr_payload(company, amount, purpose) if company else ''
        if payload:
            qr_uri, qr_path = _qr_temp_png(payload)
        elif company:
            current_app.logger.warning(
                'sale qr skipped: incomplete company requisites id=%s', company.id
            )
        html = render_template(
            'tg_sale/invoice_pdf.html',
            inv=inv,
            company=company,
            lines=pdf_lines,
            amount=amount,
            amount_fmt=_fmt_money(amount),
            amount_words=rubles_in_words(amount),
            vat=vat,
            vat_fmt=_fmt_money(vat),
            vat_included=_vat_included(vat_mode),
            vat_mode=vat_mode,
            purpose=purpose,
            date_long=_date_long(doc_date),
            pay_until=pay_until,
            supplier_line=_supplier_line(company),
            buyer_line=_buyer_line(inv),
            basis=(inv.comment or '').strip() or 'Без договора',
            sign_line=_sign_line(company),
            logo_uri=_logo_uri(),
            qr_uri=qr_uri,
            doc_date=doc_date,
        )
        return build_pdf_bytes(html, page_margin='10mm')
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


@bp.route('/api/auth', methods=['POST'])
def api_auth():
    user, is_dev, pending = resolve_user()
    if not user:
        if pending:
            return jsonify({
                'error': 'not_linked',
                'telegram_id': pending.get('id'),
                'username': (pending.get('username') or ''),
            }), 403
        log_mini_auth_fail()
        return jsonify({'error': 'unauthorized', 'hint': _auth_fail_hint()}), 401
    if not _can_sale(user):
        return jsonify({'error': 'forbidden', 'hint': 'Только admin или активный менеджер продаж'}), 403
    resp = jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'can_firms': _can_firms(user),
        'can_edit_firms': _can_firms(user),
        'dev': is_dev,
    })
    return set_mini_cookie(resp, user)


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
def api_companies(user: User):
    rows = SaleCompany.query.order_by(
        SaleCompany.sort_order, SaleCompany.id
    ).all()
    filled = [c for c in rows if c.is_active and _company_ready(c)]
    payload = {'companies': [_serialize_company(c) for c in filled]}
    if _can_firms(user):
        payload['all'] = [_serialize_company(c) for c in rows]
    return jsonify(payload)


@bp.route('/api/companies/<int:cid>', methods=['POST'])
@require_sale
def api_company_save(user: User, cid: int):
    if not _can_firms(user):
        return jsonify({'error': 'forbidden'}), 403
    c = SaleCompany.query.get_or_404(cid)
    body = request.get_json(silent=True) or {}
    for field in (
        'short_name', 'legal_name', 'inn', 'kpp', 'ogrn', 'legal_address',
        'fact_address', 'bank_name', 'bik', 'rs', 'ks', 'director', 'phone',
    ):
        if field in body:
            setattr(c, field, str(body.get(field) or '').strip()[:500])
    if body.get('vat_mode') in ('none', 'included_20', 'included_22'):
        c.vat_mode = 'included_22' if _vat_included(body['vat_mode']) else 'none'
    db.session.commit()
    return jsonify(_serialize_company(c))


def _plant_photo_url(plant_id: int, plant_name: str, *, prefer_container: bool = False) -> str:
    try:
        from app.photo_storage import PHOTO_VARIANT_CONTAINER, PHOTO_VARIANT_GROUND, resolve_photo_source
        from app.shop_images import SHOP_IMG_THUMB, shop_image_url
        root = current_app.config['UPLOAD_FOLDER']
        variants = (
            (PHOTO_VARIANT_CONTAINER, PHOTO_VARIANT_GROUND)
            if prefer_container else
            (PHOTO_VARIANT_GROUND, PHOTO_VARIANT_CONTAINER)
        )
        for variant in variants:
            rel_dir, files = resolve_photo_source(root, plant_id, plant_name, variant=variant)
            if files:
                return shop_image_url(f'{rel_dir}/{files[0]}', SHOP_IMG_THUMB) or ''
    except Exception:
        current_app.logger.exception('sale plant photo')
    return ''


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
        if is_excluded_from_product_stock(sname):
            continue
        hay = f'{pname} {sname}'.lower()
        tokens = [t for t in q.split() if t] if q else []
        if tokens and not all(t in hay for t in tokens):
            continue
        is_seedling = _is_container_size(sname)
        items.append({
            'plant_id': pid,
            'size_id': sid,
            'plant_name': pname,
            'size_name': sname,
            'free': free,
            'free_qty': free,
            'price': float(prices.get((pid, sid)) or 0),
            'is_seedling': is_seedling,
        })
    grouped: dict[int, list] = {}
    for it in items:
        grouped.setdefault(it['plant_id'], []).append(it)
    groups = []
    for pid, rows in grouped.items():
        rows.sort(key=lambda x: (1 if x['is_seedling'] else 0, size_natural_key(x['size_name'])))
        priced = [x['price'] for x in rows if x['price'] > 0]
        seedling_only = all(x['is_seedling'] for x in rows)
        groups.append({
            'plant_id': pid,
            'plant_name': rows[0]['plant_name'],
            'photo_url': _plant_photo_url(pid, rows[0]['plant_name'], prefer_container=seedling_only),
            'size_count': len(rows),
            'min_price': min(priced) if priced else 0,
            'seedling_only': seedling_only,
            'sizes': rows,
        })
    groups.sort(key=lambda g: (1 if g['seedling_only'] else 0, g['plant_name'].lower()))
    return jsonify({'groups': groups[:20], 'items': items[:120]})


@bp.route('/api/lookup-inn')
@require_sale
def api_lookup_inn(_user: User):
    data = lookup_requisites(request.args.get('inn') or '')
    return jsonify(data)


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
    inn = _inn_digits(fields.get('inn'))
    if inn:
        looked = lookup_requisites(inn)
        extra = looked.get('fields') or {}
        for key, val in extra.items():
            if val and not str(fields.get(key) or '').strip():
                fields[key] = val
        fields['inn'] = inn
        if looked.get('hint'):
            fields['_hint'] = looked['hint']
        if looked.get('client_id'):
            match = dict(extra)
            match['id'] = looked['client_id']
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
    blob = _store_pdf(inv)
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
    blob = _store_pdf(inv)
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
    text = _approved_orders_text(inv)
    try:
        tg_send_message(text, chat_type='orders')
    except Exception:
        current_app.logger.exception('sale invoice orders chat')
    return jsonify(_serialize_invoice(inv, detail=True))
