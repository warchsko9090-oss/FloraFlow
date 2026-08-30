"""Telegram Mini App «Счета к оплате».

Локальный тест (без туннеля и BotFather):
  python run.py
  открыть http://127.0.0.1:5000/tg/pay
  в шапке переключить «Счета на оплату» / «Оплата».

В Telegram debug=True сам включает dev-вход. Для явного флага: TG_MINIAPP_DEV=1.
Стыковка с чатом расходов — следующим заходом.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from decimal import Decimal, InvalidOperation
from functools import wraps
from urllib.parse import parse_qsl, unquote

from flask import (
    Blueprint, current_app, jsonify, request, render_template,
    send_from_directory, make_response,
)
from werkzeug.utils import secure_filename

from app.models import db, User, PaymentInvoice, BudgetItem
from app.tg_pay_parse import parse_invoice_file
from app.utils import msk_now
from app.telegram import (
    _get_bot_token, send_chat_message, send_chat_document, download_bot_file,
)

bp = Blueprint('tg_pay', __name__, url_prefix='/tg/pay')

_ALLOWED_EXT = {'.pdf', '.jpg', '.jpeg', '.png', '.webp'}
_DEV_COOKIE = 'tg_pay_as'


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _dev_mode() -> bool:
    # На Amvera фейковый вход запрещён: иначе Mini App открывается
    # «как попало» без привязки Telegram и счета не грузятся.
    if os.environ.get('AMVERA') or os.path.isdir('/data'):
        return False
    flag = (os.environ.get('TG_MINIAPP_DEV') or '').strip().lower()
    if flag in ('1', 'true', 'yes'):
        return True
    return bool(current_app.debug)


def _public_miniapp_url() -> str:
    env = (os.environ.get('TG_MINIAPP_URL') or '').strip()
    if env:
        return env.rstrip('/')
    try:
        host = (request.host_url or '').rstrip('/')
        if host.startswith('https://'):
            return host + '/tg/pay'
    except RuntimeError:
        pass
    if os.path.isdir('/data') or os.environ.get('AMVERA'):
        return 'https://floraflowerp-warchesko.amvera.io/tg/pay'
    return ''


def _validate_init_data(init_data: str, bot_token: str) -> dict | None:
    if not init_data or not bot_token:
        return None
    parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    got_hash = parsed.pop('hash', '')
    if not got_hash:
        return None
    check = '\n'.join(f'{k}={v}' for k, v in sorted(parsed.items()))
    secret = hmac.new(b'WebAppData', bot_token.encode('utf-8'), hashlib.sha256).digest()
    calc = hmac.new(secret, check.encode('utf-8'), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, got_hash):
        return None
    try:
        user = json.loads(unquote(parsed.get('user') or '{}'))
    except Exception:
        user = {}
    return user if isinstance(user, dict) else None


def _tg_user_id_map() -> dict[str, str]:
    mapping = {}
    raw = os.environ.get('TG_USER_ID_MAP', '') or ''
    parts = [p.strip() for p in re.split(r'[,;\s]+', raw) if p.strip()]
    for part in parts:
        if ':' not in part:
            continue
        tg_id, canonical = part.split(':', 1)
        tg_id = tg_id.strip()
        canonical = canonical.strip().lstrip('@')
        # Один Telegram id — один логин ERP. Повтор не перезаписываем.
        if tg_id and canonical and tg_id not in mapping:
            mapping[tg_id] = canonical
    return mapping


def _user_from_telegram(tg_user: dict) -> User | None:
    tg_id = tg_user.get('id')
    if not tg_id:
        return None
    found = User.query.filter_by(telegram_id=int(tg_id)).first()
    if found:
        return found
    username = (tg_user.get('username') or '').lstrip('@').lower()
    if username:
        found = User.query.filter(db.func.lower(User.username) == username).first()
        if found:
            if not found.telegram_id:
                found.telegram_id = int(tg_id)
                db.session.commit()
            return found
    canonical = _tg_user_id_map().get(str(tg_id), '')
    if canonical:
        found = User.query.filter(db.func.lower(User.username) == canonical.lower()).first()
        if found:
            if not found.telegram_id:
                found.telegram_id = int(tg_id)
                db.session.commit()
            return found
    return None


def _dev_user(as_role: str) -> User | None:
    if as_role == 'payer':
        return (
            User.query.filter_by(role='executive').first()
            or User.query.filter(User.role.in_(['user', 'executive'])).first()
            or User.query.first()
        )
    return User.query.filter_by(role='admin').first() or User.query.first()


def resolve_user() -> tuple[User | None, bool, dict | None]:
    """(user, is_dev, pending_tg_user)."""
    init_data = (
        request.headers.get('X-Telegram-Init-Data')
        or request.args.get('initData')
        or ''
    )
    token = _get_bot_token()
    if init_data and token:
        tg_user = _validate_init_data(init_data, token)
        if tg_user:
            user = _user_from_telegram(tg_user)
            if user:
                return user, False, None
            if not _dev_mode():
                return None, False, tg_user
    if not _dev_mode():
        return None, False, None
    as_role = (
        request.headers.get('X-Tg-Pay-As')
        or request.cookies.get(_DEV_COOKIE)
        or request.args.get('as')
        or 'admin'
    )
    if as_role not in ('admin', 'payer'):
        as_role = 'admin'
    return _dev_user(as_role), True, None


def _can_edit(user: User) -> bool:
    return (user.role or '') == 'admin'


def require_user(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        user, _dev, pending = resolve_user()
        if not user:
            if pending:
                return jsonify({
                    'error': 'not_linked',
                    'telegram_id': pending.get('id'),
                    'username': (pending.get('username') or ''),
                    'hint': 'Этот Telegram не привязан к пользователю ERP. Добавьте id в TG_USER_ID_MAP.',
                }), 403
            return jsonify({'error': 'unauthorized', 'hint': 'Откройте из Telegram или /tg/pay при локальном debug'}), 401
        return fn(user, *args, **kwargs)
    return wrapped


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def _lines_of(inv: PaymentInvoice) -> list:
    raw = inv.line_items or ''
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _purpose(inv: PaymentInvoice) -> str:
    return (inv.summary or inv.comment or inv.original_name or 'Счёт').strip()


def _invoice_path(inv: PaymentInvoice) -> str | None:
    upload = current_app.config.get('UPLOAD_FOLDER') or ''
    path = os.path.join(upload, 'invoices', inv.filename)
    return path if os.path.isfile(path) else None


def serialize_invoice(inv: PaymentInvoice, *, detail: bool = False) -> dict:
    item = inv.item
    data = {
        'id': inv.id,
        'summary': _purpose(inv),
        'amount': float(inv.amount or 0),
        'status': inv.status or 'new',
        'priority': inv.priority or 'normal',
        'due_date': inv.due_date.isoformat() if inv.due_date else None,
        'budget': (
            {'id': item.id, 'name': item.name, 'code': item.code}
            if item else None
        ),
        'original_name': inv.original_name,
        'source': inv.source or 'web',
        'has_file': bool(_invoice_path(inv)),
    }
    if detail:
        data['comment'] = inv.comment or ''
        data['lines'] = _lines_of(inv)
        data['budget_item_id'] = inv.budget_item_id
    return data


def _save_parsed_invoice(filename: str, original_name: str, parsed: dict, source: str) -> PaymentInvoice:
    amount = parsed.get('amount') or Decimal('0')
    if not isinstance(amount, Decimal):
        try:
            amount = Decimal(str(amount))
        except (InvalidOperation, TypeError, ValueError):
            amount = Decimal('0')
    summary = (parsed.get('summary') or original_name)[:500]
    budget_id = None
    try:
        from app.expense_chat import classify_budget_item
        budget_id, _src = classify_budget_item(summary)
    except Exception:
        budget_id = None
    inv = PaymentInvoice(
        filename=filename,
        original_name=original_name[:255],
        summary=summary,
        line_items=json.dumps(parsed.get('lines') or [], ensure_ascii=False),
        source=source,
        budget_item_id=budget_id,
        amount=amount,
        status='draft',
        priority='normal',
        comment=summary[:500],
    )
    db.session.add(inv)
    db.session.commit()
    return inv


def _store_upload(data: bytes, original_name: str) -> str:
    safe = secure_filename(original_name) or 'invoice.pdf'
    save_name = f"inv_{int(msk_now().timestamp())}_{safe}"
    inv_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'invoices')
    os.makedirs(inv_dir, exist_ok=True)
    path = os.path.join(inv_dir, save_name)
    with open(path, 'wb') as fh:
        fh.write(data)
    return save_name, path


# ---------------------------------------------------------------------------
# Pages / API
# ---------------------------------------------------------------------------

@bp.route('')
@bp.route('/')
def index():
    user, is_dev, _pending = resolve_user()
    html = render_template(
        'tg_pay/index.html',
        dev_mode=is_dev,
        has_user=bool(user),
    )
    resp = make_response(html)
    as_role = request.args.get('as')
    if is_dev and as_role in ('admin', 'payer'):
        resp.set_cookie(_DEV_COOKIE, as_role, samesite='Lax')
    return resp


@bp.route('/api/me')
@require_user
def api_me(user: User):
    return jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'can_edit': _can_edit(user),
        'dev': _dev_mode() and not request.headers.get('X-Telegram-Init-Data'),
        'telegram_id': user.telegram_id,
    })


@bp.route('/api/budget-items')
@require_user
def api_budget_items(_user: User):
    items = BudgetItem.query.order_by(BudgetItem.code, BudgetItem.name).all()
    return jsonify([
        {'id': i.id, 'name': i.name, 'code': i.code}
        for i in items
    ])


@bp.route('/api/invoices')
@require_user
def api_invoices(user: User):
    q = PaymentInvoice.query.filter(PaymentInvoice.status != 'paid')
    if not _can_edit(user):
        q = q.filter(PaymentInvoice.status == 'new')
    rows = q.order_by(
        PaymentInvoice.status.asc(),
        PaymentInvoice.priority.desc(),
        PaymentInvoice.id.desc(),
    ).all()
    invoices = [serialize_invoice(inv) for inv in rows]
    total = sum(x['amount'] for x in invoices if x['status'] == 'new')
    drafts = sum(1 for x in invoices if x['status'] == 'draft')
    return jsonify({
        'invoices': invoices,
        'total_new': total,
        'count_new': sum(1 for x in invoices if x['status'] == 'new'),
        'count_draft': drafts,
    })


@bp.route('/api/invoices/<int:inv_id>')
@require_user
def api_invoice(user: User, inv_id: int):
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if not _can_edit(user) and inv.status != 'new':
        return jsonify({'error': 'not_found'}), 404
    return jsonify(serialize_invoice(inv, detail=True))


@bp.route('/api/invoices/<int:inv_id>/file')
@require_user
def api_invoice_file(user: User, inv_id: int):
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if not _can_edit(user) and inv.status != 'new':
        return jsonify({'error': 'not_found'}), 404
    inv_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'invoices')
    try:
        return send_from_directory(
            inv_dir,
            inv.filename,
            as_attachment=False,
            download_name=inv.original_name,
        )
    except Exception:
        return jsonify({'error': 'file_missing'}), 404


@bp.route('/api/invoices/<int:inv_id>/send-pdf', methods=['POST'])
@require_user
def api_send_pdf(user: User, inv_id: int):
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if not _can_edit(user) and inv.status != 'new':
        return jsonify({'error': 'not_found'}), 404
    path = _invoice_path(inv)
    if not path:
        return jsonify({'error': 'file_missing'}), 404
    chat_id = user.telegram_id
    if not chat_id:
        return jsonify({
            'ok': False,
            'error': 'no_telegram_id',
            'file_url': f'/tg/pay/api/invoices/{inv.id}/file',
        })
    ok, err = send_chat_document(
        chat_id,
        path,
        filename=inv.original_name,
        caption=f"{_purpose(inv)} · {inv.amount} ₽",
    )
    if not ok:
        return jsonify({'ok': False, 'error': err, 'file_url': f'/tg/pay/api/invoices/{inv.id}/file'})
    return jsonify({'ok': True})


@bp.route('/api/invoices/upload', methods=['POST'])
@require_user
def api_upload(user: User):
    if not _can_edit(user):
        return jsonify({'error': 'forbidden'}), 403
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({'error': 'no_file'}), 400
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in _ALLOWED_EXT:
        return jsonify({'error': 'bad_type', 'hint': 'Нужен PDF (или фото счёта)'}), 400
    data = file.read()
    if not data:
        return jsonify({'error': 'empty'}), 400
    save_name, path = _store_upload(data, file.filename)
    parsed = parse_invoice_file(path, file.filename)
    inv = _save_parsed_invoice(save_name, file.filename, parsed, source='miniapp')
    payload = serialize_invoice(inv, detail=True)
    payload['parse_error'] = parsed.get('error')
    return jsonify(payload)


@bp.route('/api/invoices/<int:inv_id>', methods=['POST'])
@require_user
def api_save(user: User, inv_id: int):
    if not _can_edit(user):
        return jsonify({'error': 'forbidden'}), 403
    inv = PaymentInvoice.query.get_or_404(inv_id)
    body = request.get_json(silent=True) or {}
    if 'summary' in body:
        inv.summary = (body.get('summary') or '').strip()[:500]
        if inv.summary:
            inv.comment = inv.summary
    if 'amount' in body:
        try:
            inv.amount = Decimal(str(body.get('amount') or 0).replace(',', '.').replace(' ', ''))
        except (InvalidOperation, TypeError, ValueError):
            return jsonify({'error': 'bad_amount'}), 400
    if 'budget_item_id' in body:
        bid = body.get('budget_item_id')
        inv.budget_item_id = int(bid) if bid else None
    if 'priority' in body and body['priority'] in ('high', 'normal', 'low'):
        inv.priority = body['priority']
    if body.get('confirm'):
        inv.status = 'new'
    db.session.commit()
    return jsonify(serialize_invoice(inv, detail=True))


@bp.route('/api/invoices/<int:inv_id>/discard', methods=['POST'])
@require_user
def api_discard(user: User, inv_id: int):
    if not _can_edit(user):
        return jsonify({'error': 'forbidden'}), 403
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if inv.status not in ('draft', 'new'):
        return jsonify({'error': 'locked'}), 400
    path = _invoice_path(inv)
    db.session.delete(inv)
    db.session.commit()
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    return jsonify({'ok': True})


# ---------------------------------------------------------------------------
# Bot ingest (webhook)
# ---------------------------------------------------------------------------

def _tg_reply(chat_id, text, reply_markup=None):
    try:
        send_chat_message(chat_id, text, reply_markup=reply_markup)
    except Exception:
        current_app.logger.exception('tg_pay reply failed')


def _is_invoice_document(doc: dict) -> bool:
    mime = (doc.get('mime_type') or '').lower()
    name = (doc.get('file_name') or '').lower()
    if 'pdf' in mime or name.endswith('.pdf'):
        return True
    # iPhone часто шлёт PDF как octet-stream без расширения в file_name.
    if mime in ('', 'application/octet-stream', 'application/x-pdf'):
        return True
    return False


def handle_private_update(msg: dict) -> bool:
    """True, если апдейт обработан Mini App (не отдавать AI)."""
    chat = msg.get('chat') or {}
    if chat.get('type') != 'private':
        return False
    text = (msg.get('text') or '').strip()
    chat_id = chat.get('id')
    sender = msg.get('from') or {}
    tg_id = sender.get('id')

    if text.startswith('/start') or text in ('счета', 'Счета', '/pay'):
        url = _public_miniapp_url()
        if url.startswith('https://'):
            markup = {
                'inline_keyboard': [[{
                    'text': 'Счета на оплату',
                    'web_app': {'url': url},
                }]]
            }
            _tg_reply(
                chat_id,
                'Счета на оплату — список, сумма, PDF.',
                reply_markup=markup,
            )
        else:
            _tg_reply(
                chat_id,
                'Локальный режим: откройте в браузере\nhttp://127.0.0.1:5000/tg/pay\n\n'
                'Чтобы кнопка работала в Telegram, задайте TG_MINIAPP_URL (https-туннель).',
            )
        return True

    doc = msg.get('document')
    if not doc:
        return False
    if not _is_invoice_document(doc):
        _tg_reply(chat_id, 'Нужен файл PDF (счёт на оплату).')
        return True

    try:
        return _ingest_private_pdf(chat_id, sender, tg_id, doc)
    except Exception:
        current_app.logger.exception('tg_pay ingest failed')
        _tg_reply(chat_id, 'Не смог разобрать счёт. Пришлите PDF ещё раз или загрузите через кнопку + в приложении.')
        return True


def _ingest_private_pdf(chat_id, sender, tg_id, doc) -> bool:
    user = _user_from_telegram(sender)
    if not user:
        mapped = _tg_user_id_map().get(str(tg_id), '')
        hint = (
            f'Этот Telegram не привязан к ERP.\n'
            f'Ваш id: <code>{tg_id}</code>\n\n'
            f'В Amvera одна строка, логин ERP (не имя):\n'
            f'<code>TG_USER_ID_MAP={tg_id}:admin</code>'
        )
        if mapped:
            hint = (
                f'В карте указан логин «{mapped}», такого пользователя в ERP нет.\n'
                f'Нужен логин из входа в систему, обычно <code>admin</code>:\n'
                f'<code>TG_USER_ID_MAP={tg_id}:admin</code>'
            )
        _tg_reply(chat_id, hint)
        return True
    if not _can_edit(user):
        _tg_reply(
            chat_id,
            f'Сейчас вы как <b>{user.username}</b> ({user.role}). '
            f'Загружать PDF может только admin.',
        )
        return True

    name = doc.get('file_name') or 'invoice.pdf'
    blob, err = download_bot_file(doc.get('file_id'))
    if not blob:
        _tg_reply(chat_id, f'Не смог скачать файл: {err}')
        return True

    save_name, path = _store_upload(blob, name)
    parsed = parse_invoice_file(path, name)
    inv = _save_parsed_invoice(save_name, name, parsed, source='tg')
    purpose = _purpose(inv)
    amount = f"{inv.amount:,.0f}".replace(',', ' ')
    article = inv.item.name if inv.item else 'статья не выбрана'
    extra = f"\n{parsed['error']}" if parsed.get('error') else ''
    app_url = _public_miniapp_url()
    markup = None
    if app_url.startswith('https://'):
        markup = {
            'inline_keyboard': [[{
                'text': 'Проверить и подтвердить',
                'web_app': {'url': app_url},
            }]]
        }
    _tg_reply(
        chat_id,
        f'Черновик счёта #{inv.id}\n'
        f'<b>{purpose}</b>\n'
        f'{amount} ₽ · {article}{extra}\n\n'
        f'Откройте приложение, поправьте если нужно и нажмите «В оплату».',
        reply_markup=markup,
    )
    return True
