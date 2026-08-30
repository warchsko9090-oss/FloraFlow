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
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import wraps
from urllib.parse import parse_qsl, unquote

from flask import (
    Blueprint, current_app, jsonify, request, render_template,
    send_from_directory, make_response,
)
from werkzeug.utils import secure_filename

from app.models import db, User, PaymentInvoice, BudgetItem, ChatExpenseMessage
from app.tg_pay_parse import parse_invoice_file
from app.utils import msk_now
from app.telegram import (
    _get_bot_token, send_chat_message, send_chat_document, download_bot_file,
    default_miniapp_url,
)
from app.invoice_files import invoice_bytes, has_file as invoice_has_file, attach_file, flask_send

bp = Blueprint('tg_pay', __name__, url_prefix='/tg/pay')

_ALLOWED_EXT = {'.pdf', '.jpg', '.jpeg', '.png', '.webp'}
_DEV_COOKIE = 'tg_pay_as'
_LAST_TG_FILE = '/data/tg_last_update.json'


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def note_telegram_update(kind: str, tg_id=None):
    payload = {
        'at': datetime.utcnow().isoformat() + 'Z',
        'kind': kind,
        'telegram_id': tg_id,
    }
    try:
        if os.path.isdir('/data'):
            with open(_LAST_TG_FILE, 'w', encoding='utf-8') as fh:
                json.dump(payload, fh)
    except Exception:
        pass
    return payload


def _last_telegram_update() -> dict | None:
    try:
        if os.path.isfile(_LAST_TG_FILE):
            with open(_LAST_TG_FILE, encoding='utf-8') as fh:
                return json.load(fh)
    except Exception:
        return None
    return None


def _db_ok() -> bool:
    try:
        User.query.limit(1).all()
        return True
    except Exception:
        db.session.rollback()
        return False


def _start_greeting(sender, tg_id) -> str:
    lines = ['FloraFlow на связи.']
    lines.append('База данных: ок' if _db_ok() else 'База данных: ошибка')
    lines.append('Токен бота: ок' if _get_bot_token() else 'Токен бота: НЕ ЗАДАН в Amvera (TG_BOT_TOKEN)')
    user = _user_from_telegram(sender) if sender else None
    if user:
        if _can_edit(user):
            lines.append(f'Вы: {tg_id} → {user.username} (можно загружать PDF)')
        else:
            lines.append(
                f'Вы: {tg_id} → {user.username}, роль {user.role}. '
                f'Загружать PDF может только admin.'
            )
    else:
        lines.append(f'Вы: {tg_id} — не привязан к ERP.')
        lines.append(f'В Amvera одна строка: TG_USER_ID_MAP={tg_id}:admin')
    lines.append('')
    lines.append('Пришлите PDF счёта или откройте кнопку «Счета».')
    return '\n'.join(lines)


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
    hardcoded = default_miniapp_url()
    if hardcoded.startswith('https://'):
        return hardcoded
    try:
        host = (request.host_url or '').rstrip('/')
        if host.startswith('https://'):
            return host + '/tg/pay'
    except RuntimeError:
        pass
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


def _bind_telegram_id(user: User, tg_id: int) -> None:
    """Пишем telegram_id, если колонка свободна. Два Telegram на один логин
    живут через TG_USER_ID_MAP — unique не даёт хранить оба числа на одной строке."""
    if user.telegram_id == tg_id:
        return
    taken = User.query.filter_by(telegram_id=tg_id).first()
    if taken and taken.id != user.id:
        taken.telegram_id = None
    if user.telegram_id and user.telegram_id != tg_id:
        return
    user.telegram_id = tg_id
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()


def _user_from_telegram(tg_user: dict) -> User | None:
    raw_id = tg_user.get('id')
    if not raw_id:
        return None
    tg_id = int(raw_id)

    # Явная карта важнее ника Telegram и уже сохранённого telegram_id:
    # иначе @KirillT навсегда садится в менеджера, даже если в env стоит admin.
    canonical = _tg_user_id_map().get(str(tg_id), '')
    if canonical:
        found = User.query.filter(db.func.lower(User.username) == canonical.lower()).first()
        if found:
            _bind_telegram_id(found, tg_id)
            return found
        return None

    found = User.query.filter_by(telegram_id=tg_id).first()
    if found:
        return found
    username = (tg_user.get('username') or '').lstrip('@').lower()
    if username:
        found = User.query.filter(db.func.lower(User.username) == username).first()
        if found:
            _bind_telegram_id(found, tg_id)
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


def _can_inbox(user: User) -> bool:
    return (user.role or '') in ('admin', 'executive')


def _notify_watchers(inv: PaymentInvoice, except_user: User | None = None):
    purpose = _purpose(inv)
    amount = f"{float(inv.amount or 0):,.0f}".replace(',', ' ')
    text = f"К оплате: {purpose}\n{amount} ₽"
    q = User.query.filter(User.telegram_id.isnot(None))
    for u in q.all():
        if except_user is not None and u.id == except_user.id:
            continue
        send_chat_message(u.telegram_id, text)


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
    from app.invoice_files import materialize_path
    return materialize_path(inv)


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
        'has_file': invoice_has_file(inv),
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
        status='new',
        priority='normal',
        comment=summary[:500],
    )
    db.session.add(inv)
    db.session.flush()
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
        has_user=bool(user),
    )
    resp = make_response(html)
    as_role = request.args.get('as')
    if is_dev and as_role in ('admin', 'payer'):
        resp.set_cookie(_DEV_COOKIE, as_role, samesite='Lax')
    return resp


@bp.route('/api/status')
def api_status():
    """Публичная проверка: живы ли БД, токен и вебхук Telegram."""
    last = _last_telegram_update()
    token = bool(_get_bot_token())
    db_ok = _db_ok()
    return jsonify({
        'ok': db_ok and token,
        'db': db_ok,
        'bot_token': token,
        'miniapp_url': _public_miniapp_url(),
        'mapped_ids': list(_tg_user_id_map().keys()),
        'last_telegram': last,
    })


@bp.route('/api/me')
@require_user
def api_me(user: User):
    return jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'can_edit': _can_edit(user),
        'can_inbox': _can_inbox(user),
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
    _prio = {'high': 0, 'normal': 1, 'low': 2}
    rows = q.all()
    rows.sort(key=lambda inv: (
        0 if (inv.status or '') == 'draft' else 1,
        _prio.get(inv.priority or 'normal', 1),
        -(inv.id or 0),
    ))
    invoices = [serialize_invoice(inv) for inv in rows]
    unpaid = [x for x in invoices if x['status'] != 'paid']
    total = sum(x['amount'] for x in unpaid)
    drafts = sum(1 for x in invoices if x['status'] == 'draft')
    inbox_count = 0
    if _can_inbox(user):
        inbox_count = ChatExpenseMessage.query.filter(
            ChatExpenseMessage.status.in_(['pending', 'invoice_match'])
        ).count()
    return jsonify({
        'invoices': invoices,
        'total_new': total,
        'count_new': sum(1 for x in unpaid if x['status'] == 'new'),
        'count_draft': drafts,
        'inbox_count': inbox_count,
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
    resp = flask_send(inv, as_attachment=False)
    if resp is None:
        return jsonify({'error': 'file_missing'}), 404
    return resp


@bp.route('/api/invoices/<int:inv_id>/send-pdf', methods=['POST'])
@require_user
def api_send_pdf(user: User, inv_id: int):
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if not _can_edit(user) and inv.status != 'new':
        return jsonify({'error': 'not_found'}), 404
    data = invoice_bytes(inv)
    if not data:
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
        filename=inv.original_name,
        caption=f"{_purpose(inv)} · {inv.amount} ₽",
        file_bytes=data,
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
    attach_file(inv, data, save_name)
    db.session.commit()
    _notify_watchers(inv, except_user=user)
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
    became_new = False
    if body.get('confirm') or inv.status == 'draft':
        if inv.status != 'new':
            became_new = True
        inv.status = 'new'
    if became_new or body.get('confirm'):
        db.session.commit()
        _notify_watchers(inv, except_user=user)
        return jsonify(serialize_invoice(inv, detail=True))
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


@bp.route('/api/invoices/<int:inv_id>/mark-paid', methods=['POST'])
@require_user
def api_mark_paid(user: User, inv_id: int):
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if inv.status not in ('new', 'draft'):
        return jsonify({'error': 'not_open'}), 400
    inv.status = 'paid'
    try:
        from app.vium_inbox import maybe_enqueue
        maybe_enqueue(inv)
    except Exception:
        current_app.logger.exception('vium_inbox.maybe_enqueue (miniapp mark_paid)')
    db.session.commit()
    return jsonify({'ok': True, 'id': inv.id})


def _serialize_inbox(row: ChatExpenseMessage) -> dict:
    inv = row.matched_invoice
    sug = row.suggested_item
    return {
        'id': row.id,
        'status': row.status,
        'amount': float(row.parsed_amount or 0),
        'description': row.parsed_description or row.raw_text or '',
        'payment_type': row.parsed_payment_type,
        'sender': row.sender_name or '',
        'invoice': (
            {
                'id': inv.id,
                'summary': _purpose(inv),
                'amount': float(inv.amount or 0),
                'status': inv.status,
            }
            if inv else None
        ),
        'suggested_budget_item_id': row.suggested_budget_item_id,
        'suggested_budget': (
            {'id': sug.id, 'name': sug.name, 'code': sug.code}
            if sug else None
        ),
    }


@bp.route('/api/inbox')
@require_user
def api_inbox(user: User):
    if not _can_inbox(user):
        return jsonify({'items': []})
    rows = ChatExpenseMessage.query.filter(
        ChatExpenseMessage.status.in_(['pending', 'invoice_match'])
    ).order_by(ChatExpenseMessage.id.desc()).limit(50).all()
    return jsonify({'items': [_serialize_inbox(r) for r in rows]})


@bp.route('/api/inbox/<int:msg_id>/confirm', methods=['POST'])
@require_user
def api_inbox_confirm(user: User, msg_id: int):
    if not _can_inbox(user):
        return jsonify({'error': 'forbidden'}), 403
    from app.expense_chat import confirm_chat_expense
    body = request.get_json(silent=True) or {}
    if body.get('as_expense'):
        row = ChatExpenseMessage.query.get_or_404(msg_id)
        row.matched_invoice_id = None
        db.session.flush()
    bid = body.get('budget_item_id')
    ok, msg = confirm_chat_expense(msg_id, user, budget_item_id=int(bid) if bid else None)
    if not ok:
        return jsonify({'ok': False, 'error': msg}), 400
    return jsonify({'ok': True})


@bp.route('/api/inbox/<int:msg_id>/reject', methods=['POST'])
@require_user
def api_inbox_reject(user: User, msg_id: int):
    if not _can_inbox(user):
        return jsonify({'error': 'forbidden'}), 403
    from app.expense_chat import reject_chat_expense
    ok, msg = reject_chat_expense(msg_id, user)
    if not ok:
        return jsonify({'ok': False, 'error': msg}), 400
    return jsonify({'ok': True})


# ---------------------------------------------------------------------------
# Bot ingest (webhook)
# ---------------------------------------------------------------------------

def _tg_reply(chat_id, text, reply_markup=None):
    ok, err = send_chat_message(chat_id, text, reply_markup=reply_markup)
    if not ok:
        current_app.logger.warning('tg_pay reply failed chat=%s err=%s', chat_id, err)
    return ok


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
        note_telegram_update('start', tg_id)
        url = _public_miniapp_url()
        markup = None
        if url.startswith('https://'):
            markup = {
                'inline_keyboard': [[{
                    'text': 'Счета на оплату',
                    'web_app': {'url': url},
                }]]
            }
        _tg_reply(chat_id, _start_greeting(sender, tg_id), reply_markup=markup)
        return True

    doc = msg.get('document')
    if not doc:
        return False
    note_telegram_update('document', tg_id)
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
    attach_file(inv, blob, save_name)
    db.session.commit()
    _notify_watchers(inv, except_user=user)
    purpose = _purpose(inv)
    amount = f"{inv.amount:,.2f}".replace(',', ' ').replace('.', ',')
    article = inv.item.name if inv.item else 'статья не выбрана'
    extra = f"\n{parsed['error']}" if parsed.get('error') else ''
    app_url = _public_miniapp_url()
    markup = None
    if app_url.startswith('https://'):
        markup = {
            'inline_keyboard': [[{
                'text': 'Открыть счёт',
                'web_app': {'url': app_url},
            }]]
        }
    _tg_reply(
        chat_id,
        f'Счёт #{inv.id} в оплате\n'
        f'<b>{purpose}</b>\n'
        f'{amount} ₽ · {article}{extra}\n\n'
        f'Когда переведёте — в приложении нажмите «Оплачено».',
        reply_markup=markup,
    )
    return True
