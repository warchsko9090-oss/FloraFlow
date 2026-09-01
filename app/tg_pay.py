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
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from functools import wraps
from urllib.parse import parse_qsl, unquote

from flask import (
    Blueprint, current_app, jsonify, request, render_template,
    send_from_directory, make_response,
)
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.utils import secure_filename

from app.models import db, User, PaymentInvoice, BudgetItem, ChatExpenseMessage
from app.tg_pay_parse import parse_invoice_file
from app.utils import msk_now, msk_today
from app.telegram import (
    _get_bot_token, send_chat_message, send_chat_document, download_bot_file,
    default_miniapp_url,
)
from app.invoice_files import invoice_bytes, has_file as invoice_has_file, attach_file, flask_send

bp = Blueprint('tg_pay', __name__, url_prefix='/tg/pay')

_ALLOWED_EXT = {'.pdf', '.jpg', '.jpeg', '.png', '.webp'}
_DEV_COOKIE = 'tg_pay_as'
_MINI_COOKIE = 'tg_mini'
_MINI_SALT = 'tg-mini-v1'
_MINI_MAX_AGE = 7 * 24 * 3600
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


def _can_sale_role(user: User | None) -> bool:
    return bool(user) and (user.role or '') in ('admin', 'shop_manager')


def _is_admin(user: User | None) -> bool:
    return bool(user) and (user.role or '') == 'admin'


def _can_pay_app(user: User | None) -> bool:
    """Оплата поставщикам: админ и исполнители. Менеджеру продаж не нужна."""
    return not (user and (user.role or '') == 'shop_manager')


def _start_greeting(sender, tg_id) -> str:
    lines = ['FloraFlow на связи.']
    lines.append('База данных: ок' if _db_ok() else 'База данных: ошибка')
    lines.append('Токен бота: ок' if _get_bot_token() else 'Токен бота: НЕ ЗАДАН в Amvera (TG_BOT_TOKEN)')
    user = _user_from_telegram(sender) if sender else None
    if user:
        if _is_admin(user):
            lines.append(f'Вы: {tg_id} → {user.username} ({user.role})')
            lines.append('Оплата — счета поставщикам. Выставить счёт — клиенту.')
        elif _can_sale_role(user):
            lines.append(f'Вы: {tg_id} → {user.username} ({user.role})')
            lines.append('Выставить счёт клиенту.')
        elif _can_edit(user):
            lines.append(f'Вы: {tg_id} → {user.username} (черновики и планы)')
        else:
            lines.append(f'Вы: {tg_id} → {user.username}, роль {user.role}. Оплата счетов.')
    else:
        lines.append(f'Вы: {tg_id} — не привязан к ERP.')
        lines.append(f'В Amvera одна строка: TG_USER_ID_MAP={tg_id}:admin')
    lines.append('')
    if _is_admin(user):
        lines.append('Кнопки внизу чата всегда под рукой: Оплата и Выставить счёт.')
    elif _can_sale_role(user):
        lines.append('Кнопка внизу чата: Выставить счёт.')
    else:
        lines.append('Пришлите PDF — попадёт в черновики администратора.')
    return '\n'.join(lines)


def _apps_reply_keyboard(user: User | None) -> dict | None:
    """Постоянные кнопки Mini App внизу чата — чтобы переключаться без /start."""
    row = []
    pay_url = _public_miniapp_url()
    if _can_pay_app(user) and pay_url.startswith('https://'):
        pay_label = 'Оплата' if _is_admin(user) else 'Счета на оплату'
        row.append({'text': pay_label, 'web_app': {'url': pay_url}})
    if _can_sale_role(user):
        from app.tg_sale import public_sale_url
        sale_url = public_sale_url()
        if sale_url.startswith('https://'):
            row.append({'text': 'Выставить счёт', 'web_app': {'url': sale_url}})
    if not row:
        return None
    return {
        'keyboard': [row],
        'resize_keyboard': True,
        'is_persistent': True,
    }


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


def _parse_init_fields(init_data: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in (init_data or '').split('&'):
        if not part or '=' not in part:
            continue
        key, val = part.split('=', 1)
        fields[unquote(key)] = unquote(val)
    return fields


def _init_hmac_ok(fields: dict[str, str], bot_token: str, skip: tuple[str, ...]) -> bool:
    got = fields.get('hash') or ''
    if not got:
        return False
    check = '\n'.join(
        f'{k}={v}' for k, v in sorted(fields.items()) if k not in skip
    )
    secret = hmac.new(b'WebAppData', bot_token.encode('utf-8'), hashlib.sha256).digest()
    calc = hmac.new(secret, check.encode('utf-8'), hashlib.sha256).hexdigest()
    return hmac.compare_digest(calc, got)


def _user_dict_from_fields(fields: dict[str, str]) -> dict | None:
    raw = fields.get('user') or '{}'
    user = {}
    for candidate in (raw, unquote(raw)):
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            user = parsed
            break
    return user if user.get('id') else None


def _validate_init_data(init_data: str, bot_token: str) -> dict | None:
    if not init_data or not bot_token:
        return None
    variants = (
        _parse_init_fields(init_data),
        dict(parse_qsl(init_data, keep_blank_values=True)),
    )
    skip_sets = (('hash',), ('hash', 'signature'))
    for fields in variants:
        for skip in skip_sets:
            if _init_hmac_ok(fields, bot_token, skip):
                return _user_dict_from_fields(fields)
    return None


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
    if as_role == 'shop_manager':
        return (
            User.query.filter_by(role='shop_manager').first()
            or User.query.filter_by(role='admin').first()
            or User.query.first()
        )
    return User.query.filter_by(role='admin').first() or User.query.first()


def _mini_signer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(current_app.secret_key, salt=_MINI_SALT)


def _mini_cookie_secure() -> bool:
    return bool(os.environ.get('AMVERA') or os.path.isdir('/data') or request.is_secure)


def set_mini_cookie(resp, user: User):
    token = _mini_signer().dumps({'uid': int(user.id)})
    resp.set_cookie(
        _MINI_COOKIE,
        token,
        max_age=_MINI_MAX_AGE,
        httponly=True,
        secure=_mini_cookie_secure(),
        samesite='Lax',
        path='/',
    )
    return resp


def _user_from_mini_cookie() -> User | None:
    raw = request.cookies.get(_MINI_COOKIE) or ''
    if not raw:
        return None
    try:
        data = _mini_signer().loads(raw, max_age=_MINI_MAX_AGE)
        return User.query.get(int(data.get('uid')))
    except (BadSignature, SignatureExpired, TypeError, ValueError, Exception):
        return None


def _init_data_candidates() -> list[str]:
    """Все источники initData: на iPhone заголовок может быть обрезан — пробуем каждый."""
    out: list[str] = []

    def add(value: str | None):
        text = (value or '').strip()
        if text and text not in out:
            out.append(text)

    add(request.headers.get('X-Telegram-Init-Data'))
    auth = (request.headers.get('Authorization') or '').strip()
    if auth.lower().startswith('tma '):
        add(auth[4:])
    if request.is_json:
        body = request.get_json(silent=True) or {}
        if isinstance(body, dict):
            add(body.get('initData'))
    add(request.form.get('initData'))
    qs = request.query_string.decode('utf-8', 'ignore')
    for part in qs.split('&'):
        if part.startswith('initData='):
            add(unquote(part[9:]))
            break
    add(request.args.get('initData'))
    return out


def _auth_fail_hint() -> str:
    if not _get_bot_token():
        return 'no_bot_token'
    if _init_data_candidates():
        return 'bad_signature'
    return 'no_init_data'


def resolve_user() -> tuple[User | None, bool, dict | None]:
    """(user, is_dev, pending_tg_user)."""
    token = _get_bot_token()
    pending = None
    if token:
        for init_data in _init_data_candidates():
            tg_user = _validate_init_data(init_data, token)
            if not tg_user:
                continue
            user = _user_from_telegram(tg_user)
            if user:
                return user, False, None
            pending = tg_user
            break
    cookie_user = _user_from_mini_cookie()
    if cookie_user and not pending:
        return cookie_user, False, None
    if pending and not _dev_mode():
        return None, False, pending
    if not _dev_mode():
        return None, False, None
    as_role = (
        request.headers.get('X-Tg-Pay-As')
        or request.headers.get('X-Tg-Sale-As')
        or request.cookies.get(_DEV_COOKIE)
        or request.cookies.get('tg_sale_as')
        or request.args.get('as')
        or 'admin'
    )
    if as_role not in ('admin', 'payer', 'shop_manager'):
        as_role = 'admin'
    return _dev_user(as_role), True, None


def _can_edit(user: User) -> bool:
    return (user.role or '') == 'admin'


def _can_inbox(user: User) -> bool:
    return (user.role or '') == 'admin'


def _notify_admins(text: str, except_user: User | None = None):
    q = User.query.filter_by(role='admin').filter(User.telegram_id.isnot(None))
    for u in q.all():
        if except_user is not None and u.id == except_user.id:
            continue
        send_chat_message(u.telegram_id, text)


def _notify_watchers(inv: PaymentInvoice, except_user: User | None = None):
    if (inv.status or '') == 'draft':
        return
    purpose = _purpose(inv)
    amount = f"{_pay_amount(inv):,.0f}".replace(',', ' ')
    kind = 'План' if (inv.kind or '') == 'plan' and _fact_amount(inv) <= 0 else 'К оплате'
    ptype = 'нал' if (inv.payment_type or '') == 'cash' else 'безнал'
    text = f"{kind}: {purpose}\n{amount} ₽ · {ptype}"
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
            return jsonify({
                'error': 'unauthorized',
                'hint': _auth_fail_hint(),
            }), 401
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


def _monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _default_plan_week(today: date | None = None) -> date:
    """В пт–вс план на следующую неделю, иначе текущая (пн)."""
    today = today or msk_today()
    if today.weekday() >= 4:
        return today + timedelta(days=(7 - today.weekday()))
    return _monday(today)


def _parse_money(value) -> Decimal:
    return Decimal(str(value or 0).replace(',', '.').replace(' ', '').replace('\xa0', ''))


def _fact_amount(inv: PaymentInvoice) -> float:
    kids = list(getattr(inv, 'fact_invoices', None) or [])
    kid_sum = float(sum((k.amount or 0) for k in kids))
    if (inv.kind or 'invoice') == 'plan':
        own = float(inv.amount or 0) if invoice_has_file(inv) else 0.0
        return own + kid_sum
    return float(inv.amount or 0)


def _pay_amount(inv: PaymentInvoice) -> float:
    """Сколько заложено к оплате: факт, иначе план, иначе сумма счёта."""
    fact = _fact_amount(inv)
    if fact > 0:
        return fact
    planned = inv.planned_amount
    if planned is not None:
        return float(planned or 0)
    return float(inv.amount or 0)


def _invoice_path(inv: PaymentInvoice) -> str | None:
    from app.invoice_files import materialize_path
    return materialize_path(inv)


def serialize_invoice(inv: PaymentInvoice, *, detail: bool = False) -> dict:
    item = inv.item
    planned = float(inv.planned_amount) if inv.planned_amount is not None else None
    fact = _fact_amount(inv)
    linked = next(iter(getattr(inv, 'fact_invoices', None) or []), None)
    data = {
        'id': inv.id,
        'summary': _purpose(inv),
        'amount': float(inv.amount or 0),
        'status': inv.status or 'new',
        'priority': inv.priority or 'normal',
        'due_date': inv.due_date.isoformat() if inv.due_date else None,
        'kind': inv.kind or 'invoice',
        'payment_type': inv.payment_type or 'cashless',
        'planned_amount': planned,
        'fact_amount': fact,
        'pay_amount': _pay_amount(inv),
        'week_start': inv.week_start.isoformat() if inv.week_start else None,
        'plan_id': inv.plan_id,
        'linked_id': linked.id if linked else None,
        'budget': (
            {'id': item.id, 'name': item.name, 'code': item.code}
            if item else None
        ),
        'original_name': inv.original_name,
        'source': inv.source or 'web',
        'has_file': invoice_has_file(inv) or bool(linked and invoice_has_file(linked)),
    }
    if detail:
        data['comment'] = inv.comment or ''
        data['lines'] = _lines_of(inv) or (linked and _lines_of(linked)) or []
        data['budget_item_id'] = inv.budget_item_id
        open_plans = [
            {
                'id': p.id,
                'summary': _purpose(p),
                'planned_amount': float(p.planned_amount or 0),
                'fact_amount': _fact_amount(p),
            }
            for p in PaymentInvoice.query.filter_by(kind='plan').filter(
                PaymentInvoice.status != 'paid',
                PaymentInvoice.id != inv.id,
            ).order_by(PaymentInvoice.id.desc()).limit(40).all()
        ]
        data['open_plans'] = open_plans
    return data


def _save_parsed_invoice(
    filename: str, original_name: str, parsed: dict, source: str, *, status: str = 'new',
) -> PaymentInvoice:
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
        status=status or 'new',
        priority='normal',
        comment=summary[:500],
        payment_type='cashless',
        kind='invoice',
        week_start=_default_plan_week(),
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


@bp.route('/api/auth', methods=['POST'])
def api_auth():
    user, is_dev, pending = resolve_user()
    if not user:
        if pending:
            return jsonify({
                'error': 'not_linked',
                'telegram_id': pending.get('id'),
                'username': (pending.get('username') or ''),
                'hint': 'Этот Telegram не привязан к пользователю ERP. Добавьте id в TG_USER_ID_MAP.',
            }), 403
        return jsonify({'error': 'unauthorized', 'hint': _auth_fail_hint()}), 401
    resp = jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'can_edit': _can_edit(user),
        'can_inbox': _can_inbox(user),
        'dev': is_dev,
        'telegram_id': user.telegram_id,
    })
    return set_mini_cookie(resp, user)


@bp.route('/api/me')
@require_user
def api_me(user: User):
    return jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'can_edit': _can_edit(user),
        'can_inbox': _can_inbox(user),
        'dev': _dev_mode() and not _init_data_candidates(),
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
    q = q.filter(PaymentInvoice.plan_id.is_(None))
    _prio = {'high': 0, 'normal': 1, 'low': 2}
    rows = q.all()
    rows.sort(key=lambda inv: (
        0 if (inv.status or '') == 'draft' else 1,
        _prio.get(inv.priority or 'normal', 1),
        -(inv.id or 0),
    ))
    invoices = [serialize_invoice(inv) for inv in rows]
    unpaid = [x for x in invoices if x['status'] != 'paid' and not x.get('plan_id')]
    live = [x for x in unpaid if x.get('status') != 'draft']
    due = [
        x for x in live
        if (x.get('fact_amount') or 0) > 0 or (x.get('kind') != 'plan' and (x.get('amount') or 0) > 0)
    ]
    def _due_amt(x):
        return float(x.get('fact_amount') or x.get('amount') or 0)
    total = sum(_due_amt(x) for x in due)
    cash = sum(_due_amt(x) for x in due if x.get('payment_type') == 'cash')
    cashless = sum(_due_amt(x) for x in due if x.get('payment_type') != 'cash')
    plan_sum = sum((x.get('planned_amount') or 0) for x in live if x.get('planned_amount'))
    fact_sum = sum(_due_amt(x) for x in due)
    inbox_count = 0
    if _can_inbox(user):
        inbox_count = ChatExpenseMessage.query.filter(
            ChatExpenseMessage.status.in_(['pending', 'invoice_match'])
        ).count()
    return jsonify({
        'invoices': unpaid,
        'total_new': total,
        'total_cash': cash,
        'total_cashless': cashless,
        'total_plan': plan_sum,
        'total_fact': fact_sum,
        'week_start': _default_plan_week().isoformat(),
        'count_new': len(unpaid),
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
    src = inv
    if not invoice_has_file(src):
        kid = next(iter(getattr(inv, 'fact_invoices', None) or []), None)
        if kid:
            src = kid
    resp = flask_send(src, as_attachment=False)
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
        kid = next(iter(getattr(inv, 'fact_invoices', None) or []), None)
        if kid:
            data = invoice_bytes(kid)
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
    plan_id = request.form.get('plan_id') or request.args.get('plan_id')
    plan = PaymentInvoice.query.get(int(plan_id)) if plan_id else None
    if plan and (plan.kind or '') == 'plan':
        attach_file(plan, data, save_name)
        amount = parsed.get('amount') or Decimal('0')
        if not isinstance(amount, Decimal):
            try:
                amount = _parse_money(amount)
            except (InvalidOperation, TypeError, ValueError):
                amount = Decimal('0')
        plan.amount = amount
        if parsed.get('summary') and not (plan.summary or '').strip():
            plan.summary = parsed['summary'][:500]
        plan.line_items = json.dumps(parsed.get('lines') or [], ensure_ascii=False)
        plan.original_name = file.filename[:255]
        db.session.commit()
        _notify_watchers(plan, except_user=user)
        payload = serialize_invoice(plan, detail=True)
        payload['parse_error'] = parsed.get('error')
        return jsonify(payload)
    inv = _save_parsed_invoice(save_name, file.filename, parsed, source='miniapp')
    attach_file(inv, data, save_name)
    if plan_id:
        try:
            inv.plan_id = int(plan_id)
        except (TypeError, ValueError):
            pass
    ptype = (request.form.get('payment_type') or '').strip()
    if ptype in ('cash', 'cashless'):
        inv.payment_type = ptype
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
    if 'payment_type' in body and body['payment_type'] in ('cash', 'cashless'):
        inv.payment_type = body['payment_type']
    if 'planned_amount' in body:
        raw = body.get('planned_amount')
        if raw in (None, ''):
            inv.planned_amount = None
        else:
            try:
                inv.planned_amount = _parse_money(raw)
            except (InvalidOperation, TypeError, ValueError):
                return jsonify({'error': 'bad_plan'}), 400
    if 'plan_id' in body:
        pid = body.get('plan_id')
        inv.plan_id = int(pid) if pid else None
    became_new = False
    if body.get('confirm'):
        if inv.status != 'new':
            became_new = True
        inv.status = 'new'
    if became_new or body.get('confirm'):
        db.session.commit()
        _notify_watchers(inv, except_user=user)
        return jsonify(serialize_invoice(inv, detail=True))
    db.session.commit()
    return jsonify(serialize_invoice(inv, detail=True))


@bp.route('/api/invoices/<int:inv_id>/assign', methods=['POST'])
@require_user
def api_assign(user: User, inv_id: int):
    if not _can_edit(user):
        return jsonify({'error': 'forbidden'}), 403
    inv = PaymentInvoice.query.get_or_404(inv_id)
    if inv.status not in ('draft', 'new'):
        return jsonify({'error': 'locked'}), 400
    body = request.get_json(silent=True) or {}
    if body.get('as_new'):
        inv.status = 'new'
        inv.plan_id = None
        if (inv.kind or '') == 'plan':
            inv.kind = 'invoice'
        db.session.commit()
        _notify_watchers(inv, except_user=user)
        return jsonify(serialize_invoice(inv, detail=True))
    try:
        pid = int(body.get('plan_id'))
    except (TypeError, ValueError):
        return jsonify({'error': 'need_plan'}), 400
    plan = PaymentInvoice.query.get(pid)
    if not plan or (plan.kind or '') != 'plan' or plan.status == 'paid':
        return jsonify({'error': 'bad_plan'}), 400
    if plan.id == inv.id:
        return jsonify({'error': 'bad_plan'}), 400
    inv.plan_id = plan.id
    inv.status = 'new'
    inv.kind = 'invoice'
    if not inv.budget_item_id:
        inv.budget_item_id = plan.budget_item_id
    if plan.payment_type in ('cash', 'cashless'):
        inv.payment_type = plan.payment_type
    db.session.commit()
    _notify_watchers(plan, except_user=user)
    return jsonify(serialize_invoice(plan, detail=True))


@bp.route('/api/invoices/plan', methods=['POST'])
@require_user
def api_create_plan(user: User):
    if not _can_edit(user):
        return jsonify({'error': 'forbidden'}), 403
    body = request.get_json(silent=True) or {}
    summary = (body.get('summary') or '').strip()[:500]
    if not summary:
        return jsonify({'error': 'need_summary'}), 400
    try:
        planned = _parse_money(body.get('planned_amount'))
    except (InvalidOperation, TypeError, ValueError):
        return jsonify({'error': 'bad_plan'}), 400
    if planned <= 0:
        return jsonify({'error': 'bad_plan'}), 400
    ptype = body.get('payment_type') if body.get('payment_type') in ('cash', 'cashless') else 'cashless'
    bid = body.get('budget_item_id')
    week_raw = (body.get('week_start') or '').strip()
    try:
        week = date.fromisoformat(week_raw) if week_raw else _default_plan_week()
        week = _monday(week)
    except ValueError:
        week = _default_plan_week()
    stamp = f"plan_{int(msk_now().timestamp())}"
    inv = PaymentInvoice(
        filename=stamp,
        original_name=summary[:255],
        summary=summary,
        source='miniapp',
        budget_item_id=int(bid) if bid else None,
        amount=Decimal('0'),
        planned_amount=planned,
        status='new',
        priority='normal',
        comment=summary,
        payment_type=ptype,
        kind='plan',
        week_start=week,
    )
    db.session.add(inv)
    db.session.commit()
    _notify_watchers(inv, except_user=user)
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
    if inv.status != 'new':
        return jsonify({'error': 'not_open'}), 400
    inv.status = 'paid'
    for kid in list(getattr(inv, 'fact_invoices', None) or []):
        kid.status = 'paid'
    exp = None
    try:
        from app.invoice_files import ensure_expense_for_paid_invoice
        exp = ensure_expense_for_paid_invoice(inv)
    except Exception:
        current_app.logger.exception('expense from miniapp mark_paid')
    try:
        from app.vium_inbox import maybe_enqueue
        maybe_enqueue(inv, expense=exp)
    except Exception:
        current_app.logger.exception('vium_inbox.maybe_enqueue (miniapp mark_paid)')
    db.session.commit()
    try:
        from app.invoice_files import notify_invoice_paid_chat
        notify_invoice_paid_chat(inv)
    except Exception:
        current_app.logger.exception('notify invoice paid chat')
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
        user = _user_from_telegram(sender) if sender else None
        if _can_sale_role(user):
            from app.tg_sale import public_sale_url
            sale_url = public_sale_url()
            if sale_url.startswith('https://'):
                try:
                    from app.telegram import set_pay_menu_button
                    set_pay_menu_button(url=sale_url, chat_id=chat_id, text='Счёт')
                except Exception:
                    current_app.logger.exception('sale menu button')
        markup = _apps_reply_keyboard(user)
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

    name = doc.get('file_name') or 'invoice.pdf'
    blob, err = download_bot_file(doc.get('file_id'))
    if not blob:
        _tg_reply(chat_id, f'Не смог скачать файл: {err}')
        return True

    save_name, path = _store_upload(blob, name)
    parsed = parse_invoice_file(path, name)
    inv = _save_parsed_invoice(save_name, name, parsed, source='tg', status='draft')
    attach_file(inv, blob, save_name)
    db.session.commit()
    purpose = _purpose(inv)
    amount = f"{inv.amount:,.2f}".replace(',', ' ').replace('.', ',')
    extra = f"\n{parsed['error']}" if parsed.get('error') else ''
    app_url = _public_miniapp_url()
    markup = None
    if app_url.startswith('https://') and _can_edit(user):
        markup = {
            'inline_keyboard': [[{
                'text': 'Раскидать по планам',
                'web_app': {'url': app_url},
            }]]
        }
    if _can_edit(user):
        next_hint = 'Черновик. В приложении привяжите к плану или оставьте как новый счёт.'
    else:
        next_hint = 'Черновик у администратора — он привяжет к плану.'
    _tg_reply(
        chat_id,
        f'Черновик #{inv.id}\n'
        f'<b>{purpose}</b>\n'
        f'{amount} ₽{extra}\n\n'
        f'{next_hint}',
        reply_markup=markup,
    )
    _notify_admins(
        f'Черновик #{inv.id}: {purpose}\n{amount} ₽',
        except_user=user,
    )
    return True
