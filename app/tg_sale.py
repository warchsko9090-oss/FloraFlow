"""Telegram Mini App «Выставить счёт» клиенту.

Роли: admin, executive, shop_manager.
Новый счёт при согласовании создаёт заказ ERP. «На заказ» заполняет счёт
из уже существующего заказа и не создаёт второй.
"""
from __future__ import annotations

import os
import re
import tempfile
import unicodedata
from datetime import datetime, timedelta
from html import escape as html_escape
from decimal import Decimal, InvalidOperation
from functools import wraps
from pathlib import Path

from flask import (
    Blueprint, current_app, jsonify, request, render_template, make_response,
)
from sqlalchemy import event, func, inspect, or_, cast, String
from sqlalchemy.orm import Session, joinedload, selectinload
from werkzeug.utils import secure_filename

from app.models import (
    db, User, Client, Plant, Size, StockBalance, Order, OrderItem, OrderItemHistory,
    SaleCompany, SaleInvoice, SaleInvoiceLine, ShopPlantCard, Document, DocumentRow,
)
from app.tg_pay import resolve_user, _auth_fail_hint, set_mini_cookie, log_mini_auth_fail, current_telegram_id
from app.tg_sale_parse import parse_buyer_file
from app.utils import msk_now, build_pdf_bytes, size_natural_key
from app.telegram import send_chat_document, send_document, send_message as tg_send_message, default_miniapp_url
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
_ORDER_STATUS_LABEL = {
    'reserved': 'резерв',
    'in_progress': 'в работе',
    'ready': 'готов',
    'shipped': 'отгружен',
    'canceled': 'отменён',
    'ghost': 'скрыт',
}
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
    return (user.role or '') in ('admin', 'executive', 'shop_manager')


def _can_firms(user: User) -> bool:
    return (user.role or '') == 'admin'


def _is_accountant(user: User | None) -> bool:
    return bool(user) and (user.role or '') == 'accountant'


def _sale_me_payload(user: User, *, is_dev: bool = False) -> dict:
    accountant = _is_accountant(user)
    role = (user.role or '')
    return {
        'id': user.id,
        'username': user.username,
        'role': role,
        'can_firms': _can_firms(user),
        'can_edit_firms': _can_firms(user),
        'can_delete_approved': role == 'admin',
        # Бухгалтер — только вкладка УПД; админ — и счета, и вкладка УПД.
        'accountant_only': accountant,
        'can_buh': accountant or role == 'admin',
        'dev': is_dev,
    }


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
            return jsonify({'error': 'forbidden', 'hint': 'Только admin, руководитель или менеджер продаж'}), 403
        return fn(user, *args, **kwargs)
    return wrapped


def require_buh(fn):
    """Вкладка УПД: бухгалтер. Админ — чтобы проверить локально."""
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
        if not _is_accountant(user) and (user.role or '') != 'admin':
            return jsonify({'error': 'forbidden', 'hint': 'Только бухгалтер'}), 403
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


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or '').strip().lower() in ('1', 'true', 'yes', 'on')


def _find_client_by_inn(inn: str | None) -> Client | None:
    digits = _inn_digits(inn)
    if len(digits) not in (10, 12):
        return None
    for client in Client.query.filter(Client.inn.isnot(None)).all():
        if _inn_digits(client.inn) == digits:
            return client
    return None


_LEGAL_FORM = re.compile(
    r'\b(ооо|оао|пао|зао|ао|ип|нко|общество\s+с\s+ограниченной\s+ответственностью|'
    r'индивидуальный\s+предприниматель)\b',
    re.I,
)

# Латиница, которую часто ставят вместо русских букв (ООО с английской раскладки → ООО).
_HOMOGLYPHS = str.maketrans({
    'a': 'а', 'e': 'е', 'o': 'о', 'p': 'р', 'c': 'с', 'x': 'х',
    'y': 'у', 'k': 'к', 'h': 'н', 'b': 'в', 'm': 'м', 't': 'т',
    'A': 'а', 'E': 'е', 'O': 'о', 'P': 'р', 'C': 'с', 'X': 'х',
    'Y': 'у', 'K': 'к', 'H': 'н', 'B': 'в', 'M': 'м', 'T': 'т',
})


def _fold_yo(s: str) -> str:
    return (s or '').replace('ё', 'е').replace('Ё', 'Е')


def _fold_client_query(s: str) -> str:
    """Поиск без сюрпризов SQL lower(): регистр, ё/е, латиница-двойники."""
    t = unicodedata.normalize('NFKC', s or '')
    t = t.translate(_HOMOGLYPHS)
    t = _fold_yo(t).casefold()
    return re.sub(r'\s+', ' ', t).strip()


def _client_name_key(name: str | None) -> str:
    s = _fold_yo(name or '').lower()
    s = _LEGAL_FORM.sub(' ', s)
    return re.sub(r'[^a-zа-я0-9]+', '', s)


def _find_client_by_name(name: str | None) -> Client | None:
    key = _client_name_key(name)
    if len(key) < 4:
        return None
    cands = [c for c in Client.query.all() if _client_name_key(c.name) == key]
    if not cands:
        return None
    cands.sort(key=lambda c: (
        0 if _inn_digits(c.inn) else 1,
        0 if (c.bank_name or c.rs) else 1,
        -len(c.name or ''),
        c.id,
    ))
    return cands[0]


def _resolve_client(name: str | None, inn: str | None) -> Client | None:
    found = _find_client_by_inn(inn) if inn else None
    if found:
        return found
    return _find_client_by_name(name)


def apply_client_to_invoice(inv: SaleInvoice, client: Client | None) -> None:
    """Реквизиты Mini App-счёта = карточка клиента в ERP."""
    if not client:
        return
    inv.client_id = client.id
    inv.buyer_name = (client.name or inv.buyer_name or '')[:300]
    inn = _inn_digits(client.inn)
    if inn:
        inv.buyer_inn = inn
    if client.kpp:
        inv.buyer_kpp = _digits(client.kpp, 9)
    if client.ogrn:
        inv.buyer_ogrn = _digits(client.ogrn, 15)
    if client.address:
        inv.buyer_address = (client.address or '')[:500]
    if client.phone:
        inv.buyer_phone = str(client.phone)[:40]
    if client.bank_name:
        inv.buyer_bank = (client.bank_name or '')[:200]
    if client.rs:
        inv.buyer_rs = _digits(client.rs, 20)
    if client.bik:
        inv.buyer_bik = _digits(client.bik, 9)
    if client.ks:
        inv.buyer_ks = _digits(client.ks, 20)


def _fill_empty_client_fields(client: Client, inv: SaleInvoice) -> None:
    """Новые данные из Mini App только в пустые поля карточки, имя ERP не трогаем."""
    if not (client.name or '').strip() and (inv.buyer_name or '').strip():
        client.name = inv.buyer_name.strip()[:200]
    if not _inn_digits(client.inn) and _inn_digits(inv.buyer_inn):
        client.inn = _inn_digits(inv.buyer_inn)[:20]
    if not _digits(client.kpp) and inv.buyer_kpp:
        client.kpp = _digits(inv.buyer_kpp, 9)[:20]
    if not _digits(client.ogrn) and getattr(inv, 'buyer_ogrn', None):
        client.ogrn = _digits(inv.buyer_ogrn, 15)[:20]
    if not (client.address or '').strip() and inv.buyer_address:
        client.address = inv.buyer_address[:500]
    if not (client.phone or '').strip() and getattr(inv, 'buyer_phone', None):
        client.phone = str(inv.buyer_phone)[:40]
    if not (client.bank_name or '').strip() and inv.buyer_bank:
        client.bank_name = inv.buyer_bank[:200]
    if not _digits(client.rs) and inv.buyer_rs:
        client.rs = _digits(inv.buyer_rs, 20)[:40]
    if not _digits(client.bik) and inv.buyer_bik:
        client.bik = _digits(inv.buyer_bik, 9)[:20]
    if not _digits(client.ks) and inv.buyer_ks:
        client.ks = _digits(inv.buyer_ks, 20)[:40]


def _fill_invoice_gaps_from_client(inv: SaleInvoice, client: Client) -> None:
    if not (inv.buyer_name or '').strip() and client.name:
        inv.buyer_name = client.name[:300]
    if not _inn_digits(inv.buyer_inn) and _inn_digits(client.inn):
        inv.buyer_inn = _inn_digits(client.inn)
    if not _digits(inv.buyer_kpp) and client.kpp:
        inv.buyer_kpp = _digits(client.kpp, 9)
    if not _digits(inv.buyer_ogrn) and client.ogrn:
        inv.buyer_ogrn = _digits(client.ogrn, 15)
    if not (inv.buyer_address or '').strip() and client.address:
        inv.buyer_address = client.address[:500]
    if not (inv.buyer_phone or '').strip() and client.phone:
        inv.buyer_phone = str(client.phone)[:40]
    if not (inv.buyer_bank or '').strip() and client.bank_name:
        inv.buyer_bank = client.bank_name[:200]
    if not _digits(inv.buyer_rs) and client.rs:
        inv.buyer_rs = _digits(client.rs, 20)
    if not _digits(inv.buyer_bik) and client.bik:
        inv.buyer_bik = _digits(client.bik, 9)
    if not _digits(inv.buyer_ks) and client.ks:
        inv.buyer_ks = _digits(client.ks, 20)


def sync_sale_invoices_from_order(order: Order | None) -> int:
    """Если в ERP сменили клиента у заказа — те же реквизиты у связанного счёта Mini App."""
    if not order or not order.id or not order.client_id:
        return 0
    client = Client.query.get(order.client_id)
    if not client:
        return 0
    n = 0
    rows = SaleInvoice.query.filter(
        SaleInvoice.order_id == order.id,
        SaleInvoice.status != 'discarded',
    ).all()
    for inv in rows:
        before = (inv.client_id, inv.buyer_name, inv.buyer_inn)
        apply_client_to_invoice(inv, client)
        if before != (inv.client_id, inv.buyer_name, inv.buyer_inn):
            n += 1
    return n


def align_sale_invoices_with_orders(*, commit: bool = False) -> int:
    ids = {
        inv.order_id
        for inv in SaleInvoice.query.filter(
            SaleInvoice.order_id.isnot(None),
            SaleInvoice.status != 'discarded',
        ).all()
    }
    n = 0
    for oid in ids:
        n += sync_sale_invoices_from_order(Order.query.get(oid))
    if n and commit:
        db.session.commit()
    return n


def _client_buyer(c: Client) -> dict:
    inn = _inn_digits(c.inn)
    rs = _digits(c.rs, 20)
    bik = _digits(c.bik, 9)
    bank = (c.bank_name or '').strip()
    return {
        'id': c.id,
        'name': (c.name or '').strip(),
        'inn': inn,
        'kpp': _digits(c.kpp, 9),
        'ogrn': _digits(c.ogrn, 15),
        'address': (c.address or '').strip(),
        'phone': (c.phone or '').strip(),
        'bank': bank,
        'rs': rs,
        'bik': bik,
        'ks': _digits(c.ks, 20),
        'has_bank': bool(bank or len(rs) == 20),
    }


def _search_clients(q: str, limit: int = 15) -> list[Client]:
    raw = re.sub(r'[%_]+', ' ', (q or '')).strip()
    if len(raw) < 2:
        return []
    needle = _fold_client_query(raw)
    digits = _inn_digits(raw)
    hits = []
    for c in Client.query.order_by(Client.name).all():
        name_f = _fold_client_query(c.name)
        inn_ok = bool(digits and len(digits) >= 4 and digits in _inn_digits(c.inn))
        if needle in name_f or inn_ok:
            hits.append(c)

    def rank(c: Client) -> tuple:
        name = _fold_client_query(c.name)
        pos = name.find(needle)
        starts = 0 if name.startswith(needle) else 1
        return (starts, pos if pos >= 0 else 999, name)

    hits.sort(key=rank)
    return hits[:limit]


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


def _shop_cards(plant_ids) -> dict[int, ShopPlantCard]:
    ids = sorted({int(pid) for pid in plant_ids if pid})
    if not ids:
        return {}
    rows = ShopPlantCard.query.filter(ShopPlantCard.plant_id.in_(ids)).all()
    return {int(r.plant_id): r for r in rows}


def _shop_attrs(card: ShopPlantCard | None, size_name: str) -> str:
    if not card:
        return ''
    if _is_container_size(size_name):
        bits = (card.seedling_root_system, card.seedling_pruning)
    else:
        bits = (card.root_system, card.pruning)
    out = []
    for raw in bits:
        text = re.sub(r'\s+', ' ', (raw or '').strip())
        if text:
            out.append(text.upper())
    return ' '.join(out)


def _goods_title(plant_name: str, size_name: str, attrs: str = '') -> str:
    base = f'{plant_name or ""} {size_name or ""}'.strip()
    extra = (attrs or '').strip()
    if extra:
        return f'{base} {extra}'.strip()
    return base


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
        'has_stamp': bool(c.stamp_blob),
        'stamp_name': c.stamp_name or '',
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


def sale_public_number(inv: SaleInvoice) -> int:
    return int(inv.doc_number or inv.id)


def allocate_sale_doc_number(inv: SaleInvoice) -> int:
    """Счёт №100, 101… в пределах календарного года. С 1 января снова 100."""
    if inv.doc_number:
        return int(inv.doc_number)
    year = msk_now().year
    prev = (
        db.session.query(func.max(SaleInvoice.doc_number))
        .filter(SaleInvoice.doc_year == year)
        .scalar()
    )
    n = 100 if not prev or int(prev) < 100 else int(prev) + 1
    inv.doc_year = year
    inv.doc_number = n
    return n


def _copy_order_items_to_sale_invoice(inv: SaleInvoice, order: Order) -> None:
    """Позиции заказа ERP → строки счёта Mini App. Без обрезки по свободному остатку."""
    inv.lines.clear()
    db.session.flush()
    grouped: dict[tuple, dict] = {}
    for it in order.items or []:
        qty = int(it.quantity or 0)
        if qty <= 0:
            continue
        price = _money(it.price)
        key = (it.plant_id, it.size_id, str(price))
        if key not in grouped:
            grouped[key] = {
                'plant_id': it.plant_id,
                'size_id': it.size_id,
                'plant_name': (it.plant.name if it.plant else '')[:200],
                'size_name': (it.size.name if it.size else '')[:120],
                'qty': 0,
                'price': price,
            }
        grouped[key]['qty'] += qty
    for row in grouped.values():
        db.session.add(SaleInvoiceLine(
            invoice=inv,
            plant_id=row['plant_id'],
            size_id=row['size_id'],
            plant_name=row['plant_name'],
            size_name=row['size_name'],
            qty=row['qty'],
            price=row['price'],
        ))
    db.session.flush()
    inv.amount = _line_sum(inv.lines)


def create_sale_invoice_from_order(
    order: Order,
    company_id: int,
    user_id: int | None,
    *,
    kind: str = 'goods',
    always_new: bool = True,
) -> SaleInvoice:
    """Создать счёт в БД из заказа ERP и сохранить PDF (без перезаписи старых счетов).

    always_new=True (по умолчанию): каждый вызов — новый SaleInvoice с новым doc_number.
    always_new=False: устаревший режим «обновить последний» — не использовать в ERP.
    """
    if not order or not order.client_id:
        raise ValueError('no_client')
    if order.is_deleted or (order.status or '') in ('canceled', 'ghost'):
        raise ValueError('bad_order')
    company = SaleCompany.query.get(int(company_id))
    if not company or not _company_ready(company):
        raise ValueError('no_company')
    if not any(int(it.quantity or 0) > 0 for it in (order.items or [])):
        raise ValueError('no_lines')

    kind = (kind or 'goods').strip().lower()
    if kind not in ('goods', 'advance', 'balance'):
        kind = 'goods'

    now = msk_now()
    inv = None
    if not always_new:
        inv = (
            SaleInvoice.query
            .filter(
                SaleInvoice.order_id == order.id,
                SaleInvoice.status != 'discarded',
            )
            .order_by(SaleInvoice.id.desc())
            .first()
        )
    if inv is None:
        inv = SaleInvoice(
            company_id=company.id,
            user_id=user_id,
            status='approved',
            approved_at=now,
            order_id=order.id,
            origin='erp',
            kind=kind,
            from_existing_order=True,
            comment=f'Заказ №{order.id}',
        )
        db.session.add(inv)
        db.session.flush()
    else:
        inv.company_id = company.id
        inv.origin = inv.origin or 'erp'
        inv.kind = kind or inv.kind or 'goods'
        if inv.status != 'approved':
            inv.status = 'approved'
            inv.approved_at = inv.approved_at or now

    apply_client_to_invoice(inv, order.invoice_client)
    _copy_order_items_to_sale_invoice(inv, order)
    allocate_sale_doc_number(inv)
    # Order.invoice_number — старый «общий счёт» для группировки дерева; не трогаем,
    # если уже заполнен (иначе каждый новый Mini App № перезапишет группировку).
    if not (order.invoice_number or '').strip():
        order.invoice_number = str(sale_public_number(inv))
        order.invoice_date = (inv.approved_at or now).date()
    blob = _store_pdf(inv)
    if not blob:
        raise ValueError('pdf_failed')
    return inv


def _parse_custom_invoice_lines(lines: list[dict] | None) -> list[tuple[str, int, str, Decimal]]:
    """Свободные строки счёта → (name, qty, unit, price). Без справочника/остатков."""
    parsed: list[tuple[str, int, str, Decimal]] = []
    for raw in lines or []:
        name = (raw.get('name') or '').strip()
        if not name:
            continue
        try:
            qty_dec = _money(raw.get('qty') or 1)
        except Exception:
            qty_dec = Decimal('1')
        qty_i = max(1, int(qty_dec))
        try:
            price = _money(raw.get('price') or 0)
        except Exception:
            price = Decimal('0')
        unit = (raw.get('unit') or 'усл. ед.').strip() or 'усл. ед.'
        parsed.append((name[:200], qty_i, unit[:40], price))
    return parsed


def create_custom_sale_invoice(
    company_id: int,
    user_id: int | None,
    *,
    lines: list[dict],
    comment: str | None = None,
    anonymous: bool = False,
    buyer_name: str | None = None,
    buyer_line: str | None = None,
    order: Order | None = None,
    pdf_overrides: dict | None = None,
) -> SaleInvoice:
    """Произвольный счёт в реестре ERP: номер 100+, без остатков и без суммы заказа.

    order=None — счёт только в реестре; order задан — привязка к заказу без изменения позиций/total_sum.
    """
    if order is not None:
        if not order.client_id:
            raise ValueError('no_client')
        if order.is_deleted or (order.status or '') in ('canceled', 'ghost'):
            raise ValueError('bad_order')
    company = SaleCompany.query.get(int(company_id))
    if not company or not _company_ready(company):
        raise ValueError('no_company')

    parsed = _parse_custom_invoice_lines(lines)
    if not parsed:
        raise ValueError('no_lines')

    now = msk_now()
    if order is not None:
        basis = (comment or '').strip() or f'Заказ №{order.id}'
    else:
        basis = (comment or '').strip() or 'Произвольный счёт'
    inv = SaleInvoice(
        company_id=company.id,
        user_id=user_id,
        status='approved',
        approved_at=now,
        order_id=order.id if order is not None else None,
        origin='erp',
        kind='advance',
        from_existing_order=bool(order is not None),
        anonymous=bool(anonymous),
        comment=basis[:500],
        buyer_name='',
    )
    db.session.add(inv)
    db.session.flush()
    if order is not None:
        apply_client_to_invoice(inv, order.invoice_client)
    else:
        name = (buyer_name or '').strip()
        if not name and buyer_line:
            name = (buyer_line or '').strip().split('\n', 1)[0].strip()
            # «ООО Ромашка, ИНН …» → имя до первой запятой с ИНН
            for sep in (', ИНН', ',Инн', ', инн'):
                if sep.lower() in name.lower():
                    idx = name.lower().find(sep.lower())
                    name = name[:idx].strip()
                    break
        inv.buyer_name = (name or ('Без покупателя' if anonymous else 'Покупатель'))[:300]
    for name, qty, unit, price in parsed:
        db.session.add(SaleInvoiceLine(
            invoice=inv,
            plant_id=None,
            size_id=None,
            plant_name=name,
            size_name=unit,
            qty=qty,
            price=price,
        ))
    db.session.flush()
    inv.amount = _line_sum(inv.lines)
    allocate_sale_doc_number(inv)
    # Группировка «общий счёт» — только если ещё пусто; сумму заказа не меняем.
    if order is not None and not (order.invoice_number or '').strip():
        order.invoice_number = str(sale_public_number(inv))
        order.invoice_date = (inv.approved_at or now).date()

    ov = dict(pdf_overrides or {})
    ov.setdefault('lines', [
        {
            'name': name,
            'qty': str(qty),
            'unit': unit,
            'price': str(price),
        }
        for name, qty, unit, price in parsed
    ])
    if 'basis' not in ov:
        ov['basis'] = basis
    blob = render_sale_pdf(inv, ov)
    if not blob:
        raise ValueError('pdf_failed')
    inv.file_blob = blob
    inv.file_name = f'schet_{sale_public_number(inv)}.pdf'
    return inv


def create_custom_sale_invoice_from_order(
    order: Order,
    company_id: int,
    user_id: int | None,
    *,
    lines: list[dict],
    comment: str | None = None,
    anonymous: bool = False,
    pdf_overrides: dict | None = None,
) -> SaleInvoice:
    """Произвольный счёт по заказу → create_custom_sale_invoice."""
    return create_custom_sale_invoice(
        company_id,
        user_id,
        lines=lines,
        comment=comment,
        anonymous=anonymous,
        order=order,
        pdf_overrides=pdf_overrides,
    )


def create_advance_sale_invoice_from_order(
    order: Order,
    company_id: int,
    user_id: int | None,
    *,
    amount,
    title: str | None = None,
    qty: int = 1,
) -> SaleInvoice:
    """Совместимость: однострочный аванс → create_custom_sale_invoice_from_order."""
    try:
        amt = _money(amount)
    except Exception:
        amt = Decimal('0')
    if amt <= 0:
        raise ValueError('bad_amount')
    qty = max(1, int(qty or 1))
    unit_price = (amt / Decimal(qty)).quantize(Decimal('0.01'))
    line_name = (title or '').strip() or (
        f'Предварительная оплата (аванс) за посадочный материал по заказу №{order.id}'
    )
    return create_custom_sale_invoice_from_order(
        order,
        company_id,
        user_id,
        lines=[{
            'name': line_name,
            'qty': qty,
            'unit': 'усл. ед.',
            'price': unit_price,
        }],
        comment=f'Аванс · заказ №{order.id}',
    )


def _sale_chat_ref(inv: SaleInvoice, order: Order | None = None) -> str:
    oid = order.id if order is not None else inv.order_id
    num = sale_public_number(inv)
    if oid:
        return f'счёт №{num} / Заказ №{oid}'
    return f'счёт №{num}'


def _discard_orders_text(inv: SaleInvoice, order: Order | None = None) -> str:
    ref = _sale_chat_ref(inv, order)
    lines = [f'❌ {ref[0].upper() + ref[1:]} удалён']
    if order:
        lines.append(f'Заказ #{order.id} снят с резерва и скрыт в ERP')
    return '\n'.join(lines)


def _approved_orders_text(inv: SaleInvoice, order: Order | None = None) -> str:
    lines = list(inv.lines or [])
    npos = len(lines)
    pos_word = 'позиция' if npos == 1 else 'поз.'
    buyer = html_escape((inv.buyer_name or 'Без клиента').strip())
    if inv.anonymous:
        buyer = 'обезличенный (без плательщика)'
    shown = lines[:25]
    cards = _shop_cards(ln.plant_id for ln in shown)
    items = []
    for ln in shown:
        plant = html_escape(ln.plant_name or 'Растение')
        size = html_escape(ln.size_name or '')
        attrs = html_escape(_shop_attrs(cards.get(ln.plant_id), ln.size_name or ''))
        qty = int(ln.qty or 0)
        price = _fmt_money_ru(ln.price)
        total = _fmt_money_ru(Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0)))
        head = f'{plant} · {size}' if size else plant
        if attrs:
            head = f'{head} {attrs}'
        items.append(
            f'• {head}\n'
            f'<b>{qty} шт</b> по цене <b>{price}</b>\n'
            f'{total}'
        )
    extra = npos - len(shown)
    if extra > 0:
        items.append(f'• … и ещё {extra} {pos_word}')
    body = '\n'.join(items) if items else '• нет позиций'
    oid = order.id if order is not None else inv.order_id
    num = sale_public_number(inv)
    if oid:
        title = f'✅ <b>Новый заказ №{oid}</b> / счёт №{num}'
    else:
        title = f'✅ <b>Новый счёт №{num}</b>'
    text = '\n'.join([
        title,
        '',
        f'👤 {buyer}',
        f'💰 ИТОГО: {_fmt_money_ru(inv.amount)} · {npos} {pos_word}',
        '',
        f'📦 Позиции:',
        body,
    ])
    return text[:3500]


def _order_is_paid_and_shipped(order: Order | None) -> bool:
    """Как в ERP-списке заказов: полностью оплачен и всё отгружено → «неактивен»."""
    if not order:
        return False
    if getattr(order, 'is_deleted', False):
        return False
    if (order.status or '') in ('canceled', 'ghost'):
        return False
    items = list(order.items or [])
    if not items:
        return False
    if order.payment_status != 'paid':
        return False
    return all(
        int(it.shipped_quantity or 0) >= int(it.quantity or 0)
        for it in items
    )


def _invoice_is_erp_archived(inv: SaleInvoice) -> bool:
    """Счёт уходит в архив мини-приложения, если заказ ERP уже оплачен и отгружен."""
    if not inv.order_id:
        return False
    order = inv.order
    if order is None:
        order = Order.query.options(
            selectinload(Order.items),
            selectinload(Order.payments),
        ).get(inv.order_id)
    return _order_is_paid_and_shipped(order)


def _serialize_invoice(inv: SaleInvoice, *, detail: bool = False) -> dict:
    data = {
        'id': inv.id,
        'number': sale_public_number(inv),
        'doc_year': inv.doc_year,
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
        'order_id': inv.order_id,
        'origin': inv.origin or 'miniapp',
        'from_existing_order': bool(inv.from_existing_order),
        'anonymous': bool(inv.anonymous),
        'erp_archived': _invoice_is_erp_archived(inv),
    }
    if detail:
        free_map = _free_pairs()
        cards = _shop_cards(ln.plant_id for ln in (inv.lines or []))
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
                    'shop_attrs': _shop_attrs(cards.get(ln.plant_id), ln.size_name or ''),
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
        if inv.order_id:
            linked = inv.order or Order.query.get(inv.order_id)
            if linked:
                data['order'] = _serialize_order(linked, preview=False)
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

    # Явный выбор клиента из подсказки Mini App — главный источник истины.
    # Иначе при сохранении счёта, привязанного к заказу, клиент «откатывался»
    # к старому order.client_id через _link_existing_order.
    explicit_id = body.get('client_id')
    if explicit_id not in (None, '', 0, '0'):
        try:
            cid = int(explicit_id)
        except (TypeError, ValueError):
            cid = None
        if cid:
            found = Client.query.get(cid)
            if found:
                inv.client_id = found.id
                _fill_invoice_gaps_from_client(inv, found)
                return

    found = _resolve_client(inv.buyer_name, inn)
    if found:
        inv.client_id = found.id
        _fill_invoice_gaps_from_client(inv, found)


def sync_order_client_from_sale_invoice(inv: SaleInvoice | None) -> bool:
    """Смена клиента в Mini App-счёте → плательщик/клиент у связанного заказа.

    Если у заказа уже задан billing_client (двойной клиент) — обновляем
    только его, ключевого client_id не трогаем. Иначе — как раньше:
    пишем в order.client_id (менеджерский поток «заказ = плательщик»).
    """
    if not inv or not inv.order_id or not inv.client_id:
        return False
    order = inv.order or Order.query.get(inv.order_id)
    if not order:
        return False
    if order.billing_client_id:
        if int(order.billing_client_id or 0) == int(inv.client_id):
            return False
        order.billing_client_id = inv.client_id
        return True
    if int(order.client_id or 0) == int(inv.client_id):
        return False
    order.client_id = inv.client_id
    return True


def _apply_anonymous(inv: SaleInvoice, body: dict):
    if 'anonymous' in body:
        inv.anonymous = _as_bool(body.get('anonymous'))


def _sync_client(inv: SaleInvoice):
    """Привязать счёт к существующему клиенту ERP. Карточку не переименовываем."""
    name = (inv.buyer_name or '').strip()
    inn = _inn_digits(inv.buyer_inn)
    if inn:
        inv.buyer_inn = inn
    if not name and not inn:
        return
    client = _resolve_client(name, inn)
    if not client:
        client = Client(name=(name or inn)[:200])
        db.session.add(client)
        db.session.flush()
    _fill_empty_client_fields(client, inv)
    apply_client_to_invoice(inv, client)


def _buyer_from_client(client: Client | None) -> dict:
    if not client:
        return {}
    return {
        'name': client.name or '',
        'inn': client.inn or '',
        'kpp': client.kpp or '',
        'ogrn': client.ogrn or '',
        'address': client.address or '',
        'phone': client.phone or '',
        'bank': client.bank_name or '',
        'rs': client.rs or '',
        'bik': client.bik or '',
        'ks': client.ks or '',
    }


def _order_composition(order: Order) -> list[dict]:
    grouped: dict[tuple[int, int], dict] = {}
    for it in order.items or []:
        pid = int(it.plant_id or 0)
        sid = int(it.size_id or 0)
        if not pid or not sid:
            continue
        rec = grouped.setdefault((pid, sid), {
            'plant_id': pid,
            'size_id': sid,
            'plant_name': (it.plant.name if it.plant else '') or '',
            'size_name': (it.size.name if it.size else '') or '',
            'qty': 0,
            'sum': 0.0,
        })
        qty = int(it.quantity or 0)
        rec['qty'] += qty
        rec['sum'] += float(it.price or 0) * qty
    rows = []
    for rec in grouped.values():
        qty = rec['qty']
        rec['price'] = (rec['sum'] / qty) if qty else 0.0
        rows.append(rec)
    rows.sort(key=lambda r: ((r.get('plant_name') or '').lower(), r.get('size_name') or ''))
    return rows


def _serialize_order(order: Order, *, preview: bool = True) -> dict:
    lines = _order_composition(order)
    shown = lines[:4] if preview else lines
    more = max(0, len(lines) - len(shown))
    client = order.client
    payer = order.invoice_client
    return {
        'id': order.id,
        'status': order.status or '',
        'status_label': _ORDER_STATUS_LABEL.get(order.status or '', order.status or ''),
        'date': order.date.isoformat() if order.date else None,
        'amount': float(order.total_sum or 0),
        'client_id': order.client_id,
        'client_name': (client.name if client else '') or '',
        'client_inn': (client.inn if client else '') or '',
        'invoice_number': order.invoice_number or '',
        'items_count': len(lines),
        'more_count': more,
        'lines': shown if preview else lines,
        'buyer': _buyer_from_client(payer),
    }


def _order_query():
    return (
        Order.query
        .options(
            joinedload(Order.client),
            selectinload(Order.items).options(
                joinedload(OrderItem.plant),
                joinedload(OrderItem.size),
            ),
        )
    )


def _link_existing_order(inv: SaleInvoice, order_id) -> tuple[Order | None, str | None]:
    if order_id in (None, '', 0, '0'):
        inv.order_id = None
        inv.from_existing_order = False
        return None, None
    try:
        oid = int(order_id)
    except (TypeError, ValueError):
        return None, 'bad_order'
    order = _order_query().filter(Order.id == oid).first()
    if not order or order.is_deleted:
        return None, 'order_missing'
    if (order.status or '') in ('canceled', 'ghost'):
        return None, 'order_closed'
    prev_oid = inv.order_id
    first_link = prev_oid != order.id
    inv.order_id = order.id
    inv.from_existing_order = True
    if not (inv.comment or '').strip():
        inv.comment = f'Заказ №{order.id}'
    # Реквизиты заказа подставляем только при первой привязке / смене заказа.
    # При обычном «Сохранить» клиент, выбранный в Mini App, не должен
    # затираться старым order.client_id.
    if first_link:
        payer = order.invoice_client
        payer_id = order.invoice_client_id
        inv.client_id = payer_id
        buyer = _buyer_from_client(payer)
        if buyer.get('name'):
            _apply_buyer(inv, {
                'client_id': payer_id,
                'buyer_name': buyer.get('name'),
                'buyer_inn': buyer.get('inn'),
                'buyer_kpp': buyer.get('kpp'),
                'buyer_ogrn': buyer.get('ogrn'),
                'buyer_address': buyer.get('address'),
                'buyer_phone': buyer.get('phone'),
                'buyer_bank': buyer.get('bank'),
                'buyer_rs': buyer.get('rs'),
                'buyer_bik': buyer.get('bik'),
                'buyer_ks': buyer.get('ks'),
            })
    elif not inv.client_id and order.invoice_client_id:
        inv.client_id = order.invoice_client_id
    return order, None


def _order_link_hint(err: str) -> str:
    return {
        'bad_order': 'Некорректный номер заказа',
        'order_missing': 'Заказ не найден',
        'order_closed': 'Этот заказ отменён',
    }.get(err, 'Не удалось привязать заказ')


def _allocate_sale_qty(plant_id: int, size_id: int, qty: int) -> list[tuple[int | None, int | None, int]]:
    """Разложить штуки по полям со свободным остатком; хвост — без поля."""
    from app.stock_helpers import compute_free
    remaining = int(qty or 0)
    if remaining <= 0:
        return []
    lots = []
    rows = StockBalance.query.filter_by(plant_id=plant_id, size_id=size_id).all()
    for sb in rows:
        if not sb.field_id:
            continue
        year = sb.year or msk_now().year
        _fact, _res, free = compute_free(plant_id, size_id, sb.field_id, year)
        if free > 0:
            lots.append((int(free), int(sb.field_id), int(year)))
    lots.sort(key=lambda x: -x[0])
    chunks = []
    for free, fid, year in lots:
        if remaining <= 0:
            break
        take = min(free, remaining)
        chunks.append((fid, year, take))
        remaining -= take
    if remaining > 0:
        chunks.append((None, None, remaining))
    return chunks


def create_order_from_sale_invoice(
    inv: SaleInvoice,
    user_id: int | None,
    *,
    allocate_fields: bool = True,
) -> Order | None:
    """Создаёт заказ ERP из согласованного счёта.

    allocate_fields=True (Mini App): раскладывает по полям со свободным остатком.
    allocate_fields=False (ERP): позиции без field_id/year — поле копки заполняет пользователь.
    """
    if inv.order_id:
        order = Order.query.get(inv.order_id)
        if order and inv.from_existing_order and not (order.invoice_number or '').strip():
            order.invoice_number = f'ТГ-{inv.id}'
            order.invoice_date = (inv.approved_at or msk_now()).date()
        return order
    if not inv.client_id:
        _sync_client(inv)
    if not inv.client_id:
        return None
    usable = [ln for ln in (inv.lines or []) if ln.plant_id and ln.size_id and int(ln.qty or 0) > 0]
    if not usable:
        return None

    allocate_sale_doc_number(inv)
    order = Order(
        client_id=inv.client_id,
        date=inv.approved_at or msk_now(),
        status='reserved',
        invoice_number=str(sale_public_number(inv)),
        invoice_date=(inv.approved_at or msk_now()).date(),
        created_by_user_id=user_id or inv.user_id,
    )
    db.session.add(order)
    db.session.flush()
    inv.order_id = order.id
    inv.from_existing_order = False

    created = []
    for ln in usable:
        price = _money(ln.price)
        chunks = (
            _allocate_sale_qty(int(ln.plant_id), int(ln.size_id), int(ln.qty))
            if allocate_fields
            else [(None, None, int(ln.qty))]
        )
        for field_id, year, qty in chunks:
            oi = OrderItem(
                order_id=order.id,
                plant_id=int(ln.plant_id),
                size_id=int(ln.size_id),
                field_id=field_id,
                year=year,
                quantity=qty,
                price=price,
            )
            db.session.add(oi)
            db.session.flush()
            db.session.add(OrderItemHistory(
                order_id=order.id,
                order_item_id=oi.id,
                action_type='initial_item',
                before_quantity=0,
                after_quantity=qty,
                delta_quantity=qty,
                changed_by_user_id=user_id or inv.user_id,
                created_at=msk_now(),
            ))
            created.append(oi)

    if not created:
        inv.order_id = None
        db.session.delete(order)
        db.session.flush()
        return None

    if allocate_fields and not order.project_id:
        from app.finance import resolve_project_id_for_yard_fields
        linked = resolve_project_id_for_yard_fields([it.field_id for it in created if it.field_id])
        if linked:
            order.project_id = linked

    return order


def backfill_approved_sale_orders() -> int:
    rows = (
        SaleInvoice.query
        .filter(SaleInvoice.status == 'approved', SaleInvoice.order_id.is_(None))
        .order_by(SaleInvoice.id.asc())
        .all()
    )
    n = 0
    for inv in rows:
        try:
            order = create_order_from_sale_invoice(inv, inv.user_id)
            if order:
                db.session.commit()
                n += 1
            else:
                db.session.rollback()
        except Exception:
            db.session.rollback()
            current_app.logger.exception('sale invoice %s -> order backfill failed', inv.id)
    return n


def _free_pairs(exclude_invoice_id: int | None = None) -> dict[tuple[int, int], int]:
    """Свободно по (plant, size): склад − резерв с полем − резерв без поля − чужие открытые счета."""
    from app.stock_helpers import _active_order_filter

    rmap = get_reserved_map()
    reserved: dict[tuple[int, int], int] = {}
    for (pid, sid, _f, _y), qty in rmap.items():
        reserved[(pid, sid)] = reserved.get((pid, sid), 0) + int(qty or 0)

    # Позиции заказов ещё без поля копки — тоже держат остаток.
    bare = (
        db.session.query(
            OrderItem.plant_id,
            OrderItem.size_id,
            func.coalesce(func.sum(OrderItem.quantity - OrderItem.shipped_quantity), 0),
        )
        .join(Order)
        .filter(*_active_order_filter(), OrderItem.field_id.is_(None))
        .group_by(OrderItem.plant_id, OrderItem.size_id)
        .all()
    )
    for pid, sid, qty in bare:
        if not pid or not sid:
            continue
        reserved[(int(pid), int(sid))] = reserved.get((int(pid), int(sid)), 0) + int(qty or 0)

    # Черновики/согласованные счета без заказа — оперативный холд при наборе позиций.
    inv_q = (
        db.session.query(
            SaleInvoiceLine.plant_id,
            SaleInvoiceLine.size_id,
            func.coalesce(func.sum(SaleInvoiceLine.qty), 0),
        )
        .join(SaleInvoice)
        .filter(
            SaleInvoice.status.in_(('draft', 'approved')),
            SaleInvoice.order_id.is_(None),
            SaleInvoiceLine.plant_id.isnot(None),
            SaleInvoiceLine.size_id.isnot(None),
        )
    )
    if exclude_invoice_id:
        inv_q = inv_q.filter(SaleInvoice.id != int(exclude_invoice_id))
    for pid, sid, qty in inv_q.group_by(SaleInvoiceLine.plant_id, SaleInvoiceLine.size_id).all():
        reserved[(int(pid), int(sid))] = reserved.get((int(pid), int(sid)), 0) + int(qty or 0)

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
        free = int(qty or 0) - reserved.get((int(pid), int(sid)), 0)
        if free > 0:
            out[(int(pid), int(sid))] = free
    return out


def stock_catalog_for_sale(q: str = '', *, exclude_invoice_id: int | None = None, limit: int = 40) -> list[dict]:
    """Поиск позиций для UI ERP: название, размер, свободно, опт/розница."""
    from app.shop_catalog import _price_history_map
    from app.seedlings import is_excluded_from_product_stock
    from app.shop_prices import get_shop_price_map, resolve_shop_price

    free_map = _free_pairs(exclude_invoice_id=exclude_invoice_id)
    if not free_map:
        return []
    prices = _price_history_map()
    overrides = get_shop_price_map()
    plant_ids = {pid for pid, _ in free_map}
    size_ids = {sid for _, sid in free_map}
    plants = {p.id: p for p in Plant.query.filter(Plant.id.in_(plant_ids)).all()}
    sizes = {s.id: s for s in Size.query.filter(Size.id.in_(size_ids)).all()}
    needle = (q or '').strip().lower()
    rows = []
    for (pid, sid), free in free_map.items():
        plant = plants.get(pid)
        size = sizes.get(sid)
        if not plant or not size:
            continue
        if is_excluded_from_product_stock(size.name or ''):
            continue
        pname = plant.name or ''
        sname = size.name or ''
        if needle and needle not in pname.lower() and needle not in sname.lower():
            continue
        wholesale = float(prices.get((pid, sid)) or 0)
        retail = float(resolve_shop_price(pid, sid, wholesale, overrides))
        rows.append({
            'plant_id': pid,
            'size_id': sid,
            'plant_name': pname,
            'size_name': sname,
            'free': int(free),
            'wholesale': wholesale,
            'retail': retail,
            'wholesale_price': wholesale,
            'retail_price': retail,
            'price': retail,
            'label': f'{pname} · {sname} · свободно {int(free)}',
        })
    rows.sort(key=lambda r: (r['plant_name'].lower(), size_natural_key(r['size_name'])))
    return rows[: max(1, int(limit or 40))]


def _replace_lines(inv: SaleInvoice, rows: list):
    inv.lines.clear()
    db.session.flush()
    free_map = _free_pairs(exclude_invoice_id=inv.id)
    clamp_free = not bool(inv.from_existing_order)
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
        if pid and sid and clamp_free:
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


def _logo_uri() -> str:
    path = Path(current_app.root_path) / 'static' / 'tg_sale' / 'mark.png'
    if path.is_file():
        return path.resolve().as_uri()
    return ''


def _blob_temp_uri(data: bytes, filename: str) -> tuple[str, str]:
    ext = Path(filename or 'stamp.png').suffix.lower()
    if ext not in ('.png', '.jpg', '.jpeg', '.gif'):
        ext = '.png'
    fd, path = tempfile.mkstemp(suffix=ext)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
    return Path(path).resolve().as_uri(), path


_STAMP_MAX = 2 * 1024 * 1024
_STAMP_EXT = {'.png', '.jpg', '.jpeg', '.webp'}


def _normalize_stamp(data: bytes, filename: str) -> tuple[bytes, str]:
    from io import BytesIO
    from PIL import Image
    im = Image.open(BytesIO(data))
    im = im.convert('RGBA')
    max_side = 1400
    if max(im.size) > max_side:
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    buf = BytesIO()
    im.save(buf, format='PNG', optimize=True)
    return buf.getvalue(), 'stamp.png'


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


DEFAULT_INVOICE_FOOTER = (
    'Оплата данного счета означает согласие с условиями поставки товара.\n'
    'Уведомление об оплате обязательно, в противном случае не гарантируется наличие товара на складе.\n'
    'Товар отпускается по факту прихода денег на р/с Поставщика, самовывозом, при наличии доверенности и паспорта.'
)


def _parse_print_lines(raw_lines) -> list[dict]:
    """Свободные строки PDF: без привязки к справочнику растений."""
    out = []
    for i, ln in enumerate(raw_lines or [], 1):
        if not isinstance(ln, dict):
            continue
        name = (ln.get('name') or '').strip()
        if not name:
            continue
        try:
            qty = Decimal(str(ln.get('qty') or 0))
        except Exception:
            qty = Decimal('0')
        try:
            price = Decimal(str(ln.get('price') or 0))
        except Exception:
            price = Decimal('0')
        unit = (ln.get('unit') or 'шт').strip() or 'шт'
        sm = (qty * price).quantize(Decimal('0.01'))
        out.append({
            'n': i,
            'name': name,
            'qty': _fmt_qty(qty),
            'unit': unit,
            'price': _fmt_money(price),
            'sum': _fmt_money(sm),
            'qty_raw': qty,
            'price_raw': price,
            'sum_raw': sm,
        })
    # перенумеровать после фильтра пустых
    for i, row in enumerate(out, 1):
        row['n'] = i
    return out


def _fmt_qty(value) -> str:
    q = Decimal(str(value or 0))
    if q == q.to_integral_value():
        return str(int(q))
    return f'{q.normalize()}'


def render_sale_pdf(inv: SaleInvoice, overrides: dict | None = None) -> bytes | None:
    """PDF счёта. overrides — только для печати из ERP, в БД не пишется."""
    company = inv.company
    if not company:
        return None
    ov = overrides if isinstance(overrides, dict) else {}

    vat_mode = _vat_mode_norm(ov.get('vat_mode') or company.vat_mode)
    doc_date = ov.get('doc_date') or inv.created_at or msk_now()
    if isinstance(doc_date, str):
        try:
            doc_date = datetime.strptime(doc_date[:10], '%Y-%m-%d')
        except ValueError:
            doc_date = inv.created_at or msk_now()

    cards = _shop_cards(ln.plant_id for ln in (inv.lines or []))
    default_lines = []
    for i, ln in enumerate(inv.lines or [], 1):
        sm = Decimal(str(ln.qty or 0)) * Decimal(str(ln.price or 0))
        attrs = _shop_attrs(cards.get(ln.plant_id), ln.size_name or '')
        title = _goods_title(ln.plant_name, ln.size_name, attrs)
        default_lines.append({
            'n': i,
            'name': title,
            'qty': _fmt_qty(ln.qty),
            'unit': 'шт',
            'price': _fmt_money(ln.price),
            'sum': _fmt_money(sm),
            'qty_raw': Decimal(str(ln.qty or 0)),
            'price_raw': Decimal(str(ln.price or 0)),
            'sum_raw': sm,
        })

    if 'lines' in ov:
        pdf_lines = _parse_print_lines(ov.get('lines'))
    else:
        pdf_lines = default_lines

    if ov.get('amount') is not None:
        try:
            amount = Decimal(str(ov.get('amount'))).quantize(Decimal('0.01'))
        except Exception:
            amount = sum((ln.get('sum_raw') or Decimal('0')) for ln in pdf_lines)
    else:
        amount = sum((ln.get('sum_raw') or Decimal('0')) for ln in pdf_lines)
        if not pdf_lines:
            amount = Decimal(str(inv.amount or 0))

    vat = _vat_amount(amount, vat_mode)
    # Всегда выдаём публичный № (100+) перед PDF — иначе в шаблон уходит inv.id.
    ov_num = ov.get('doc_number')
    if ov_num not in (None, ''):
        inv_no = ov_num
    else:
        if getattr(inv, 'id', None) and hasattr(inv, 'doc_number') and not inv.doc_number:
            try:
                allocate_sale_doc_number(inv)
            except Exception:
                current_app.logger.exception('allocate_sale_doc_number failed inv=%s', getattr(inv, 'id', None))
        inv_no = getattr(inv, 'doc_number', None) or getattr(inv, 'id', None)
    purpose = (ov.get('purpose') or '').strip() or (
        f'Оплата по счету N {inv_no} от {doc_date.strftime("%d.%m.%Y")}'
    )
    default_pay = (doc_date + timedelta(days=3)).strftime('%d.%m.%Y')
    pay_until = (ov.get('pay_until') or '').strip() or default_pay
    pay_until_line = (ov.get('pay_until_line') or '').strip() or f'Оплатить не позднее {pay_until}'
    basis = (ov.get('basis') if 'basis' in ov else None)
    if basis is None:
        basis = (inv.comment or '').strip() or 'Без договора'
    else:
        basis = (basis or '').strip() or 'Без договора'
    footer_text = ov.get('footer_text')
    if footer_text is None:
        footer_text = DEFAULT_INVOICE_FOOTER
    footer_text = (footer_text or '').strip()
    amount_words = (ov.get('amount_words') or '').strip() or rubles_in_words(amount)
    anonymous = bool(ov['anonymous']) if 'anonymous' in ov else bool(inv.anonymous)
    buyer_line = ov.get('buyer_line')
    if buyer_line is None:
        buyer_line = '' if anonymous else _buyer_line(inv)
    else:
        buyer_line = (buyer_line or '').strip()
    vat_note = (ov.get('vat_note') or '').strip()
    if not vat_note:
        vat_note = 'в т.ч. НДС 22%:' if _vat_included(vat_mode) else 'Без налога (НДС)'

    qr_uri, qr_path = '', ''
    stamp_uri, stamp_path = '', ''
    try:
        payload = _qr_payload(company, amount, purpose) if company else ''
        if payload:
            qr_uri, qr_path = _qr_temp_png(payload)
        elif company:
            current_app.logger.warning(
                'sale qr skipped: incomplete company requisites id=%s', company.id
            )
        if company and company.stamp_blob:
            stamp_uri, stamp_path = _blob_temp_uri(
                bytes(company.stamp_blob), company.stamp_name or 'stamp.png'
            )
        html = render_template(
            'tg_sale/invoice_pdf.html',
            inv=inv,
            inv_no=inv_no,
            company=company,
            lines=pdf_lines,
            amount=amount,
            amount_fmt=_fmt_money(amount),
            amount_words=amount_words,
            vat=vat,
            vat_fmt=_fmt_money(vat),
            vat_included=_vat_included(vat_mode),
            vat_mode=vat_mode,
            vat_note=vat_note,
            purpose=purpose,
            date_long=_date_long(doc_date),
            pay_until=pay_until,
            pay_until_line=pay_until_line,
            supplier_line=_supplier_line(company),
            buyer_line=buyer_line,
            anonymous=anonymous,
            basis=basis,
            footer_text=footer_text,
            footer_paragraphs=[p.strip() for p in footer_text.split('\n') if p.strip()],
            sign_line=_sign_line(company),
            logo_uri=_logo_uri(),
            qr_uri=qr_uri,
            stamp_uri=stamp_uri,
            doc_date=doc_date,
        )
        return build_pdf_bytes(html, page_margin='10mm')
    finally:
        for p in (qr_path, stamp_path):
            if p:
                try:
                    os.remove(p)
                except OSError:
                    pass


def render_sale_pdf_custom(
    company: SaleCompany,
    *,
    lines: list[dict],
    basis: str = '',
    buyer_line: str = '',
    anonymous: bool = False,
    pay_until: str = '',
    pay_until_line: str = '',
    footer_text: str | None = None,
    amount_words: str = '',
    vat_note: str = '',
    doc_number: str | int = '',
    doc_date=None,
    purpose: str = '',
    vat_mode: str | None = None,
) -> bytes | None:
    """PDF без SaleInvoice в БД — только печать произвольного счёта."""
    if not company:
        return None
    from types import SimpleNamespace
    tmp = SimpleNamespace(
        id=doc_number or '—',
        doc_number=None,
        created_at=doc_date or msk_now(),
        anonymous=bool(anonymous),
        comment=basis or '',
        company=company,
        lines=[],
        amount=0,
        buyer_name='',
        buyer_inn='',
        buyer_kpp='',
        buyer_address='',
    )
    return render_sale_pdf(tmp, {
        'lines': lines,
        'basis': basis,
        'buyer_line': buyer_line,
        'anonymous': anonymous,
        'pay_until': pay_until,
        'pay_until_line': pay_until_line,
        'footer_text': DEFAULT_INVOICE_FOOTER if footer_text is None else footer_text,
        'amount_words': amount_words,
        'vat_note': vat_note,
        'doc_number': doc_number or 'б/н',
        'doc_date': doc_date,
        'purpose': purpose,
        'vat_mode': vat_mode,
    })


def _store_pdf(inv: SaleInvoice) -> bytes | None:
    allocate_sale_doc_number(inv)
    blob = render_sale_pdf(inv)
    if not blob:
        return None
    inv.file_blob = blob
    inv.file_name = f'schet_{sale_public_number(inv)}.pdf'
    return blob


# ---------------------------------------------------------------------------
# Pages / API
# ---------------------------------------------------------------------------

@bp.route('')
@bp.route('/')
def index():
    html = render_template('tg_sale/index.html')
    resp = make_response(html)
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    as_role = request.args.get('as')
    if as_role in ('admin', 'shop_manager', 'accountant'):
        from app.tg_pay import _dev_mode
        if _dev_mode():
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
    if not _can_sale(user) and not _is_accountant(user):
        return jsonify({'error': 'forbidden', 'hint': 'Нет доступа к счетам'}), 403
    session_tg = current_telegram_id()
    resp = jsonify(_sale_me_payload(user, is_dev=is_dev))
    return set_mini_cookie(resp, user, session_tg)


@bp.route('/api/me')
def api_me():
    user, is_dev, pending = resolve_user()
    if not user:
        if pending:
            return jsonify({
                'error': 'not_linked',
                'telegram_id': pending.get('id'),
                'username': (pending.get('username') or ''),
            }), 403
        return jsonify({'error': 'unauthorized', 'hint': _auth_fail_hint()}), 401
    if not _can_sale(user) and not _is_accountant(user):
        return jsonify({'error': 'forbidden'}), 403
    return jsonify(_sale_me_payload(user, is_dev=is_dev))


def _buh_shipped_order_ids():
    return (
        db.session.query(Document.order_id)
        .filter(Document.doc_type == 'shipment', Document.order_id.isnot(None))
        .distinct()
    )


def _fmt_doc_date(value) -> str:
    if not value:
        return ''
    try:
        return value.strftime('%d.%m.%Y')
    except Exception:
        return str(value)[:10]


def _shipment_map(order_id: int) -> dict[tuple, list[dict]]:
    """(plant_id, size_id) → [{date, qty}] по документам отгрузки заказа."""
    docs = (
        Document.query.options(joinedload(Document.rows))
        .filter(Document.doc_type == 'shipment', Document.order_id == order_id)
        .order_by(Document.date, Document.id)
        .all()
    )
    grouped: dict[tuple, list[dict]] = {}
    for doc in docs:
        label = _fmt_doc_date(doc.date)
        for row in doc.rows or []:
            key = (row.plant_id, row.size_id)
            grouped.setdefault(key, []).append({
                'date': label,
                'qty': int(row.quantity or 0),
            })
    return grouped


def _order_line_payload(it: OrderItem, ships: list[dict]) -> dict:
    qty = int(it.quantity or 0)
    price = float(it.price or 0)
    return {
        'plant_name': it.plant.name if it.plant else '',
        'size_name': it.size.name if it.size else '',
        'qty': qty,
        'price': price,
        'sum': round(price * qty, 2),
        'shipped_qty': int(it.shipped_quantity or 0),
        'shipments': ships,
    }


def _buh_order_is_shipped(order: Order) -> bool:
    if Document.query.filter(Document.doc_type == 'shipment', Document.order_id == order.id).first():
        return True
    return any(int(it.shipped_quantity or 0) > 0 for it in (order.items or []))


def _buh_shipment_journal(order_id: int) -> list[dict]:
    docs = (
        Document.query.options(
            joinedload(Document.rows).joinedload(DocumentRow.plant),
            joinedload(Document.rows).joinedload(DocumentRow.size),
        )
        .filter(Document.doc_type == 'shipment', Document.order_id == order_id)
        .order_by(Document.date, Document.id)
        .all()
    )
    journal = []
    for doc in docs:
        rows = []
        for row in doc.rows or []:
            rows.append({
                'plant_name': row.plant.name if row.plant else '',
                'size_name': row.size.name if row.size else '',
                'qty': int(row.quantity or 0),
            })
        journal.append({
            'date': _fmt_doc_date(doc.date),
            'qty': sum(r['qty'] for r in rows),
            'rows': rows,
        })
    return journal


def _buh_invoice_brief(inv: SaleInvoice) -> dict:
    order = inv.order
    return {
        'id': inv.id,
        'number': sale_public_number(inv),
        'kind': inv.kind or 'goods',
        'buyer_name': inv.buyer_name or (
            order.invoice_client.name if order and order.invoice_client else ''
        ),
        'company_name': inv.company.short_name if inv.company else '',
        'amount': float(inv.amount or 0),
        'created_at': inv.created_at.isoformat() if inv.created_at else None,
        'order_id': order.id if order else None,
    }


def _buh_order_invoices(order_id: int) -> list[SaleInvoice]:
    return (
        SaleInvoice.query
        .options(joinedload(SaleInvoice.company))
        .filter(
            SaleInvoice.order_id == order_id,
            SaleInvoice.status != 'discarded',
        )
        .order_by(SaleInvoice.id.desc())
        .all()
    )


def _buh_order_list_card(order: Order) -> dict:
    items = list(order.items or [])
    preview = []
    for it in items[:2]:
        preview.append({
            'plant_name': it.plant.name if it.plant else '',
            'size_name': it.size.name if it.size else '',
            'qty': int(it.quantity or 0),
            'shipped_qty': int(it.shipped_quantity or 0),
        })
    inv_count = (
        SaleInvoice.query
        .filter(SaleInvoice.order_id == order.id, SaleInvoice.status != 'discarded')
        .count()
    )
    return {
        'order_id': order.id,
        'client_name': order.client.name if order.client else '',
        'order_sum': float(order.total_sum or 0),
        'order_status': order.status or '',
        'invoice_count': inv_count,
        'lines': preview,
        'more_count': max(0, len(items) - len(preview)),
        'posted': bool(order.buh_posted_at),
        'posted_at': order.buh_posted_at.isoformat() if order.buh_posted_at else None,
        'order_date': order.date.isoformat() if order.date else None,
    }


def _buh_order_detail_payload(order: Order) -> dict:
    ships = _shipment_map(order.id)
    order_lines = [
        _order_line_payload(it, ships.get((it.plant_id, it.size_id), []))
        for it in (order.items or [])
    ]
    invoices = [_buh_invoice_brief(inv) for inv in _buh_order_invoices(order.id)]
    return {
        'order_id': order.id,
        'client_name': order.client.name if order.client else '',
        'order_sum': float(order.total_sum or 0),
        'order_status': order.status or '',
        'invoice_count': len(invoices),
        'invoices': invoices,
        'order_lines': order_lines,
        'shipments': _buh_shipment_journal(order.id),
        'posted': bool(order.buh_posted_at),
        'posted_at': order.buh_posted_at.isoformat() if order.buh_posted_at else None,
    }


def _buh_load_shipped_order(order_id: int) -> Order | None:
    order = (
        Order.query
        .options(
            joinedload(Order.client),
            selectinload(Order.items).joinedload(OrderItem.plant),
            selectinload(Order.items).joinedload(OrderItem.size),
        )
        .filter(Order.id == order_id, Order.is_deleted.is_(False))
        .first()
    )
    if not order or getattr(order, 'buh_exclude', False):
        return None
    if not _buh_order_is_shipped(order):
        return None
    return order


def _buh_allowed_invoice(inv_id: int) -> SaleInvoice | None:
    """Счёт для PDF бухгалтера: привязан к отгруженному заказу."""
    inv = (
        SaleInvoice.query
        .options(joinedload(SaleInvoice.order).selectinload(Order.items))
        .filter(SaleInvoice.id == inv_id, SaleInvoice.status != 'discarded')
        .first()
    )
    if not inv or not inv.order_id or not inv.order or inv.order.is_deleted:
        return None
    if getattr(inv.order, 'buh_exclude', False):
        return None
    if not _buh_order_is_shipped(inv.order):
        return None
    return inv


def _buh_orders_query(scope: str = 'active'):
    """Базовый запрос отгруженных заказов для экрана бухгалтера."""
    q = (
        Order.query
        .options(
            joinedload(Order.client),
            selectinload(Order.items).joinedload(OrderItem.plant),
            selectinload(Order.items).joinedload(OrderItem.size),
        )
        .filter(
            Order.is_deleted.is_(False),
            Order.id.in_(_buh_shipped_order_ids()),
            or_(Order.buh_exclude.is_(False), Order.buh_exclude.is_(None)),
        )
    )
    if scope == 'archive':
        q = q.filter(Order.buh_posted_at.isnot(None))
    else:
        q = q.filter(Order.buh_posted_at.is_(None))
    return q


@bp.route('/api/buh/orders')
@require_buh
def api_buh_orders(_user: User):
    """Заказы с отгрузкой — активные или архив проведённых."""
    scope = (request.args.get('scope') or 'active').strip().lower()
    if scope not in ('active', 'archive'):
        scope = 'active'
    q_text = (request.args.get('q') or '').strip()
    sort = (request.args.get('sort') or 'date').strip().lower()
    direction = (request.args.get('dir') or 'desc').strip().lower()
    if direction not in ('asc', 'desc'):
        direction = 'desc'

    q = _buh_orders_query(scope)
    if q_text:
        like = f'%{q_text}%'
        id_filters = [
            Client.name.ilike(like),
            Order.invoice_number.ilike(like),
            cast(Order.id, String).ilike(like),
        ]
        if q_text.isdigit():
            id_filters.append(Order.id == int(q_text))
        q = q.outerjoin(Client, Order.client_id == Client.id).filter(or_(*id_filters))

    orders = q.all()
    reverse = direction == 'desc'
    if sort == 'client':
        orders.sort(key=lambda o: ((o.client.name if o.client else '') or '').lower(), reverse=reverse)
    elif sort == 'sum':
        orders.sort(key=lambda o: float(o.total_sum or 0), reverse=reverse)
    elif sort == 'posted' and scope == 'archive':
        orders.sort(key=lambda o: o.buh_posted_at or datetime.min, reverse=reverse)
    else:
        if scope == 'archive' and sort == 'date':
            orders.sort(key=lambda o: o.buh_posted_at or o.date or datetime.min, reverse=reverse)
        else:
            orders.sort(key=lambda o: o.date or datetime.min, reverse=reverse)

    seen = set()
    items = []
    for order in orders:
        if order.id in seen:
            continue
        seen.add(order.id)
        items.append(_buh_order_list_card(order))
    return jsonify({'orders': items, 'scope': scope, 'count': len(items)})


@bp.route('/api/buh/orders/<int:order_id>')
@require_buh
def api_buh_order(_user: User, order_id: int):
    order = _buh_load_shipped_order(order_id)
    if not order:
        return jsonify({'error': 'not_found'}), 404
    return jsonify(_buh_order_detail_payload(order))


@bp.route('/api/buh/orders/<int:order_id>/post', methods=['POST'])
@require_buh
def api_buh_order_post(user: User, order_id: int):
    """Пометить отгрузку проведённой бухгалтером → в архив."""
    order = _buh_load_shipped_order(order_id)
    if not order:
        return jsonify({'error': 'not_found'}), 404
    if order.buh_posted_at:
        return jsonify({'ok': True, 'posted': True, 'already': True})
    order.buh_posted_at = msk_now()
    order.buh_posted_by_id = user.id
    db.session.commit()
    return jsonify({'ok': True, 'posted': True})


@bp.route('/api/buh/orders/<int:order_id>/unpost', methods=['POST'])
@require_buh
def api_buh_order_unpost(_user: User, order_id: int):
    """Вернуть заказ из архива на главную бухгалтера."""
    order = _buh_load_shipped_order(order_id)
    if not order:
        return jsonify({'error': 'not_found'}), 404
    order.buh_posted_at = None
    order.buh_posted_by_id = None
    db.session.commit()
    return jsonify({'ok': True, 'posted': False})


@bp.route('/api/buh/invoices/<int:inv_id>/pdf')
@require_buh
def api_buh_invoice_pdf(_user: User, inv_id: int):
    inv = _buh_allowed_invoice(inv_id)
    if not inv:
        return jsonify({'error': 'not_found'}), 404
    blob = _store_pdf(inv)
    db.session.commit()
    if not blob:
        return jsonify({'error': 'pdf_failed'}), 500
    resp = make_response(bytes(blob))
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = f'inline; filename=schet_{sale_public_number(inv)}.pdf'
    return resp


@bp.route('/api/buh/invoices/<int:inv_id>/send-pdf', methods=['POST'])
@require_buh
def api_buh_invoice_send_pdf(user: User, inv_id: int):
    inv = _buh_allowed_invoice(inv_id)
    if not inv:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    blob = _store_pdf(inv)
    db.session.commit()
    if not blob:
        return jsonify({'ok': False, 'error': 'pdf_failed'}), 500
    chat_id = current_telegram_id()
    if not chat_id:
        current_app.logger.warning('buh send-pdf no telegram_id inv=%s user=%s', inv.id, user.username)
        return jsonify({'ok': False, 'error': 'no_telegram_id'})
    caption = (
        f'Счёт №{sale_public_number(inv)} · заказ №{inv.order_id} · {inv.amount} ₽'
    )
    ok, err = send_chat_document(
        chat_id,
        filename=inv.file_name or f'schet_{sale_public_number(inv)}.pdf',
        caption=caption,
        file_bytes=bytes(blob),
    )
    if not ok:
        current_app.logger.warning('buh send-pdf failed inv=%s chat=%s err=%s', inv.id, chat_id, err)
    return jsonify({'ok': bool(ok), 'error': err if not ok else None})


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


@bp.route('/api/companies/<int:cid>/stamp', methods=['POST', 'DELETE'])
@require_sale
def api_company_stamp(user: User, cid: int):
    if not _can_firms(user):
        return jsonify({'error': 'forbidden'}), 403
    c = SaleCompany.query.get_or_404(cid)
    if request.method == 'DELETE':
        c.stamp_blob = None
        c.stamp_name = None
        db.session.commit()
        return jsonify(_serialize_company(c))
    file = request.files.get('file')
    if not file or not file.filename:
        return jsonify({'error': 'Выберите PNG 800–1200 px, печать и подпись, до 2 МБ'}), 400
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in _STAMP_EXT:
        return jsonify({'error': 'Нужен PNG, JPG или WebP'}), 400
    data = file.read()
    if not data:
        return jsonify({'error': 'Файл пустой'}), 400
    if len(data) > _STAMP_MAX:
        return jsonify({'error': 'Файл больше 2 МБ — уменьшите изображение'}), 400
    try:
        blob, name = _normalize_stamp(data, file.filename)
    except Exception:
        current_app.logger.exception('sale company stamp')
        return jsonify({'error': 'Не удалось прочитать картинку'}), 400
    c.stamp_blob = blob
    c.stamp_name = name
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
    from app.shop_prices import get_shop_price_map, resolve_shop_price
    overrides = get_shop_price_map()
    pairs = _free_pairs()
    cards = {c.plant_id: c for c in ShopPlantCard.query.all()}
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
        wholesale = float(prices.get((pid, sid)) or 0)
        retail = float(resolve_shop_price(pid, sid, wholesale, overrides))
        items.append({
            'plant_id': pid,
            'size_id': sid,
            'plant_name': pname,
            'size_name': sname,
            'shop_attrs': _shop_attrs(cards.get(pid), sname),
            'free': free,
            'free_qty': free,
            'wholesale': wholesale,
            'retail': retail,
            'wholesale_price': wholesale,
            'retail_price': retail,
            'price': retail,
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


@bp.route('/api/clients', methods=['GET', 'POST'])
@require_sale
def api_clients(_user: User):
    if request.method == 'POST':
        payload = request.get_json(silent=True) or {}
        q = (payload.get('q') or request.form.get('q') or '').strip()
    else:
        q = (request.args.get('q') or '').strip()
    rows = _search_clients(q)
    return jsonify({'clients': [_client_buyer(c) for c in rows]})


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
    try:
        align_sale_invoices_with_orders(commit=True)
    except Exception:
        db.session.rollback()
        current_app.logger.exception('align sale invoices with orders')
    scope = (request.args.get('scope') or 'active').strip().lower()
    if scope not in ('active', 'archive'):
        scope = 'active'
    # Берём запас по id: часть уйдёт в архив (оплачен+отгружен в ERP).
    candidates = (
        SaleInvoice.query
        .options(
            joinedload(SaleInvoice.company),
            joinedload(SaleInvoice.order).selectinload(Order.items),
            joinedload(SaleInvoice.order).selectinload(Order.payments),
        )
        .filter(SaleInvoice.status != 'discarded')
        .order_by(SaleInvoice.id.desc())
        .limit(500)
        .all()
    )
    if scope == 'archive':
        rows = [r for r in candidates if _invoice_is_erp_archived(r)]
    else:
        rows = [r for r in candidates if not _invoice_is_erp_archived(r)]
    rows = rows[:80]
    return jsonify({'invoices': [_serialize_invoice(r) for r in rows], 'scope': scope})


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
        origin='miniapp',
    )
    _apply_buyer(inv, body)
    _apply_anonymous(inv, body)
    db.session.add(inv)
    db.session.flush()
    allocate_sale_doc_number(inv)
    if 'order_id' in body:
        order, err = _link_existing_order(inv, body.get('order_id'))
        if err:
            db.session.rollback()
            return jsonify({'error': err, 'hint': _order_link_hint(err)}), 400
        # После привязки заказа снова применяем buyer из запроса —
        # иначе клиент из формы теряется при create+link в одном запросе.
        if body.get('client_id') or body.get('buyer_name') or body.get('name'):
            _apply_buyer(inv, body)
    else:
        order = None
    rows = body.get('lines')
    if rows:
        _replace_lines(inv, rows)
    elif order:
        _replace_lines(inv, _order_composition(order))
    else:
        _replace_lines(inv, [])
    sync_order_client_from_sale_invoice(inv)
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
    if inv.status == 'discarded':
        return jsonify({'error': 'not_found'}), 404
    if inv.status not in ('draft', 'approved'):
        return jsonify({'error': 'locked'}), 400
    body = request.get_json(silent=True) or {}
    if body.get('company_id'):
        company = SaleCompany.query.get(int(body['company_id']))
        if company:
            inv.company_id = company.id
    if 'comment' in body:
        inv.comment = str(body.get('comment') or '')[:500]
    _apply_buyer(inv, body)
    _apply_anonymous(inv, body)
    if 'order_id' in body:
        _order, err = _link_existing_order(inv, body.get('order_id'))
        if err:
            return jsonify({'error': err, 'hint': _order_link_hint(err)}), 400
        # Повторно применяем клиента из формы: при уже привязанном заказе
        # _link_existing_order больше не трогает buyer, но на смене order_id
        # он мог подставить клиента заказа — явный выбор из формы важнее.
        if body.get('client_id') or body.get('buyer_name') or body.get('name'):
            _apply_buyer(inv, body)
    if 'lines' in body:
        _replace_lines(inv, body.get('lines') or [])
    elif inv.order_id and (inv.order or Order.query.get(inv.order_id)):
        order = inv.order or Order.query.get(inv.order_id)
        _replace_lines(inv, _order_composition(order))
    else:
        inv.amount = _line_sum(inv.lines)
    sync_order_client_from_sale_invoice(inv)
    if inv.status == 'approved':
        blob = _store_pdf(inv)
        if not blob:
            return jsonify({'error': 'pdf_failed'}), 500
    db.session.commit()
    return jsonify(_serialize_invoice(inv, detail=True))


def void_sale_invoice(inv: SaleInvoice) -> Order | None:
    """Убирает счёт. Заказ ERP снимаем, только если этот счёт его создал в боте."""
    order = Order.query.get(inv.order_id) if inv.order_id else None
    from_erp = (inv.origin or '') == 'erp'
    owns_order = bool(order and not from_erp and not inv.from_existing_order)
    if owns_order:
        if (order.status or '') == 'shipped':
            raise ValueError('shipped')
        if (order.status or '') not in ('canceled', 'ghost'):
            order.status = 'canceled'
            order.canceled_at = msk_now()
        order.is_deleted = True
        inv.status = 'discarded'
        return order
    inv.status = 'discarded'
    return None


@bp.route('/api/invoices/<int:inv_id>/discard', methods=['POST'])
@require_sale
def api_discard(user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    if inv.status == 'discarded':
        return jsonify({'ok': True, 'order_id': inv.order_id})
    if inv.status == 'approved' and (user.role or '') != 'admin':
        return jsonify({'error': 'Согласованный счёт может удалить только админ'}), 403
    try:
        order = void_sale_invoice(inv)
    except ValueError as err:
        if str(err) == 'shipped':
            return jsonify({
                'error': 'Заказ уже отгружен — снимите его в ERP вручную',
            }), 409
        raise
    db.session.commit()
    try:
        tg_send_message(_discard_orders_text(inv, order), chat_type='orders')
    except Exception:
        current_app.logger.exception('sale invoice discard chat')
    return jsonify({'ok': True, 'order_id': order.id if order else None})


@bp.route('/api/orders')
@require_sale
def api_orders(_user: User):
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify({'orders': []})
    digits = re.sub(r'\D+', '', q)
    clauses = []
    if q.isdigit():
        clauses.append(Order.id == int(q))
    like = f'%{q}%'
    clauses.append(Client.name.ilike(like))
    clauses.append(Order.invoice_number.ilike(like))
    if len(digits) >= 4:
        clauses.append(Client.inn.ilike(f'%{digits}%'))
    rows = (
        _order_query()
        .join(Client, Order.client_id == Client.id)
        .filter(Order.is_deleted.is_(False))
        .filter(~Order.status.in_(('canceled', 'ghost')))
        .filter(or_(*clauses))
        .order_by(Order.id.desc())
        .limit(8)
        .all()
    )
    return jsonify({'orders': [_serialize_order(o, preview=True) for o in rows]})


@bp.route('/api/orders/<int:order_id>')
@require_sale
def api_order(_user: User, order_id: int):
    order = _order_query().filter(Order.id == order_id, Order.is_deleted.is_(False)).first()
    if not order:
        return jsonify({'error': 'order_missing'}), 404
    if (order.status or '') in ('canceled', 'ghost'):
        return jsonify({'error': 'order_closed'}), 400
    return jsonify(_serialize_order(order, preview=False))


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
    resp.headers['Content-Disposition'] = f'inline; filename=schet_{sale_public_number(inv)}.pdf'
    return resp


@bp.route('/api/invoices/<int:inv_id>/send-pdf', methods=['POST'])
@require_sale
def api_send_pdf(user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    blob = _store_pdf(inv)
    db.session.commit()
    if not blob:
        return jsonify({'ok': False, 'error': 'pdf_failed'}), 500
    chat_id = current_telegram_id()
    if not chat_id:
        current_app.logger.warning('sale send-pdf no telegram_id inv=%s user=%s', inv.id, user.username)
        return jsonify({'ok': False, 'error': 'no_telegram_id'})
    caption = (
        f'Счёт №{sale_public_number(inv)} · без плательщика · {inv.amount} ₽'
        if inv.anonymous else
        f'Счёт №{sale_public_number(inv)} · {inv.buyer_name or "клиент"} · {inv.amount} ₽'
    )
    ok, err = send_chat_document(
        chat_id,
        filename=inv.file_name or f'schet_{sale_public_number(inv)}.pdf',
        caption=caption,
        file_bytes=bytes(blob),
    )
    if not ok:
        current_app.logger.warning('sale send-pdf failed inv=%s chat=%s err=%s', inv.id, chat_id, err)
    return jsonify({'ok': bool(ok), 'error': err if not ok else None})


@bp.route('/api/invoices/<int:inv_id>/approve', methods=['POST'])
@require_sale
def api_approve(user: User, inv_id: int):
    inv = SaleInvoice.query.get_or_404(inv_id)
    if inv.status != 'draft':
        return jsonify({'error': 'locked'}), 400
    if not inv.lines:
        return jsonify({'error': 'no_lines'}), 400
    if not inv.anonymous and not (inv.buyer_name or '').strip():
        return jsonify({'error': 'need_buyer'}), 400
    inv.amount = _line_sum(inv.lines)
    _sync_client(inv)
    blob = _store_pdf(inv)
    if not blob:
        return jsonify({'error': 'pdf_failed'}), 500
    inv.status = 'approved'
    inv.approved_at = msk_now()
    order = create_order_from_sale_invoice(inv, user.id)
    sync_order_client_from_sale_invoice(inv)
    db.session.commit()
    text = _approved_orders_text(inv, order)
    try:
        ok_msg, err_msg = tg_send_message(text, chat_type='orders')
        if not ok_msg:
            current_app.logger.warning('sale invoice orders chat inv=%s err=%s', inv.id, err_msg)
        ok_doc, err_doc = send_document(
            filename=inv.file_name or f'schet_{sale_public_number(inv)}.pdf',
            caption=f'Счёт №{sale_public_number(inv)} · {inv.buyer_name or "клиент"} · {inv.amount} ₽',
            file_bytes=bytes(blob),
            chat_type='orders',
        )
        if not ok_doc:
            current_app.logger.warning('sale invoice orders pdf inv=%s err=%s', inv.id, err_doc)
    except Exception:
        current_app.logger.exception('sale invoice orders chat')
    return jsonify(_serialize_invoice(inv, detail=True))


@event.listens_for(Session, 'before_flush')
def _sale_invoices_follow_order_client(session, flush_context, instances):
    for obj in session.dirty:
        if not isinstance(obj, Order):
            continue
        try:
            if inspect(obj).attrs.client_id.history.has_changes():
                sync_sale_invoices_from_order(obj)
        except Exception:
            current_app.logger.exception('sale invoice follow order client')
