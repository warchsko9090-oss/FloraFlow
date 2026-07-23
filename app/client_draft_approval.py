"""Двойное согласование заявок с публичной витрины (client_draft)."""
import json
from datetime import datetime

from sqlalchemy import func

from app.models import db, Order, OrderItem, Client, Field
from app.utils import msk_now, check_stock_availability, get_actual_price

APPROVAL_SLOTS = (
    ('user', 'Менеджер питомника'),
    ('shop_manager', 'Менеджер сайта'),
)

ROLES_APPROVE_AS_USER = frozenset({'user', 'admin'})
ROLES_APPROVE_AS_SHOP = frozenset({'shop_manager', 'admin'})
ROLES_REJECT_DRAFT = frozenset({'user', 'shop_manager', 'admin'})


def can_approve_as_user(role, doc_type='client_draft'):
    return doc_type == 'client_draft' and role in ROLES_APPROVE_AS_USER


def can_approve_as_shop_manager(role, doc_type='client_draft', shop_slot_done=False):
    if doc_type != 'client_draft':
        return False
    if role not in ROLES_APPROVE_AS_SHOP:
        return False
    return not shop_slot_done


def can_reject_draft(role, doc_type='client_draft'):
    return doc_type == 'client_draft' and role in ROLES_REJECT_DRAFT


def load_draft_payload(comment):
    if not comment:
        return {}
    try:
        data = json.loads(comment)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_draft_payload(doc, payload):
    doc.comment = json.dumps(payload, ensure_ascii=False)


def parse_client_draft_meta(comment):
    if not comment:
        return {
            'customer_name': '-',
            'phone': '',
            'comment': '',
            'email': '',
            'source': '',
            'lines': [],
            'total_sum': None,
            'approvals': {},
        }
    data = load_draft_payload(comment)
    if not data:
        return {
            'customer_name': str(comment),
            'phone': '',
            'comment': '',
            'email': '',
            'source': '',
            'lines': [],
            'total_sum': None,
            'approvals': {},
        }
    return {
        'customer_name': data.get('customer_name') or '-',
        'phone': data.get('phone') or '',
        'comment': data.get('comment') or '',
        'email': data.get('email') or '',
        'source': data.get('source') or '',
        'lines': data.get('lines') or [],
        'total_sum': data.get('total_sum'),
        'approvals': data.get('approvals') or {},
    }


def _format_at(iso_str):
    if not iso_str:
        return ''
    try:
        dt = datetime.fromisoformat(str(iso_str).replace('Z', '+00:00'))
        return dt.strftime('%d.%m.%Y %H:%M')
    except Exception:
        return str(iso_str)


def approval_status(meta):
    approvals = meta.get('approvals') or {}
    slots = {}
    done_count = 0
    for key, label in APPROVAL_SLOTS:
        rec = approvals.get(key) or {}
        is_done = bool(rec.get('user_id') and rec.get('at'))
        if is_done:
            done_count += 1
        slots[key] = {
            'key': key,
            'label': label,
            'done': is_done,
            'username': rec.get('username') or '',
            'at': rec.get('at') or '',
            'at_display': _format_at(rec.get('at')),
            'user_id': rec.get('user_id'),
            'client_id': rec.get('client_id'),
            'client_name': rec.get('client_name') or '',
            'allocations': rec.get('allocations') or {},
        }
    required = len(APPROVAL_SLOTS)
    return {
        'slots': slots,
        'done_count': done_count,
        'required_count': required,
        'complete': done_count >= required,
    }


def approvals_complete(meta):
    return approval_status(meta)['complete']


def serialize_allocations(allocations):
    return {str(k): v for k, v in (allocations or {}).items()}


def deserialize_allocations(raw):
    if not raw:
        return {}
    out = {}
    for k, v in raw.items():
        try:
            out[int(k)] = v
        except (TypeError, ValueError):
            continue
    return out


def parse_allocations_from_form(form):
    alloc_row_ids = form.getlist('alloc_row_id[]')
    alloc_fields = form.getlist('alloc_field_id[]')
    alloc_years = form.getlist('alloc_year[]')
    alloc_qtys = form.getlist('alloc_qty[]')

    allocations = {}
    for i in range(len(alloc_row_ids)):
        try:
            row_id = int(alloc_row_ids[i])
            field_id = int(alloc_fields[i])
            year = int(alloc_years[i])
            qty = int(alloc_qtys[i])
        except Exception:
            continue
        if qty <= 0:
            continue
        allocations.setdefault(row_id, []).append({
            'field_id': field_id,
            'year': year,
            'qty': qty,
        })
    return allocations


def validate_allocations(doc, allocations):
    for r in doc.rows:
        alloc_sum = sum(x['qty'] for x in allocations.get(r.id, []))
        if alloc_sum != int(r.quantity or 0):
            return False, (
                f'Для позиции "{r.plant.name} {r.size.name}" '
                f'сумма распределения должна быть {r.quantity}'
            )
    return True, None


def resolve_or_create_client(client_id, new_client_name, meta):
    if client_id:
        return Client.query.get(client_id)
    new_name = (new_client_name or meta.get('customer_name') or '').strip()
    if not new_name:
        return None
    client = Client.query.filter(func.lower(Client.name) == new_name.lower()).first()
    if client:
        return client
    client = Client(name=new_name)
    db.session.add(client)
    db.session.flush()
    return client


def stamp_user_approval(payload, user, client, allocations):
    if 'approvals' not in payload:
        payload['approvals'] = {}
    payload['approvals']['user'] = {
        'user_id': user.id,
        'username': user.username,
        'at': msk_now().isoformat(),
        'client_id': client.id,
        'client_name': client.name,
        'allocations': serialize_allocations(allocations),
    }
    return payload


def stamp_shop_manager_approval(payload, user):
    if 'approvals' not in payload:
        payload['approvals'] = {}
    payload['approvals']['shop_manager'] = {
        'user_id': user.id,
        'username': user.username,
        'at': msk_now().isoformat(),
    }
    return payload


def build_allocation_rows_view(doc, meta):
    """Таблица распределения для просмотра (после согласования менеджера)."""
    user_rec = (meta.get('approvals') or {}).get('user') or {}
    allocations = deserialize_allocations(user_rec.get('allocations'))
    if not allocations:
        return []

    row_map = {r.id: r for r in doc.rows}
    field_map = {f.id: f.name for f in Field.query.all()}
    rows = []
    for row_id, allocs in allocations.items():
        r = row_map.get(row_id)
        if not r:
            continue
        for al in allocs:
            rows.append({
                'plant_name': r.plant.name,
                'size_name': r.size.name,
                'field_name': field_map.get(al['field_id'], f'#{al["field_id"]}'),
                'year': al['year'],
                'qty': al['qty'],
            })
    return rows


def finalize_client_draft(doc, payload, created_by_user_id):
    """Создаёт заказ в резерве, если оба согласования на месте."""
    from app.orders import _record_order_item_history

    if not approvals_complete(parse_client_draft_meta(doc.comment)):
        raise ValueError('Не все согласования получены')

    user_approval = (payload.get('approvals') or {}).get('user') or {}
    client_id = user_approval.get('client_id')
    client = Client.query.get(client_id) if client_id else None
    if not client:
        raise ValueError('Клиент не указан в согласовании менеджера питомника')

    allocations = deserialize_allocations(user_approval.get('allocations'))
    ok, err = validate_allocations(doc, allocations)
    if not ok:
        raise ValueError(err)

    line_prices = {}
    for line in payload.get('lines') or []:
        try:
            line_prices[(int(line['plant_id']), int(line['size_id']))] = float(line['price'])
        except (KeyError, TypeError, ValueError):
            continue

    order = Order(
        client_id=client.id,
        date=msk_now(),
        status='reserved',
        created_by_user_id=created_by_user_id,
    )
    db.session.add(order)
    db.session.flush()

    created_items = []
    for r in doc.rows:
        for al in allocations.get(r.id, []):
            ok_stock, free = check_stock_availability(
                r.plant_id, r.size_id, al['field_id'], al['year'], al['qty']
            )
            if not ok_stock:
                raise ValueError(
                    f'Недостаточно остатка для "{r.plant.name} {r.size.name}" '
                    f'(поле {al["field_id"]}, {al["year"]}), доступно: {free}'
                )
            price = line_prices.get((r.plant_id, r.size_id))
            if price is None:
                from app.shop_prices import order_default_price
                wholesale = get_actual_price(r.plant_id, r.size_id, al['field_id'])
                price = order_default_price(r.plant_id, r.size_id, wholesale)
            oi = OrderItem(
                order_id=order.id,
                plant_id=r.plant_id,
                size_id=r.size_id,
                field_id=al['field_id'],
                year=al['year'],
                quantity=al['qty'],
                price=price,
            )
            db.session.add(oi)
            db.session.flush()
            created_items.append(oi)

    for created_item in created_items:
        _record_order_item_history(
            order_id=order.id,
            item=created_item,
            action_type='initial_item',
            before_qty=0,
            after_qty=created_item.quantity,
        )

    if not order.project_id and created_items:
        from app.finance import resolve_project_id_for_yard_fields
        linked = resolve_project_id_for_yard_fields([it.field_id for it in created_items])
        if linked:
            order.project_id = linked

    doc.doc_type = 'client_draft_approved'
    doc.order_id = order.id
    return order
