from sqlalchemy import func, or_
from app.models import db, Order, OrderItem, StockBalance, Document, DocumentRow


# Единые правила «активного» заказа для всех расчётов резерва.
# Меняются только здесь — все функции и все эндпоинты считают одинаково.
def _active_order_filter():
    return (
        Order.status != "canceled",
        Order.status != "ghost",
        Order.is_deleted == False,
    )


def get_reserved_map(plant_id=None, size_id=None, field_id=None, year=None,
                     exclude_item_id=None):
    """Build a dict {(plant_id, size_id, field_id, year): reserved_qty}.

    Все опциональные параметры сужают запрос. exclude_item_id пропускает
    конкретную позицию (нужно для split, чтобы не учитывать «саму себя»).
    «Активные» заказы — не canceled, не ghost, не is_deleted.
    """
    q = (
        db.session.query(
            OrderItem.plant_id,
            OrderItem.size_id,
            OrderItem.field_id,
            OrderItem.year,
            func.sum(OrderItem.quantity - OrderItem.shipped_quantity),
        )
        .join(Order)
        .filter(*_active_order_filter())
    )
    if plant_id is not None:
        q = q.filter(OrderItem.plant_id == plant_id)
    if size_id is not None:
        q = q.filter(OrderItem.size_id == size_id)
    if field_id is not None:
        q = q.filter(OrderItem.field_id == field_id)
    if year is not None:
        q = q.filter(OrderItem.year == year)
    if exclude_item_id:
        q = q.filter(OrderItem.id != exclude_item_id)

    q = q.group_by(OrderItem.plant_id, OrderItem.size_id, OrderItem.field_id, OrderItem.year)
    return {(r[0], r[1], r[2], r[3]): int(r[4] or 0) for r in q.all()}


def compute_reserved(plant_id, size_id, field_id, year, exclude_item_id=None):
    """Сколько штук партии (plant, size, field, year) сейчас зарезервировано
    в активных заказах. Возвращает int.
    """
    q = (
        db.session.query(func.sum(OrderItem.quantity - OrderItem.shipped_quantity))
        .join(Order)
        .filter(
            OrderItem.plant_id == plant_id,
            OrderItem.size_id == size_id,
            OrderItem.field_id == field_id,
            OrderItem.year == year,
            *_active_order_filter(),
        )
    )
    if exclude_item_id:
        q = q.filter(OrderItem.id != exclude_item_id)
    return int(q.scalar() or 0)


def compute_free(plant_id, size_id, field_id, year, exclude_item_id=None):
    """Возвращает кортеж (stock_qty, reserved_qty, free_qty) по партии.
    free_qty может быть отрицательным, если резерв уже превышает факт —
    вызывающий код должен это учесть.
    """
    stock = StockBalance.query.filter_by(
        plant_id=plant_id, size_id=size_id, field_id=field_id, year=year
    ).first()
    fact = int(stock.quantity or 0) if stock else 0
    reserved = compute_reserved(plant_id, size_id, field_id, year, exclude_item_id)
    return fact, reserved, fact - reserved


def snapshot_overcommit_for_order(order_id):
    """Снимает baseline-карту нарушений по каждой партии заказа: dict
    {(plant, size, field, year): overcommit_qty}, где overcommit = max(0,
    reserved - fact). Нужен, чтобы потом отличить «новое» нарушение,
    созданное текущей операцией, от уже существовавшего «до».
    """
    items = (
        OrderItem.query.filter_by(order_id=order_id)
        .with_entities(
            OrderItem.plant_id,
            OrderItem.size_id,
            OrderItem.field_id,
            OrderItem.year,
        )
        .distinct()
        .all()
    )
    snap = {}
    for pid, sid, fid, yr in items:
        fact, reserved, _ = compute_free(pid, sid, fid, yr)
        if reserved > fact:
            snap[(pid, sid, fid, yr)] = reserved - fact
    return snap


def assert_stock_for_order(order_id, baseline=None):
    """Защита от гонки. Вызывается ПЕРЕД commit'ом изменений в заказе.

    Бросает ValueError только если по какой-то партии overcommit стал
    БОЛЬШЕ, чем был в baseline (т.е. именно текущая операция создала новое
    нарушение или ухудшила старое). Исторические разъезды, которые уже
    были до операции, мы не блокируем — иначе менеджер не сможет править
    проблемные заказы, чтобы их же и починить.

    Использование:
        baseline = snapshot_overcommit_for_order(order_id)
        # ... делаем изменения ...
        db.session.flush()
        assert_stock_for_order(order_id, baseline=baseline)
        db.session.commit()
    """
    baseline = baseline or {}

    items = (
        OrderItem.query.filter_by(order_id=order_id)
        .with_entities(
            OrderItem.plant_id,
            OrderItem.size_id,
            OrderItem.field_id,
            OrderItem.year,
        )
        .distinct()
        .all()
    )
    keys = set((pid, sid, fid, yr) for pid, sid, fid, yr in items)
    # baseline-партии тоже проверим: вдруг позицию удалили и резерв надо
    # сравнить с прежним нарушением.
    keys.update(baseline.keys())

    problems = []
    for pid, sid, fid, yr in keys:
        fact, reserved, _ = compute_free(pid, sid, fid, yr)
        cur_overcommit = max(0, reserved - fact)
        prev_overcommit = baseline.get((pid, sid, fid, yr), 0)
        # Допускаем уже существовавшее нарушение, не позволяем его расширить.
        if cur_overcommit > prev_overcommit:
            from app.models import Plant, Size, Field
            from app.seedlings import allows_order_deficit
            sz = Size.query.get(sid)
            # Саженцы можно уводить в минус — не блокируем заказ.
            if sz and allows_order_deficit(sz.name):
                continue
            pl = Plant.query.get(pid)
            fld = Field.query.get(fid) if fid else None
            name = ' · '.join([
                (pl.name if pl else f'#{pid}'),
                (sz.name if sz else f'#{sid}'),
                (fld.name if fld else ''),
                str(yr),
            ])
            problems.append(
                f'«{name}» — резерв {reserved} шт превышает факт {fact} шт '
                f'(дефицит {cur_overcommit} шт, был {prev_overcommit}).'
            )
    if problems:
        raise ValueError(
            'Конфликт остатков: эта операция создаёт новый дефицит. '
            'Возможно, параллельный заказ только что забрал ту же партию. '
            + '; '.join(problems[:5])
        )


_INCOME_DOC_TYPES = ('income', 'correction', 'field_recount', 'potting_recount')
_OUT_DOC_TYPES = ('writeoff', 'shipment')


def _add_qty(target, key, qty):
    target[key] = target.get(key, 0) + int(qty or 0)


def aggregate_stock_movements(end_date, selected_fields):
    """
    Агрегация движений документов до end_date (SQL GROUP BY).
    Возвращает (fact_map, income_map, shipped_map) с ключами
    (plant_id, size_id, field_id, year) — та же семантика, что в stock.py.
    """
    if not selected_fields:
        return {}, {}, {}

    field_set = set(selected_fields)
    fact_map = {}
    income_map = {}
    shipped_map = {}
    date_filter = Document.date <= end_date

    income_rows = (
        db.session.query(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_to_id,
            DocumentRow.year,
            func.sum(DocumentRow.quantity),
        )
        .join(Document)
        .filter(
            date_filter,
            Document.doc_type.in_(_INCOME_DOC_TYPES),
            DocumentRow.field_to_id.in_(field_set),
        )
        .group_by(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_to_id,
            DocumentRow.year,
        )
        .all()
    )
    for pid, sid, fid, yr, qty in income_rows:
        if fid not in field_set:
            continue
        k = (pid, sid, fid, yr)
        q = int(qty or 0)
        _add_qty(fact_map, k, q)
        _add_qty(income_map, k, q)

    out_rows = (
        db.session.query(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_from_id,
            DocumentRow.year,
            func.sum(DocumentRow.quantity),
            Document.doc_type,
        )
        .join(Document)
        .filter(
            date_filter,
            Document.doc_type.in_(_OUT_DOC_TYPES),
            DocumentRow.field_from_id.in_(field_set),
        )
        .group_by(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_from_id,
            DocumentRow.year,
            Document.doc_type,
        )
        .all()
    )
    for pid, sid, fid, yr, qty, dtype in out_rows:
        if fid not in field_set:
            continue
        k = (pid, sid, fid, yr)
        q = int(qty or 0)
        _add_qty(fact_map, k, -q)
        if dtype == 'shipment':
            _add_qty(shipped_map, k, q)

    move_from = (
        db.session.query(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_from_id,
            DocumentRow.year,
            func.sum(DocumentRow.quantity),
        )
        .join(Document)
        .filter(
            date_filter,
            Document.doc_type == 'move',
            DocumentRow.field_from_id.in_(field_set),
        )
        .group_by(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_from_id,
            DocumentRow.year,
        )
        .all()
    )
    for pid, sid, fid, yr, qty in move_from:
        if fid not in field_set:
            continue
        _add_qty(fact_map, (pid, sid, fid, yr), -int(qty or 0))

    move_to = (
        db.session.query(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_to_id,
            DocumentRow.year,
            func.sum(DocumentRow.quantity),
        )
        .join(Document)
        .filter(
            date_filter,
            Document.doc_type == 'move',
            DocumentRow.field_to_id.in_(field_set),
        )
        .group_by(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.field_to_id,
            DocumentRow.year,
        )
        .all()
    )
    for pid, sid, fid, yr, qty in move_to:
        if fid not in field_set:
            continue
        _add_qty(fact_map, (pid, sid, fid, yr), int(qty or 0))

    regrade_rows = (
        db.session.query(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.size_to_id,
            DocumentRow.field_from_id,
            DocumentRow.field_to_id,
            DocumentRow.year,
            func.sum(DocumentRow.quantity),
        )
        .join(Document)
        .filter(
            date_filter,
            Document.doc_type == 'regrading',
            or_(
                DocumentRow.field_from_id.in_(field_set),
                DocumentRow.field_to_id.in_(field_set),
            ),
        )
        .group_by(
            DocumentRow.plant_id,
            DocumentRow.size_id,
            DocumentRow.size_to_id,
            DocumentRow.field_from_id,
            DocumentRow.field_to_id,
            DocumentRow.year,
        )
        .all()
    )
    for pid, sid, sid_to, fid_from, fid_to, yr, qty in regrade_rows:
        q = int(qty or 0)
        if fid_from in field_set:
            _add_qty(fact_map, (pid, sid, fid_from, yr), -q)
        dst = fid_to or fid_from
        if sid_to and dst in field_set:
            _add_qty(fact_map, (pid, sid_to, dst, yr), q)

    return fact_map, income_map, shipped_map
