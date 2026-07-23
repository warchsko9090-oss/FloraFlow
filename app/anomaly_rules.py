"""Детекторы аномалий. Чистая Python-логика (без LLM) — для надёжности и
предсказуемости. Каждый детектор возвращает список словарей:

    {
        'dedup_key': 'debt_new_order:client=128',       # стабильный ключ
        'kind': 'debt_new_order',                       # тип правила
        'severity': 'danger',                           # info / warning / danger
        'title': 'Клиент должен и делает новый заказ',  # короткий заголовок
        'details': 'Камолов должен 3,9 млн ...',        # текст карточки
        'url': '/orders/203',                           # куда перейти (если есть)
        'roles': ['admin', 'executive'],                # кому показывать
    }

Оркестратор в anomaly_engine.py уже разберётся, как дедуплицировать и что
завести в TgTask.
"""

from __future__ import annotations

from datetime import timedelta, datetime, date
from calendar import monthrange

from sqlalchemy import func, and_

from app.models import (
    db, Order, OrderItem, Payment, Client,
    Expense, BudgetItem, BudgetPlan,
    DiggingTask, DiggingLog, Document, DocumentRow,
    StockBalance, Plant, Size, Field,
)
from app.utils import msk_today


_BOSS_ROLES = ['admin', 'executive']


def _safe_float(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _money(v):
    try:
        return f'{float(v):,.0f} ₽'.replace(',', ' ')
    except Exception:
        return str(v)


# ---------------------------------------------------------------------------
# 1. КЛИЕНТ С ДОЛГОМ ДЕЛАЕТ НОВЫЙ ЗАКАЗ
# ---------------------------------------------------------------------------

def detect_debtor_with_new_order(today, debt_days=30, lookback_days=14):
    """Клиент у которого есть старый неоплаченный заказ (>=debt_days дней)
    завёл новый (за последние lookback_days) — это повод позвонить.
    """
    results = []
    cutoff_new = today - timedelta(days=lookback_days)
    cutoff_old = today - timedelta(days=debt_days)

    # Клиенты с новыми заказами за последние lookback_days
    new_orders = Order.query.filter(
        Order.is_deleted.is_(False),
        Order.status.in_(('reserved', 'in_progress', 'ready')),
        func.date(Order.date) >= cutoff_new,
    ).all()
    clients_with_new = {}
    for o in new_orders:
        clients_with_new.setdefault(o.client_id, []).append(o)

    for client_id, orders in clients_with_new.items():
        client = Client.query.get(client_id)
        if not client:
            continue
        # Старые долги у этого клиента
        old_orders = Order.query.filter(
            Order.client_id == client_id,
            Order.is_deleted.is_(False),
            Order.status.in_(('reserved', 'in_progress', 'ready', 'shipped')),
            func.date(Order.date) < cutoff_old,
        ).all()
        old_debt = 0.0
        for oo in old_orders:
            diff = _safe_float(oo.total_sum) - _safe_float(oo.paid_sum)
            if diff > 0.5:
                old_debt += diff
        if old_debt <= 0:
            continue  # старых долгов нет — это нормальный клиент

        new_total = sum(_safe_float(o.total_sum) for o in orders)
        newest = max(orders, key=lambda o: o.date)
        results.append({
            'dedup_key': f'debt_new_order:client={client_id}',
            'kind': 'debt_new_order',
            'severity': 'danger' if old_debt >= 500_000 else 'warning',
            'title': f'«{client.name}»: долг + новый заказ',
            'details': (
                f'Клиент должен {_money(old_debt)} по старым заказам '
                f'(дольше {debt_days} дн.), но за последние {lookback_days} дн. '
                f'завёл новый заказ #{newest.id} на {_money(new_total)}. '
                f'Нужно согласовать, как закрываем долг.'
            ),
            'url': f'/orders?client_id={client_id}',
            'roles': _BOSS_ROLES,
            'payload': {
                'client_id': client_id,
                'client_name': client.name,
                'old_debt': round(old_debt, 2),
                'new_order_id': newest.id,
                'new_order_total': round(new_total, 2),
            },
        })
    return results


# ---------------------------------------------------------------------------
# 2. ЗАКАЗ В СТАТУСЕ "READY" ВИСИТ ДОЛЬШЕ 7 ДНЕЙ
# ---------------------------------------------------------------------------

def detect_stale_ready_orders(today, days_threshold=7):
    """Заказ готов к отгрузке, но отгрузка не происходит."""
    results = []
    cutoff = today - timedelta(days=days_threshold)
    orders = Order.query.filter(
        Order.is_deleted.is_(False),
        Order.status == 'ready',
    ).all()
    for o in orders:
        # берём дату последнего документа по заказу — если его нет, дату заказа
        last_dt = o.date.date() if isinstance(o.date, datetime) else (o.date or today)
        if last_dt > cutoff:
            continue
        days_stale = (today - last_dt).days
        client_name = o.client.name if o.client else '—'
        results.append({
            'dedup_key': f'ready_stale:order={o.id}',
            'kind': 'ready_stale',
            'severity': 'warning' if days_stale < 14 else 'danger',
            'title': f'Заказ №{o.id} готов, но не отгружен',
            'details': (
                f'Клиент: {client_name}. Сумма: {_money(_safe_float(o.total_sum))}. '
                f'Ждёт отгрузки {days_stale} дн. Возможно, забыли согласовать дату.'
            ),
            'url': f'/orders/{o.id}',
            'roles': _BOSS_ROLES,
            'payload': {
                'order_id': o.id,
                'client_name': client_name,
                'days_stale': days_stale,
                'total': round(_safe_float(o.total_sum), 2),
            },
        })
    return results


# ---------------------------------------------------------------------------
# 3. ПЕРЕРАСХОД ПО СТАТЬЕ (ФАКТ > ПЛАНА ЗА МЕСЯЦ)
# ---------------------------------------------------------------------------

def detect_budget_overrun(today, overrun_threshold_pct=5):
    """Для текущего месяца: факт по статье больше плана больше чем на N%."""
    results = []
    first = today.replace(day=1)
    last = today.replace(day=monthrange(today.year, today.month)[1])

    # Планы бюджета на этот месяц
    plans = BudgetPlan.query.filter(
        BudgetPlan.year == today.year,
        BudgetPlan.month == today.month,
    ).all()
    if not plans:
        return []

    # Для каждой статьи считаем факт расходов за месяц
    for plan in plans:
        plan_amount = _safe_float(plan.amount)
        if plan_amount <= 0:
            continue
        fact = db.session.query(func.coalesce(func.sum(Expense.amount), 0)).filter(
            Expense.budget_item_id == plan.budget_item_id,
            Expense.date >= first,
            Expense.date <= last,
        ).scalar() or 0
        fact = _safe_float(fact)
        if fact <= 0:
            continue
        pct = (fact - plan_amount) / plan_amount * 100
        if pct < overrun_threshold_pct:
            continue

        cat_name = plan.item.name if plan.item else '—'
        dedup = f'budget_overrun:year={today.year}:month={today.month}:item={plan.budget_item_id}'
        severity = 'danger' if pct >= 30 else 'warning'
        results.append({
            'dedup_key': dedup,
            'kind': 'budget_overrun',
            'severity': severity,
            'title': f'Перерасход: «{cat_name}» {int(round(pct))}%',
            'details': (
                f'За {today.strftime("%m.%Y")} по статье «{cat_name}» потратили '
                f'{_money(fact)} при плане {_money(plan_amount)} '
                f'(+{int(round(pct))}%). Проверьте транзакции.'
            ),
            'url': '/expenses',
            'roles': _BOSS_ROLES,
            'payload': {
                'budget_item_id': plan.budget_item_id,
                'category_name': cat_name,
                'plan': round(plan_amount, 2),
                'fact': round(fact, 2),
                'overrun_pct': round(pct, 1),
            },
        })
    return results


# ---------------------------------------------------------------------------
# 4. НЕТ ОПЛАТЫ ЗА ЭЛЕКТРИЧЕСТВО В ТЕКУЩЕМ МЕСЯЦЕ
# ---------------------------------------------------------------------------

def detect_no_electricity_payment(today):
    """В текущем месяце нет ни одной проводки по статье «Электричество»."""
    first = today.replace(day=1)
    last = today.replace(day=monthrange(today.year, today.month)[1])

    item = BudgetItem.query.filter(
        func.lower(BudgetItem.name).like('%электрич%')
    ).first()
    if not item:
        return []  # статьи нет в справочнике — делать нечего

    count = db.session.query(func.count(Expense.id)).filter(
        Expense.budget_item_id == item.id,
        Expense.date >= first,
        Expense.date <= last,
    ).scalar() or 0

    if count > 0:
        return []

    # Только после 10-го числа: напоминание имеет смысл, когда месяц уже идёт.
    if today.day < 10:
        return []

    return [{
        'dedup_key': f'no_electricity_payment:year={today.year}:month={today.month}',
        'kind': 'no_electricity_payment',
        'severity': 'warning',
        'title': 'Не оплачено электричество в этом месяце',
        'details': (
            f'За {today.strftime("%m.%Y")} по статье «{item.name}» '
            f'нет ни одной проводки. Проверьте счёт.'
        ),
        'url': '/expenses',
        'roles': _BOSS_ROLES,
        'payload': {'budget_item_id': item.id, 'month': today.month, 'year': today.year},
    }]


# ---------------------------------------------------------------------------
# 5. ИТОГИ ПО ВЫКОПКЕ ЗА НЕДЕЛЮ (недовыполнено / перевыполнено)
# ---------------------------------------------------------------------------

def detect_digging_weekly_balance(today):
    """Сравниваем план недели (DiggingTask за пн-вс прошлой недели) с фактом
    (DiggingLog за те же даты). Создаём одну карточку с итогом ±.

    Показывается в понедельник-вторник, пока акутально.
    """
    # Прошедшая неделя пн-вс
    weekday = today.weekday()  # 0=пн, 6=вс
    this_monday = today - timedelta(days=weekday)
    prev_sunday = this_monday - timedelta(days=1)
    prev_monday = prev_sunday - timedelta(days=6)

    # Планы за ту неделю
    plan_q = db.session.query(func.coalesce(func.sum(DiggingTask.planned_qty), 0)).filter(
        DiggingTask.planned_date >= prev_monday,
        DiggingTask.planned_date <= prev_sunday,
    )
    planned = int(plan_q.scalar() or 0)

    fact_q = db.session.query(func.coalesce(func.sum(DiggingLog.quantity), 0)).filter(
        DiggingLog.date >= prev_monday,
        DiggingLog.date <= prev_sunday,
        DiggingLog.status != 'rejected',
    )
    fact = int(fact_q.scalar() or 0)

    if planned == 0 and fact == 0:
        return []

    diff = fact - planned
    pct = round(diff * 100 / planned, 1) if planned else 0

    if diff == 0:
        severity = 'info'
        title = f'Выкопка: план выполнен ровно ({fact} шт)'
    elif diff > 0:
        severity = 'info'
        title = f'Выкопка: перевыполнение +{diff} шт'
    else:
        severity = 'warning' if pct > -20 else 'danger'
        title = f'Выкопка: недовыполнение {diff} шт'

    dedup = f'digging_week:from={prev_monday.isoformat()}:to={prev_sunday.isoformat()}'
    details = (
        f'За {prev_monday.strftime("%d.%m")}–{prev_sunday.strftime("%d.%m")}: '
        f'план {planned} шт, факт {fact} шт '
        f'(разница {"+" if diff >= 0 else ""}{diff} шт, {pct:+.0f}%).'
    )
    return [{
        'dedup_key': dedup,
        'kind': 'digging_week',
        'severity': severity,
        'title': title,
        'details': details,
        'url': '/digging/analytics',
        'roles': _BOSS_ROLES,
        'payload': {
            'planned': planned,
            'fact': fact,
            'diff': diff,
            'pct': pct,
            'week_from': prev_monday.isoformat(),
            'week_to': prev_sunday.isoformat(),
        },
    }]


# ---------------------------------------------------------------------------
# 6. ЗАКАЗ С ПОЗИЦИЯМИ БЕЗ ЦЕНЫ (<= 1 руб)
# ---------------------------------------------------------------------------

_MANAGER_AND_BOSS_ROLES = ['admin', 'executive', 'user']


def detect_orders_missing_price(today=None, order_id=None):
    """Активные заказы, где хотя бы у одной не отгруженной позиции цена <= 1 руб.

    Проверяет заказы в статусах reserved/in_progress/ready (то, что сейчас
    в работе — старые отгруженные не трогаем). Если передан order_id, проверка
    ограничивается одним заказом (для синхронного режима после редактирования).
    """
    results = []
    q = Order.query.filter(
        Order.is_deleted.is_(False),
        Order.status.in_(('reserved', 'in_progress', 'ready')),
    )
    if order_id is not None:
        q = q.filter(Order.id == order_id)
    orders = q.all()

    for o in orders:
        bad_items = []
        for it in (o.items or []):
            qty = int(it.quantity or 0)
            if qty <= 0:
                continue
            # shipped_quantity >= quantity означает полностью отгруженную позицию —
            # цену уже задним числом не поменяешь разумно, но раз fully_shipped,
            # то проблема не блокирующая. Считаем такие позиции ок.
            shipped = int(it.shipped_quantity or 0)
            if shipped >= qty:
                continue
            price = _safe_float(it.price)
            if price <= 1.0:
                bad_items.append(it)
        if not bad_items:
            continue

        client_name = o.client.name if o.client else '—'
        sample_parts = []
        for it in bad_items[:5]:
            plant_name = it.plant.name if it.plant else '—'
            size_name = it.size.name if it.size else '—'
            sample_parts.append(f"{plant_name} {size_name} × {int(it.quantity or 0)}")
        more = f' и ещё {len(bad_items) - 5}' if len(bad_items) > 5 else ''

        order_url = f'/order/{o.id}'
        results.append({
            'dedup_key': f'order_no_price:order={o.id}',
            'kind': 'order_no_price',
            'severity': 'warning',
            'title': f'Заказ №{o.id}: нет цены у {len(bad_items)} позиций',
            'details': (
                f'Клиент: {client_name}. Без цены: {"; ".join(sample_parts)}{more}. '
                f'Откройте заказ и проставьте цены (минимум 1 ₽).'
            ),
            'url': order_url,
            'roles': _MANAGER_AND_BOSS_ROLES,
            'payload': {
                # url дублируется в payload, чтобы feed-карточка смогла показать
                # прямую ссылку на редактор заказа (action_payload — единственный
                # свободный текстовый контейнер в TgTask).
                'url': order_url,
                'order_id': o.id,
                'client_id': o.client_id,
                'client_name': client_name,
                'order_status': o.status,
                'bad_items_count': len(bad_items),
                'bad_items': [
                    {
                        'item_id': it.id,
                        'plant': it.plant.name if it.plant else None,
                        'size': it.size.name if it.size else None,
                        'field': it.field.name if it.field else None,
                        'year': it.year,
                        'quantity': int(it.quantity or 0),
                        'price': _safe_float(it.price),
                    }
                    for it in bad_items
                ],
            },
        })
    return results


# ---------------------------------------------------------------------------
# 7. ЗАКАЗ В РЕЗЕРВЕ ВИСИТ 14+ ДНЕЙ БЕЗ ДВИЖЕНИЯ
# ---------------------------------------------------------------------------

def detect_stale_reserved_orders(today, days_threshold=14):
    """Заказ в статусе 'reserved', по которому ВООБЩЕ не было никакой работы,
    висит дольше порога (по умолчанию 14 дней).

    Правило проверки — «голый резерв»: пропускаем заказ, если у него есть
    хотя бы одно из движений (неважно когда):
      - Оплата (Payment)
      - Запланированная копка (DiggingTask)
      - Факт копки (DiggingLog)
      - Отгрузка (Document.doc_type='shipment')

    Иначе говоря: карточка появится только у заказов, где клиент просто
    «забронировал» позиции и на них никто даже плана копки не создал.
    Именно такие заказы и нужно прозванивать менеджеру.

    Для «голого» заказа возраст считаем от:
      - Order.date (дата создания),
      - Order.reserve_ack_at (метка «менеджер подтвердил актуальность»).
    Больше из этих двух — точка отсчёта. Если разница ≥ days_threshold — флаг.
    """
    results = []
    orders = Order.query.filter(
        Order.is_deleted.is_(False),
        Order.status == 'reserved',
    ).all()

    if not orders:
        return results

    order_ids = [o.id for o in orders]

    # Существует ЛЮБАЯ оплата по заказу → заказ в работе, не трогаем.
    paid_ids = {
        r[0] for r in db.session.query(Payment.order_id)
        .filter(Payment.order_id.in_(order_ids)).distinct().all()
    }
    # Существует ЛЮБАЯ отгрузка.
    shipped_ids = {
        r[0] for r in db.session.query(Document.order_id)
        .filter(
            Document.order_id.in_(order_ids),
            Document.doc_type == 'shipment',
        ).distinct().all()
    }
    # Есть хотя бы одна ЗАПЛАНИРОВАННАЯ копка (DiggingTask) — заказ готовится.
    task_ids = {
        r[0] for r in db.session.query(OrderItem.order_id)
        .join(DiggingTask, DiggingTask.order_item_id == OrderItem.id)
        .filter(OrderItem.order_id.in_(order_ids)).distinct().all()
    }
    # Есть хотя бы один ФАКТ копки (DiggingLog).
    log_ids = {
        r[0] for r in db.session.query(OrderItem.order_id)
        .join(DiggingLog, DiggingLog.order_item_id == OrderItem.id)
        .filter(OrderItem.order_id.in_(order_ids)).distinct().all()
    }
    in_work_ids = paid_ids | shipped_ids | task_ids | log_ids

    def _to_date(v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.date()
        return v

    for o in orders:
        if o.id in in_work_ids:
            continue  # есть оплата/копка/отгрузка — уже не «голый резерв»

        candidates = []
        base = _to_date(o.date)
        if base:
            candidates.append(base)
        ack = _to_date(getattr(o, 'reserve_ack_at', None))
        if ack:
            candidates.append(ack)
        if not candidates:
            continue

        last_activity = max(candidates)
        days_stale = (today - last_activity).days
        if days_stale < days_threshold:
            continue

        client_name = o.client.name if o.client else '—'
        total = _safe_float(o.total_sum)
        severity = 'danger' if days_stale >= 30 else 'warning'
        order_url = f'/order/{o.id}'

        results.append({
            'dedup_key': f'stale_reserved:order={o.id}',
            'kind': 'stale_reserved',
            'severity': severity,
            'title': f'Заказ №{o.id} в резерве {days_stale} дн. без движения',
            'details': (
                f'Клиент: {client_name}. Сумма: {_money(total)}. '
                f'По заказу нет оплат, копки и отгрузок — висит с '
                f'{last_activity.strftime("%d.%m.%Y")} ({days_stale} дн.). '
                f'Нужно уточнить у клиента актуальность.'
            ),
            'url': order_url,
            'roles': _MANAGER_AND_BOSS_ROLES,
            'payload': {
                'url': order_url,
                'order_id': o.id,
                'client_id': o.client_id,
                'client_name': client_name,
                'days_stale': days_stale,
                'last_activity_at': last_activity.isoformat(),
                'total_sum': round(total, 2),
            },
        })
    return results


# ---------------------------------------------------------------------------
# ВСЕ ДЕТЕКТОРЫ В ОДНОМ СПИСКЕ
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 8. РАСХОЖДЕНИЕ ГОДА ПАРТИИ С ФАКТИЧЕСКИМ ОСТАТКОМ
# ---------------------------------------------------------------------------

def detect_year_mismatch_with_stock(today=None, order_id=None):
    """Активные позиции заказов, у которых год партии не сходится с фактом.

    Год партии — обязательная часть составного ключа
    (plant_id, size_id, field_id, year). Если год позиции не совпадает с
    реальным годом StockBalance, остатки и резервы расходятся, себестоимость
    и маржа считаются неверно.

    Три типа аномалий:
      • year_no_batch — на (plant, size, field) StockBalance с этим годом нет,
                        но есть строки с другими годами и положительным остатком
                        (явный кандидат «не тот год»);
      • year_overcommit — StockBalance.quantity меньше суммарного резерва
                          активных позиций по этой партии (партия переисчерпана);
      • year_no_stock_anywhere — StockBalance вообще нет на (plant, size, field),
                                 но позиция активна (вероятно, поле/размер
                                 тоже неверные).

    Если передан order_id — ограничиваемся одним заказом (для синхронной
    проверки после редактирования).
    """
    results = []

    item_q = (
        db.session.query(OrderItem, Order)
        .join(Order, OrderItem.order_id == Order.id)
        .filter(
            Order.is_deleted.is_(False),
            Order.status.notin_(('canceled', 'ghost')),
            OrderItem.shipped_quantity < OrderItem.quantity,
        )
    )
    if order_id is not None:
        item_q = item_q.filter(Order.id == order_id)
    items = item_q.all()
    if not items:
        return results

    # Сгруппируем активные резервы по ключу (plant, size, field, year),
    # чтобы посчитать overcommit без N+1 запросов.
    reserved_rows = (
        db.session.query(
            OrderItem.plant_id, OrderItem.size_id,
            OrderItem.field_id, OrderItem.year,
            func.sum(OrderItem.quantity - OrderItem.shipped_quantity),
        )
        .join(Order, OrderItem.order_id == Order.id)
        .filter(
            Order.is_deleted.is_(False),
            Order.status.notin_(('canceled', 'ghost')),
        )
        .group_by(OrderItem.plant_id, OrderItem.size_id,
                  OrderItem.field_id, OrderItem.year)
        .all()
    )
    reserved_map = {
        (r[0], r[1], r[2], r[3]): int(r[4] or 0)
        for r in reserved_rows
    }

    # Карта StockBalance по полному ключу и по (plant, size, field) для поиска
    # альтернативных годов.
    stock_full = {}
    stock_by_psf = {}
    for sb in StockBalance.query.all():
        stock_full[(sb.plant_id, sb.size_id, sb.field_id, sb.year)] = sb
        stock_by_psf.setdefault(
            (sb.plant_id, sb.size_id, sb.field_id), []
        ).append(sb)

    for it, o in items:
        if not it.plant_id or not it.size_id or not it.field_id or not it.year:
            continue

        key = (it.plant_id, it.size_id, it.field_id, it.year)
        psf_key = (it.plant_id, it.size_id, it.field_id)
        sb = stock_full.get(key)
        siblings = stock_by_psf.get(psf_key) or []

        client_name = o.client.name if o.client else '—'
        plant_name = it.plant.name if it.plant else '—'
        size_name = it.size.name if it.size else '—'
        field_name = it.field.name if it.field else '—'
        order_url = f'/order/{o.id}'

        if sb is None and not siblings:
            kind = 'year_no_stock_anywhere'
            severity = 'warning'
            title = f'Заказ №{o.id}: партия отсутствует в остатках'
            details = (
                f'Клиент: {client_name}. Позиция «{plant_name} {size_name}», '
                f'поле {field_name}, год {it.year}: на остатках нет ни одной '
                f'строки по (растение, размер, поле). Проверьте поле/размер.'
            )
            suggested = []
        elif sb is None and siblings:
            kind = 'year_no_batch'
            severity = 'warning'
            available = sorted(
                [(s.year, s.quantity - reserved_map.get((s.plant_id, s.size_id, s.field_id, s.year), 0))
                 for s in siblings if (s.quantity or 0) > 0],
                key=lambda t: -t[0],
            )
            suggested = [
                {'year': y, 'free': max(f, 0)}
                for y, f in available[:5]
            ]
            sug_text = ', '.join(
                f'{x["year"]} (своб.: {x["free"]} шт)' for x in suggested
            ) or 'нет годов с положительным остатком'
            title = f'Заказ №{o.id}: год партии {it.year} не существует на остатке'
            details = (
                f'Клиент: {client_name}. Позиция «{plant_name} {size_name}», '
                f'поле {field_name}: указан год {it.year}, но в StockBalance его нет. '
                f'Доступные годы на этом поле/размере: {sug_text}.'
            )
        else:
            # sb есть. Проверим overcommit.
            res_qty = reserved_map.get(key, 0)
            if (sb.quantity or 0) >= res_qty:
                continue
            kind = 'year_overcommit'
            severity = 'danger'
            title = f'Заказ №{o.id}: партия {it.year} переисчерпана резервами'
            details = (
                f'Клиент: {client_name}. Позиция «{plant_name} {size_name}», '
                f'поле {field_name}, год {it.year}: на остатке {sb.quantity} шт, '
                f'а в активных резервах уже {res_qty} шт. '
                f'Это означает что заказы конкурируют за одну партию.'
            )
            suggested = []

        results.append({
            'dedup_key': f'{kind}:item={it.id}',
            'kind': kind,
            'severity': severity,
            'title': title,
            'details': details,
            'url': order_url,
            'roles': _BOSS_ROLES,
            'payload': {
                'url': order_url,
                'order_id': o.id,
                'item_id': it.id,
                'plant': plant_name,
                'size': size_name,
                'field': field_name,
                'year': it.year,
                'quantity': int(it.quantity or 0),
                'shipped': int(it.shipped_quantity or 0),
                'suggested_years': suggested,
            },
        })

    return results


def detect_year_mismatch_for_order(order_id):
    """Узкая версия для синхронной проверки одного заказа после правок."""
    return detect_year_mismatch_with_stock(today=msk_today(), order_id=order_id)


# === Расхождения при пересчёте на поле (field_recount) ====================
#
# Триггер: менеджер сохранил карточку пересчёта по полю. В DocumentRow.quantity
# хранится знаковая дельта — плюс излишек, минус недостача.
# Цель: подсветить admin/executive подозрительные карточки, в которых:
#   1) асимметрия плюсов и минусов в одной карточке > 5%
#      |Σ+ - |Σ-|| / (Σ+ + |Σ-|)  > 0.05
#      Сценарий: «нашёл +500 одного и -300 другого» — это не классический
#      пересчёт, а перетасовка/ошибка маркировки. Исключение: чисто плюсовая
#      или чисто минусовая карточка (одна сторона ноль) — асимметрия 100% по
#      формуле, но это легитимная инвентаризация излишков/недостач, поэтому
#      такие карточки не считаем подозрительными.
#   2) объём изменений к остатку поля > 5%
#      (Σ+ + |Σ-|) / S_field > 0.05, где S_field — суммарный остаток по полю
#      на момент пересчёта (после применения дельт).
#
# Возвращаем dict с ключами роутинга карточки в TgTask: order_id здесь не
# применим, для перехода используем /document/edit/<id>.
RECOUNT_ASYMMETRY_THRESHOLD = 0.05
RECOUNT_VOLUME_THRESHOLD = 0.05


def _recount_doc_totals(doc):
    """Возвращает (sum_plus, sum_minus_abs, by_field, has_signed_mix).

    by_field — словарь field_id -> {'plus': int, 'minus': int}.
    has_signed_mix — True, если в карточке есть и плюсы, и минусы хотя бы у
    одной партии (индикатор «реального» пересчёта).
    """
    sum_plus = 0
    sum_minus = 0
    by_field = {}
    for row in (doc.rows or []):
        try:
            q = int(row.quantity or 0)
        except Exception:
            q = 0
        if q == 0:
            continue
        fid = row.field_to_id or row.field_from_id
        bucket = by_field.setdefault(fid, {'plus': 0, 'minus': 0})
        if q > 0:
            sum_plus += q
            bucket['plus'] += q
        else:
            sum_minus += -q
            bucket['minus'] += -q
    return sum_plus, sum_minus, by_field


def _field_total_stock(field_id):
    """Текущий остаток на поле — сумма quantity по StockBalance этого поля."""
    if not field_id:
        return 0
    total = db.session.query(func.coalesce(func.sum(StockBalance.quantity), 0)) \
        .filter(StockBalance.field_id == field_id).scalar() or 0
    try:
        return int(total)
    except Exception:
        return 0


def detect_recount_anomaly_for_doc(doc_id):
    """Возвращает список аномалий по конкретной карточке пересчёта.

    Применяется только к Document.doc_type='field_recount'. Прочие типы
    (correction, inventory_draft, ghost_*, shipment) игнорируем.
    """
    if not doc_id:
        return []
    doc = Document.query.get(doc_id)
    if not doc or doc.doc_type != 'field_recount':
        return []

    sum_plus, sum_minus, by_field = _recount_doc_totals(doc)
    if sum_plus == 0 and sum_minus == 0:
        return []

    results = []

    # 1) Асимметрия плюсов и минусов
    total_abs = sum_plus + sum_minus
    diff = abs(sum_plus - sum_minus)
    has_both_sides = sum_plus > 0 and sum_minus > 0
    if has_both_sides and total_abs > 0:
        ratio = diff / total_abs
        if ratio > RECOUNT_ASYMMETRY_THRESHOLD:
            field_names = []
            for fid in by_field.keys():
                if fid:
                    f = Field.query.get(fid)
                    if f:
                        field_names.append(f.name)
            field_label = ', '.join(field_names) if field_names else 'не указано'
            details = (
                f'Карточка пересчёта №{doc.id} ({field_label}): '
                f'+{sum_plus} шт. излишков и -{sum_minus} шт. недостач. '
                f'Разница {diff} шт. — {ratio*100:.1f}% от общего объёма '
                f'правок ({total_abs} шт.). '
                f'Превышен порог в {RECOUNT_ASYMMETRY_THRESHOLD*100:.0f}%. '
                f'Возможна ошибка в маркировке партий или в учёте.'
            )
            results.append({
                'dedup_key': f'recount_asymmetry:doc={doc.id}',
                'kind': 'recount_asymmetry',
                'severity': 'warning',
                'title': f'Перекос +/− при пересчёте: {ratio*100:.1f}%',
                'details': details,
                'url': f'/document/edit/{doc.id}',
                'roles': _BOSS_ROLES,
            })

    # 2) Объём правок относительно остатка поля
    for fid, vals in by_field.items():
        if not fid:
            continue
        f_plus = vals.get('plus', 0)
        f_minus = vals.get('minus', 0)
        f_total_abs = f_plus + f_minus
        if f_total_abs == 0:
            continue
        stock_total = _field_total_stock(fid)
        if stock_total <= 0:
            continue
        volume_ratio = f_total_abs / stock_total
        if volume_ratio > RECOUNT_VOLUME_THRESHOLD:
            f = Field.query.get(fid)
            f_name = f.name if f else f'#{fid}'
            details = (
                f'Карточка пересчёта №{doc.id} (поле {f_name}): '
                f'правок на {f_total_abs} шт. (+{f_plus}, -{f_minus}) '
                f'при текущем остатке поля {stock_total} шт. '
                f'Доля правок {volume_ratio*100:.1f}%, порог '
                f'{RECOUNT_VOLUME_THRESHOLD*100:.0f}%. '
                f'Стоит сверить с фактом перед применением.'
            )
            results.append({
                'dedup_key': f'recount_volume_high:doc={doc.id}:field={fid}',
                'kind': 'recount_volume_high',
                'severity': 'warning',
                'title': f'Крупный пересчёт на поле {f_name}: {volume_ratio*100:.1f}%',
                'details': details,
                'url': f'/document/edit/{doc.id}',
                'roles': _BOSS_ROLES,
            })

    return results


# ---------------------------------------------------------------------------
# 9. ПУСТЫЕ ЦЕНЫ ПРОДАЖИ В ОСТАТКАХ СКЛАДА
# ---------------------------------------------------------------------------
#
# Какая боль: бригадир/менеджер может вводить позиции в заказах на партии,
# по которым в StockBalance не задана цена продажи. Это ломает суммы
# заказов, отчёт «Влияние на себестоимость» и продаваемость в чате.
# Размеры «Нетов.» (продажа сеткой) и «Саженцы» (продажа саженцев) ценой
# по штуке не торгуются — их исключаем, иначе будет постоянный фейк-алерт.

# Только для админа: операционных людей (executive/менеджер) пустыми ценами
# на остатках не дёргаем — это вотчина админа, который ведёт прайс.
_ADMIN_ONLY_ROLES = ['admin']

# Размеры, у которых цена в StockBalance не обязательна (товар не продаётся
# поштучно по этому размеру). Сравниваем по нижнему регистру и по префиксу,
# чтобы покрыть варианты «Нетов.» / «Нетов» / «Нет.».
_STOCK_PRICE_EXCLUDED_SIZE_PREFIXES = ('нет', 'саж', 'товарн')


def _is_excluded_size_for_price(size_name):
    if not size_name:
        return False
    s = size_name.strip().lower()
    return any(s.startswith(p) for p in _STOCK_PRICE_EXCLUDED_SIZE_PREFIXES)


def detect_stock_missing_price(today=None):
    """Партии на складе с положительным остатком, у которых не задана цена
    продажи (price <= 0 или NULL). Исключаем размеры «Нетов.» и «Саженцы» —
    они не продаются поштучно по прайсу.

    Идея: ОДНА карточка на всю проблему (dedup_key='stock_no_price'),
    в details — сводка («N партий, например: …»). Если все цены проставлены —
    карточка автоматически закроется на следующем скане.
    Получатель — только админ (он ведёт прайс).
    """
    rows = (
        db.session.query(StockBalance, Plant, Size, Field)
        .join(Plant, StockBalance.plant_id == Plant.id)
        .join(Size, StockBalance.size_id == Size.id)
        .join(Field, StockBalance.field_id == Field.id)
        .filter(StockBalance.quantity > 0)
        .all()
    )

    bad = []
    for sb, plant, size, field in rows:
        if _is_excluded_size_for_price(size.name):
            continue
        price = _safe_float(sb.price)
        if price > 0:
            continue
        bad.append({
            'plant': plant.name or '—',
            'size': size.name or '—',
            'field': field.name or '—',
            'year': sb.year,
            'qty': int(sb.quantity or 0),
        })

    if not bad:
        return []

    # Сортируем по «крупнее проблема первее»: партии с большим остатком
    # важнее (потенциально больше потерь), внутри — по растению.
    bad.sort(key=lambda b: (-b['qty'], b['plant'].lower(), b['size'].lower()))

    sample_parts = []
    for b in bad[:6]:
        sample_parts.append(
            f"{b['plant']} {b['size']} ({b['field']}, {b['year']} г.) — {b['qty']} шт."
        )
    more = f' и ещё {len(bad) - 6}' if len(bad) > 6 else ''

    # Плюрализация (родительный падеж): 1 партии / 2-4 партий / 5+ партий.
    n = len(bad)
    n_mod10 = n % 10
    n_mod100 = n % 100
    if n_mod10 == 1 and n_mod100 != 11:
        word_partii = 'партии'
    else:
        word_partii = 'партий'

    details = (
        f'У {n} {word_partii} на складе не задана цена продажи. '
        f'Без цены ИИ-чат показывает 0 ₽, а заказы формируются с пустой суммой. '
        f'Например: {"; ".join(sample_parts)}{more}. '
        f'Откройте раздел «Склад» и проставьте цены в карточках партий.'
    )

    return [{
        'dedup_key': 'stock_no_price',
        'kind': 'stock_no_price',
        'severity': 'warning',
        'title': f'Склад: нет цены у {n} {word_partii}',
        'details': details,
        'url': '/stock',
        'roles': _ADMIN_ONLY_ROLES,
        'payload': {
            'url': '/stock',
            'count': len(bad),
            'sample': bad[:20],
        },
    }]


ALL_DETECTORS = [
    detect_debtor_with_new_order,
    detect_stale_ready_orders,
    detect_budget_overrun,
    detect_no_electricity_payment,
    detect_digging_weekly_balance,
    detect_orders_missing_price,
    detect_stale_reserved_orders,
    detect_year_mismatch_with_stock,
    detect_stock_missing_price,
]


def run_all_detectors(today=None, collect_errors=False):
    """Запускает все правила и возвращает плоский список аномалий.

    Каждое правило защищено try/except — сбой одного не ломает остальные.
    Если передан collect_errors=True — возвращает кортеж (anomalies, errors),
    где errors = [{'detector': name, 'error': str}, ...]. Удобно для
    админского /api/anomaly/rescan, чтобы сразу видеть упавшие детекторы.
    """
    if today is None:
        today = msk_today()
    all_anomalies = []
    errors = []
    for detector in ALL_DETECTORS:
        try:
            all_anomalies.extend(detector(today) or [])
        except Exception as e:
            import traceback
            traceback.print_exc()
            errors.append({'detector': detector.__name__, 'error': str(e)})
            try:
                from flask import current_app
                current_app.logger.exception(
                    'anomaly detector %s failed', detector.__name__
                )
            except Exception:
                pass
    if collect_errors:
        return all_anomalies, errors
    return all_anomalies
