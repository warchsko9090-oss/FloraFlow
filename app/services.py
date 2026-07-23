import json
import re
from datetime import datetime, time
from decimal import Decimal
from sqlalchemy import func, case, or_, and_
from app.models import db, Document, DocumentRow, StockBalance, Expense, BudgetItem, UnitCostOverride, Order, OrderItem, Field, Plant, Size, AppSetting, Project
from app.utils import get_actual_price, msk_now, msk_today  # Импортируем вспомогательную функцию из utils
from app.stock_helpers import aggregate_stock_movements

# --- ЛОГИКА РАСЧЕТОВ (ВЫНЕСЕНО ИЗ UTILS) ---

_BED_FIELD_RE = re.compile(r'грядк', re.I)
_INCOME_LIKE_DOC_TYPES = frozenset({
    'income', 'correction', 'field_recount', 'potting_recount',
})
COST_CONTAINER_PROJECTS_KEY = 'cost_container_project_ids'
BASE_COST_YEAR = 2017


def _cost_years(selected_year=None):
    max_year = max(int(selected_year or BASE_COST_YEAR), msk_now().year)
    return list(range(BASE_COST_YEAR, max_year + 1))


def is_bed_field_name(name):
    """Поле-грядка: в названии есть «грядк» (как на карте склада)."""
    return bool(name and _BED_FIELD_RE.search(name))


def get_bed_field_ids():
    """ID полей-грядок для исключения из отчёта «Влияние на цену»."""
    return {f.id for f in Field.query.all() if is_bed_field_name(f.name)}


CONTAINER_FIELD_NAME = 'контейнерная площадка'
CONTAINER_FIELD_NAME_PREFIX = 'контейнерная площадка'


def _is_container_yard_field_name(name: str) -> bool:
    n = (name or '').strip().lower()
    if not n:
        return False
    if n == CONTAINER_FIELD_NAME or n.startswith(CONTAINER_FIELD_NAME_PREFIX):
        return True
    return 'контейнер' in n and 'площадка' in n


def list_container_yard_fields():
    """Все поля-площадки под контейнеры (1, 2, …), отсортированные по имени."""
    fields = [f for f in Field.query.all() if _is_container_yard_field_name(f.name)]
    return sorted(fields, key=lambda f: ((f.name or '').lower(), f.id))


def get_container_field_id():
    """Основное поле контейнерной площадки (без привязки к проекту).

    Предпочитает «Контейнерная площадка 1», затем точное «Контейнерная площадка»,
    затем любое «…площадка…» с «контейнер».
    """
    fields = list_container_yard_fields()
    if not fields:
        return None
    for f in fields:
        n = (f.name or '').strip().lower()
        if n == f'{CONTAINER_FIELD_NAME} 1' or n.endswith('площадка 1'):
            return f.id
    for f in fields:
        if (f.name or '').strip().lower() == CONTAINER_FIELD_NAME:
            return f.id
    return fields[0].id


def get_container_field_label():
    fid = get_container_field_id()
    if not fid:
        return None
    field = Field.query.get(fid)
    return field.name if field else None


def get_container_field_ids():
    """ID всех контейнерных площадок (для исключения из общей себестоимости)."""
    return {f.id for f in list_container_yard_fields()}


def ensure_numbered_container_yards():
    """Если есть ровно «Контейнерная площадка» без номера — переименовать в «… 1»."""
    bare = Field.query.filter(func.lower(Field.name) == CONTAINER_FIELD_NAME).first()
    if not bare:
        return
    numbered = Field.query.filter(Field.name.ilike('Контейнерная площадка %')).count()
    others = [
        f for f in list_container_yard_fields()
        if f.id != bare.id
    ]
    if others:
        return
    if numbered:
        return
    bare.name = 'Контейнерная площадка 1'
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()


def _container_stock_end_date(selected_year=None):
    """
    Дата среза остатков — как в /stock?end_date=ДД.ММ.ГГГГ (конец выбранного дня).
    Для текущего года — конец сегодняшнего дня, не «сейчас».
    """
    today = msk_today()
    year = int(selected_year) if selected_year else today.year
    if year < today.year:
        return datetime(year, 12, 31, 23, 59, 59)
    return datetime.combine(today, time(23, 59, 59))


def get_cost_container_project_ids():
    """Проекты, расходы которых входят в себестоимость контейнерной посадки."""
    setting = AppSetting.query.get(COST_CONTAINER_PROJECTS_KEY)
    if not setting or not setting.value:
        return []
    try:
        data = json.loads(setting.value)
        if isinstance(data, list):
            return [int(x) for x in data if str(x).strip().isdigit() or isinstance(x, int)]
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    return [int(x) for x in setting.value.split(',') if x.strip().isdigit()]


def save_cost_container_project_ids(project_ids):
    """Сохраняет список проектов для вкладки «Контейнерная площадка»."""
    clean = sorted({int(x) for x in project_ids if x is not None})
    setting = AppSetting.query.get(COST_CONTAINER_PROJECTS_KEY)
    if not setting:
        setting = AppSetting(key=COST_CONTAINER_PROJECTS_KEY)
        db.session.add(setting)
    setting.value = json.dumps(clean)
    db.session.commit()
    return clean


def calculate_qty_at_year_end(year, include_field_ids=None, exclude_field_ids=None):
    """Остаток растений на конец года с опциональным фильтром по полям."""
    if include_field_ids is not None or exclude_field_ids:
        rows = get_detailed_stock_at_year_end(year)
        total = 0
        include_set = set(include_field_ids or [])
        exclude_set = set(exclude_field_ids or [])
        for row in rows:
            fid = row['field_id']
            if include_field_ids is not None and fid not in include_set:
                continue
            if fid in exclude_set:
                continue
            total += int(row['quantity'] or 0)
        return total

    target_date = datetime(year, 12, 31, 23, 59, 59)
    query = db.session.query(func.sum(case(
        (Document.doc_type.in_(['income', 'correction']), DocumentRow.quantity),
        (Document.doc_type.in_(['writeoff', 'shipment']), -DocumentRow.quantity),
        (Document.doc_type == 'move', 0), else_=0))).join(Document).filter(Document.date <= target_date)
    return query.scalar() or 0


def calculate_total_qty_at_year_end(year, exclude_field_ids=None):
    """Считает общее количество растений на складе на конец года."""
    return calculate_qty_at_year_end(year, exclude_field_ids=exclude_field_ids)


def calculate_container_qty_at_year_end(year, field_ids=None):
    """Алиас: актуальный остаток на «Контейнерная площадка» (отчёт /stock)."""
    _ = year, field_ids
    return get_container_stock_total_qty()


def get_container_stock_rows(field_id=None, as_of=None, selected_year=None):
    """
    Остатки на «Контейнерная площадка» — как отчёт /stock на выбранную дату.
    Источник: движения документов + дополнение из StockBalance (все размеры).
    """
    fid = field_id or get_container_field_id()
    if not fid:
        return []

    end_date = as_of or _container_stock_end_date(selected_year)
    fact_map, _, _ = aggregate_stock_movements(end_date, [fid])

    for sb in StockBalance.query.filter(
        StockBalance.field_id == fid,
        StockBalance.quantity > 0,
    ).all():
        key = (sb.plant_id, sb.size_id, sb.field_id, sb.year)
        sb_qty = int(sb.quantity or 0)
        if sb_qty > 0:
            fact_map[key] = max(int(fact_map.get(key, 0) or 0), sb_qty)

    plants = {p.id: p.name for p in Plant.query.all()}
    sizes = {s.id: s.name for s in Size.query.all()}
    field_obj = Field.query.get(fid)
    field_label = field_obj.name if field_obj else 'Контейнерная площадка'

    purchase_map = {
        (sb.plant_id, sb.size_id, sb.field_id, sb.year): sb.purchase_price or Decimal(0)
        for sb in StockBalance.query.filter(StockBalance.field_id == fid).all()
    }

    rows = []
    for (pid, sid, f_id, batch_year), qty in fact_map.items():
        qty = int(qty or 0)
        if qty <= 0:
            continue
        rows.append({
            'plant_id': pid,
            'size_id': sid,
            'field_id': f_id,
            'year': batch_year,
            'name': plants.get(pid, 'Unknown'),
            'size': sizes.get(sid, 'Unknown'),
            'field': field_label,
            'quantity': qty,
            'purchase_price': purchase_map.get((pid, sid, f_id, batch_year), Decimal(0)),
            'lot_id': None,
            'supplier_id': None,
        })

    # Лоты закупки: если есть — раскрываем строки с разными ценой/поставщиком
    try:
        from app.models import StockPurchaseLot, Supplier
        lots = StockPurchaseLot.query.filter(
            StockPurchaseLot.field_id == fid,
            StockPurchaseLot.quantity > 0,
        ).all()
    except Exception:
        lots = []
    if lots:
        suppliers = {s.id: s.name for s in Supplier.query.all()}
        covered = set()
        lot_rows = []
        for lot in lots:
            if selected_year is not None and int(lot.year) > int(selected_year):
                continue
            covered.add((lot.plant_id, lot.size_id, lot.field_id, lot.year))
            name = plants.get(lot.plant_id, 'Unknown')
            if lot.supplier_id and suppliers.get(lot.supplier_id):
                name = f"{name} · {suppliers[lot.supplier_id]}"
            lot_rows.append({
                'plant_id': lot.plant_id,
                'size_id': lot.size_id,
                'field_id': lot.field_id,
                'year': lot.year,
                'name': name,
                'size': sizes.get(lot.size_id, 'Unknown'),
                'field': field_label,
                'quantity': int(lot.quantity or 0),
                'purchase_price': Decimal(str(lot.purchase_price or 0)),
                'supplier_id': lot.supplier_id,
                'lot_id': lot.id,
            })
        for r in rows:
            key = (r['plant_id'], r['size_id'], r['field_id'], r['year'])
            if key not in covered:
                lot_rows.append(r)
        rows = lot_rows

    rows.sort(key=lambda r: (r['name'], r['size'], r['year']))
    return rows


def get_container_stock_total_qty(field_id=None, as_of=None, selected_year=None):
    return sum(
        r['quantity']
        for r in get_container_stock_rows(field_id=field_id, as_of=as_of, selected_year=selected_year)
    )


def build_container_cost_table_rows(container_cd, plant_ids=None):
    """Таблица контейнерной себестоимости: каждая позиция остатков (все размеры)."""
    accum_opex_map = container_cd.get('accum_opex_map') or {}
    stock_rows = get_container_stock_rows(
        container_cd.get('container_field_id'),
        selected_year=container_cd.get('selected_year'),
    )
    if plant_ids:
        plant_set = set(plant_ids)
        stock_rows = [r for r in stock_rows if r['plant_id'] in plant_set]

    rows = []
    total_qty = 0
    table_total = Decimal(0)
    for r in stock_rows:
        qty = r['quantity']
        if qty <= 0:
            continue
        pp = r['purchase_price'] or Decimal(0)
        batch_year = r['year']
        ac_opex = accum_opex_map.get(batch_year, Decimal(0))
        total_unit = pp + ac_opex
        total_cost = total_unit * Decimal(qty)
        rows.append({
            'name': r['name'],
            'size': r['size'],
            'field': r['field'],
            'year': batch_year,
            'quantity': qty,
            'purchase_price': pp,
            'accum_opex': ac_opex,
            'total_unit_cost': total_unit,
            'total_cost': total_cost,
            'plant_id': r['plant_id'],
            'field_id': r['field_id'],
            'size_id': r['size_id'],
            'lot_id': r.get('lot_id'),
            'supplier_id': r.get('supplier_id'),
        })
        total_qty += qty
        table_total += total_cost
    return rows, total_qty, table_total

def get_detailed_stock_at_year_end(year):
    """Возвращает детальные остатки (какие растения, какие поля) на конец года."""
    target_date = datetime(year, 12, 31, 23, 59, 59)
    movements = db.session.query(DocumentRow.plant_id, DocumentRow.size_id, DocumentRow.field_to_id, DocumentRow.field_from_id, DocumentRow.quantity, Document.doc_type, DocumentRow.year, DocumentRow.size_to_id).join(Document).filter(Document.date <= target_date).all()
    stock_map = {} 
    for m in movements:
        pid, sid, qty, dtype, batch_year = m.plant_id, m.size_id, m.quantity, m.doc_type, m.year
        if dtype in _INCOME_LIKE_DOC_TYPES:
            k = (pid, sid, m.field_to_id, batch_year)
            stock_map[k] = stock_map.get(k, 0) + qty
        elif dtype in ['writeoff', 'shipment']:
            k = (pid, sid, m.field_from_id, batch_year)
            stock_map[k] = stock_map.get(k, 0) - qty
        elif dtype == 'move':
            k_from = (pid, sid, m.field_from_id, batch_year); k_to = (pid, sid, m.field_to_id, batch_year)
            stock_map[k_from] = stock_map.get(k_from, 0) - qty; stock_map[k_to] = stock_map.get(k_to, 0) + qty
        elif dtype == 'regrading':
            k_old = (pid, sid, m.field_from_id, batch_year)
            stock_map[k_old] = stock_map.get(k_old, 0) - qty
            dst_field = m.field_to_id or m.field_from_id
            k_new = (pid, m.size_to_id, dst_field, batch_year)
            stock_map[k_new] = stock_map.get(k_new, 0) + qty
    
    result = []
    current_prices = { (s.plant_id, s.size_id, s.field_id, s.year): s.purchase_price for s in StockBalance.query.all() }
    plants = {p.id: p.name for p in Plant.query.all()}; sizes = {s.id: s.name for s in Size.query.all()}; fields = {f.id: f.name for f in Field.query.all()}
    
    for (pid, sid, fid, batch_year), qty in stock_map.items():
        if qty > 0:
            result.append({
                'plant_id': pid, 'size_id': sid, 'field_id': fid, 'year': batch_year,
                'name': plants.get(pid, 'Unknown'), 'size': sizes.get(sid, 'Unknown'), 'field': fields.get(fid, 'Unknown'),
                'quantity': qty, 'purchase_price': current_prices.get((pid, sid, fid, batch_year), Decimal(0)),
                'selling_price': get_actual_price(pid, sid, fid, year)
            })
    return result

def _parse_cost_period(period, selected_year):
    """Интервал для фильтра расходов (квартал/сезон) — как в calculate_cost_data."""
    period = (period or '').lower()
    period_start = period_end = None
    if period in ['q1', 'q2', 'q3', 'q4', 'spring', 'autumn'] and selected_year:
        if period == 'q1':
            period_start, period_end = datetime(selected_year, 1, 1), datetime(selected_year, 3, 31, 23, 59, 59)
        elif period == 'q2':
            period_start, period_end = datetime(selected_year, 4, 1), datetime(selected_year, 6, 30, 23, 59, 59)
        elif period == 'q3':
            period_start, period_end = datetime(selected_year, 7, 1), datetime(selected_year, 9, 30, 23, 59, 59)
        elif period == 'q4':
            period_start, period_end = datetime(selected_year, 10, 1), datetime(selected_year, 12, 31, 23, 59, 59)
        elif period == 'spring':
            period_start, period_end = datetime(selected_year, 1, 1), datetime(selected_year, 6, 30, 23, 59, 59)
        elif period == 'autumn':
            period_start, period_end = datetime(selected_year, 7, 1), datetime(selected_year, 12, 31, 23, 59, 59)
    return period_start, period_end


def _build_cost_expense_maps(period=None, selected_year=None):
    """Расходы, амортизационный «водопад» и ручные корректировки (без qty по годам)."""
    years = _cost_years(selected_year)
    period_start, period_end = _parse_cost_period(period, selected_year)

    all_expenses_query = db.session.query(
        func.extract('year', Expense.date).label('year'),
        Expense.budget_item_id,
        func.sum(Expense.amount),
    ).filter(
        Expense.order_id.is_(None),
        Expense.project_id.is_(None),
    )
    if period_start and period_end:
        all_expenses_query = all_expenses_query.filter(
            Expense.date >= period_start, Expense.date <= period_end,
        )
    all_expenses = all_expenses_query.group_by('year', Expense.budget_item_id).all()

    budget_items = {i.id: i for i in BudgetItem.query.all()}
    overrides_db = UnitCostOverride.query.all()
    overrides = {o.year: {'total': o.amount, 'amort': o.amortization} for o in overrides_db}

    amort_waterfall = {}
    detailed_expenses = {y: {} for y in years}
    raw_amort_source = {y: Decimal(0) for y in years}

    for year, item_id, amount in all_expenses:
        year = int(year)
        if year < BASE_COST_YEAR or year > years[-1]:
            continue
        item = budget_items.get(item_id)
        if not item:
            continue
        if item.is_amortization:
            raw_amort_source[year] += amount
            chunk = amount / 5
            for i in range(5):
                t_year = year + i
                if t_year > years[-1]:
                    continue
                if t_year not in amort_waterfall:
                    amort_waterfall[t_year] = {}
                amort_waterfall[t_year][year] = amort_waterfall[t_year].get(year, Decimal(0)) + chunk
        else:
            if year in detailed_expenses:
                detailed_expenses[year][item_id] = detailed_expenses[year].get(item_id, Decimal(0)) + amount

    return {
        'years': years,
        'budget_items': budget_items,
        'all_expenses': all_expenses,
        'amort_waterfall': amort_waterfall,
        'detailed_expenses': detailed_expenses,
        'raw_amort_source': raw_amort_source,
        'overrides': overrides,
    }


def _unit_cost_for_year_from_maps(year, expense_maps, exclude_field_ids=None):
    """total_unit для одного года — та же формула, что в calculate_cost_data."""
    detailed_expenses = expense_maps['detailed_expenses']
    amort_waterfall = expense_maps['amort_waterfall']
    overrides = expense_maps['overrides']

    op_sum = sum(detailed_expenses.get(year, {}).values())
    amort_sum = sum(amort_waterfall.get(year, {}).values())
    q = calculate_total_qty_at_year_end(year, exclude_field_ids=exclude_field_ids)

    calc_amort_unit = amort_sum / Decimal(q) if q > 0 else Decimal(0)
    calc_op_unit = op_sum / Decimal(q) if q > 0 else Decimal(0)
    manual_data = overrides.get(year, {})

    if manual_data.get('amort') is not None:
        final_amort_unit = manual_data['amort']
    else:
        final_amort_unit = calc_amort_unit

    if manual_data.get('total') is not None:
        return manual_data['total']
    return calc_op_unit + final_amort_unit


def get_unit_cost_for_year(year, period=None):
    """
    Узкая версия calculate_cost_data: только summary_totals[year]['total_unit']
    для колонки «нетов» на /stock (без 34 запросов qty по всем годам).
    """
    year = int(year)
    if year < BASE_COST_YEAR or year > max(msk_now().year, year):
        return Decimal(0)
    expense_maps = _build_cost_expense_maps(period=period, selected_year=year)
    return _unit_cost_for_year_from_maps(
        year, expense_maps, exclude_field_ids=get_container_field_ids(),
    )


def calculate_cost_data(selected_year=None, period=None):
    """
    Сложный расчет себестоимости:
    1. Берет все общие расходы (не привязанные к заказам).
    2. Распределяет амортизацию на 5 лет ("Водопад").
    3. Делит сумму расходов на количество растений в конце года.
    4. Учитывает ручные корректировки (UnitCostOverride).

    Дополнительно можно ограничить расходы интервалом (квартал/сезон) через аргумент period.
    """
    expense_maps = _build_cost_expense_maps(period=period, selected_year=selected_year)
    years = expense_maps['years']
    budget_items = expense_maps['budget_items']
    all_expenses = expense_maps['all_expenses']
    amort_waterfall = expense_maps['amort_waterfall']
    detailed_expenses = expense_maps['detailed_expenses']
    raw_amort_source = expense_maps['raw_amort_source']
    overrides = expense_maps['overrides']
    exclude_field_ids = get_container_field_ids()

    qty_by_year = {}
    summary_totals = {
        y: {
            'op': Decimal(0), 'amort': Decimal(0), 'total': Decimal(0),
            'is_manual_total': False, 'is_manual_amort': False,
        }
        for y in years
    }
    unit_cost_by_year = {}

    for y in years:
        qty_by_year[y] = calculate_total_qty_at_year_end(y, exclude_field_ids=exclude_field_ids)
        op_sum = sum(detailed_expenses.get(y, {}).values())
        amort_sum = sum(amort_waterfall.get(y, {}).values())
        q = qty_by_year[y]
        calc_amort_unit = amort_sum / Decimal(q) if q > 0 else Decimal(0)
        calc_op_unit = op_sum / Decimal(q) if q > 0 else Decimal(0)
        manual_data = overrides.get(y, {})

        if manual_data.get('amort') is not None:
            final_amort_unit = manual_data['amort']
            summary_totals[y]['is_manual_amort'] = True
        else:
            final_amort_unit = calc_amort_unit

        if manual_data.get('total') is not None:
            unit_cost_by_year[y] = manual_data['total']
            summary_totals[y]['is_manual_total'] = True
        else:
            unit_cost_by_year[y] = calc_op_unit + final_amort_unit

        summary_totals[y]['op'] = op_sum
        summary_totals[y]['amort'] = amort_sum
        summary_totals[y]['total'] = op_sum + amort_sum
        summary_totals[y]['qty'] = q
        summary_totals[y]['op_unit'] = calc_op_unit
        summary_totals[y]['amort_unit'] = final_amort_unit
        summary_totals[y]['total_unit'] = unit_cost_by_year[y]

    accumulated_costs_map = {}
    accum_opex_map = {}
    accum_amort_map = {}

    if selected_year:
        for start_year in range(2017, selected_year + 1):
            total_accum = Decimal(0)
            opex_accum = Decimal(0)
            amort_accum = Decimal(0)
            
            for y in range(start_year, selected_year + 1): 
                u_total = unit_cost_by_year.get(y, Decimal(0))
                u_amort = summary_totals[y]['amort_unit']
                u_opex = u_total - u_amort 
                
                total_accum += u_total
                amort_accum += u_amort
                opex_accum += u_opex
                
            accumulated_costs_map[start_year] = total_accum
            accum_opex_map[start_year] = opex_accum
            accum_amort_map[start_year] = amort_accum

    # ---- ВиУМ ₽/шт и ФОТ ₽/шт по году (фаза 4) -----------------------------
    # Расчёт on-demand. Учёт ведём только за годы, где есть данные —
    # ранние периоды (до запуска фазы 4) останутся 0 без штрафа на
    # производительность.
    vium_unit_by_year = {y: Decimal(0) for y in years}
    fot_unit_by_year = {y: Decimal(0) for y in years}
    try:
        from app import vium_techcard, vium_fot
        from app.models import ViumPlannedConsume
        from datetime import date as _d

        # Соберём список годов, где есть плановые расходы или выкопка.
        plan_years = {
            int(y) for (y,) in db.session.query(
                func.extract('year', ViumPlannedConsume.log_date)
            ).filter(
                ViumPlannedConsume.log_date.isnot(None)
            ).distinct().all() if y is not None
        }
        # Год запуска фазы 4 (берём текущий) — также пробуем посчитать ФОТ.
        if selected_year:
            plan_years.add(int(selected_year))

        # Средние цены (по партиям) — берём один раз.
        fact_overview = vium_techcard.vium_service.materials_overview()

        for y in plan_years:
            if y < BASE_COST_YEAR or y > years[-1]:
                continue
            yr_start = _d(y, 1, 1)
            yr_end = _d(y, 12, 31)
            qty_dug_year = vium_fot.period_dug_qty(yr_start, yr_end) or 0

            # ВиУМ: сумма (qty_planned × avg_price) за год / qty_dug_year.
            plan_rows = (
                db.session.query(
                    ViumPlannedConsume.material_id,
                    func.coalesce(func.sum(ViumPlannedConsume.qty_planned), 0)
                ).filter(
                    ViumPlannedConsume.log_date >= yr_start,
                    ViumPlannedConsume.log_date <= yr_end,
                ).group_by(ViumPlannedConsume.material_id).all()
            )
            vium_total = Decimal(0)
            for mid, qty in plan_rows:
                qty_d = Decimal(str(qty or 0))
                avg = fact_overview.get(int(mid), {}).get('avg_price', Decimal(0))
                vium_total += qty_d * (avg or Decimal(0))
            if qty_dug_year > 0:
                vium_unit_by_year[y] = vium_total / Decimal(qty_dug_year)
                fot_unit_by_year[y] = vium_fot.period_fot_per_unit(yr_start, yr_end)

        # Дополним сводку year-уровневыми деривативами.
        for y in years:
            v_unit = vium_unit_by_year.get(y, Decimal(0))
            f_unit = fot_unit_by_year.get(y, Decimal(0))
            summary_totals[y]['vium_unit'] = v_unit
            summary_totals[y]['fot_unit'] = f_unit
            summary_totals[y]['total_unit_with_extras'] = (
                summary_totals[y].get('total_unit', Decimal(0)) + v_unit + f_unit
            )
    except Exception:
        # Если что-то отвалилось (например, миграция ещё не накатилась) —
        # отчёт должен открыться без двух новых колонок.
        try:
            from flask import current_app as _ca
            _ca.logger.exception('vium/fot unit cost calculation failed')
        except Exception:
            pass

    return {
        'years': years, 'budget_items': budget_items, 'all_expenses': all_expenses,
        'amort_waterfall': amort_waterfall, 'detailed_expenses': detailed_expenses,
        'qty_by_year': qty_by_year, 'summary_totals': summary_totals, 'unit_cost_by_year': unit_cost_by_year, 
        'accumulated_costs_map': accumulated_costs_map,
        'accum_opex_map': accum_opex_map, 
        'accum_amort_map': accum_amort_map, 
        'cumulative_cost': accumulated_costs_map.get(2017, Decimal(0)), 
        'raw_amort_source': raw_amort_source,
        'vium_unit_by_year': vium_unit_by_year,
        'fot_unit_by_year': fot_unit_by_year,
    }


def _build_container_cost_expense_maps(project_ids, period=None, selected_year=None):
    """Расходы выбранных проектов (без амортизационного водопада)."""
    years = _cost_years(selected_year)
    period_start, period_end = _parse_cost_period(period, selected_year)
    project_ids = [int(x) for x in (project_ids or [])]

    detailed_expenses = {y: {} for y in years}
    project_totals = {y: Decimal(0) for y in years}
    all_expenses = []
    budget_items = {i.id: i for i in BudgetItem.query.all()}

    if not project_ids:
        return {
            'years': years,
            'budget_items': budget_items,
            'all_expenses': all_expenses,
            'detailed_expenses': detailed_expenses,
            'project_totals': project_totals,
            'project_ids': project_ids,
        }

    query = db.session.query(
        func.extract('year', Expense.date).label('year'),
        Expense.budget_item_id,
        Expense.project_id,
        func.sum(Expense.amount),
    ).filter(
        Expense.order_id.is_(None),
        Expense.project_id.in_(project_ids),
    )
    if period_start and period_end:
        query = query.filter(Expense.date >= period_start, Expense.date <= period_end)
    rows = query.group_by('year', Expense.budget_item_id, Expense.project_id).all()

    for year, item_id, _project_id, amount in rows:
        year = int(year)
        if year < BASE_COST_YEAR or year > years[-1]:
            continue
        amount = amount or Decimal(0)
        all_expenses.append((year, item_id, amount))
        detailed_expenses[year][item_id] = detailed_expenses[year].get(item_id, Decimal(0)) + amount
        project_totals[year] += amount

    return {
        'years': years,
        'budget_items': budget_items,
        'all_expenses': all_expenses,
        'detailed_expenses': detailed_expenses,
        'project_totals': project_totals,
        'project_ids': project_ids,
    }


def calculate_container_cost_data(project_ids=None, selected_year=None, period=None):
    """
    Себестоимость контейнерной посадки:
    - расходы из привязанных проектов (без амортизации);
    - знаменатель — текущий остаток на «Контейнерная площадка» (StockBalance).
    """
    container_field_id = get_container_field_id()
    stock_as_of = _container_stock_end_date(selected_year)
    current_qty = get_container_stock_total_qty(
        container_field_id, as_of=stock_as_of, selected_year=selected_year,
    )
    expense_maps = _build_container_cost_expense_maps(project_ids, period=period, selected_year=selected_year)
    years = expense_maps['years']
    budget_items = expense_maps['budget_items']
    all_expenses = expense_maps['all_expenses']
    detailed_expenses = expense_maps['detailed_expenses']

    qty_by_year = {}
    summary_totals = {
        y: {
            'op': Decimal(0), 'amort': Decimal(0), 'total': Decimal(0),
            'qty': 0, 'op_unit': Decimal(0), 'amort_unit': Decimal(0), 'total_unit': Decimal(0),
        }
        for y in years
    }
    unit_cost_by_year = {}

    for y in years:
        qty_by_year[y] = current_qty
        op_sum = sum(detailed_expenses.get(y, {}).values())
        q = current_qty
        calc_op_unit = op_sum / Decimal(q) if q > 0 else Decimal(0)
        unit_cost_by_year[y] = calc_op_unit
        summary_totals[y]['op'] = op_sum
        summary_totals[y]['total'] = op_sum
        summary_totals[y]['qty'] = q
        summary_totals[y]['op_unit'] = calc_op_unit
        summary_totals[y]['total_unit'] = calc_op_unit

    accumulated_costs_map = {}
    accum_opex_map = {}

    if selected_year:
        for start_year in range(2017, selected_year + 1):
            opex_accum = Decimal(0)
            for y in range(start_year, selected_year + 1):
                opex_accum += unit_cost_by_year.get(y, Decimal(0))
            accumulated_costs_map[start_year] = opex_accum
            accum_opex_map[start_year] = opex_accum

    return {
        'years': years,
        'budget_items': budget_items,
        'all_expenses': all_expenses,
        'detailed_expenses': detailed_expenses,
        'qty_by_year': qty_by_year,
        'summary_totals': summary_totals,
        'unit_cost_by_year': unit_cost_by_year,
        'accumulated_costs_map': accumulated_costs_map,
        'accum_opex_map': accum_opex_map,
        'container_field_id': container_field_id,
        'container_field_label': get_container_field_label(),
        'container_field_ids': get_container_field_ids(),
        'current_stock_qty': current_qty,
        'stock_as_of': stock_as_of,
        'selected_year': selected_year,
        'project_ids': expense_maps['project_ids'],
        'project_totals': expense_maps['project_totals'],
        'cumulative_cost': accumulated_costs_map.get(2017, Decimal(0)),
    }


def calculate_investor_debt(year, accum_costs_map):
    """Рассчитывает долг перед инвесторами за проданные растения."""
    total_debt = Decimal(0)
    breakdown = {}
    
    results = db.session.query(OrderItem, Field).join(Order).join(Field).filter(
        func.extract('year', Order.date) == year, 
        Order.status != 'canceled', 
        Order.is_deleted == False, 
        OrderItem.shipped_quantity > 0, 
        Field.investor_id.isnot(None), 
        # Условие: Либо год меньше 2025, ЛИБО (если 2025+) клиент НЕ равен инвестору
        or_(
            func.extract('year', Order.date) < 2025, 
            Order.client_id != Field.investor_id
        )
    ).all()
    
    stock_prices = { (sb.plant_id, sb.size_id, sb.field_id, sb.year): sb.purchase_price for sb in StockBalance.query.all() }
    
    for item, field in results:
        investor_name = field.investor.name
        qty = Decimal(item.shipped_quantity)
        admin_price = item.price 
        purchase_price = stock_prices.get((item.plant_id, item.size_id, item.field_id, item.year), Decimal(0))
        
        accum_cost = accum_costs_map.get(item.year, Decimal(0))
        
        share_unit = (admin_price / 2) - (purchase_price + accum_cost)
        total_owed = share_unit * qty
        
        total_debt += total_owed
        breakdown[investor_name] = breakdown.get(investor_name, Decimal(0)) + total_owed

    return total_debt, breakdown