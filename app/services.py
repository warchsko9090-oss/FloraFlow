from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, case, or_, and_
from app.models import db, Document, DocumentRow, StockBalance, Expense, BudgetItem, UnitCostOverride, Order, OrderItem, Field, Plant, Size
from app.utils import get_actual_price  # Импортируем вспомогательную функцию из utils

# --- ЛОГИКА РАСЧЕТОВ (ВЫНЕСЕНО ИЗ UTILS) ---

def calculate_total_qty_at_year_end(year):
    """Считает общее количество растений на складе на конец года."""
    target_date = datetime(year, 12, 31, 23, 59, 59)
    query = db.session.query(func.sum(case(
        (Document.doc_type.in_(['income', 'correction']), DocumentRow.quantity),
        (Document.doc_type.in_(['writeoff', 'shipment']), -DocumentRow.quantity),
        (Document.doc_type == 'move', 0), else_=0))).join(Document).filter(Document.date <= target_date)
    return query.scalar() or 0

def get_detailed_stock_at_year_end(year):
    """Возвращает детальные остатки (какие растения, какие поля) на конец года."""
    target_date = datetime(year, 12, 31, 23, 59, 59)
    movements = db.session.query(DocumentRow.plant_id, DocumentRow.size_id, DocumentRow.field_to_id, DocumentRow.field_from_id, DocumentRow.quantity, Document.doc_type, DocumentRow.year, DocumentRow.size_to_id).join(Document).filter(Document.date <= target_date).all()
    stock_map = {} 
    for m in movements:
        pid, sid, qty, dtype, batch_year = m.plant_id, m.size_id, m.quantity, m.doc_type, m.year
        if dtype in ['income', 'correction']:
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
            k_new = (pid, m.size_to_id, m.field_from_id, batch_year)
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

def calculate_cost_data(selected_year=None, period=None):
    """
    Сложный расчет себестоимости:
    1. Берет все общие расходы (не привязанные к заказам).
    2. Распределяет амортизацию на 5 лет ("Водопад").
    3. Делит сумму расходов на количество растений в конце года.
    4. Учитывает ручные корректировки (UnitCostOverride).

    Дополнительно можно ограничить расходы интервалом (квартал/сезон) через аргумент period.
    """
    years = list(range(2017, 2051))

    # Период (например, q1/q2/q3/q4/spring/autumn)
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

    # ВАЖНО: Берем только расходы БЕЗ привязки к заказу И БЕЗ привязки к проекту (общие, накладные)
    # Расходы проектов считаются прямыми и не размазываются на весь склад.
    all_expenses_query = db.session.query(
        func.extract('year', Expense.date).label('year'), 
        Expense.budget_item_id, 
        func.sum(Expense.amount)
    ).filter(
        Expense.order_id.is_(None),
        Expense.project_id.is_(None)
    )

    if period_start and period_end:
        all_expenses_query = all_expenses_query.filter(Expense.date >= period_start, Expense.date <= period_end)

    all_expenses = all_expenses_query.group_by('year', Expense.budget_item_id).all()
    
    budget_items = {i.id: i for i in BudgetItem.query.all()}
    
    overrides_db = UnitCostOverride.query.all()
    overrides = {o.year: {'total': o.amount, 'amort': o.amortization} for o in overrides_db}
    
    amort_waterfall = {}; detailed_expenses = {y: {} for y in years}; raw_amort_source = {}
    for y in years:
        raw_amort_source[y] = Decimal(0)
        if y not in amort_waterfall: amort_waterfall[y] = {}

    for year, item_id, amount in all_expenses:
        year = int(year)
        if year < 2017 or year > 2050: continue
        item = budget_items.get(item_id)
        if not item: continue
        if item.is_amortization:
            raw_amort_source[year] += amount
            chunk = amount / 5 
            for i in range(5):
                t_year = year + i
                if t_year > 2050: continue
                if t_year not in amort_waterfall: amort_waterfall[t_year] = {}
                amort_waterfall[t_year][year] = amort_waterfall[t_year].get(year, Decimal(0)) + chunk
        else:
            if year in detailed_expenses: detailed_expenses[year][item_id] = detailed_expenses[year].get(item_id, Decimal(0)) + amount

    qty_by_year = {}; summary_totals = {y: {'op': Decimal(0), 'amort': Decimal(0), 'total': Decimal(0), 'is_manual_total': False, 'is_manual_amort': False} for y in years}
    unit_cost_by_year = {}
    
    for y in years:
        qty_by_year[y] = calculate_total_qty_at_year_end(y)
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
            
    return {
        'years': years, 'budget_items': budget_items, 'all_expenses': all_expenses,
        'amort_waterfall': amort_waterfall, 'detailed_expenses': detailed_expenses,
        'qty_by_year': qty_by_year, 'summary_totals': summary_totals, 'unit_cost_by_year': unit_cost_by_year, 
        'accumulated_costs_map': accumulated_costs_map,
        'accum_opex_map': accum_opex_map, 
        'accum_amort_map': accum_amort_map, 
        'cumulative_cost': accumulated_costs_map.get(2017, Decimal(0)), 
        'raw_amort_source': raw_amort_source
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