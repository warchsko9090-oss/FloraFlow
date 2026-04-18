import io
import calendar
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from flask_login import login_required, current_user
from sqlalchemy import func, and_, extract
from sqlalchemy.orm import joinedload
from openpyxl import load_workbook, Workbook
from app.models import db, Order, OrderItem, Client, CompetitorSnapshot, CompetitorRow, Plant, Size, StockBalance, Field
from app.utils import msk_now, MONTH_NAMES, log_action
from app.services import calculate_cost_data

bp = Blueprint('crm', __name__)

RUSSIAN_REGIONS_LIST = [
    "Все регионы РФ", "Москва и Московская обл.", "Санкт-Петербург и Ленинградская обл.",
    "Тульская обл.", "Воронежская обл.", "Калужская обл.", "Тверская обл.", "Рязанская обл.",
    "Ярославская обл.", "Владимирская обл.", "Ивановская обл.", "Костромская обл.", 
    "Смоленская обл.", "Брянская обл.", "Орловская обл.", "Липецкая обл.", "Тамбовская обл.",
    "Курская обл.", "Белгородская обл.", "Краснодарский край", "Ростовская обл.",
    "Нижегородская обл.", "Республика Татарстан", "Свердловская обл."
]

@bp.route('/crm/client_analytics')
@login_required
def crm_client_analytics():
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))

    # Фильтры
    client_id = request.args.get('client_id', type=int)
    year_start = request.args.get('year_start', type=int, default=msk_now().year)
    year_end = request.args.get('year_end', type=int, default=msk_now().year)
    
    start_date = datetime(year_start, 1, 1)
    end_date = datetime(year_end, 12, 31, 23, 59, 59)

    clients = Client.query.order_by(Client.name).all()
    
    stats = {
        'total_qty': 0, 'total_sum': 0, 'avg_check': 0,
        'plants_breakdown': [], # Для таблицы (иерархия)
        'top_item': {'name': '-', 'size': '-', 'sum': 0}, # Топ позиция
        'status_funnel': {'shipped_pct': 0, 'canceled_pct': 0, 'reserved_pct': 0}, # Проценты
        'charts': {'labels': [], 'qty': [], 'sum': [], 'avg': [], 'colors': []} # Данные графиков
    }
    
    if client_id:
        # 1. Запрос заказов
        orders = Order.query.options(
            joinedload(Order.items).joinedload(OrderItem.plant),
            joinedload(Order.items).joinedload(OrderItem.size),
        ).filter(
            Order.client_id == client_id,
            Order.date >= start_date,
            Order.date <= end_date,
            Order.is_deleted == False 
        ).order_by(Order.date).all()

        agg_plants = {} 
        monthly_stats = {} 
        funnel_sum = {'total_potential': 0, 'shipped': 0, 'canceled': 0, 'reserved': 0}
        active_orders_count = 0
        flat_items_stats = {}

        for order in orders:
            order_potential = 0
            for item in order.items:
                order_potential += item.quantity * item.price
            
            funnel_sum['total_potential'] += order_potential

            if order.status == 'canceled':
                funnel_sum['canceled'] += order_potential
                continue 
            
            active_orders_count += 1
            
            m_key = (order.date.year, order.date.month)
            if m_key not in monthly_stats: monthly_stats[m_key] = {'qty': 0, 'sum': 0, 'count': 0}
            monthly_stats[m_key]['count'] += 1 

            order_fact_sum = 0 

            for item in order.items:
                fact_qty = item.shipped_quantity
                fact_sum = fact_qty * item.price
                
                reserved_qty = item.quantity - item.shipped_quantity
                funnel_sum['reserved'] += reserved_qty * item.price
                
                funnel_sum['shipped'] += fact_sum
                order_fact_sum += fact_sum

                if fact_qty > 0:
                    stats['total_qty'] += fact_qty
                    stats['total_sum'] += fact_sum
                    
                    monthly_stats[m_key]['qty'] += fact_qty
                    monthly_stats[m_key]['sum'] += fact_sum
                    
                    p_name = item.plant.name
                    s_name = item.size.name
                    
                    if p_name not in agg_plants:
                        agg_plants[p_name] = {'name': p_name, 'sum': 0, 'qty': 0, 'sizes': {}}
                    
                    agg_plants[p_name]['sum'] += fact_sum
                    agg_plants[p_name]['qty'] += fact_qty
                    
                    if s_name not in agg_plants[p_name]['sizes']:
                        agg_plants[p_name]['sizes'][s_name] = {'name': s_name, 'sum': 0, 'qty': 0}
                    
                    agg_plants[p_name]['sizes'][s_name]['sum'] += fact_sum
                    agg_plants[p_name]['sizes'][s_name]['qty'] += fact_qty

                    combo_key = (p_name, s_name)
                    flat_items_stats[combo_key] = flat_items_stats.get(combo_key, 0) + fact_sum

        # 2. Итоги
        if active_orders_count > 0:
            stats['avg_check'] = stats['total_sum'] / active_orders_count

        if funnel_sum['total_potential'] > 0:
            stats['status_funnel']['shipped_pct'] = (funnel_sum['shipped'] / funnel_sum['total_potential']) * 100
            stats['status_funnel']['canceled_pct'] = (funnel_sum['canceled'] / funnel_sum['total_potential']) * 100
            stats['status_funnel']['reserved_pct'] = (funnel_sum['reserved'] / funnel_sum['total_potential']) * 100

        if flat_items_stats:
            best_combo = max(flat_items_stats.items(), key=lambda x: x[1])
            stats['top_item'] = {'name': best_combo[0][0], 'size': best_combo[0][1], 'sum': best_combo[1]}

        sorted_plants = sorted(agg_plants.values(), key=lambda x: x['sum'], reverse=True)
        for p in sorted_plants:
            p['sizes_list'] = sorted(p['sizes'].values(), key=lambda x: x['sum'], reverse=True)
        stats['plants_breakdown'] = sorted_plants

        sorted_keys = sorted(monthly_stats.keys())
        
        season_spring = 'rgba(76, 175, 80, 0.6)'
        season_autumn = 'rgba(255, 152, 0, 0.6)'
        season_none = 'rgba(200, 200, 200, 0.3)'

        for (y, m) in sorted_keys:
            d = monthly_stats[(y, m)]
            label = f"{MONTH_NAMES[m]} {y}"
            stats['charts']['labels'].append(label)
            stats['charts']['qty'].append(d['qty'])
            stats['charts']['sum'].append(d['sum'])
            
            avg = d['sum'] / d['count'] if d['count'] > 0 else 0
            stats['charts']['avg'].append(avg)

            if 3 <= m <= 7: color = season_spring
            elif 8 <= m <= 12: color = season_autumn
            else: color = season_none
            stats['charts']['colors'].append(color)

    return render_template(
        'crm/crm_analytics.html',
        clients=clients,
        stats=stats,
        filters={'client_id': client_id, 'year_start': year_start, 'year_end': year_end},
        years_range=range(2017, msk_now().year + 2)
    )

@bp.route('/crm/abc')
@login_required
def crm_abc():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    
    start_year = request.args.get('year_start', 2017, type=int)
    end_year = request.args.get('year_end', msk_now().year, type=int)
    
    # 1. Получаем данные в разбивке: Растение + Размер
    # Исключаем удаленные и отмененные
    results = db.session.query(
        OrderItem.plant_id,
        OrderItem.size_id,
        func.sum(OrderItem.shipped_quantity * OrderItem.price).label('rev'),
        func.sum(OrderItem.shipped_quantity).label('qty'),
        func.count(func.distinct(func.strftime('%Y-%m', Order.date))).label('months')
    ).join(Order).filter(
        Order.is_deleted == False,
        Order.status != 'canceled',
        func.extract('year', Order.date) >= start_year,
        func.extract('year', Order.date) <= end_year,
        OrderItem.shipped_quantity > 0
    ).group_by(OrderItem.plant_id, OrderItem.size_id).all()

    # 2. Подготовка справочников
    from app.models import Plant, Size
    plants_map = {p.id: p.name for p in Plant.query.all()}
    sizes_map = {s.id: s.name for s in Size.query.all()}

    # 3. Собираем плоский список для расчета долей
    all_items = []
    total_revenue = 0
    
    for r in results:
        rev = float(r.rev)
        total_revenue += rev
        all_items.append({
            'plant_id': r.plant_id,
            'size_id': r.size_id,
            'plant_name': plants_map.get(r.plant_id, 'Unknown'),
            'size_name': sizes_map.get(r.size_id, 'Unknown'),
            'revenue': rev,
            'qty': r.qty,
            'months': r.months
        })

    # 4. Сортируем по выручке для расчета ABC
    all_items.sort(key=lambda x: x['revenue'], reverse=True)
    
    # 5. Расчет классов для каждой позиции (SKU)
    current_accum = 0
    grouped_data = {} # plant_id -> { summary, items: [] }

    for item in all_items:
        current_accum += item['revenue']
        share_accum = (current_accum / total_revenue * 100) if total_revenue > 0 else 0
        
        # ABC
        if share_accum <= 80: abc = 'A'
        elif share_accum <= 95: abc = 'B'
        else: abc = 'C'
        
        # XYZ (Стабильность)
        if item['months'] >= 8: xyz = 'X'
        elif item['months'] >= 4: xyz = 'Y'
        else: xyz = 'Z'
        
        item['abc'] = abc
        item['xyz'] = xyz
        item['matrix'] = f"{abc}{xyz}"
        item['share_accum'] = share_accum

        # Группировка по растению
        pid = item['plant_id']
        if pid not in grouped_data:
            pid = item['plant_id']
        if pid not in grouped_data:
            grouped_data[pid] = {
                'name': item['plant_name'],
                'total_revenue': 0,
                'total_qty': 0,
                'variants': [], # <--- ПЕРЕИМЕНОВАЛИ В variants
                'months_max': 0
            }
        
        grouped_data[pid]['variants'].append(item)
        grouped_data[pid]['total_revenue'] += item['revenue']
        grouped_data[pid]['total_qty'] += item['qty']
        if item['months'] > grouped_data[pid]['months_max']:
            grouped_data[pid]['months_max'] = item['months']

    # 6. Расчет ABC для ГРУПП (Растений целиком)
    # Преобразуем словарь в список и сортируем по общей выручке растения
    final_list = sorted(grouped_data.values(), key=lambda x: x['total_revenue'], reverse=True)
    
    plant_accum = 0
    for plant in final_list:
        plant_accum += plant['total_revenue']
        p_share = (plant_accum / total_revenue * 100) if total_revenue > 0 else 0
        
        if p_share <= 80: p_abc = 'A'
        elif p_share <= 95: p_abc = 'B'
        else: p_abc = 'C'
        
        # XYZ для растения берем по максимуму его размеров
        if plant['months_max'] >= 8: p_xyz = 'X'
        elif plant['months_max'] >= 4: p_xyz = 'Y'
        else: p_xyz = 'Z'
        
        plant['abc'] = p_abc
        plant['xyz'] = p_xyz
        plant['matrix'] = f"{p_abc}{p_xyz}"
        plant['share_accum'] = p_share
        
        # Сортируем размеры внутри растения (A -> C)
        plant['variants'].sort(key=lambda x: x['revenue'], reverse=True)

    return render_template('crm/crm_abc.html', 
                           data=final_list, 
                           filters={'start': start_year, 'end': end_year},
                           years=range(2017, msk_now().year + 2))

@bp.route('/crm/yoy')
@login_required
def crm_yoy():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    
    f_clients = [int(x) for x in request.args.getlist('client_id')]
    start_year = request.args.get('year_start', 2017, type=int)
    end_year = request.args.get('year_end', msk_now().year, type=int)
    
    q = db.session.query(
        func.extract('year', Order.date).label('yr'),
        func.extract('month', Order.date).label('mth'),
        func.sum(OrderItem.shipped_quantity * OrderItem.price)
    ).join(OrderItem).filter(
        Order.is_deleted == False,
        Order.status != 'canceled',
        func.extract('year', Order.date) >= start_year,
        func.extract('year', Order.date) <= end_year,
        OrderItem.shipped_quantity > 0
    )
    
    if f_clients:
        q = q.filter(Order.client_id.in_(f_clients))
        
    results = q.group_by('yr', 'mth').all()
    
    years_data = {}
    for yr in range(start_year, end_year + 1):
        years_data[yr] = [0] * 12
        
    for r in results:
        y, m, val = int(r[0]), int(r[1]), float(r[2])
        if y in years_data:
            years_data[y][m-1] = val
            
    colors = ['#4CAF50', '#2196F3', '#FF9800', '#F44336', '#9C27B0', '#795548', '#607D8B', '#00BCD4']
    chart_datasets = []
    i = 0
    for yr, data in years_data.items():
        if sum(data) > 0:
            chart_datasets.append({
                'label': str(yr),
                'data': data,
                'borderColor': colors[i % len(colors)],
                'backgroundColor': 'transparent',
                'tension': 0.3
            })
            i += 1

    return render_template('crm/crm_yoy.html', 
                           datasets=chart_datasets,
                           all_clients=Client.query.order_by(Client.name).all(),
                           selected_clients=f_clients,
                           filters={'start': start_year, 'end': end_year},
                           years=range(2017, msk_now().year + 2))

@bp.route('/crm/seasonality')
@login_required
def crm_seasonality():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    
    year = request.args.get('year', msk_now().year, type=int)
    
    results = db.session.query(
        Client.name,
        func.extract('month', Order.date).label('mth'),
        func.sum(OrderItem.shipped_quantity * OrderItem.price)
    ).join(Order, Client.id == Order.client_id)\
     .join(OrderItem, Order.id == OrderItem.order_id)\
     .filter(
        Order.is_deleted == False,
        Order.status != 'canceled',
        func.extract('year', Order.date) == year,
        OrderItem.shipped_quantity > 0
    ).group_by(Client.name, 'mth').all()
    
    matrix = {}
    max_val = 0
    
    for r in results:
        c_name, m, val = r[0], int(r[1]), float(r[2])
        if c_name not in matrix: matrix[c_name] = {'data': [0]*12, 'total': 0}
        matrix[c_name]['data'][m-1] = val
        matrix[c_name]['total'] += val
        if val > max_val: max_val = val
        
    sorted_matrix = sorted(matrix.items(), key=lambda x: x[1]['total'], reverse=True)
    
    return render_template('crm/crm_seasonality.html', 
                           matrix=sorted_matrix, 
                           max_val=max_val,
                           target_year=year,
                           years=range(2017, msk_now().year + 2),
                           month_names=MONTH_NAMES)

@bp.route('/crm/price_calculator', methods=['GET', 'POST'])
@login_required
def crm_price_calculator():
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))

    # ... (код обработки POST/загрузки файла оставляем без изменений, он выше) ...
    # Если ты используешь старый код, убедись, что блок if request.method == 'POST': остался на месте.
    # Я привожу код начиная с блока GET (отображения), где меняется логика сортировки.
    
    # Обработка загрузки файла (Только Админ)
    if request.method == 'POST':
        if current_user.role != 'admin':
            flash('Только администратор может загружать цены конкурентов')
            return redirect(url_for('crm.crm_price_calculator'))
            
        file = request.files.get('file')
        snap_name = request.form.get('name', f'Загрузка от {msk_now().strftime("%d.%m.%Y")}')
        
        if file:
            try:
                # 1. Кэш себестоимости и цен
                calc_year = msk_now().year - 1
                cost_data = calculate_cost_data(calc_year)
                base_cost = cost_data['cumulative_cost']
                
                our_prices_cache = {}
                stocks = db.session.query(Plant.name, Size.name, func.max(StockBalance.price))\
                    .join(Plant).join(Size)\
                    .group_by(Plant.name, Size.name).all()
                for p_name, s_name, price in stocks:
                    our_prices_cache[(p_name.lower().strip(), s_name.lower().strip())] = float(price or 0)

                # 2. Читаем Excel
                wb = load_workbook(file)
                ws = wb.active
                
                # --- ЛОГИКА ОБЪЕДИНЕНИЯ ПО ДАТЕ ---
                now = msk_now()
                # Определяем начало и конец сегодняшнего дня
                start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
                end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)

                # Ищем, есть ли уже отчет за сегодня
                snapshot = CompetitorSnapshot.query.filter(
                    CompetitorSnapshot.date >= start_of_day,
                    CompetitorSnapshot.date <= end_of_day
                ).first()

                if snapshot:
                    # Если нашли - обновляем имя (чтобы было видно название последней загрузки)
                    snapshot.name = snap_name
                    # Мы НЕ создаем новый объект, а используем найденный
                    flash(f'Данные добавлены в существующий отчет за сегодня.')
                else:
                    # Если отчета за сегодня нет - создаем новый
                    snapshot = CompetitorSnapshot(name=snap_name, user_id=current_user.id, date=now)
                    db.session.add(snapshot)
                
                db.session.flush() # Получаем ID (старого или нового отчета)
                # ----------------------------------
                
                count = 0
                for row in ws.iter_rows(min_row=2, values_only=True):
                    if not row[0] or not row[2]: continue
                    
                    p_name = str(row[0]).strip()
                    s_name = str(row[1]).strip() if row[1] else "-"
                    raw_price = str(row[2]) if row[2] else "0"
                    clean_price = raw_price.replace(' ', '').replace('\xa0', '').replace('руб', '').replace('р', '').replace(',', '.')
                    try: c_price = float(clean_price)
                    except ValueError: c_price = 0.0
                    comp_name = str(row[3]).strip() if len(row) > 3 and row[3] else "Неизвестно"
                    link_val = str(row[4]).strip() if len(row) > 4 and row[4] else None # <--- Читаем 5-ю колонку
                    
                    key = (p_name.lower(), s_name.lower())
                    my_price = our_prices_cache.get(key, 0)
                    
                    item = CompetitorRow(
                        snapshot_id=snapshot.id,
                        plant_name=p_name,
                        size_name=s_name,
                        competitor_name=comp_name,
                        competitor_price=c_price,
                        source_link=link_val, # <--- Записываем в базу
                        our_price_at_moment=my_price,
                        our_cost_at_moment=base_cost
                    )
                    db.session.add(item)
                    count += 1
                
                db.session.commit()
                log_action(f"Загрузил цены конкурентов: {snap_name} ({count} поз.)")
                flash(f'Успешно загружено {count} позиций.')
                return redirect(url_for('crm.crm_price_calculator', snapshot_id=snapshot.id))
                
            except Exception as e:
                db.session.rollback()
                flash(f'Ошибка обработки файла: {e}')
                
        return redirect(url_for('crm.crm_price_calculator'))

    # Отображение (GET)
    snapshot_id = request.args.get('snapshot_id', type=int)
    snapshots = CompetitorSnapshot.query.order_by(CompetitorSnapshot.date.desc()).all()
    
    current_snapshot = None
    rows_data = []
    
    if snapshot_id:
        current_snapshot = CompetitorSnapshot.query.get(snapshot_id)
    elif snapshots:
        current_snapshot = snapshots[0]
        
    if current_snapshot:
        # Подготовка данных
        for r in current_snapshot.rows:
            our_p = float(r.our_price_at_moment or 0)
            comp_p = float(r.competitor_price or 0)
            cost = float(r.our_cost_at_moment or 0)
            
            diff_rub = our_p - comp_p
            diff_pct = ((our_p - comp_p) / comp_p * 100) if comp_p > 0 else 0
            
            our_margin = our_p - cost
            comp_margin_scenario = comp_p - cost 
            
            rows_data.append({
                'id': r.id,
                'snapshot_id': r.snapshot_id,
                'plant': r.plant_name,
                'size': r.size_name,
                'competitor': r.competitor_name,
                'link': r.source_link, # <--- Передаем ссылку в шаблон
                'comp_price': comp_p,
                'our_price': our_p,
                'diff_rub': diff_rub,
                'cost': cost,
                'our_margin': our_margin,
                'comp_margin_scenario': comp_margin_scenario
            })
            
        # --- ЛОГИКА СОРТИРОВКИ ---
        import re
        def size_sort_key(s_str):
            # Извлекаем первое число из размера (например, "100-120" -> 100, "C3" -> 3)
            match = re.search(r'\d+', s_str)
            if match:
                return int(match.group())
            return 0

        # Сортировка: Сначала по Имени (А-Я), потом по Размеру (от большего к меньшему)
        rows_data.sort(key=lambda x: (x['plant'], -size_sort_key(x['size'])))

    # Список товаров для графика
    history_products = db.session.query(
        CompetitorRow.plant_name, 
        CompetitorRow.size_name
    ).distinct().order_by(CompetitorRow.plant_name, CompetitorRow.size_name).all()
    product_options = [f"{p} | {s}" for p, s in history_products]

     # Импортируем список регионов (если он в том же файле, просто берем переменную)
    from app.crm import RUSSIAN_REGIONS_LIST 

    return render_template('crm/crm_price_calc.html', 
                           snapshots=snapshots, 
                           current=current_snapshot,
                           rows=rows_data,
                           product_options=product_options,
                           regions_list=RUSSIAN_REGIONS_LIST) # <--- Передали список

@bp.route('/crm/price_calculator/template')
@login_required
def crm_price_calculator_template():
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Шаблон цен"
    
    # Заголовки (для понятности делаем их жирными и цветными не обязательно, главное текст)
    headers = ["Наименование (как в базе)", "Размер", "Цена конкурента", "Конкурент", "Ссылка (необязательно)"]
    ws.append(headers)
    
    # Пример данных
    ws.append(["Туя западная Смарагд", "C3", 850, "Питомник А", "http://example.com"])
    ws.append(["Сосна горная Мугус", "C10", 3500, "Садовый Центр Б", ""])
    
    # Настройка ширины колонок для удобства
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 15
    ws.column_dimensions['C'].width = 15
    ws.column_dimensions['D'].width = 25
    ws.column_dimensions['E'].width = 25
    
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    
    return send_file(buf, download_name='competitor_prices_template.xlsx', as_attachment=True)

@bp.route('/crm/price_calculator/delete/<int:snapshot_id>', methods=['POST'])
@login_required
def crm_delete_snapshot(snapshot_id):
    if current_user.role != 'admin':
        flash('Только администратор может удалять снимки')
        return redirect(url_for('crm.crm_price_calculator'))
        
    try:
        snapshot = CompetitorSnapshot.query.get_or_404(snapshot_id)
        db.session.delete(snapshot)
        db.session.commit()
        log_action(f"Удалил сравнение цен конкурентов: {snapshot.name}")
        flash('Сравнение цен удалено')
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка при удалении: {e}')
        
    return redirect(url_for('crm.crm_price_calculator'))


@bp.route('/crm/price_calculator/delete_row/<int:row_id>', methods=['POST'])
@login_required
def crm_delete_row(row_id):
    if current_user.role != 'admin':
        flash('Только администратор может удалять строки')
        return redirect(url_for('crm.crm_price_calculator'))

    try:
        row = CompetitorRow.query.get_or_404(row_id)
        snapshot_id = row.snapshot_id
        db.session.delete(row)
        db.session.commit()
        log_action(f"Удалил строку сравнения: {row.plant_name} / {row.size_name} (конкурент {row.competitor_name})")
        flash('Строка удалена')
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка при удалении строки: {e}')
        snapshot_id = request.args.get('snapshot_id')

    return redirect(url_for('crm.crm_price_calculator', snapshot_id=snapshot_id))

@bp.route('/crm/api/price_history', methods=['POST'])
@login_required
def crm_api_price_history():
    # Получаем "Растение | Размер"
    selection = request.json.get('selection', '')
    if not selection or ' | ' not in selection:
        return jsonify({'error': 'Invalid selection'})
    
    plant, size = selection.split(' | ', 1)
    
    # Ищем все записи по этому товару, сортируем по дате загрузки
    rows = db.session.query(CompetitorRow, CompetitorSnapshot.date)\
        .join(CompetitorSnapshot)\
        .filter(CompetitorRow.plant_name == plant, CompetitorRow.size_name == size)\
        .order_by(CompetitorSnapshot.date).all()
    
    if not rows:
        return jsonify({'error': 'No data'})

    # Собираем данные
    dates = []
    datasets = {} # { 'Наш питомник': [100, 120...], 'Конкурент А': [90, 95...] }
    
    # 1. Собираем все уникальные даты
    sorted_rows = sorted(rows, key=lambda x: x[1])
    unique_dates = sorted(list(set([r[1].strftime('%d.%m.%Y') for r in sorted_rows])))
    
    for d in unique_dates:
        dates.append(d)
    
    # Инициализируем массивы нулями/None
    datasets['Наш Питомник'] = [None] * len(dates)
    
    # 2. Заполняем данными
    for row_obj, snap_date in rows:
        d_str = snap_date.strftime('%d.%m.%Y')
        idx = dates.index(d_str)
        
        # Наша цена (она одна)
        if row_obj.our_price_at_moment:
            datasets['Наш Питомник'][idx] = float(row_obj.our_price_at_moment)
            
        # Цена конкурента
        c_name = row_obj.competitor_name
        if c_name not in datasets:
            datasets[c_name] = [None] * len(dates)
        
        if row_obj.competitor_price:
            datasets[c_name][idx] = float(row_obj.competitor_price)

    return jsonify({'labels': dates, 'datasets': datasets})

@bp.route('/crm/download_prompt_input_template')
@login_required
def crm_download_prompt_input_template():
    wb = Workbook()
    ws = wb.active
    ws.title = "Список растений"
    # Заголовки для пользователя
    ws.append(["Наименование", "Размер", "Стрижка (шар/стрижка/пусто)"])
    # Примеры
    ws.append(["Туя западная Smaragd", "100-120", ""])
    ws.append(["Спирея серая", "80-100", "шар"])
    
    # Ширина колонок
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 15
    ws.column_dimensions['C'].width = 20
    
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, download_name='prompt_plant_list_template.xlsx', as_attachment=True)

@bp.route('/crm/download_ai_prompt', methods=['GET', 'POST'])
@login_required
def crm_download_ai_prompt():
    # Если просто открыли ссылку (GET) - редирект или ошибка, так как нужна форма
    if request.method != 'POST':
        return redirect(url_for('crm.crm_price_calculator'))

    # 1. Получаем настройки из формы
    regions = request.form.getlist('regions') # Список выбранных регионов
    if not regions:
        regions_str = "ЦФО (Москва и МО, Тула, Воронеж, Калуга, Тверь и др.)"
    else:
        regions_str = ", ".join(regions)

    # 2. Обрабатываем файл с растениями
    file = request.files.get('plant_file')
    items_text = ""
    
    if file:
        try:
            wb = load_workbook(file)
            ws = wb.active
            unique_items = set()
            # Читаем со 2-й строки
            for row in ws.iter_rows(min_row=2, values_only=True):
                if not row[0]: continue # Пропуск пустых имен
                
                p_name = str(row[0]).strip()
                s_name = str(row[1]).strip() if len(row) > 1 and row[1] else ""
                haircut = str(row[2]).strip() if len(row) > 2 and row[2] else ""
                
                # Формируем строку: Растение | Размер | Стрижка
                item_str = f"{p_name} | {s_name} | {haircut}"
                unique_items.add(item_str)
            
            items_text = "\n".join(sorted(list(unique_items)))
        except Exception as e:
            flash(f"Ошибка чтения файла: {e}")
            return redirect(url_for('crm.crm_price_calculator'))
    else:
        items_text = "Список растений не был загружен. Вставьте сюда данные вручную."

    # 3. Формируем текст промта (Новый шаблон)
    prompt_text = f"""Роль: Ты профессиональный аналитик рынка посадочного материала и ландшафтного дизайна со стажем работы на рынке более 20 лет.
Задача: Провести анализ рынка декоративных растений в РФ на основе предоставленного списка. Необходимо найти актуальные предложения конкурентов (питомники, садовые центры).

Вводные данные (Словарь):
Я предоставлю тебе список позиций в формате: "Растение | Размер | Стрижка".

Строгие правила поиска и фильтрации:
1. Регион поиска: {regions_str}.
2. Тип товара: Ищем ТОЛЬКО растения в комах (RB / WRB / грунт).
3. ИСКЛЮЧИТЬ: Растения в пластиковых контейнерах (P9, C2, C3, C5, C10 и т.д.). Если в прайсе указан литраж горшка — пропускаем. Нас интересует только "ком", "сетка", "мешковина".
4. Форма (Стрижка):
   - Если в столбце "Стрижка" указано "шар" или "стрижка" — ищи именно формованные растения (топиары, бонсаи, ниваки или стриженые шары). Обычные кусты/деревья не подходят.
   - Если столбец "Стрижка" пуст — ищи классическую форму (свободнорастущие).
5. Размеры: Ищи позиции, максимально близкие к указанным.

Правила формирования ответа (Таблица):
1. Результат должен быть строго в виде таблицы.
2. Столбцы таблицы:
   - Наименование (как в базе)
   - Размер (как в базе)
   - Цена конкурента (число)
   - Конкурент (Название)
   - Ссылка (URL)
3. ВАЖНО ПО ФОРМАТИРОВАНИЮ: В столбцах "Наименование" и "Размер" ты должен использовать ТОЛЬКО текст из моего исходного списка.
   - Пример: Если конкурент продает "Туя Смарагд 105-125 см", ты должен записать её как "Туя западная "Smaragd"" и размер "100-120" (наиболее подходящий из моего списка). Не выдумывай новые диапазоны.
   - Столбец "Стрижка" в итоговую таблицу НЕ добавлять.
4. Столбец "Ссылка": Вставь прямой URL на страницу товара или страницу прайс-листа, где была найдена эта цена. и напиши ее полностью, чтобы она отобразилась в чате целиком.
5. Мультипликативность: Если на одну позицию найдено 5 разных конкурентов — в таблице должно быть 5 строк. Не группируй их.

СПИСОК РАСТЕНИЙ ДЛЯ АНАЛИЗА:
{items_text}
"""
    
    # 4. Отдаем файл
    buf = io.BytesIO()
    buf.write(prompt_text.encode('utf-8'))
    buf.seek(0)
    return send_file(buf, download_name='market_analysis_prompt.txt', mimetype='text/plain', as_attachment=True)