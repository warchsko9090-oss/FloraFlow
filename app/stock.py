import json
import io
import os
from datetime import datetime, date
from decimal import Decimal
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file, current_app, send_from_directory, make_response
from flask_login import login_required, current_user
from sqlalchemy import func, or_
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from app.models import (
    db, Plant, Size, Field, StockBalance, Document, DocumentRow, 
    AppSetting, PriceHistory, Order, OrderItem, Client, ActionLog
)
from app.utils import log_action, get_or_create_stock, msk_now, natural_key, apply_excel_styles
from app.services import calculate_cost_data
from app.stock_helpers import get_reserved_map

bp = Blueprint('stock', __name__)


def _group_recount_rows(doc):
    """Разделяет строки пересчета на приход/списание для карточки."""
    rows_in = []
    rows_out = []
    total_in = 0
    total_out = 0
    field_name = ''

    sorted_rows = sorted(
        doc.rows,
        key=lambda r: (
            r.plant.name if r.plant else '',
            r.size.name if r.size else '',
            r.year or 0
        )
    )
    for row in sorted_rows:
        qty = int(row.quantity or 0)
        if not field_name and row.field_to:
            field_name = row.field_to.name
        item = {
            'plant_name': row.plant.name if row.plant else '-',
            'size_name': row.size.name if row.size else '-',
            'year': row.year,
            'qty': abs(qty),
            'delta': qty,
            'row': row
        }
        if qty >= 0:
            rows_in.append(item)
            total_in += abs(qty)
        else:
            rows_out.append(item)
            total_out += abs(qty)

    return {
        'doc': doc,
        'field_name': field_name or '-',
        'rows_in': rows_in,
        'rows_out': rows_out,
        'total_in': total_in,
        'total_out': total_out
    }


def build_stock_report_data(report_mode, end_date, selected_fields=None, selected_plants=None, selected_sizes=None, selected_years=None, all_plants=None, all_sizes=None, all_fields=None):
    """Собирает данные для отчета по остаткам (та же логика, что и в stock_report).

    Возвращает кортеж (sorted_groups, grand_total).
    """
    # --- Загрузочные справочники ---
    if all_plants is None:
        all_plants = sorted(Plant.query.all(), key=lambda x: x.name)
    if all_sizes is None:
        all_sizes = sorted(Size.query.all(), key=natural_key)
    if all_fields is None:
        all_fields = sorted(Field.query.all(), key=natural_key)

    if selected_fields is None or not selected_fields:
        selected_fields = [f.id for f in all_fields]

    # --- Движения документов (Приходы, Списания, Перемещения) ---
    movements = db.session.query(
        DocumentRow.plant_id,
        DocumentRow.size_id,
        DocumentRow.field_to_id,
        DocumentRow.field_from_id,
        DocumentRow.quantity,
        Document.doc_type,
        DocumentRow.year,
        DocumentRow.size_to_id
    ).join(Document).filter(Document.date <= end_date).all()

    fact_map, income_map, shipped_map = {}, {}, {}
    for m in movements:
        pid, sid, qty, dtype, byear = m.plant_id, m.size_id, m.quantity, m.doc_type, m.year
        if dtype in ['income', 'correction', 'field_recount']:
            if m.field_to_id in selected_fields:
                k = (pid, sid, m.field_to_id, byear)
                fact_map[k] = fact_map.get(k, 0) + qty
                income_map[k] = income_map.get(k, 0) + qty
        elif dtype in ['writeoff', 'shipment']:
            if m.field_from_id in selected_fields:
                k = (pid, sid, m.field_from_id, byear)
                fact_map[k] = fact_map.get(k, 0) - qty
                if dtype == 'shipment':
                    shipped_map[k] = shipped_map.get(k, 0) + qty
        elif dtype == 'move':
            if m.field_from_id in selected_fields:
                k = (pid, sid, m.field_from_id, byear)
                fact_map[k] = fact_map.get(k, 0) - qty
            if m.field_to_id in selected_fields:
                k = (pid, sid, m.field_to_id, byear)
                fact_map[k] = fact_map.get(k, 0) + qty
        elif dtype == 'regrading':
            if m.field_from_id in selected_fields:
                k_old = (pid, sid, m.field_from_id, byear)
                fact_map[k_old] = fact_map.get(k_old, 0) - qty
                k_new = (pid, m.size_to_id, m.field_from_id, byear)
                fact_map[k_new] = fact_map.get(k_new, 0) + qty

    # --- Резервы (из Заказов) ---
    active_items = db.session.query(OrderItem).join(Order).filter(Order.status != 'canceled', Order.status != 'ghost', Order.is_deleted == False).all()
    reserve_map = {}
    for item in active_items:
        if item.field_id in selected_fields:
            k = (item.plant_id, item.size_id, item.field_id, item.year)
            res_q = item.quantity - item.shipped_quantity
            if res_q > 0:
                reserve_map[k] = reserve_map.get(k, 0) + res_q

    # --- Собираем уникальные ключи ---
    all_keys = set(fact_map.keys()) | set(income_map.keys()) | set(reserve_map.keys())

    plants_dict = {p.id: p for p in all_plants}
    sizes_dict = {s.id: s.name for s in all_sizes}
    fields_dict = {f.id: f.name for f in all_fields}

    # --- Логика цен и себестоимости ---
    target_price_year = end_date.year
    try:
        cost_data = calculate_cost_data(target_price_year)
        current_unit_cost = float(cost_data['summary_totals'].get(target_price_year, {}).get('total_unit', 0))
    except:
        current_unit_cost = 0.0

    hist_prices_q = PriceHistory.query.all()
    hist_prices_map = {(h.plant_id, h.size_id, h.field_id, h.year): h.price for h in hist_prices_q}
    stock_prices_q = StockBalance.query.all()
    stock_prices_map = {(sb.plant_id, sb.size_id, sb.field_id, sb.year): sb.price for sb in stock_prices_q}

    aggregated = {}
    grand_total = {'income': 0, 'reserved': 0, 'free': 0, 'shipped': 0, 'qty': 0, 'sum': 0, 'free_sum': 0}

    for (pid, sid, fid, byear) in all_keys:
        if selected_plants and pid not in selected_plants:
            continue
        if selected_sizes and sid not in selected_sizes:
            continue
        if selected_years and byear not in selected_years:
            continue

        fact = fact_map.get((pid, sid, fid, byear), 0)
        inc = income_map.get((pid, sid, fid, byear), 0)
        res = reserve_map.get((pid, sid, fid, byear), 0)
        shp = shipped_map.get((pid, sid, fid, byear), 0)
        if fact == 0 and inc == 0 and res == 0 and shp == 0:
            continue

        size_name = sizes_dict.get(sid, '')
        is_netov = 'нетов' in size_name.lower()

        if is_netov:
            calc_price = current_unit_cost
            display_price = current_unit_cost
        else:
            actual_price = hist_prices_map.get((pid, sid, fid, target_price_year))
            if actual_price is None:
                actual_price = hist_prices_map.get((pid, sid, fid, byear))
            if actual_price is None:
                actual_price = stock_prices_map.get((pid, sid, fid, byear), 0)
            calc_price = float(actual_price)
            display_price = calc_price

        batch_sum = calc_price * fact

        if report_mode == 'inventory':
            k_agg = (pid, fid, byear)
            size_val = None
        else:
            k_agg = (pid, sid)
            size_val = size_name

        pl_obj = plants_dict.get(pid)

        if k_agg not in aggregated:
            aggregated[k_agg] = {
                'plant_id': pid,
                'size_id': sid if report_mode != 'inventory' else 0,
                'plant': pl_obj.name if pl_obj else 'Unknown',
                'latin_name': pl_obj.latin_name if pl_obj else '',
                'characteristic': pl_obj.characteristic if pl_obj else '',
                'size': size_val,
                'fields': set(),
                'field_id': fid if report_mode == 'inventory' else None,
                'year': byear if report_mode == 'inventory' else None,
                'price': display_price,
                'income': 0,
                'reserved': 0,
                'shipped': 0,
                'quantity': 0,
                'total_val': 0
            }

        aggregated[k_agg]['fields'].add(fields_dict.get(fid, 'Unknown'))
        aggregated[k_agg]['income'] += inc
        aggregated[k_agg]['reserved'] += res
        aggregated[k_agg]['shipped'] += shp
        aggregated[k_agg]['quantity'] += fact
        aggregated[k_agg]['total_val'] += batch_sum

        if not is_netov and display_price > aggregated[k_agg]['price']:
            aggregated[k_agg]['price'] = display_price

    grouped_final = {}
    for k, v in aggregated.items():
        free = v['quantity'] - v['reserved']
        sm = v['total_val']
        free_sm = (v['price'] or 0) * free
        row = {
            'plant_id': v['plant_id'],
            'size_id': v['size_id'],
            'field_id': v.get('field_id'),
            'size': v['size'],
            'fields_str': ", ".join(sorted(list(v['fields']))),
            'characteristic': v['characteristic'],
            'latin_name': v['latin_name'],
            'year': v.get('year'),
            'price': v['price'],
            'sum': sm,
            'free_sum': free_sm,
            'income': v['income'],
            'reserved': v['reserved'],
            'free': free,
            'shipped': v['shipped'],
            'quantity': v['quantity']
        }

        p_name = v['plant']
        p_latin = v['latin_name']
        if p_name not in grouped_final:
            grouped_final[p_name] = {'latin_name': p_latin, 'rows': [], 'totals': {'sum': 0, 'free_sum': 0, 'income': 0, 'reserved': 0, 'free': 0, 'shipped': 0, 'qty': 0}}

        grouped_final[p_name]['rows'].append(row)
        t = grouped_final[p_name]['totals']
        t['sum'] += sm
        t['free_sum'] += free_sm
        t['income'] += v['income']
        t['reserved'] += v['reserved']
        t['free'] += free
        t['shipped'] += v['shipped']
        t['qty'] += v['quantity']
        grand_total['sum'] += sm
        grand_total['free_sum'] += free_sm
        grand_total['income'] += v['income']
        grand_total['reserved'] += v['reserved']
        grand_total['free'] += free
        grand_total['shipped'] += v['shipped']
        grand_total['qty'] += v['quantity']

    sorted_groups = []
    for p_name in sorted(grouped_final.keys()):
        group = grouped_final[p_name]
        if report_mode == 'inventory':
            group['rows'].sort(key=lambda x: (x.get('year') or 0, x['fields_str']))
        else:
            group['rows'].sort(key=lambda x: (natural_key(x['size']), x['fields_str']))
        sorted_groups.append({'name': p_name, 'data': group})

    return sorted_groups, grand_total


@bp.route('/price_history', methods=['GET', 'POST'])
@login_required
def price_history():
    if current_user.role != 'admin': 
        return redirect(url_for('directory.directory'))
    
    if request.method == 'POST':
        act = request.form.get('action')
        if act == 'add':
            p = int(request.form.get('plant_id'))
            s = int(request.form.get('size_id'))
            f = int(request.form.get('field_id'))
            y = int(request.form.get('year'))
            pr = float(request.form.get('price'))
            
            ex = PriceHistory.query.filter_by(plant_id=p, size_id=s, field_id=f, year=y).first()
            if ex: ex.price = pr
            else: db.session.add(PriceHistory(plant_id=p, size_id=s, field_id=f, year=y, price=pr))
            
            db.session.commit()
            log_action(f"Установил цену {pr} для PlantID {p}")
        elif act == 'delete':
            PriceHistory.query.filter_by(id=request.form.get('id')).delete()
            db.session.commit()
        return redirect(url_for('stock.price_history'))
        
    return render_template('stock/price_history.html', 
                           history=PriceHistory.query.order_by(PriceHistory.year.desc()).all(), 
                           plants=sorted(Plant.query.all(), key=lambda x: x.name), 
                           sizes=sorted(Size.query.all(), key=natural_key), 
                           fields=sorted(Field.query.all(), key=natural_key), 
                           current_year=msk_now().year)

@bp.route('/stock', methods=['GET', 'POST'])
@login_required
def stock_report():
    report_mode = request.args.get('mode', 'product')
    end_date_str = request.args.get('end_date')
    # Если дата не выбрана, берем текущий момент
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59) if end_date_str else msk_now()
    
    # --- Фильтры ---
    selected_sizes =[int(x) for x in request.args.getlist('filter_size')]
    
    # АВТОФИЛЬТР: Если фильтр пуст и режим "По товарам" -> выбираем все, кроме "Нетов" и "Саженцы"
    if not selected_sizes and report_mode == 'product':
        all_s = Size.query.all()
        excluded_ids =[]
        for s in all_s:
            name_lower = s.name.lower()
            if 'нетов' in name_lower or 'саженцы' in name_lower:
                excluded_ids.append(s.id)
                
        if excluded_ids:
            selected_sizes = [s.id for s in all_s if s.id not in excluded_ids]
        else:
            selected_sizes = [s.id for s in all_s]
            
    selected_fields = [int(x) for x in request.args.getlist('filter_field')]
    selected_plants = [int(x) for x in request.args.getlist('filter_plant')] 
    selected_years = [int(x) for x in request.args.getlist('filter_year')]

    # Обработка POST
    if request.method == 'POST':
        if current_user.role == 'admin':
            # Логика обновления цены
            if 'update_price' in request.form:
                p_id, s_id = request.form.get('plant_id'), request.form.get('size_id')
                try:
                    new_price = float(request.form.get('price').replace(' ', '').replace('₽', ''))
                    stocks = StockBalance.query.filter_by(plant_id=p_id, size_id=s_id).all()
                    for st in stocks: st.price = new_price
                    db.session.commit()
                    log_action(f"Обновил цену в остатках PlantID {p_id}")
                except ValueError: pass
            
            # Логика обновления ИНФО (Латинское название)
            elif 'update_info' in request.form:
                p_id = request.form.get('plant_id')
                latin_val = request.form.get('latin_name')
                plant = Plant.query.get(p_id)
                if plant:
                    plant.latin_name = latin_val
                    db.session.commit()
                    log_action(f"Обновил латинское название для {plant.name}")

        return redirect(url_for('stock.stock_report', end_date=request.args.get('end_date'), mode=report_mode, filter_field=selected_fields, filter_plant=selected_plants, filter_size=selected_sizes, filter_year=selected_years))
    
    # --- Загрузка данных ---
    all_plants = sorted(Plant.query.all(), key=lambda x: x.name)
    all_sizes = sorted(Size.query.all(), key=natural_key)
    all_fields = sorted(Field.query.all(), key=natural_key)
    all_years = sorted([r[0] for r in db.session.query(StockBalance.year).distinct().all() if r[0] is not None])

    sorted_groups, grand_total = build_stock_report_data(
        report_mode,
        end_date,
        selected_fields=selected_fields,
        selected_plants=selected_plants,
        selected_sizes=selected_sizes,
        selected_years=selected_years,
        all_plants=all_plants,
        all_sizes=all_sizes,
        all_fields=all_fields,
    )

    # NOTE: sorted_groups already contains all grouping/totals data
    
    print_settings_obj = AppSetting.query.get('print_header_settings')
    print_settings = json.loads(print_settings_obj.value) if print_settings_obj else {}
    
    return render_template('stock/stock.html', 
                           report_data=sorted_groups, 
                           grand_total=grand_total, 
                           end_date=end_date.strftime('%Y-%m-%d'), 
                           report_mode=report_mode, 
                           all_plants=all_plants, selected_plants=selected_plants, 
                           all_sizes=all_sizes, selected_sizes=selected_sizes, 
                           all_fields=all_fields, selected_fields=selected_fields, 
                           all_years=all_years, selected_years=selected_years, 
                           print_settings=print_settings)

@bp.route('/stock/export')
@login_required
def stock_report_export():
    report_mode = request.args.get('mode', 'product')
    end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').replace(hour=23, minute=59) if request.args.get('end_date') else msk_now()

    # Фильтры (те же, что и на основном отчёте)
    selected_sizes = [int(x) for x in request.args.getlist('filter_size')]
    if not selected_sizes and report_mode == 'product':
        all_s = Size.query.all()
        excluded_ids = [s.id for s in all_s if 'нетов' in s.name.lower() or 'саженцы' in s.name.lower()]
        selected_sizes = [s.id for s in all_s if s.id not in excluded_ids] if excluded_ids else [s.id for s in all_s]

    selected_fields = [int(x) for x in request.args.getlist('filter_field')]
    selected_plants = [int(x) for x in request.args.getlist('filter_plant')]
    selected_years = [int(x) for x in request.args.getlist('filter_year')]

    all_plants = sorted(Plant.query.all(), key=lambda x: x.name)
    all_sizes = sorted(Size.query.all(), key=natural_key)
    all_fields = sorted(Field.query.all(), key=natural_key)

    sorted_groups, grand_total = build_stock_report_data(
        report_mode,
        end_date,
        selected_fields=selected_fields,
        selected_plants=selected_plants,
        selected_sizes=selected_sizes,
        selected_years=selected_years,
        all_plants=all_plants,
        all_sizes=all_sizes,
        all_fields=all_fields,
    )

    # --- Создание Excel ---
    wb = Workbook()
    ws = wb.active
    ws.title = "Остатки"

    # Стили (ближе к экспорту заказов)
    title_fill = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")
    title_font = Font(bold=True, color="FFFFFF", size=14)
    header_fill = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    header_font = Font(bold=True, color="000000")
    group_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    border_total = Border(top=Side(style='thick'))
    align_center = Alignment(horizontal="center", vertical="center")
    align_right = Alignment(horizontal="right", vertical="center")

    # Колонки, которые присутствуют в таблице на экране
    columns = ["Растение"]
    include_size = report_mode != 'inventory'
    if include_size:
        columns.append("Размер")

    columns.append("Поле")

    include_price = current_user.role != 'user2'
    if include_price:
        columns.append("Цена")

    columns.extend(["Резерв", "Отгружено", "Свободно", "Факт"])

    if include_price:
        columns.append("Сумма")

    if report_mode == 'inventory':
        columns.append("Год")

    # Заголовок файла
    title = f"Остатки на {end_date.strftime('%d.%m.%Y')} ({'Товарные' if report_mode=='product' else 'По полям'})"
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(columns))
    title_cell = ws.cell(row=1, column=1, value=title)
    title_cell.fill = title_fill
    title_cell.font = title_font
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 26

    # Заголовки столбцов
    header_row = 2
    for col_idx, col_name in enumerate(columns, start=1):
        cell = ws.cell(row=header_row, column=col_idx, value=col_name)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = align_center
        cell.border = thin_border

    row_idx = header_row + 1

    # Данные: группы + детали
    for group in sorted_groups:
        # Группирующая строка
        gcell = ws.cell(row=row_idx, column=1, value=group['name'])
        gcell.font = Font(bold=True)
        gcell.fill = group_fill
        gcell.border = thin_border

        def _set_group_value(col_name, value):
            if col_name in columns:
                idx = columns.index(col_name) + 1
                c = ws.cell(row=row_idx, column=idx, value=value)
                c.font = Font(bold=True)
                c.fill = group_fill
                c.border = thin_border
                c.alignment = align_center

        _set_group_value("Резерв", group['data']['totals']['reserved'])
        _set_group_value("Отгружено", group['data']['totals']['shipped'])
        _set_group_value("Свободно", group['data']['totals']['free'])
        _set_group_value("Факт", group['data']['totals']['qty'])
        if include_price:
            _set_group_value("Сумма", float(group['data']['totals']['sum']))

        row_idx += 1

        # Строки деталей
        for row in group['data']['rows']:
            col = 1
            ws.cell(row=row_idx, column=col, value="").border = thin_border
            col += 1

            if include_size:
                ws.cell(row=row_idx, column=col, value=row.get('size') or '').border = thin_border
                col += 1

            ws.cell(row=row_idx, column=col, value=row.get('fields_str')).border = thin_border
            col += 1

            if include_price:
                c_price = ws.cell(row=row_idx, column=col, value=float(row.get('price') or 0))
                c_price.number_format = '#,##0.00'
                c_price.alignment = align_right
                c_price.border = thin_border
                col += 1

            for field_name in ["reserved", "shipped", "free", "quantity"]:
                c = ws.cell(row=row_idx, column=col, value=row.get(field_name) or 0)
                c.alignment = align_center
                c.border = thin_border
                col += 1

            if include_price:
                c_sum = ws.cell(row=row_idx, column=col, value=float(row.get('sum') or 0))
                c_sum.number_format = '#,##0.00'
                c_sum.font = Font(bold=True)
                c_sum.alignment = align_right
                c_sum.border = thin_border
                col += 1

            if report_mode == 'inventory':
                c_year = ws.cell(row=row_idx, column=col, value=row.get('year'))
                c_year.alignment = align_center
                c_year.border = thin_border

            row_idx += 1

    # Итоговая строка
    total_label = ws.cell(row=row_idx, column=1, value="ИТОГО")
    total_label.font = Font(bold=True)
    total_label.border = border_total

    def _write_total(col_name, value, number_format=None):
        if col_name in columns:
            idx = columns.index(col_name) + 1
            c = ws.cell(row=row_idx, column=idx, value=value)
            c.font = Font(bold=True)
            c.border = border_total
            c.alignment = align_center if number_format is None else align_right
            if number_format:
                c.number_format = number_format

    _write_total("Резерв", grand_total['reserved'])
    _write_total("Отгружено", grand_total['shipped'])
    _write_total("Свободно", grand_total['free'])
    _write_total("Факт", grand_total['qty'])
    if include_price:
        _write_total("Сумма", float(grand_total['sum'] or 0), number_format='#,##0.00 "₽"')

    # Автоширина колонок
    # Берем номера колонок по индексам (чтобы избежать ошибок с MergedCell)
    for idx, col in enumerate(ws.columns, start=1):
        max_length = 0
        for cell in col:
            if cell.value is None:
                continue
            length = len(str(cell.value))
            if length > max_length:
                max_length = length
        ws.column_dimensions[get_column_letter(idx)].width = max_length + 2

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f'stock_export_{end_date.strftime("%Y%m%d")}.xlsx'
    response = make_response(buf.getvalue())
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

@bp.route('/stock/save_settings', methods=['POST'])
@login_required
def save_stock_settings():
    if current_user.role != 'admin': return jsonify({'status': 'error'})
    data = request.json
    setting = AppSetting.query.get('stock_widths')
    if not setting: 
        setting = AppSetting(key='stock_widths')
        db.session.add(setting)
    setting.value = json.dumps(data)
    db.session.commit()
    return jsonify({'status': 'ok'})

@bp.route('/settings/save_print_header', methods=['POST'])
@login_required
def save_print_header():
    if current_user.role != 'admin': return redirect(url_for('stock.stock_report'))
    
    setting = AppSetting.query.get('print_header_settings')
    data = json.loads(setting.value) if setting and setting.value else {}

    data.update({
        'company_name': request.form.get('company_name'),
        'phone': request.form.get('phone'),
        'email': request.form.get('email'),
        'website': request.form.get('website'),
        'footer_text': request.form.get('footer_text')
    })
    
    def save_file_safely(field_name, target_name):
        file = request.files.get(field_name)
        if file and file.filename and file.filename.strip() != '':
            try:
                ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'jpg'
                filename = f"{target_name}.{ext}"
                save_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
                file.save(save_path)
                data[f'{target_name}_url'] = url_for('main.serve_uploaded_file', filename=filename)
            except Exception as e:
                print(f"Error saving {field_name}: {e}")

    save_file_safely('logo', 'company_logo')
    save_file_safely('photo1', 'promo_photo_1')
    save_file_safely('photo2', 'promo_photo_2')
    save_file_safely('photo3', 'promo_photo_3')

    if not setting:
        setting = AppSetting(key='print_header_settings')
        db.session.add(setting)
    
    setting.value = json.dumps(data)
    db.session.commit()
    
    flash('Настройки печати и фото сохранены')
    return redirect(url_for('stock.stock_report'))


@bp.route('/api/field_recount/items')
@login_required
def api_field_recount_items():
    """Возвращает фактические остатки по выбранному полю для формы пересчета."""
    field_id = request.args.get('field_id', type=int)
    if not field_id:
        return jsonify({'items': []})

    # Считаем ФАКТ по документам (аналогично отчету по остаткам), а не из StockBalance,
    # чтобы не зависеть от возможного рассинхрона таблицы остатков.
    fact_map = {}
    movements = db.session.query(
        Document.doc_type,
        DocumentRow.plant_id,
        DocumentRow.size_id,
        DocumentRow.size_to_id,
        DocumentRow.field_from_id,
        DocumentRow.field_to_id,
        DocumentRow.year,
        DocumentRow.quantity
    ).join(Document).all()

    for m in movements:
        dtype = m.doc_type
        pid = m.plant_id
        sid = m.size_id
        yr = m.year
        qty = int(m.quantity or 0)

        if dtype in ['income', 'correction', 'field_recount']:
            if m.field_to_id == field_id:
                k = (pid, sid, yr)
                fact_map[k] = fact_map.get(k, 0) + qty
        elif dtype in ['writeoff', 'shipment']:
            if m.field_from_id == field_id:
                k = (pid, sid, yr)
                fact_map[k] = fact_map.get(k, 0) - qty
        elif dtype == 'move':
            if m.field_from_id == field_id:
                k = (pid, sid, yr)
                fact_map[k] = fact_map.get(k, 0) - qty
            if m.field_to_id == field_id:
                k = (pid, sid, yr)
                fact_map[k] = fact_map.get(k, 0) + qty
        elif dtype == 'regrading':
            if m.field_from_id == field_id:
                k_old = (pid, sid, yr)
                fact_map[k_old] = fact_map.get(k_old, 0) - qty
                if m.size_to_id:
                    k_new = (pid, m.size_to_id, yr)
                    fact_map[k_new] = fact_map.get(k_new, 0) + qty

    plants_map = {p.id: p.name for p in Plant.query.all()}
    sizes_map = {s.id: s.name for s in Size.query.all()}
    rmap = get_reserved_map()
    items = []
    for (pid, sid, yr), fact_qty in fact_map.items():
        if fact_qty <= 0:
            continue
        reserved = int(rmap.get((pid, sid, field_id, yr), 0) or 0)
        free = max(0, int(fact_qty) - reserved)
        items.append({
            'plant_id': pid,
            'plant_name': plants_map.get(pid, '-'),
            'size_id': sid,
            'size_name': sizes_map.get(sid, '-'),
            'year': yr,
            'current_qty': int(fact_qty),
            'reserved': reserved,
            'free': free,
        })
    items.sort(key=lambda x: (natural_key(x['plant_name']), natural_key(x['size_name']), x['year'] or 0))
    return jsonify({'items': items})

@bp.route('/documents', methods=['GET', 'POST'])
@login_required
def documents():
    if request.method == 'POST':
        dt = request.form.get('doc_type')
        if dt in ['income', 'writeoff', 'shipment'] and current_user.role != 'admin': 
            flash('Только для администратора')
            return redirect(url_for('stock.documents'))
        if dt == 'field_recount' and current_user.role == 'user2':
            flash('Только для менеджера/админа')
            return redirect(url_for('stock.documents', active_tab='regrading'))

        if dt == 'field_recount':
            field_id = request.form.get('recount_field_id', type=int)
            doc_date = msk_now()

            plant_ids = request.form.getlist('recount_plant_id[]')
            size_ids = request.form.getlist('recount_size_id[]')
            years = request.form.getlist('recount_year[]')
            fact_qtys = request.form.getlist('recount_fact_qty[]')

            if not field_id:
                flash('Выберите поле для пересчета')
                return redirect(url_for('stock.documents', active_tab='regrading'))

            try:
                # агрегируем по ключу, чтобы избежать дублей строк
                fact_map = {}
                for i in range(len(plant_ids)):
                    try:
                        pid = int(plant_ids[i])
                        sid = int(size_ids[i])
                        yr = int(years[i])
                        fact_qty = int(fact_qtys[i])
                    except Exception:
                        continue
                    if fact_qty < 0:
                        raise ValueError('Фактическое количество не может быть отрицательным')
                    fact_map[(pid, sid, yr)] = fact_qty

                if not fact_map:
                    flash('Нет данных для пересчета')
                    return redirect(url_for('stock.documents', active_tab='regrading'))

                # Валидация: фактическое кол-во не должно быть меньше зарезервированного
                rmap = get_reserved_map()
                for (pid, sid, yr), fact_qty in fact_map.items():
                    reserved = int(rmap.get((pid, sid, field_id, yr), 0) or 0)
                    if int(fact_qty) < reserved:
                        pl = Plant.query.get(pid); sz = Size.query.get(sid)
                        name = (pl.name if pl else '') + ((' · ' + sz.name) if sz else '')
                        raise ValueError(f'Пересчёт {fact_qty} шт по позиции «{name}» меньше резерва {reserved} шт')

                field_obj = Field.query.get(field_id)
                doc_comment = f'Пересчет по полю: {field_obj.name if field_obj else field_id}'

                doc = Document(
                    doc_type='field_recount',
                    user_id=current_user.id,
                    date=doc_date,
                    comment=doc_comment
                )
                db.session.add(doc)
                db.session.flush()

                changed_rows = 0
                for (pid, sid, yr), fact_qty in fact_map.items():
                    stock = get_or_create_stock(pid, sid, field_id, yr)
                    current_qty = int(stock.quantity or 0)
                    delta = int(fact_qty) - current_qty
                    if delta == 0:
                        continue
                    stock.quantity += delta
                    db.session.add(DocumentRow(
                        document_id=doc.id,
                        plant_id=pid,
                        size_id=sid,
                        field_to_id=field_id,
                        year=yr,
                        quantity=delta
                    ))
                    changed_rows += 1

                if changed_rows == 0:
                    db.session.delete(doc)
                    db.session.commit()
                    flash('Изменений нет: фактические остатки совпадают с базой')
                else:
                    db.session.commit()
                    flash(f'Карточка пересчета сохранена (изменено строк: {changed_rows})')
                    log_action(f"Создал карточку пересчета #{doc.id} по полю {field_id}")

            except Exception as e:
                db.session.rollback()
                flash(f'Ошибка пересчета: {e}')

            return redirect(url_for('stock.documents', active_tab='regrading'))
        
        p_ids = request.form.getlist('plant[]')
        s_ids = request.form.getlist('size[]')
        q_ids = request.form.getlist('quantity[]')
        y_ids = request.form.getlist('year[]')
        f_to = request.form.getlist('field_to[]')
        f_from = request.form.getlist('field_from[]')
        
        form_date = request.form.get('date')
        doc_date = datetime.strptime(form_date, '%Y-%m-%d') if form_date else msk_now()
        
        try:
            inc_yr = int(request.form.get('income_year', msk_now().year))
        except (ValueError, TypeError):
            inc_yr = msk_now().year
            
        comm = request.form.get('comment', '')

        # если админ создаёт ручную отгрузку с указанием клиента, заведём "ghost"‑заказ
        ghost_order = None
        if dt == 'shipment':
            client_id = request.form.get('client_id', type=int)
            if client_id:
                ghost_order = Order(client_id=client_id, date=doc_date, status='ghost')
                db.session.add(ghost_order)
                db.session.flush()

        try:
            doc = Document(doc_type=dt, user_id=current_user.id, date=doc_date, comment=comm)
            if ghost_order:
                doc.order_id = ghost_order.id
            db.session.add(doc)
            db.session.flush()

            # Карта резервов нужна только для операций, ограниченных свободным остатком
            rmap_local = get_reserved_map() if dt == 'writeoff' else {}

            for i in range(len(p_ids)):
                try: qty = int(q_ids[i])
                except: continue
                
                if qty <= 0: continue
                pid = int(p_ids[i])
                year = int(y_ids[i])

                if dt == 'income':
                    sid = int(s_ids[i]); ft = int(f_to[i])
                    get_or_create_stock(pid, sid, ft, inc_yr).quantity += qty
                    db.session.add(DocumentRow(document_id=doc.id, plant_id=pid, size_id=sid, field_to_id=ft, year=inc_yr, quantity=qty))
                
                elif dt == 'writeoff':
                    sid = int(s_ids[i]); ff = int(f_from[i])
                    stock = get_or_create_stock(pid, sid, ff, year)
                    reserved = int(rmap_local.get((pid, sid, ff, year), 0) or 0)
                    free = max(0, int(stock.quantity or 0) - reserved)
                    if qty > free:
                        pl = Plant.query.get(pid); sz = Size.query.get(sid)
                        name = (pl.name if pl else '') + ((' · ' + sz.name) if sz else '')
                        raise ValueError(f'Списание {qty} шт по позиции «{name}» превышает свободный остаток {free} шт (резерв: {reserved})')
                    stock.quantity -= qty
                    db.session.add(DocumentRow(document_id=doc.id, plant_id=pid, size_id=sid, field_from_id=ff, year=year, quantity=qty))
                
                elif dt == 'move':
                    sid = int(s_ids[i]); ff = int(f_from[i]); ft = int(f_to[i])
                    get_or_create_stock(pid, sid, ff, year).quantity -= qty
                    get_or_create_stock(pid, sid, ft, year).quantity += qty
                    db.session.add(DocumentRow(document_id=doc.id, plant_id=pid, size_id=sid, field_from_id=ff, field_to_id=ft, year=year, quantity=qty))
                
                elif dt == 'shipment':
                    sid = int(s_ids[i]); ff = int(f_from[i])
                    get_or_create_stock(pid, sid, ff, year).quantity -= qty
                    db.session.add(DocumentRow(document_id=doc.id, plant_id=pid, size_id=sid, field_from_id=ff, year=year, quantity=qty))
                    # если мы создали ghost-заказ, сразу добавим позицию туда тоже
                    if ghost_order:
                        db.session.add(OrderItem(order_id=ghost_order.id, plant_id=pid, size_id=sid, field_id=ff, year=year, quantity=qty, shipped_quantity=qty, price=0))

                elif dt == 'regrading':
                    sid_from = int(s_ids[i])       
                    sid_to = int(f_to[i]) 
                    field_id = int(f_from[i])      
                    get_or_create_stock(pid, sid_from, field_id, year).quantity -= qty
                    get_or_create_stock(pid, sid_to, field_id, year).quantity += qty
                    db.session.add(DocumentRow(document_id=doc.id, plant_id=pid, size_id=sid_from, size_to_id=sid_to, field_from_id=field_id, year=year, quantity=qty))

            db.session.commit()
            flash('Документ проведен')
            log_action(f"Создал документ {dt} #{doc.id}")
        except Exception as e: 
            db.session.rollback()
            flash(f'Error: {e}')
            
        return redirect(url_for('stock.documents'))
    
    # --- ЛОГИКА GET-ЗАПРОСА (С ФИЛЬТРАМИ) ---
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    f_client = request.args.get('client_id', type=int)
    f_doc_type = request.args.get('doc_type')

    # дополнительные умные фильтры
    f_plant = request.args.get('plant_id', type=int)
    f_size = request.args.get('size_id', type=int)
    f_field = request.args.get('field_id', type=int)

    # Текущая вкладка (чтобы фильтры не сбрасывали видимую вкладку)
    # Если параметр передан, но пуст, по умолчанию остаемся на "regrading" (Изменение размера).
    active_tab = request.args.get('active_tab') or 'regrading'

    # 1. Отгрузки (Shipments)
    ship_q = Document.query.filter_by(doc_type='shipment')
    # исключаем документы, связанные с ghost‑заказами — они показываются ниже отдельным блоком
    ship_q = ship_q.outerjoin(Order, Document.order_id == Order.id)
    ship_q = ship_q.filter(or_(Order.id == None, Order.status != 'ghost'))
    if f_start: ship_q = ship_q.filter(func.date(Document.date) >= f_start)
    if f_end: ship_q = ship_q.filter(func.date(Document.date) <= f_end)
    if f_client: ship_q = ship_q.join(Order).filter(Order.client_id == f_client)
    # фильтрация по строкам документа
    if f_plant or f_size or f_field:
        ship_q = ship_q.join(DocumentRow)
        if f_plant: ship_q = ship_q.filter(DocumentRow.plant_id == f_plant)
        if f_size: ship_q = ship_q.filter(DocumentRow.size_id == f_size)
        if f_field: ship_q = ship_q.filter(DocumentRow.field_from_id == f_field)
        ship_q = ship_q.distinct()
    if f_doc_type and f_doc_type != 'shipment':
        shipments = []
    else:
        shipments = ship_q.order_by(Document.date.desc()).all()

    # 2. Исторические (Ghost)
    ghost_q = Order.query.filter_by(status='ghost')
    if f_start: ghost_q = ghost_q.filter(func.date(Order.date) >= f_start)
    if f_end: ghost_q = ghost_q.filter(func.date(Order.date) <= f_end)
    if f_client: ghost_q = ghost_q.filter(Order.client_id == f_client)
    if f_plant or f_size or f_field:
        ghost_q = ghost_q.join(OrderItem)
        if f_plant: ghost_q = ghost_q.filter(OrderItem.plant_id == f_plant)
        if f_size: ghost_q = ghost_q.filter(OrderItem.size_id == f_size)
        if f_field: ghost_q = ghost_q.filter(OrderItem.field_id == f_field)
        ghost_q = ghost_q.distinct()
    if f_doc_type and f_doc_type != 'ghost':
        ghost_orders = []
    else:
        ghost_orders = ghost_q.order_by(Order.date.desc()).all()

    # Фильтруем показываемые строчки внутри ghost-заказов по выбранным фильтрам
    # (если мы отфильтровали по товару/размеру/полю, то показываем только подходящие позиции)
    if f_plant or f_size or f_field:
        filtered_ghost_orders = []
        for o in ghost_orders:
            filtered_items = []
            for item in o.items:
                if f_plant and item.plant_id != f_plant: continue
                if f_size and item.size_id != f_size: continue
                if f_field and item.field_id != f_field: continue
                filtered_items.append(item)
            if filtered_items:
                o.filtered_items = filtered_items
                filtered_ghost_orders.append(o)
        ghost_orders = filtered_ghost_orders
    else:
        for o in ghost_orders:
            o.filtered_items = o.items

    # 3. Черновики инвентаризации (Drafts)
    draft_q = Document.query.filter_by(doc_type='inventory_draft')
    if f_start: draft_q = draft_q.filter(func.date(Document.date) >= f_start)
    if f_end: draft_q = draft_q.filter(func.date(Document.date) <= f_end)
    # Фильтр по клиенту к инвентаризации не применим, поэтому его тут нет
    if f_doc_type and f_doc_type != 'inventory_draft':
        draft_docs = []
    else:
        draft_docs = draft_q.order_by(Document.date.desc()).all()

    # Сбор данных для черновиков (предзагрузка остатков для быстрого поиска)
    stock_lookup = {}
    if draft_docs:
        for sb in StockBalance.query.all():
            stock_lookup[(sb.plant_id, sb.size_id, sb.field_id, sb.year)] = sb.quantity or 0
    drafts_data = []
    for d in draft_docs:
        rows_data = []
        for r in d.rows:
            db_qty = stock_lookup.get((r.plant_id, r.size_id, r.field_to_id, r.year), 0)
            rows_data.append({
                'row_id': r.id,
                'plant_name': r.plant.name if r.plant else '-',
                'size_name': r.size.name if r.size else '-',
                'fact': r.quantity, 
                'db_qty': db_qty,   
                'delta': r.quantity - db_qty
            })
        drafts_data.append({'doc': d, 'rows': rows_data})

    # 4. Карточки пересчета по полю
    recount_q = Document.query.filter_by(doc_type='field_recount')
    if f_start: recount_q = recount_q.filter(func.date(Document.date) >= f_start)
    if f_end: recount_q = recount_q.filter(func.date(Document.date) <= f_end)
    if f_plant or f_size or f_field:
        recount_q = recount_q.join(DocumentRow)
        if f_plant: recount_q = recount_q.filter(DocumentRow.plant_id == f_plant)
        if f_size: recount_q = recount_q.filter(DocumentRow.size_id == f_size)
        if f_field: recount_q = recount_q.filter(DocumentRow.field_to_id == f_field)
        recount_q = recount_q.distinct()
    if f_doc_type and f_doc_type != 'field_recount':
        recount_docs = []
    else:
        recount_docs = recount_q.order_by(Document.date.desc(), Document.id.desc()).all()
    recount_cards = [_group_recount_rows(d) for d in recount_docs]

    # Сбор остатков для JS
    all_stocks = StockBalance.query.all()
    stock_data = {f"{s.plant_id}_{s.size_id}_{s.field_id}": s.quantity for s in all_stocks}

    filters = {
        'start': f_start,
        'end': f_end,
        'client_id': f_client,
        'doc_type': f_doc_type,
        'plant_id': f_plant,
        'size_id': f_size,
        'field_id': f_field,
    }

    resp = make_response(render_template('stock/documents.html',
                           plants=Plant.query.all(),
                           sizes=sorted(Size.query.all(), key=natural_key),
                           fields=sorted(Field.query.all(), key=natural_key),
                           clients=Client.query.all(),
                           shipments=shipments,
                           ghost_orders=ghost_orders,
                           stock_json=json.dumps(stock_data),
                           drafts_data=drafts_data,
                           recount_cards=recount_cards,
                           current_year=msk_now().year,
                           filters=filters,
                           active_tab=active_tab,
                           current_url=request.url))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp

@bp.route('/order/create_ghost', methods=['GET', 'POST'])
@login_required
def order_create_ghost():
    if current_user.role != 'admin': 
        flash('Только для администратора')
        return redirect(url_for('stock.documents'))

    if request.method == 'POST':
        try:
            client_id = int(request.form.get('client'))
            date_str = request.form.get('date')
            order_date = datetime.strptime(date_str, '%Y-%m-%d') if date_str else msk_now()

            order = Order(client_id=client_id, date=order_date, status='ghost', invoice_number='HISTORY')
            db.session.add(order)
            db.session.commit()

            p_ids = request.form.getlist('plant[]')
            s_ids = request.form.getlist('size[]')
            f_ids = request.form.getlist('field[]')
            y_ids = request.form.getlist('year[]')
            q_ids = request.form.getlist('quantity[]')
            prices = request.form.getlist('price[]')

            for i in range(len(p_ids)):
                try:
                    qty = int(q_ids[i])
                    if qty <= 0: continue
                    price = float(prices[i])
                    item = OrderItem(
                        order_id=order.id, plant_id=int(p_ids[i]), size_id=int(s_ids[i]),
                        field_id=int(f_ids[i]), year=int(y_ids[i]), quantity=qty,
                        shipped_quantity=qty, price=price
                    )
                    db.session.add(item)
                except ValueError: continue

            db.session.commit()
            log_action(f"Создал историческую отгрузку (Ghost) #{order.id}")
            flash(f"Историческая отгрузка #{order.id} создана")
            return redirect(url_for('stock.documents'))

        except Exception as e:
            db.session.rollback()
            flash(f"Ошибка: {e}")

    return render_template('stock/ghost_order_form.html', clients=Client.query.all(), plants=Plant.query.all(), sizes=Size.query.all(), fields=Field.query.all())

@bp.route('/order/delete_ghost/<int:order_id>', methods=['POST'])
@login_required
def delete_ghost_order(order_id):
    if current_user.role != 'admin': return redirect(url_for('stock.documents'))
    o = Order.query.get_or_404(order_id)
    if o.status == 'ghost':
        # прежде чем стереть заказ, удалим связанные документы (например, ручную отгрузку)
        docs = Document.query.filter_by(order_id=o.id).all()
        for d in docs:
            # восстанавливаем остаток только для реальных отгрузок
            if d.doc_type == 'shipment':
                for r in d.rows:
                    # если поле указан, возвращаем на склад
                    if r.field_from_id:
                        st = get_or_create_stock(r.plant_id, r.size_id, r.field_from_id, r.year)
                        st.quantity += r.quantity
            db.session.delete(d)
        db.session.delete(o)
        db.session.commit()
        flash('Историческая запись удалена')
        log_action(f"Удалил историческую отгрузку #{order_id}")
    return redirect(url_for('stock.documents'))

@bp.route('/order/mass_edit_ghost', methods=['POST'])
@login_required
def mass_edit_ghost_orders():
    if current_user.role != 'admin': return redirect(url_for('stock.documents'))
    try:
        new_client_id = request.form.get('new_client_id')
        order_ids = request.form.getlist('order_ids[]')
        
        if not new_client_id or not order_ids:
            flash('Не выбраны заказы или новый клиент')
            return redirect(url_for('stock.documents'))

        count = 0
        for oid in order_ids:
            order = Order.query.filter_by(id=int(oid), status='ghost').first()
            if order:
                order.client_id = int(new_client_id)
                count += 1
        
        db.session.commit()
        if count > 0:
            flash(f'Обновлен клиент у {count} исторических отгрузок')
            log_action(f"Массовая смена клиента у Ghost-заказов (кол-во: {count})")
        else:
            flash('Ничего не обновлено')

    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка: {e}')
    return redirect(url_for('stock.documents'))

@bp.route('/order/merge_ghost', methods=['POST'])
@login_required
def merge_ghost_orders():
    if current_user.role != 'admin': return redirect(url_for('stock.documents'))
    
    order_ids = request.form.getlist('order_ids[]')
    
    if not order_ids or len(order_ids) < 2:
        flash('Выберите минимум 2 заказа для объединения')
        return redirect(url_for('stock.documents'))
    
    try:
        # Сортируем ID, чтобы первый (самый старый) стал основным
        sorted_ids = sorted([int(x) for x in order_ids])
        main_order_id = sorted_ids[0]
        other_ids = sorted_ids[1:]
        
        main_order = Order.query.get(main_order_id)
        if not main_order or main_order.status != 'ghost':
            flash('Ошибка: Основной заказ не найден или не является Ghost')
            return redirect(url_for('stock.documents'))
            
        items_moved = 0
        merged_orders_count = 0
        
        for oid in other_ids:
            sub_order = Order.query.get(oid)
            if sub_order and sub_order.status == 'ghost':
                # Переносим позиции товаров в основной заказ
                for item in sub_order.items:
                    item.order_id = main_order.id
                    items_moved += 1
                
                # Если у объединяемых заказов есть номер счета, а у основного нет - берем его
                if not main_order.invoice_number and sub_order.invoice_number:
                    main_order.invoice_number = sub_order.invoice_number
                    main_order.invoice_date = sub_order.invoice_date
                
                # Удаляем пустой заказ-донор
                db.session.delete(sub_order)
                merged_orders_count += 1
                
        db.session.commit()
        flash(f'Успешно! Объединено {merged_orders_count + 1} заказов в заказ #{main_order_id}. Перенесено позиций: {items_moved}.')
        log_action(f"Объединение Ghost-заказов {order_ids} в #{main_order_id}")
        
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка объединения: {e}')
        
    return redirect(url_for('stock.documents'))

@bp.route('/order/download_ghost_template')
@login_required
def download_ghost_template():
    if current_user.role != 'admin': return redirect(url_for('stock.documents'))
    wb = Workbook()
    ws = wb.active
    ws.append(["Клиент", "Дата (YYYY-MM-DD)", "Растение", "Размер", "Поле (Откуда)", "Год партии", "Кол-во", "Цена продажи"])
    ws.append(["Иванов И.И.", "2023-05-20", "Сосна горная", "C3", "Поле 1", 2020, 10, 1500])
    apply_excel_styles(ws)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, download_name='ghost_import_template.xlsx', as_attachment=True)

@bp.route('/order/import_ghost', methods=['POST'])
@login_required
def import_ghost_orders():
    if current_user.role != 'admin': return redirect(url_for('stock.documents'))
    file = request.files.get('file')
    if not file: flash('Файл не выбран'); return redirect(url_for('stock.documents'))
    try:
        wb = load_workbook(file)
        ws = wb.active
        plants_map = {p.name.lower().strip(): p.id for p in Plant.query.all()}
        sizes_map = {s.name.lower().strip(): s.id for s in Size.query.all()}
        fields_map = {f.name.lower().strip(): f.id for f in Field.query.all()}
        orders_batch = {} 
        
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row[0] or not row[2]: continue
            c_name = str(row[0]).strip()
            raw_date = row[1]
            if isinstance(raw_date, datetime): d_date = raw_date
            elif isinstance(raw_date, date): d_date = datetime.combine(raw_date, datetime.min.time())
            else:
                try: d_date = datetime.strptime(str(raw_date), '%Y-%m-%d')
                except: d_date = msk_now()
            
            p_id = plants_map.get(str(row[2]).lower().strip())
            s_id = sizes_map.get(str(row[3]).lower().strip())
            f_id = fields_map.get(str(row[4]).lower().strip())
            
            if p_id and s_id and f_id:
                key = (c_name, d_date)
                if key not in orders_batch: orders_batch[key] = []
                orders_batch[key].append({
                    'plant_id': p_id, 'size_id': s_id, 'field_id': f_id,
                    'year': int(row[5]) if row[5] else 2017,
                    'qty': int(row[6] or 0), 'price': float(row[7] or 0)
                })

        created_count = 0
        for (client_name, order_date), items in orders_batch.items():
            if not items: continue
            client = Client.query.filter(func.lower(Client.name) == client_name.lower()).first()
            if not client:
                client = Client(name=client_name)
                db.session.add(client)
                db.session.flush()
            
            order = Order(client_id=client.id, date=order_date, status='ghost', invoice_number='IMPORT')
            db.session.add(order)
            db.session.flush()
            
            for item in items:
                oi = OrderItem(
                    order_id=order.id, plant_id=item['plant_id'], size_id=item['size_id'],
                    field_id=item['field_id'], year=item['year'], quantity=item['qty'],
                    shipped_quantity=item['qty'], price=item['price']
                )
                db.session.add(oi)
            created_count += 1

        db.session.commit()
        flash(f'Успешно импортировано {created_count} исторических заказов')
        log_action(f"Импорт Ghost-заказов ({created_count} шт.)")
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка импорта: {e}')
    return redirect(url_for('stock.documents'))

@bp.route('/document/delete/<int:doc_id>', methods=['POST'])
@login_required
def document_delete(doc_id):
    if current_user.role != 'admin': return redirect(url_for('stock.logs'))
    return_to = request.args.get('return_to') or request.form.get('return_to')
    if return_to == 'None': return_to = None
    doc = Document.query.get_or_404(doc_id)
    try:
        for row in doc.rows:
            if doc.doc_type in ['income', 'correction']:
                if row.field_to_id:
                    get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year).quantity -= row.quantity
            elif doc.doc_type == 'field_recount':
                if row.field_to_id:
                    get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year).quantity -= row.quantity
            elif doc.doc_type in ['writeoff', 'shipment']:
                get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
            elif doc.doc_type == 'move':
                get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
                get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year).quantity -= row.quantity
            elif doc.doc_type == 'regrading':
                get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
                get_or_create_stock(row.plant_id, row.size_to_id, row.field_from_id, row.year).quantity -= row.quantity

            if doc.doc_type == 'shipment' and doc.order_id:
                oi = OrderItem.query.filter_by(order_id=doc.order_id, plant_id=row.plant_id, size_id=row.size_id, field_id=row.field_from_id, year=row.year).first()
                if oi: oi.shipped_quantity = max(0, oi.shipped_quantity - row.quantity)

        db.session.delete(doc)
        db.session.commit()
        flash('Документ удален, остатки пересчитаны')
        log_action(f"Удалил документ #{doc.id}")
    except Exception as e: 
        db.session.rollback()
        flash(f'Ошибка удаления: {e}')

    if return_to:
        return redirect(return_to)
    return redirect(url_for('stock.documents', active_tab='shipments'))

@bp.route('/document/edit/<int:doc_id>', methods=['GET', 'POST'])
@login_required
def document_edit(doc_id):
    if current_user.role != 'admin': 
        flash('Только админ')
        return redirect(url_for('stock.logs'))
    doc = Document.query.get_or_404(doc_id)
    
    if request.method == 'POST':
        return_to = request.form.get('return_to') or request.args.get('return_to')
        if return_to == 'None': return_to = None
        try:
            for row in doc.rows:
                if doc.doc_type in ['income', 'correction']:
                    if row.field_to_id: get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year).quantity -= row.quantity
                elif doc.doc_type == 'field_recount':
                    if row.field_to_id: get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year).quantity -= row.quantity
                elif doc.doc_type == 'writeoff':
                    if row.field_from_id: get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
                elif doc.doc_type == 'move':
                    if row.field_from_id: get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
                    if row.field_to_id: get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year).quantity -= row.quantity
                elif doc.doc_type == 'regrading':
                    if row.field_from_id: get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
                    if row.field_from_id and row.size_to_id: get_or_create_stock(row.plant_id, row.size_to_id, row.field_from_id, row.year).quantity -= row.quantity
                elif doc.doc_type == 'shipment':
                    if row.field_from_id: get_or_create_stock(row.plant_id, row.size_id, row.field_from_id, row.year).quantity += row.quantity
                    if doc.order_id:
                        oi = OrderItem.query.filter_by(order_id=doc.order_id, plant_id=row.plant_id, size_id=row.size_id, field_id=row.field_from_id, year=row.year).first()
                        if oi: oi.shipped_quantity = max(0, oi.shipped_quantity - row.quantity)
            
            for row in doc.rows: db.session.delete(row)
            
            new_date_str = request.form.get('date')
            if new_date_str: doc.date = datetime.strptime(new_date_str, '%Y-%m-%d')
            
            p_ids = request.form.getlist('plant[]')
            s_ids = request.form.getlist('size[]')
            q_ids = request.form.getlist('quantity[]')
            y_ids = request.form.getlist('year[]')
            
            f_to_ids = request.form.getlist('field_to[]')
            f_from_ids = request.form.getlist('field_from[]')
            op_types = request.form.getlist('operation_type[]')

            for i in range(len(p_ids)):
                try: 
                    if i >= len(s_ids) or i >= len(q_ids) or i >= len(y_ids): continue
                    
                    qty = int(q_ids[i])
                    if doc.doc_type != 'field_recount' and qty <= 0:
                        continue
                    if doc.doc_type == 'field_recount' and qty < 0:
                        continue
                    
                    pid = int(p_ids[i])
                    sid = int(s_ids[i])
                    year = int(y_ids[i])
                    
                    new_row = DocumentRow(document_id=doc.id, plant_id=pid, size_id=sid, year=year, quantity=qty)
                    
                    if doc.doc_type in ['income', 'correction']:
                        if i < len(f_to_ids):
                            ft = int(f_to_ids[i])
                            new_row.field_to_id = ft
                            get_or_create_stock(pid, sid, ft, year).quantity += qty
                        else:
                            continue 

                    elif doc.doc_type == 'writeoff':
                        if i < len(f_from_ids):
                            ff = int(f_from_ids[i])
                            new_row.field_from_id = ff
                            get_or_create_stock(pid, sid, ff, year).quantity -= qty
                        else: continue

                    elif doc.doc_type == 'move':
                        if i < len(f_from_ids) and i < len(f_to_ids):
                            ff = int(f_from_ids[i]); ft = int(f_to_ids[i])
                            new_row.field_from_id = ff; new_row.field_to_id = ft
                            get_or_create_stock(pid, sid, ff, year).quantity -= qty
                            get_or_create_stock(pid, sid, ft, year).quantity += qty
                        else: continue

                    elif doc.doc_type == 'regrading':
                        if i < len(f_from_ids) and i < len(f_to_ids):
                            ff = int(f_from_ids[i]); s_to = int(f_to_ids[i])
                            new_row.field_from_id = ff; new_row.size_to_id = s_to
                            get_or_create_stock(pid, sid, ff, year).quantity -= qty
                            get_or_create_stock(pid, s_to, ff, year).quantity += qty
                        else: continue

                    elif doc.doc_type == 'shipment':
                        if i < len(f_from_ids):
                            ff = int(f_from_ids[i])
                            new_row.field_from_id = ff
                            get_or_create_stock(pid, sid, ff, year).quantity -= qty
                            if doc.order_id:
                                oi = OrderItem.query.filter_by(order_id=doc.order_id, plant_id=pid, size_id=sid, field_id=ff, year=year).first()
                                if oi: oi.shipped_quantity += qty
                        else: continue

                    elif doc.doc_type == 'field_recount':
                        if i < len(f_to_ids):
                            ft = int(f_to_ids[i])
                            op = op_types[i] if i < len(op_types) else 'income'
                            signed_qty = qty if op == 'income' else -qty
                            if signed_qty == 0:
                                continue
                            new_row.field_to_id = ft
                            new_row.quantity = signed_qty
                            get_or_create_stock(pid, sid, ft, year).quantity += signed_qty
                        else:
                            continue
                        
                    db.session.add(new_row)
                    
                except (ValueError, IndexError) as row_err:
                    print(f"Row error: {row_err}")
                    continue 

            db.session.commit()
            flash('Документ обновлен')
            log_action(f"Отредактировал документ #{doc.id}")
            if return_to:
                return redirect(return_to)
            return redirect(url_for('stock.documents', active_tab='shipments'))
            
        except Exception as e: 
            db.session.rollback()
            flash(f'Ошибка обновления: {e}')
            if return_to:
                return redirect(return_to)
            return redirect(url_for('stock.document_edit', doc_id=doc.id))

    return render_template(
        'stock/document_edit.html',
        doc=doc,
        plants=Plant.query.all(),
        sizes=Size.query.all(),
        fields=Field.query.all(),
        return_to=request.args.get('return_to')
    )

@bp.route('/admin/recalc_stock_balances')
@login_required
def recalc_stock_balances():
    if current_user.role != 'admin': 
        return redirect(url_for('main.index'))
    
    try:
        StockBalance.query.update({StockBalance.quantity: 0})
        all_docs = Document.query.order_by(Document.date.asc(), Document.id.asc()).all()
        doc_count = 0
        
        for doc in all_docs:
            doc_count += 1
            for row in doc.rows:
                pid, sid, yr, qty = row.plant_id, row.size_id, row.year, row.quantity
                
                if doc.doc_type in ['income', 'correction']:
                    if row.field_to_id:
                        get_or_create_stock(pid, sid, row.field_to_id, yr).quantity += qty
                
                elif doc.doc_type == 'field_recount':
                    if row.field_to_id:
                        get_or_create_stock(pid, sid, row.field_to_id, yr).quantity += qty
                
                elif doc.doc_type in ['writeoff', 'shipment']:
                    if row.field_from_id:
                        get_or_create_stock(pid, sid, row.field_from_id, yr).quantity -= qty
                
                elif doc.doc_type == 'move':
                    if row.field_from_id and row.field_to_id:
                        get_or_create_stock(pid, sid, row.field_from_id, yr).quantity -= qty
                        get_or_create_stock(pid, sid, row.field_to_id, yr).quantity += qty
                
                elif doc.doc_type == 'regrading':
                    if row.field_from_id and row.size_to_id:
                        get_or_create_stock(pid, sid, row.field_from_id, yr).quantity -= qty
                        get_or_create_stock(pid, row.size_to_id, row.field_from_id, yr).quantity += qty

        db.session.commit()
        flash(f'Успешно! Пересчитано {doc_count} документов. Остатки синхронизированы.')
        log_action("Запустил полный пересчет остатков (Repair DB)")
        
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка пересчета: {e}')
        
    return redirect(url_for('stock.stock_report'))

@bp.route('/logs')
@login_required
def logs():
    resp = make_response(render_template('stock/logs.html',
                           logs=ActionLog.query.order_by(ActionLog.date.desc()).limit(500).all(),
                           documents=Document.query.order_by(Document.date.desc()).limit(500).all()))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp

@bp.route('/changelog', methods=['GET', 'POST'])
@login_required
def changelog():
    # Импорт внутри функции, чтобы избежать циклических ссылок, если они возникнут
    from app.models import ChangeLog 
    
    if request.method == 'POST':
        if current_user.role != 'admin':
            flash('Только админ может создавать релизы')
            return redirect(url_for('stock.changelog'))
            
        version = request.form.get('version')
        changes_text = request.form.get('changes')
        
        if version and changes_text:
            # Сохраняем как есть, переносы строк обработаем в шаблоне
            log = ChangeLog(
                version=version,
                content=changes_text,
                date=msk_now().date() # Автоматическая дата
            )
            db.session.add(log)
            db.session.commit()
            log_action(f"Опубликовал версию системы {version}")
            flash(f'Версия {version} опубликована!')
            
        elif request.form.get('action') == 'delete':
            # Логика удаления (на всякий случай)
            log_id = request.form.get('log_id')
            ChangeLog.query.filter_by(id=log_id).delete()
            db.session.commit()
            flash('Запись удалена')
            
        return redirect(url_for('stock.changelog'))

    logs = ChangeLog.query.order_by(ChangeLog.date.desc(), ChangeLog.id.desc()).all()
    return render_template('stock/changelog.html', logs=logs, today=msk_now().date())

@bp.route('/price_history/download_template')
@login_required
def price_history_download_template():
    if current_user.role != 'admin': return redirect(url_for('stock.price_history'))
    wb = Workbook()
    ws = wb.active
    ws.title = "Шаблон цен"
    ws.append(["Растение", "Размер", "Поле", "Год (Партия)", "Цена продажи (Руб)"])
    ws.append(["Туя западная Smaragd", "C3", "Поле 1", msk_now().year, 500])
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 15
    ws.column_dimensions['C'].width = 15
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, download_name='price_history_template.xlsx', as_attachment=True)

@bp.route('/price_history/import', methods=['POST'])
@login_required
def price_history_import():
    if current_user.role != 'admin': return redirect(url_for('stock.price_history'))
    file = request.files.get('file')
    if not file: return redirect(url_for('stock.price_history'))
    
    try:
        wb = load_workbook(file)
        ws = wb.active
        plants_map = {p.name.lower().strip(): p.id for p in Plant.query.all()}
        sizes_map = {s.name.lower().strip(): s.id for s in Size.query.all()}
        fields_map = {f.name.lower().strip(): f.id for f in Field.query.all()}
        
        count = 0
        updated = 0
        
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row[0] or not row[1] or not row[2] or not row[4]: continue
            
            p_name = str(row[0]).lower().strip()
            s_name = str(row[1]).lower().strip()
            f_name = str(row[2]).lower().strip()
            
            pid = plants_map.get(p_name)
            sid = sizes_map.get(s_name)
            fid = fields_map.get(f_name)
            
            try: year = int(row[3])
            except: year = msk_now().year
            
            try: price = float(row[4])
            except: continue
            
            if pid and sid and fid:
                hist = PriceHistory.query.filter_by(
                    plant_id=pid, size_id=sid, field_id=fid, year=year
                ).first()
                
                if hist:
                    hist.price = price
                    updated += 1
                else:
                    db.session.add(PriceHistory(
                        plant_id=pid, size_id=sid, field_id=fid, year=year, price=price
                    ))
                    count += 1
                    
        db.session.commit()
        flash(f'Успешно: добавлено {count}, обновлено {updated} цен.')
        log_action(f"Импорт истории цен: +{count}, up:{updated}")
        
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка импорта: {e}')
        
    return redirect(url_for('stock.price_history'))

@bp.route('/document/approve_inventory/<int:doc_id>', methods=['POST'])
@login_required
def approve_inventory(doc_id):
    """Проведение черновика инвентаризации в чистовой документ"""
    # Доступ разрешен admin и user (менеджер). user2 (бригадир) - запрещен.
    if current_user.role == 'user2':
        return redirect(url_for('stock.documents'))
        
    doc = Document.query.get_or_404(doc_id)
    if doc.doc_type != 'inventory_draft':
        return redirect(url_for('stock.documents'))

    try:
        # 1. Обновляем факты из формы (вдруг админ/менеджер поправил руками)
        for row in doc.rows:
            fact_val = request.form.get(f'fact_{row.id}')
            if fact_val is not None:
                row.quantity = int(fact_val)

        # 2. Применяем к складу и превращаем факт в дельту для архива
        for row in doc.rows:
            st = get_or_create_stock(row.plant_id, row.size_id, row.field_to_id, row.year)
            
            delta = row.quantity - st.quantity # Вычисляем излишек/недостачу
            
            st.quantity = row.quantity # Записываем на склад ФАКТ
            row.quantity = delta       # В документе оставляем ДЕЛЬТУ (для истории)

        # 3. Меняем статус документа на "Проведен"
        doc.doc_type = 'correction'
        doc.comment = str(doc.comment).replace("Черновик", "Проведена")
        
        db.session.commit()
        flash('Инвентаризация успешно проведена! Остатки обновлены.')
        log_action(f"Провел инвентаризацию #{doc.id}")
        
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка проведения: {e}')
        
    return redirect(url_for('stock.documents'))

@bp.route('/api/inventory/get_years')
@login_required
def api_inventory_get_years():
    """Отдает список партий (годов) для конкретного растения на поле"""
    p_id = request.args.get('plant_id', type=int)
    f_id = request.args.get('field_id', type=int)
    
    # Ищем уникальные года, где остаток больше 0
    stocks = StockBalance.query.filter_by(plant_id=p_id, field_id=f_id).filter(StockBalance.quantity > 0).all()
    
    # Собираем уникальные года и сортируем (свежие сверху)
    years = sorted(list(set([st.year for st in stocks])), reverse=True)
    return jsonify({'years': years})

@bp.route('/api/inventory/get_sizes')
@login_required
def api_inventory_get_sizes():
    """Отдает список размеров, которые числятся на поле для конкретного растения"""
    p_id = request.args.get('plant_id', type=int)
    f_id = request.args.get('field_id', type=int)
    y = request.args.get('year', type=int)
    
    # Ищем только те, где остаток больше 0
    stocks = StockBalance.query.filter_by(plant_id=p_id, field_id=f_id, year=y).filter(StockBalance.quantity > 0).all()
    result =[{'id': st.size_id, 'name': st.size.name, 'db_qty': st.quantity} for st in stocks]
    
    return jsonify({'sizes': result})

@bp.route('/inventory/mobile', methods=['GET'])
@login_required
def inventory_mobile():
    plants = Plant.query.order_by(Plant.name).all()
    fields = sorted(Field.query.all(), key=natural_key)
    sizes = sorted(Size.query.all(), key=natural_key)
    return render_template('stock/inventory_mobile.html', plants=plants, fields=fields, sizes=sizes, current_year=msk_now().year)

@bp.route('/api/inventory/save', methods=['POST'])
@login_required
def api_inventory_save():
    """Сохранение инвентаризации как ЧЕРНОВИКА (без изменения склада)"""
    data = request.json
    field_id = data.get('field_id')
    plant_id = data.get('plant_id')
    year = data.get('year')
    counts = data.get('counts') # Словарь: {size_id: quantity}

    try:
        # Создаем документ ЧЕРНОВИКА
        doc = Document(
            doc_type='inventory_draft', 
            user_id=current_user.id, 
            date=msk_now(), 
            comment=f"Черновик инвентаризации: Поле {field_id}, Растение {plant_id}"
        )
        db.session.add(doc)
        db.session.flush()

        for size_id_str, new_qty in counts.items():
            # Записываем в документ то, что накликал менеджер (ФАКТ)
            # ВАЖНО: Мы пока НЕ трогаем склад (get_or_create_stock)
            db.session.add(DocumentRow(
                document_id=doc.id, plant_id=plant_id, size_id=int(size_id_str),
                field_to_id=field_id, year=year, quantity=int(new_qty)
            ))

        db.session.commit()
        return jsonify({'status': 'ok', 'doc_id': doc.id})

    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500