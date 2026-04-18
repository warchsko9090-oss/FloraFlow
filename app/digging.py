from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import func, inspect, text, or_
from sqlalchemy.exc import OperationalError
from app.models import db, Order, OrderItem, DiggingLog, Client, Plant, Size, Field, TimeLog, ActionLog
from app.utils import msk_now, log_action

import requests # <--- ДОБАВИЛИ ИМПОРТ
import os       # <--- ДОБАВЛЯЕМ ДЛЯ РАБОТЫ С СЕКРЕТАМИ

from app.telegram import send_message as _tg_send

def ensure_digging_table_exists():
    """Создает таблицу digging_log или дополняет её столбцами, если схема устарела.

    Используем sqlite PRAGMA для определения, какие столбцы уже существуют.
    Это позволяет внедрять изменения схемы без миграций при обновлении кода.
    """
    insp = inspect(db.engine)
    if not insp.has_table('digging_log'):
        DiggingLog.__table__.create(db.engine)
        return

    # Убедимся, что в таблице есть все нужные столбцы (могут отсутствовать в старой БД)
    rows = db.session.execute(text("PRAGMA table_info('digging_log')")).fetchall()
    existing_cols = {row[1]: row for row in rows}  # столбец 1 = name

    # В старой схеме order_item_id мог быть NOT NULL — нужно сделать его NULLABLE.
    if 'order_item_id' in existing_cols and existing_cols['order_item_id'][3] == 1:
        # В sqlite нельзя просто изменить NULL/NOT NULL, поэтому пересоздадим таблицу.
        db.session.execute(text("PRAGMA foreign_keys=OFF"))
        db.session.execute(text("ALTER TABLE digging_log RENAME TO digging_log_old"))
        db.session.execute(text(
            """
            CREATE TABLE digging_log (
                id INTEGER PRIMARY KEY,
                date DATE,
                created_at DATETIME,
                order_item_id INTEGER,
                plant_id INTEGER,
                size_id INTEGER,
                field_id INTEGER,
                year INTEGER,
                user_id INTEGER,
                quantity INTEGER,
                status TEXT DEFAULT 'pending'
            )
            """
        ))
        db.session.execute(text(
            "INSERT INTO digging_log (id, date, created_at, order_item_id, plant_id, size_id, field_id, year, user_id, quantity, status) "
            "SELECT id, date, created_at, order_item_id, plant_id, size_id, field_id, year, user_id, quantity, status "
            "FROM digging_log_old"
        ))
        db.session.execute(text("DROP TABLE digging_log_old"))
        db.session.execute(text("PRAGMA foreign_keys=ON"))
        db.session.commit()

    needed_columns = {
        'plant_id': 'INTEGER',
        'size_id': 'INTEGER',
        'field_id': 'INTEGER',
        'year': 'INTEGER',
    }
    added = False
    for col, col_type in needed_columns.items():
        if col not in existing_cols:
            db.session.execute(text(f"ALTER TABLE digging_log ADD COLUMN {col} {col_type}"))
            added = True
    if added:
        db.session.commit()

def ensure_payment_file_column():
    """Добавляет колонку file_path в таблицу payment, если её там нет."""
    insp = inspect(db.engine)
    if not insp.has_table('payment'):
        return
    rows = db.session.execute(text("PRAGMA table_info('payment')")).fetchall()
    existing_cols = {row[1]: row for row in rows}
    if 'file_path' not in existing_cols:
        db.session.execute(text("ALTER TABLE payment ADD COLUMN file_path VARCHAR(255)"))
        db.session.commit()

bp = Blueprint('digging', __name__)

@bp.route('/digging', methods=['GET', 'POST'])
@login_required
def mobile_index():
    # Главная страница бригадира: ввод выкопки по всем активным заказам (без выбора заказа)
    # Собираем суммарный план по каждой комбинации (растение/размер/поле/год)
    ensure_digging_table_exists()

    if request.method == 'POST':
        # --- ЗАЩИТА ОТ ДУБЛИКАТОВ ВЫКОПКИ (2 минуты) ---
        cutoff_time = msk_now() - timedelta(minutes=2)
        recent_log = ActionLog.query.filter(
            ActionLog.user_id == current_user.id,
            ActionLog.action.like("Бригадир внес выкопку%"),
            ActionLog.date >= cutoff_time
        ).first()
        
        if recent_log:
            flash('Вы уже сохраняли выкопку только что. Подождите пару минут, чтобы случайно не задвоить данные.', 'warning')
            return redirect(url_for('digging.mobile_index'))
        # -----------------------------------------------
        
        # Обработка формы выкопки
        try:
            count_saved = 0
            tg_grouped_details = {}
            tg_total_qty = 0
            orders_just_ready = set()
            all_affected_items = []
            plant_name_cache = {}
            size_name_cache = {}
            field_name_cache = {}

            for key, val in request.form.items():
                if not key.startswith('qty_'): continue
                if not val or not str(val).strip():
                    continue

                parts = key.split('_')
                if len(parts) != 5: continue
                _, plant_id, size_id, field_id, year = parts

                try:
                    qty_dig = int(val)
                except (ValueError, TypeError):
                    continue

                if qty_dig > 0:
                    # --- НОВАЯ ЗАЩИТА: ПРОВЕРЯЕМ ОСТАТОК ---
                    matching_items = OrderItem.query.join(Order).filter(
                        Order.status.in_(['reserved', 'in_progress']),
                        Order.is_deleted == False,
                        OrderItem.plant_id == int(plant_id),
                        OrderItem.size_id == int(size_id),
                        OrderItem.field_id == int(field_id),
                        OrderItem.year == int(year)
                    ).all()

                    total_ordered = sum(item.quantity for item in matching_items)
                    
                    # Считаем выкопанное по привязанным позициям (включая старые записи)
                    assigned_dug = sum(item.dug_quantity or 0 for item in matching_items)
                    
                    # Считаем выкопанное по НЕпривязанным позициям
                    unassigned_dug = db.session.query(func.sum(DiggingLog.quantity)).filter(
                        DiggingLog.plant_id == int(plant_id),
                        DiggingLog.size_id == int(size_id),
                        DiggingLog.field_id == int(field_id),
                        DiggingLog.year == int(year),
                        DiggingLog.order_item_id == None,
                        DiggingLog.status != 'rejected'
                    ).scalar() or 0
                    
                    total_already_dug = assigned_dug + unassigned_dug
                    
                    left_to_dig = total_ordered - total_already_dug

                    if qty_dig > left_to_dig:
                        p_obj = Plant.query.get(int(plant_id))
                        flash(f'❌ Ошибка: Нельзя выкопать {qty_dig} шт. (Осталось выкопать всего {left_to_dig} шт.) для {p_obj.name if p_obj else ""}. Эта строка пропущена.', 'danger')
                        continue # Пропускаем сохранение этой строки
                    # --- КОНЕЦ ЗАЩИТЫ ---

                    # Считаем, сколько было выкопано сегодня до этого сохранения
                    today_date = msk_now().date()
                    dug_today_before = db.session.query(func.sum(DiggingLog.quantity)).filter(
                        DiggingLog.date == today_date,
                        DiggingLog.plant_id == int(plant_id),
                        DiggingLog.size_id == int(size_id),
                        DiggingLog.field_id == int(field_id),
                        DiggingLog.year == int(year),
                        DiggingLog.status != 'rejected'
                    ).scalar() or 0

                    log = DiggingLog(
                        date=today_date,
                        plant_id=int(plant_id),
                        size_id=int(size_id),
                        field_id=int(field_id),
                        year=int(year),
                        user_id=current_user.id,
                        quantity=qty_dig,
                        status='pending'
                    )
                    db.session.add(log)
                    count_saved += 1
                    tg_total_qty += qty_dig

                    # --- Достаем названия для отчета ---
                    p_id = int(plant_id)
                    s_id = int(size_id)
                    f_id = int(field_id)

                    if p_id not in plant_name_cache:
                        p_obj = Plant.query.get(p_id)
                        plant_name_cache[p_id] = p_obj.name if p_obj else "Неизвестно"
                    if s_id not in size_name_cache:
                        s_obj = Size.query.get(s_id)
                        size_name_cache[s_id] = s_obj.name if s_obj else "-"
                    if f_id not in field_name_cache:
                        f_obj = Field.query.get(f_id)
                        field_name_cache[f_id] = f_obj.name if f_obj else "-"

                    p_name = plant_name_cache[p_id]
                    s_name = size_name_cache[s_id]
                    f_name = field_name_cache[f_id]

                    start_of_day_qty = total_already_dug - dug_today_before
                    dug_today_total = dug_today_before + qty_dig

                    total_after_save = total_already_dug + qty_dig
                    status_icon = "✅" if total_after_save >= total_ordered else ("⏳" if total_after_save > 0 else "❌")
                    detail_line = f"{status_icon} {s_name} ({f_name}) ➔ {start_of_day_qty} + {dug_today_total} из {total_ordered} шт."
                    tg_grouped_details.setdefault(p_name, []).append(detail_line)

                    all_affected_items.extend(matching_items)

            # Batch-update dug_quantity for all affected items
            if all_affected_items:
                item_ids = list(set(it.id for it in all_affected_items))
                dug_agg = db.session.query(
                    DiggingLog.order_item_id, func.sum(DiggingLog.quantity)
                ).filter(
                    DiggingLog.order_item_id.in_(item_ids),
                    DiggingLog.status != 'rejected'
                ).group_by(DiggingLog.order_item_id).all()
                dug_map = {r[0]: int(r[1] or 0) for r in dug_agg}
                
                seen_items = set()
                for item in all_affected_items:
                    if item.id in seen_items:
                        continue
                    seen_items.add(item.id)
                    item.dug_quantity = dug_map.get(item.id, 0)
                    old_status = item.order.status
                    item.order.refresh_status_by_dug()
                    if old_status != 'ready' and item.order.status == 'ready':
                        orders_just_ready.add(item.order)

            if count_saved > 0:
                db.session.commit() # Сохраняем в базу данных
                flash(f'Записано позиций: {count_saved}. Спасибо за работу!', 'success')
                log_action(f"Бригадир внес выкопку ({count_saved} строк)")
                
                # --- 1. ОТПРАВКА В ТЕЛЕГРАМ (ОБЩИЙ ОТЧЕТ ПО ВЫКОПКЕ) ---
                if tg_grouped_details:
                    msg = f"🚜 <b>Новая выкопка!</b>\n👤 Внес(ла): {current_user.username}\n\n"
                    for plant_name, plant_lines in tg_grouped_details.items():
                        msg += f"🌲 <b>{plant_name}</b>\n"
                        msg += "\n".join(plant_lines)
                        msg += "\n\n"
                    msg = msg.rstrip()
                    msg += f"\n\n📊 <b>Всего выкопано: {tg_total_qty} шт.</b>"
                    _tg_send(msg, chat_type="digging")
                
                # --- 2. ОТПРАВКА В ТЕЛЕГРАМ (ЕСЛИ ЗАКАЗ СТАЛ ГОТОВ) ---
                for r_order in orders_just_ready:
                    ready_msg = f"✅ <b>Заказ № {r_order.id} готов к отгрузке!</b> Клиент {r_order.client.name}"
                    _tg_send(ready_msg, chat_type="orders")
                # ------------------------------------------------------
                
            else:
                flash('Вы ничего не ввели.', 'warning')
        except Exception as e:
            db.session.rollback()
            flash(f'Ошибка: {e}', 'danger')

        return redirect(url_for('digging.mobile_index'))

    # Группируем остатки по растению/размеру/полю/году
    grouped = db.session.query(
        OrderItem.plant_id,
        OrderItem.size_id,
        OrderItem.field_id,
        OrderItem.year,
        Plant.name.label('plant_name'),
        Size.name.label('size_name'),
        Field.name.label('field_name'),
        func.sum(OrderItem.quantity - func.coalesce(OrderItem.dug_quantity, 0)).label('left_qty')
    ).join(Order).join(Plant).join(Size).join(Field)
    grouped = grouped.filter(
        Order.status.in_(['reserved', 'in_progress']),
        Order.is_deleted == False
    ).group_by(
        OrderItem.plant_id, OrderItem.size_id, OrderItem.field_id, OrderItem.year
    ).having(func.sum(OrderItem.quantity - func.coalesce(OrderItem.dug_quantity, 0)) > 0)
    grouped = grouped.order_by(Plant.name, Size.name, Field.name, OrderItem.year).all()

    return render_template('digging/digging_form.html', groups=grouped)


@bp.route('/digging/order/<int:order_id>', methods=['GET', 'POST'])
@login_required
def mobile_order(order_id):
    # Оставлен для совместимости (но не используется в новом UI)
    return redirect(url_for('digging.mobile_index'))

@bp.route('/digging/report', methods=['GET', 'POST'])
@login_required
def manager_report():
    # Отчет для менеджера: Просмотр и правка выкопки за дату
    ensure_digging_table_exists()
    
    # По умолчанию сегодня
    date_str = request.args.get('date')
    if date_str:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    else:
        target_date = msk_now().date()

    # Обработка правок (POST)
    if request.method == 'POST':
        try:
            changed_count = 0

            orders_just_ready = set() # Создаем множество для отлова готовых заказов

            def recalc_affected(p_id, s_id, f_id, y, oi_id=None):
                """Вспомогательная функция для пересчета выкопанного по затронутым товарам"""
                items =[]
                if oi_id:
                    it = OrderItem.query.get(oi_id)
                    if it: items.append(it)
                else:
                    items = OrderItem.query.join(Order).filter(
                        Order.status.in_(['reserved', 'in_progress', 'ready']),
                        Order.is_deleted == False,
                        OrderItem.plant_id == p_id,
                        OrderItem.size_id == s_id,
                        OrderItem.field_id == f_id,
                        OrderItem.year == y
                    ).all()
                
                for it in items:
                    total_dug = db.session.query(func.sum(DiggingLog.quantity))\
                        .filter(
                            DiggingLog.order_item_id == it.id,
                            DiggingLog.status != 'rejected'
                        ).scalar() or 0
                    it.dug_quantity = total_dug
                    
                    # Отслеживаем изменение статуса именно при привязке
                    old_status = it.order.status
                    it.order.refresh_status_by_dug()
                    new_status = it.order.status
                    
                    if old_status != 'ready' and new_status == 'ready':
                        orders_just_ready.add(it.order)

            # 0) Удаление строк
            for key in request.form.keys():
                if not key.startswith('delete_'): continue
                log_id = int(key.split('_')[1])
                log_entry = DiggingLog.query.get(log_id)
                if log_entry and (log_entry.status == 'pending' or current_user.role == 'admin'):
                    p_id, s_id, f_id, y = log_entry.plant_id, log_entry.size_id, log_entry.field_id, log_entry.year
                    oi_id = log_entry.order_item_id
                    
                    db.session.delete(log_entry)
                    db.session.flush() # Применяем удаление для честного пересчета сумм
                    
                    # Пересчитываем все позиции, которые могли использовать этот лог
                    recalc_affected(p_id, s_id, f_id, y, oi_id)
                    changed_count += 1

            # 1) Редактирование количества
            for key, val in request.form.items():
                if not key.startswith('log_qty_'): continue
                log_id = int(key.split('_')[2])
                new_qty = int(val)

                log_entry = DiggingLog.query.get(log_id)
                # Только в статусе pending или админ может править запись
                if log_entry and (log_entry.status == 'pending' or current_user.role == 'admin') and log_entry.quantity != new_qty:
                    log_entry.quantity = new_qty
                    log_entry.status = 'approved'
                    db.session.flush() # Применяем изменение перед пересчетом

                    recalc_affected(log_entry.plant_id, log_entry.size_id, log_entry.field_id, log_entry.year, log_entry.order_item_id)
                    changed_count += 1

            # 2) Привязка незакрепленных записей к заказам (распределение)
            for key, val in request.form.items():
                if not key.startswith('assign_'): continue
                log_id = int(key.split('_')[1])
                try:
                    target_item_id = int(val)
                except (TypeError, ValueError):
                    continue

                log_entry = DiggingLog.query.get(log_id)
                if log_entry and log_entry.order_item_id != target_item_id:
                    p_id, s_id, f_id, y = log_entry.plant_id, log_entry.size_id, log_entry.field_id, log_entry.year
                    
                    log_entry.order_item_id = target_item_id
                    log_entry.status = 'approved'
                    db.session.flush()
                    
                    # Пересчитываем обе "стороны" (Откуда ушло нераспределенное и куда пришло распределенное)
                    recalc_affected(p_id, s_id, f_id, y, None) 
                    recalc_affected(p_id, s_id, f_id, y, target_item_id) 

                    changed_count += 1

            db.session.commit()
            if changed_count > 0:
                flash(f'Обновлено записей: {changed_count}', 'success')
                log_action(f"Менеджер обновил {changed_count} запись(ей) выкопки за {target_date}")
                
                # --- ОТПРАВКА В ТЕЛЕГРАМ ГОТОВЫХ ЗАКАЗОВ ---
                for r_order in orders_just_ready:
                    ready_msg = f"✅ <b>Заказ № {r_order.id} готов к отгрузке!</b>\n👤 Клиент: {r_order.client.name}\n\n📦 <b>Состав заказа:</b>\n"
                    
                    # Проходимся по всем позициям в готовом заказе
                    for item in r_order.items:
                        p_name = item.plant.name if item.plant else "Неизвестно"
                        s_name = item.size.name if item.size else "-"
                        ready_msg += f"— {p_name} ({s_name}): {item.quantity} шт.\n"
                        
                    _tg_send(ready_msg, chat_type="orders")
                # ------------------------------------------
                
            else:
                flash('Изменений не найдено', 'info')
                log_action(f"Менеджер открыл отчет по выкопке за {target_date}, но изменений не было")

        except Exception as e:
            db.session.rollback()
            flash(f'Ошибка сохранения: {e}', 'danger')

        return redirect(url_for('digging.manager_report', date=target_date))

    # Получаем логи за выбранную дату + все нераспределенные логи из прошлых дат
    # (с защитой от устаревшей схемы базы)
    try:
        logs = DiggingLog.query.filter(
            or_(
                DiggingLog.date == target_date,  # Логи за выбранную дату
                (DiggingLog.date < target_date) & (DiggingLog.order_item_id == None)  # Прошлые нераспределенные логи
            )
        ).outerjoin(OrderItem).outerjoin(Order).outerjoin(Client)\
            .order_by(Order.id.nullsfirst(), DiggingLog.created_at.desc()).all()
    except OperationalError as oe:
        # Иногда в старой БД отсутствуют новые колонки (plant_id/size_id/...) - добавляем и пробуем ещё раз
        if 'digging_log.plant_id' in str(oe) or 'digging_log.size_id' in str(oe) or 'digging_log.field_id' in str(oe) or 'digging_log.year' in str(oe):
            ensure_digging_table_exists()
            logs = DiggingLog.query.filter(
                or_(
                    DiggingLog.date == target_date,  # Логи за выбранную дату
                    (DiggingLog.date < target_date) & (DiggingLog.order_item_id == None)  # Прошлые нераспределенные логи
                )
            ).outerjoin(OrderItem).outerjoin(Order).outerjoin(Client)\
                .order_by(Order.id.nullsfirst(), DiggingLog.created_at.desc()).all()
        else:
            raise

    # Подготовка вариантов распределения для незакрепленных записей
    candidates = {}
    for log in logs:
        if log.order_item_id:
            continue
        if not (log.plant_id and log.size_id and log.field_id and log.year):
            continue

        items = OrderItem.query.join(Order).filter(
            Order.status.in_(['reserved', 'in_progress']),
            Order.is_deleted == False,
            OrderItem.plant_id == log.plant_id,
            OrderItem.size_id == log.size_id,
            OrderItem.field_id == log.field_id,
            OrderItem.year == log.year
        ).all()
        candidates[log.id] = items

    return render_template('digging/digging_report.html', logs=logs, target_date=target_date, candidates=candidates)


@bp.route('/digging/analytics', methods=['GET'])
@login_required
def analytics():
    # Аналитика по выкопке и рабочим часам
    end_date = msk_now().date()
    start_date = end_date - timedelta(days=30)

    date_from = request.args.get('from')
    date_to = request.args.get('to')
    if date_from:
        try: start_date = datetime.strptime(date_from, '%Y-%m-%d').date()
        except ValueError: pass
    if date_to:
        try: end_date = datetime.strptime(date_to, '%Y-%m-%d').date()
        except ValueError: pass

    # 1) Кол-во сотрудников, работавших в день (часы > 1 по любой ставке)
    tl = db.session.query(
        TimeLog.date,
        TimeLog.employee_id,
        (TimeLog.hours_norm + TimeLog.hours_norm_over + TimeLog.hours_spec + TimeLog.hours_spec_over).label('hours')
    ).filter(TimeLog.date >= start_date, TimeLog.date <= end_date).all()

    worked = {}
    for row in tl:
        if row.hours and row.hours > 1:
            # ЗАЩИТА ОТ ОШИБОК SQLITE С ДАТАМИ: приводим все к строке YYYY-MM-DD
            d_str = row.date.strftime('%Y-%m-%d') if hasattr(row.date, 'strftime') else str(row.date)
            worked.setdefault(d_str, set()).add(row.employee_id)

    # 2) Суммы выкопки по дням (нужны до списка дней на оси X)
    dug = db.session.query(
        DiggingLog.date,
        func.sum(DiggingLog.quantity).label('qty')
    ).filter(
        DiggingLog.date >= start_date,
        DiggingLog.date <= end_date,
        DiggingLog.status != 'rejected'
    ).group_by(DiggingLog.date).all()

    dug_map = { (r.date.strftime('%Y-%m-%d') if hasattr(r.date, 'strftime') else str(r.date)): int(r.qty or 0) for r in dug }

    # 3) Список дней на графике: был табель (часы > 1) ИЛИ есть выкопка.
    # Раньше брали только дни с табелем — при отсутствии подходящего TimeLog график был пустым,
    # хотя выкопка в журнале есть.
    days = []
    cur = start_date
    while cur <= end_date:
        cur_str = cur.strftime('%Y-%m-%d')
        has_workers = cur_str in worked and len(worked[cur_str]) > 0
        if has_workers or cur_str in dug_map:
            days.append(cur)
        cur += timedelta(days=1)

    labels = [d.strftime('%d.%m') for d in days]
    workers_per_day = [len(worked.get(d.strftime('%Y-%m-%d'), set())) for d in days]
    dug_per_day = [dug_map.get(d.strftime('%Y-%m-%d'), 0) for d in days]

    # 4) Разбивка по видам растений (без размеров)
    plant_rows = db.session.query(
        DiggingLog.date,
        Plant.name,
        func.sum(DiggingLog.quantity).label('qty')
    ).join(Plant, DiggingLog.plant_id == Plant.id)
    plant_rows = plant_rows.filter(
        DiggingLog.date >= start_date,
        DiggingLog.date <= end_date,
        DiggingLog.status != 'rejected'
    ).group_by(DiggingLog.date, Plant.name).all()

    # Группируем данные в словарь { 'YYYY-MM-DD': { 'Растение': кол-во } }
    plant_data_map = {}
    for r in plant_rows:
        d_str = r.date.strftime('%Y-%m-%d') if hasattr(r.date, 'strftime') else str(r.date)
        if d_str not in plant_data_map:
            plant_data_map[d_str] = {}
        plant_data_map[d_str][r.name] = int(r.qty or 0)

    plant_names = sorted({r.name for r in plant_rows})[:10]
    palette =['#3366CC','#DC3912','#FF9900','#109618','#990099','#0099C6','#DD4477','#66AA00','#B82E2E','#316395']
    plant_series =[]
    plant_totals = {}
    
    for idx, plant in enumerate(plant_names):
        data =[]
        total = 0
        for d in days:
            d_str = d.strftime('%Y-%m-%d')
            qty = plant_data_map.get(d_str, {}).get(plant, 0)
            data.append(qty)
            total += qty
            
        plant_series.append({
            'label': plant,
            'data': data,
            'borderColor': palette[idx % len(palette)],
            'backgroundColor': palette[idx % len(palette)],
            'fill': False,
            'hidden': True,
            'yAxisID': 'y1',
        })
        plant_totals[plant] = total

    plant_totals_list = sorted([{'plant': k, 'total': v} for k, v in plant_totals.items()],
        key=lambda x: x['total'],
        reverse=True
    )

    # Итоги: «рабочие дни на графике» — дни с табелем или выкопкой; среднее по выкопке — только по дням, где выкопано > 0
    active_days_count = len(days)
    days_with_digging = sum(1 for q in dug_per_day if q > 0)
    total_dug = sum(dug_per_day)
    total_workers_days = sum(workers_per_day)

    summary = {
        'days': active_days_count,
        'days_with_digging': days_with_digging,
        'total_workers': total_workers_days,
        'total_dug': total_dug,
        'avg_dug_per_day': int(round(total_dug / days_with_digging)) if days_with_digging > 0 else 0,
        'avg_dug_per_worker': int(round(total_dug / total_workers_days)) if total_workers_days > 0 else 0,
    }

    chart_data = {
        'labels': labels,
        'workers': workers_per_day,
        'dug': dug_per_day,
        'plantSeries': plant_series,
    }

    return render_template('digging/digging_analytics.html',
                           chart_data=chart_data,
                           plant_totals=plant_totals_list,
                           start_date=start_date,
                           end_date=end_date,
                           summary=summary)
