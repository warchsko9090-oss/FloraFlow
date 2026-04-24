from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for, flash, make_response
from flask_login import login_required, current_user
from sqlalchemy import func, inspect, text, or_
from sqlalchemy.exc import OperationalError
from app.models import db, Order, OrderItem, DiggingLog, Client, Plant, Size, Field, TimeLog, ActionLog, DiggingTask
from app.utils import msk_now, msk_today, log_action, natural_key
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

    today_date = msk_today()

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

        # Обработка формы выкопки — теперь по конкретным заданиям диспетчерской на сегодня
        try:
            count_saved = 0
            tg_total_qty = 0
            orders_just_ready = set()
            affected_items = {}  # {item_id: OrderItem}
            tasks_to_check = set()
            # Для Telegram-отчёта: агрегируем по (order_id, plant_id, size_id) — поля в отчёт не попадают.
            tg_added_by_pos = {}  # {(order_id, plant_id, size_id): qty_added_in_submit}
            tg_order_ids = set()

            for key, val in request.form.items():
                if not key.startswith('qty_task_'):
                    continue
                if not val or not str(val).strip():
                    continue
                try:
                    task_id = int(key.replace('qty_task_', ''))
                    qty_dig = int(val)
                except (ValueError, TypeError):
                    continue
                if qty_dig <= 0:
                    continue

                task = DiggingTask.query.get(task_id)
                # Разрешаем вносить факт по задачам на сегодня ИЛИ просроченным (planned_date < today).
                # Будущие (завтра и позже) бригадиру трогать нельзя — их в форме быть не должно вовсе.
                if not task or task.status == 'done' or task.planned_date > today_date:
                    continue
                item = task.item
                if not item or item.order.is_deleted:
                    continue

                ordered_total = item.quantity or 0
                dug_before = item.dug_total or 0
                left_to_dig = ordered_total - dug_before
                if qty_dig > left_to_dig:
                    flash(
                        f'❌ Нельзя выкопать {qty_dig} шт. для {item.plant.name} · {item.size.name} '
                        f'(осталось {left_to_dig} шт.). Строка пропущена.',
                        'danger'
                    )
                    continue

                log = DiggingLog(
                    date=today_date,
                    order_item_id=item.id,
                    plant_id=item.plant_id,
                    size_id=item.size_id,
                    field_id=item.field_id,
                    year=item.year,
                    user_id=current_user.id,
                    quantity=qty_dig,
                    status='approved'
                )
                db.session.add(log)
                count_saved += 1
                tg_total_qty += qty_dig

                pos_key = (item.order_id, item.plant_id, item.size_id)
                tg_added_by_pos[pos_key] = tg_added_by_pos.get(pos_key, 0) + qty_dig
                tg_order_ids.add(item.order_id)

                affected_items[item.id] = item
                tasks_to_check.add(task.id)

            # Пересчет dug_quantity по затронутым позициям
            if affected_items:
                item_ids = list(affected_items.keys())
                dug_agg = db.session.query(
                    DiggingLog.order_item_id, func.sum(DiggingLog.quantity)
                ).filter(
                    DiggingLog.order_item_id.in_(item_ids),
                    DiggingLog.status != 'rejected'
                ).group_by(DiggingLog.order_item_id).all()
                dug_map = {r[0]: int(r[1] or 0) for r in dug_agg}

                for item in affected_items.values():
                    item.dug_quantity = dug_map.get(item.id, 0)
                    old_status = item.order.status
                    item.order.refresh_status_by_dug()
                    if old_status != 'ready' and item.order.status == 'ready':
                        orders_just_ready.add(item.order)

            # Автозакрытие заданий: задание done, если по позиции выкопано >= planned_qty
            for tid in tasks_to_check:
                t = DiggingTask.query.get(tid)
                if not t:
                    continue
                dug_for_item = (t.item.dug_total or 0) if t.item else 0
                if dug_for_item >= (t.planned_qty or 0):
                    t.status = 'done'

            if count_saved > 0:
                db.session.commit()
                flash(f'Записано позиций: {count_saved}. Спасибо за работу!', 'success')
                log_action(f"Бригадир внес выкопку ({count_saved} строк)")

                # --- Telegram-отчёт по выкопке (формат E.3) ---
                if tg_order_ids:
                    def _pct(a, b):
                        return int(round((a or 0) * 100 / b)) if b else 0

                    def _icon(dug, ordered):
                        if ordered and dug >= ordered:
                            return "✅"
                        if dug and dug > 0:
                            return "⏳"
                        return "❌"

                    # Telegram на мобиле использует пропорциональный шрифт — выравнивание
                    # пробелами не работает. Поэтому форматируем «человеческим» образом:
                    # короткие разделители, жирные числа, без «дырок» слева.
                    divider = "━━━━━━━━━━━━━"
                    msg_lines = [
                        f"🚜 <b>{current_user.username}</b> · выкопка {today_date.strftime('%d.%m')}",
                    ]

                    for order_id in sorted(tg_order_ids):
                        order = Order.query.get(order_id)
                        if not order:
                            continue
                        client_name = order.client.name if order.client else "—"

                        # Все позиции заказа — нужны для итога по заказу целиком.
                        order_items = OrderItem.query.filter_by(order_id=order_id).all()

                        # Агрегация по (plant_id, size_id): одна строка на позицию в отчёте.
                        pos_map = {}
                        for oi in order_items:
                            key = (oi.plant_id, oi.size_id)
                            if key not in pos_map:
                                pos_map[key] = {
                                    'plant_name': oi.plant.name if oi.plant else '—',
                                    'size_name': oi.size.name if oi.size else '—',
                                    'ordered': 0,
                                    'dug': 0,
                                }
                            pos_map[key]['ordered'] += oi.quantity or 0
                            pos_map[key]['dug'] += oi.dug_quantity or 0

                        order_total_ordered = sum(p['ordered'] for p in pos_map.values())
                        order_total_dug = sum(p['dug'] for p in pos_map.values())

                        # В отчёт пишем ВСЕ позиции заказа: для тронутых — с пометкой «+N»,
                        # для остальных — текущий прогресс, чтобы было видно общий расклад по заказу.
                        rows = []
                        for (p_id, s_id), pos in pos_map.items():
                            added = tg_added_by_pos.get((order_id, p_id, s_id), 0)
                            rows.append({**pos, 'added': added})

                        # Нужно хоть одно тронутое по заказу, иначе заказ в отчёт не идёт вовсе.
                        if not any(r['added'] > 0 for r in rows):
                            continue

                        rows.sort(key=lambda r: (
                            0 if r['added'] > 0 else 1,  # тронутые сверху
                            (r['plant_name'] or '').lower(),
                            natural_key(r['size_name'] or '')
                        ))

                        msg_lines.append("")
                        msg_lines.append(f"🏷 <b>{client_name}</b> · #Заказ-{order_id}")
                        msg_lines.append(divider)
                        for p in rows:
                            icon = _icon(p['dug'], p['ordered'])
                            pct = _pct(p['dug'], p['ordered'])
                            title = f"🌳 <b>{p['plant_name']} {p['size_name']}</b>"
                            if p['added'] > 0:
                                # Тронутая позиция: сверху — «Выкопано N», снизу — общий прогресс.
                                msg_lines.append(f"{title}  <b>Выкопано {p['added']}</b>")
                                msg_lines.append(
                                    f"   └ {p['dug']}/{p['ordered']} ({pct}%) {icon}"
                                )
                            else:
                                # Нетронутая: одна строка, светлее — чтобы не тянула внимание.
                                msg_lines.append(
                                    f"{title}  <i>{p['dug']}/{p['ordered']} ({pct}%) {icon}</i>"
                                )
                        msg_lines.append(divider)
                        opct = _pct(order_total_dug, order_total_ordered)
                        msg_lines.append(
                            f"📦 По заказу: <b>{order_total_dug}/{order_total_ordered}</b> ({opct}%)"
                        )

                    msg_lines.append("")
                    msg_lines.append(f"📊 <b>Итого сегодня: {tg_total_qty} шт.</b>")
                    _tg_send("\n".join(msg_lines), chat_type="digging")

                # Заказ готов к отгрузке
                for r_order in orders_just_ready:
                    ready_msg = f"✅ <b>Заказ № {r_order.id} готов к отгрузке!</b> Клиент {r_order.client.name}"
                    _tg_send(ready_msg, chat_type="orders")

            else:
                flash('Вы ничего не ввели.', 'warning')
        except Exception as e:
            db.session.rollback()
            flash(f'Ошибка: {e}', 'danger')

        return redirect(url_for('digging.mobile_index'))

    # GET: показываем задания диспетчерской на сегодня + просроченные (которые ещё не закрыты).
    # Завтрашние и более поздние задания бригадиру не показываем — чтобы не копал вперёд плана.
    today_tasks = (DiggingTask.query
                   .filter(DiggingTask.planned_date <= today_date,
                           DiggingTask.status != 'done')
                   .join(OrderItem, DiggingTask.order_item_id == OrderItem.id)
                   .join(Order, OrderItem.order_id == Order.id)
                   .filter(Order.is_deleted == False)
                   .all())

    tasks_view = []
    for t in today_tasks:
        item = t.item
        if not item:
            continue
        # Если позиция уже закопана полностью (left_to_dig == 0) — нет смысла держать её в форме.
        ordered = item.quantity or 0
        dug_total = item.dug_total or 0
        if ordered and dug_total >= ordered:
            continue
        fact_today = db.session.query(func.sum(DiggingLog.quantity)).filter(
            DiggingLog.order_item_id == item.id,
            DiggingLog.date == today_date,
            DiggingLog.status != 'rejected'
        ).scalar() or 0
        is_overdue = t.planned_date < today_date
        days_overdue = (today_date - t.planned_date).days if is_overdue else 0
        tasks_view.append({
            'id': t.id,
            'planned_date': t.planned_date,
            'planned_qty': t.planned_qty,
            'comment': t.comment,
            'plant_name': item.plant.name if item.plant else '—',
            'size_name': item.size.name if item.size else '—',
            'field_name': item.field.name if item.field else '—',
            'year': item.year,
            'order_id': item.order_id,
            'client_name': item.order.client.name if item.order and item.order.client else '—',
            'ordered': ordered,
            'dug_total': dug_total,
            'fact_today': int(fact_today),
            'already_before_today': max(0, dug_total - int(fact_today)),
            'left_to_dig': max(0, ordered - dug_total),
            'is_overdue': is_overdue,
            'days_overdue': days_overdue,
        })

    # Сортировка: сначала просроченные (самые старые сверху), затем сегодняшние.
    # Внутри одинакового статуса — натуральная сортировка по растению/размеру/полю/году.
    tasks_view.sort(key=lambda r: (
        0 if r['is_overdue'] else 1,
        r['planned_date'],
        r['plant_name'].lower(),
        natural_key(r['size_name']),
        natural_key(r['field_name']),
        r['year'] or 0,
    ))

    overdue_count = sum(1 for r in tasks_view if r['is_overdue'])

    return render_template('digging/digging_form.html',
                           tasks=tasks_view,
                           today=today_date,
                           overdue_count=overdue_count)


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

    # КПД: сколько растений на 1 работника в день.
    # Если в этот день никто не отмечен в табеле (workers=0) — ставим None,
    # чтобы линия на графике прервалась и не вводила в заблуждение.
    efficiency_per_day = []
    for d_qty, w_qty in zip(dug_per_day, workers_per_day):
        if w_qty and w_qty > 0:
            efficiency_per_day.append(round(d_qty / w_qty, 1))
        else:
            efficiency_per_day.append(None)

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

    # Средний КПД по дням, где работали люди И была выкопка (обычная арифметическая средняя)
    eff_valid = [e for e in efficiency_per_day if e is not None and e > 0]
    avg_efficiency_by_day = round(sum(eff_valid) / len(eff_valid), 1) if eff_valid else 0
    # Пиковый день
    peak_efficiency = max(eff_valid) if eff_valid else 0

    summary = {
        'days': active_days_count,
        'days_with_digging': days_with_digging,
        'total_workers': total_workers_days,
        'total_dug': total_dug,
        'avg_dug_per_day': int(round(total_dug / days_with_digging)) if days_with_digging > 0 else 0,
        'avg_dug_per_worker': round(total_dug / total_workers_days, 1) if total_workers_days > 0 else 0,
        'avg_efficiency_by_day': avg_efficiency_by_day,
        'peak_efficiency': peak_efficiency,
    }

    chart_data = {
        'labels': labels,
        'workers': workers_per_day,
        'dug': dug_per_day,
        'efficiency': efficiency_per_day,
        'plantSeries': plant_series,
    }

    return render_template('digging/digging_analytics.html',
                           chart_data=chart_data,
                           plant_totals=plant_totals_list,
                           start_date=start_date,
                           end_date=end_date,
                           summary=summary)

from app.models import DiggingTask # Убедись, что импортировал новую модель в начале файла

from datetime import timedelta # Убедись, что это импортировано

import calendar
from datetime import timedelta
from sqlalchemy import func
from app.models import DiggingTask, Order, OrderItem
from app.utils import msk_today, MONTH_NAMES

@bp.route('/digging/planning', methods=['GET', 'POST'])
@login_required
def digging_planning():
    if current_user.role not in ['admin', 'user', 'executive']:
        return redirect(url_for('main.index'))

    if request.method == 'POST':
        action = request.form.get('action')
        
        # Перетаскивание заказа на календарь
        if action == 'create_order_tasks':
            item_ids = request.form.getlist('item_id[]')
            qtys = request.form.getlist('qty[]')
            date_str = request.form.get('planned_date')
            comment = request.form.get('comment')
            
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            added_count = 0
            
            for i_id, q_str in zip(item_ids, qtys):
                q = int(q_str) if q_str.isdigit() else 0
                if q > 0:
                    task = DiggingTask(
                        order_item_id=int(i_id),
                        planned_date=target_date,
                        planned_qty=q,
                        comment=comment,
                        created_by_user_id=current_user.id
                    )
                    db.session.add(task)
                    added_count += 1
            
            if added_count > 0:
                db.session.commit()
                flash(f'Задания на {target_date.strftime("%d.%m.%Y")} успешно добавлены!', 'success')
            
        elif action == 'delete_task':
            task = DiggingTask.query.get(request.form.get('task_id'))
            if task:
                db.session.delete(task)
                db.session.commit()
                flash('Задание отменено, объем вернулся в заказ.', 'info')
                
        return redirect(url_for('digging.digging_planning'))

    # 1. СОБИРАЕМ ЗАКАЗЫ ДЛЯ ЛЕВОЙ КОЛОНКИ (Ждут распределения)
    active_orders = Order.query.filter(Order.status.in_(['reserved', 'in_progress']), Order.is_deleted == False).order_by(Order.date).all()
    orders_to_plan = []

    for o in active_orders:
        left_for_order = 0
        items_preview = []  # детализация позиций, у которых осталось что копать
        for i in o.items:
            planned_qty = sum(t.planned_qty for t in i.digging_tasks if t.status == 'pending')
            left_to_plan = i.quantity - i.dug_total - planned_qty
            if left_to_plan > 0:
                left_for_order += left_to_plan
                items_preview.append({
                    'plant': i.plant.name if i.plant else '—',
                    'size': i.size.name if i.size else '—',
                    'field': i.field.name if i.field else '—',
                    'left': left_to_plan,
                    'total': i.quantity,
                    'dug': i.dug_total or 0,
                    'planned': planned_qty,
                })

        if left_for_order > 0:
            # Временные атрибуты для шаблона.
            o.left_to_plan = left_for_order
            o.items_preview = items_preview
            orders_to_plan.append(o)

    # 2. ГЕНЕРИРУЕМ СЕТКУ КАЛЕНДАРЯ
    today = msk_today()
    # Получаем месяц и год из URL (если нет - берем текущие)
    year = request.args.get('year', type=int, default=today.year)
    month = request.args.get('month', type=int, default=today.month)

    cal = calendar.Calendar(firstweekday=0) # Понедельник первый
    month_days = cal.monthdatescalendar(year, month) # Список недель

    # Достаем все задачи за этот период (от начала первой недели до конца последней)
    start_date = month_days[0][0]
    end_date = month_days[-1][-1]
    
    tasks = DiggingTask.query.filter(
        DiggingTask.planned_date >= start_date,
        DiggingTask.planned_date <= end_date,
        DiggingTask.status == 'pending'
    ).all()

    # Собираем матрицу: список недель, внутри список дней (словари).
    # Для каждого дня дополнительно строим группировку по заказу: это
    # нужно и для превью в ячейке (имена клиентов), и для hover-попапа.
    calendar_grid = []
    for week in month_days:
        week_data = []
        for d in week:
            day_tasks = [t for t in tasks if t.planned_date == d]

            # Группируем задачи этого дня по заказу: {order_id: {...}}.
            orders_map = {}
            for t in day_tasks:
                oi = t.item
                o = oi.order if oi else None
                if not o:
                    continue
                grp = orders_map.setdefault(o.id, {
                    'order_id': o.id,
                    'client': o.client.name if o.client else '—',
                    'total_qty': 0,
                    'tasks_count': 0,
                    'items': [],
                })
                grp['total_qty'] += t.planned_qty or 0
                grp['tasks_count'] += 1
                grp['items'].append({
                    'plant': oi.plant.name if oi.plant else '—',
                    'size': oi.size.name if oi.size else '—',
                    'field': oi.field.name if oi.field else '—',
                    'qty': t.planned_qty or 0,
                    'comment': t.comment or '',
                })
            orders_groups = sorted(orders_map.values(), key=lambda g: (-g['total_qty'], g['client']))
            # Список уникальных клиентов в порядке "кто больше копает — тот первее".
            clients_summary = []
            for g in orders_groups:
                clients_summary.append({'client': g['client'], 'qty': g['total_qty']})

            week_data.append({
                'date_obj': d,
                'date_str': d.strftime('%Y-%m-%d'),
                'day': d.day,
                'is_current_month': d.month == month,
                'is_today': d == today,
                'tasks_count': len(day_tasks),
                'total_plants': sum(t.planned_qty for t in day_tasks),
                'orders_groups': orders_groups,   # для hover-попапа
                'clients_summary': clients_summary,  # для чипов клиентов в ячейке
            })
        calendar_grid.append(week_data)

    # Вычисляем предыдущий и следующий месяц для кнопок навигации
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1

    resp = make_response(render_template('digging/planning.html',
                           orders_to_plan=orders_to_plan,
                           calendar_grid=calendar_grid,
                           current_year=year,
                           month_name=MONTH_NAMES.get(month, ''),
                           prev_month=prev_month, prev_year=prev_year,
                           next_month=next_month, next_year=next_year))
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    return resp

# API: Форма ПРИ ПЕРЕТАСКИВАНИИ заказа на календарь
@bp.route('/api/digging/order_plan_form/<int:order_id>/<date_str>')
@login_required
def get_order_plan_form(order_id, date_str):
    order = Order.query.get_or_404(order_id)
    items_html = ""
    
    for i in order.items:
        planned_qty = sum(t.planned_qty for t in i.digging_tasks if t.status == 'pending')
        left_to_plan = i.quantity - i.dug_total - planned_qty
        
        if left_to_plan > 0:
            items_html += f"""
            <div class="d-flex justify-content-between align-items-center mb-2 p-2 border rounded bg-light">
                <div style="line-height: 1.2;">
                    <input type="hidden" name="item_id[]" value="{i.id}">
                    <strong class="text-dark">{i.plant.name}</strong><br>
                    <small class="text-muted">{i.size.name} | Поле {i.field.name}</small>
                </div>
                <div style="width: 100px;">
                    <label class="small text-muted text-center w-100 mb-0">Осталось: {left_to_plan}</label>
                    <input type="number" name="qty[]" class="form-control form-control-sm text-center fw-bold border-success text-success" value="{left_to_plan}" min="0" max="{left_to_plan}">
                </div>
            </div>
            """
            
    if not items_html:
        return "<div class='alert alert-success'>Весь объем по этому заказу уже распределен!</div>"
        
    date_formatted = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    
    form_html = f"""
    <form method="POST" action="/digging/planning" hx-boost="false">
        <input type="hidden" name="action" value="create_order_tasks">
        <input type="hidden" name="planned_date" value="{date_str}">
        <div class="mb-3 text-center">
            <h5 class="fw-bold text-primary mb-1">Заказ #{order.id} ({order.client.name})</h5>
            <span class="badge bg-success fs-6">Назначаем выкопку на: {date_formatted}</span>
        </div>
        <div class="mb-3" style="max-height: 300px; overflow-y: auto;">
            {items_html}
        </div>
        <div class="mb-3">
            <input type="text" name="comment" class="form-control" placeholder="Комментарий бригадиру (напр: копать первыми)...">
        </div>
        <button type="submit" class="btn btn-success w-100 fw-bold py-2"><i class="fas fa-save me-2"></i> Сохранить в план</button>
    </form>
    """
    return form_html

def _render_day_details_html(target_date):
    """Рендерит HTML для модалки «план на день». Вынесено, чтобы один и
    тот же рендер использовать и при первичной загрузке, и при массовом
    удалении (свап в ту же модалку без её закрытия)."""
    tasks = DiggingTask.query.filter_by(planned_date=target_date, status='pending').all()
    date_formatted = target_date.strftime("%d.%m.%Y")
    date_str = target_date.strftime("%Y-%m-%d")

    if not tasks:
        return (
            f"<div id='dayDetailsBody' data-date='{date_str}'>"
            f"<div class='text-center p-4 text-muted'>"
            f"<h5 class='fw-bold'>{date_formatted}</h5>"
            f"На этот день заданий нет. Перетащите сюда заказ."
            f"</div></div>"
        )

    total_qty = sum(t.planned_qty or 0 for t in tasks)

    cards_html = ""
    for t in tasks:
        comment_html = (
            f'<div class="mt-3 bg-warning bg-opacity-10 border border-warning p-2 rounded small text-dark">'
            f'<i class="fas fa-comment-dots text-warning me-1"></i> {t.comment}</div>'
        ) if t.comment else ''

        cards_html += f"""
        <div class="card mb-3 border-0 shadow-sm border-start border-4 border-success js-task-card" data-task-id="{t.id}">
            <div class="card-body p-3">
                <div class="d-flex justify-content-between align-items-start mb-2 pb-2 border-bottom gap-2">
                    <label class="d-flex align-items-center gap-2 flex-grow-1" style="cursor:pointer;">
                        <input type="checkbox"
                               class="form-check-input js-task-check"
                               name="task_ids[]"
                               value="{t.id}"
                               style="width:18px;height:18px;margin:0;">
                        <span class="small text-muted text-uppercase fw-bold">
                            Заказ #{t.item.order_id} <span class="mx-1">&bull;</span> {t.item.order.client.name}
                        </span>
                    </label>
                    <button type="button"
                            class="btn btn-link text-danger p-0 m-0 js-task-delete-one"
                            data-task-id="{t.id}"
                            title="Снять это задание">
                        <i class="fas fa-trash-alt"></i>
                    </button>
                </div>

                <div class="d-flex justify-content-between align-items-start mt-2">
                    <div>
                        <div class="fw-bold text-dark lh-1 mb-2" style="font-size: 1.15rem;">
                            {t.item.plant.name} <span class="badge bg-light text-dark border ms-1" style="font-size: 0.9rem; vertical-align: middle;">{t.item.size.name}</span>
                        </div>
                        <div class="badge bg-secondary px-2 py-1 shadow-sm fw-normal">
                            <i class="fas fa-map-marker-alt opacity-75 me-1"></i>Поле {t.item.field.name}
                        </div>
                    </div>
                    <div class="text-end">
                        <div class="small text-muted text-uppercase fw-bold mb-1">План:</div>
                        <h4 class="fw-bold text-success mb-0">{t.planned_qty} <span class="fs-6 text-muted fw-normal">шт</span></h4>
                    </div>
                </div>

                {comment_html}
            </div>
        </div>
        """

    header_html = f"""
    <div id='dayDetailsBody' data-date='{date_str}'>
      <div class="d-flex justify-content-between align-items-center mb-3 flex-wrap gap-2">
        <div>
          <h5 class="fw-bold text-dark m-0">План на {date_formatted}</h5>
          <div class="small text-muted">{len(tasks)} задан. · {total_qty} шт</div>
        </div>
        <div class="d-flex align-items-center gap-2">
          <label class="d-flex align-items-center gap-2 small text-muted" style="cursor:pointer;">
            <input type="checkbox" class="form-check-input js-task-check-all" style="width:16px;height:16px;margin:0;">
            Выбрать все
          </label>
          <button type="button" class="btn btn-danger btn-sm fw-bold js-task-delete-bulk" disabled>
            <i class="fas fa-trash-alt me-1"></i>Удалить выбранные (<span class="js-selected-count">0</span>)
          </button>
        </div>
      </div>
      {cards_html}
    </div>
    """
    return header_html


# API: Детализация при КЛИКЕ на ячейку календаря
@bp.route('/api/digging/day_details/<date_str>')
@login_required
def get_day_details(date_str):
    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    return _render_day_details_html(target_date)


# API: массовое удаление задач за один день. Принимает task_ids[] + date.
# В ответ отдаёт свежий HTML детализации дня, чтобы модалка НЕ закрывалась,
# а просто обновилась на месте.
@bp.route('/api/digging/day_tasks_bulk_delete', methods=['POST'])
@login_required
def digging_day_tasks_bulk_delete():
    if current_user.role not in ['admin', 'user', 'executive']:
        return ("forbidden", 403)

    date_str = request.form.get('date_str') or ''
    raw_ids = request.form.getlist('task_ids[]') or request.form.getlist('task_ids')
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        return ("bad date", 400)

    task_ids = []
    for raw in raw_ids:
        try:
            task_ids.append(int(raw))
        except (TypeError, ValueError):
            continue

    deleted = 0
    if task_ids:
        tasks = DiggingTask.query.filter(
            DiggingTask.id.in_(task_ids),
            DiggingTask.planned_date == target_date,
            DiggingTask.status == 'pending'
        ).all()
        for t in tasks:
            db.session.delete(t)
            deleted += 1
        if deleted:
            db.session.commit()

    return _render_day_details_html(target_date)


# API: перенос всего плана с одной даты на другую (drag ячейки календаря).
# Возвращает JSON; клиент после успеха перезагружает страницу, чтобы
# перерисовать сетку с новыми позициями.
@bp.route('/api/digging/day_move', methods=['POST'])
@login_required
def digging_day_move():
    if current_user.role not in ['admin', 'user', 'executive']:
        return ({"ok": False, "error": "forbidden"}, 403)

    from_str = (request.form.get('from_date') or '').strip()
    to_str = (request.form.get('to_date') or '').strip()
    try:
        from_date = datetime.strptime(from_str, '%Y-%m-%d').date()
        to_date = datetime.strptime(to_str, '%Y-%m-%d').date()
    except Exception:
        return ({"ok": False, "error": "bad_date"}, 400)

    if from_date == to_date:
        return {"ok": True, "moved": 0, "skipped": "same_date"}

    tasks = DiggingTask.query.filter_by(planned_date=from_date, status='pending').all()
    moved = 0
    for t in tasks:
        t.planned_date = to_date
        moved += 1
    if moved:
        db.session.commit()

    return {
        "ok": True,
        "moved": moved,
        "from": from_str,
        "to": to_str,
    }