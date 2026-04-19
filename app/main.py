import os
import json
from flask import Blueprint, redirect, url_for, send_file, send_from_directory, current_app, render_template, jsonify, request
from flask_login import current_user, login_required
from datetime import timedelta
from sqlalchemy import func
from app.models import db, Order, PaymentInvoice, Document, PatentPeriod, OrderItem, DiggingTask, DiggingLog, ActionLog, TgTask
from app.utils import msk_today, msk_now

bp = Blueprint('main', __name__)

@bp.route('/')
def index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.login'))

    today = msk_today()
    
    # Структура папок для дашборда
    groups = {
        'overdue': {'id': 'overdue', 'label': '🔥 Срочно / Просрочено', 'color': 'danger', 'icon': 'fa-exclamation-circle', 'cards': [], 'is_open': True, 'order': 1},
        'today': {'id': 'today', 'label': '✅ План на сегодня', 'color': 'success', 'icon': 'fa-calendar-day', 'cards': [], 'is_open': True, 'order': 2},
        'tomorrow': {'id': 'tomorrow', 'label': '➡️ В планах на завтра', 'color': 'primary', 'icon': 'fa-arrow-right', 'cards': [], 'is_open': False, 'order': 3},
        'future': {'id': 'future', 'label': '⏳ Предстоящие задачи', 'color': 'secondary', 'icon': 'fa-calendar-alt', 'cards': [], 'is_open': False, 'order': 4}
    }

    def add_to_group(card, target_date, is_forced_urgent=False):
        if is_forced_urgent:
            groups['overdue']['cards'].append(card)
            return
        
        diff = (target_date - today).days
        if diff < 0:
            groups['overdue']['cards'].append(card)
        elif diff == 0:
            groups['today']['cards'].append(card)
        elif diff == 1:
            groups['tomorrow']['cards'].append(card)
        else:
            groups['future']['cards'].append(card)

    # 1. Счета на оплату
    if current_user.role in ['admin', 'executive']:
        invoices = PaymentInvoice.query.filter(PaymentInvoice.status != 'paid').all()
        for inv in invoices:
            remaining = inv.amount - sum(e.amount for e in inv.expenses)
            if remaining > 0:
                d = inv.due_date or today
                is_urgent = inv.priority == 'high' or (d - today).days <= 3
                card = {
                    'id': f'inv_{inv.id}',
                    'type': 'invoice',
                    'title': 'Счет на оплату',
                    'text': f'{inv.item.name if inv.item else "Без статьи"}: {inv.original_name}',
                    'amount': remaining,
                    'url': url_for('finance.expenses', invoice_id=inv.id),
                    'date_str': d.strftime('%d.%m.%Y'),
                    'raw_date': d
                }
                add_to_group(card, d, is_forced_urgent=is_urgent)

    # 2. Черновики с сайта
    if current_user.role in ['admin', 'user']:
        drafts = Document.query.filter_by(doc_type='client_draft').all()
        for d in drafts:
            card = {
                'id': f'draft_{d.id}',
                'type': 'draft',
                'title': 'Новая заявка с сайта',
                'text': 'Требует подтверждения менеджером.',
                'url': url_for('orders.client_draft_detail', doc_id=d.id),
                'date_str': d.date.strftime('%d.%m.%Y'),
                'raw_date': today
            }
            # Черновики считаем срочными
            add_to_group(card, today, is_forced_urgent=True)

    # 3. Выкопка (Задачи)
    if current_user.role in ['admin', 'brigadier', 'user2']:
        tasks = DiggingTask.query.filter(DiggingTask.status == 'pending').all()
        for t in tasks:
            if t.item and t.item.order and not t.item.order.is_deleted:
                d = t.planned_date
                card = {
                    'id': f'task_{t.id}',
                    'type': 'task',
                    'order_id': t.item.order_id,
                    'client_name': t.item.order.client.name,
                    'plant_name': t.item.plant.name,
                    'size_name': t.item.size.name,
                    'field_name': t.item.field.name,
                    'qty': t.planned_qty,
                    'comment': t.comment,
                    'task_id': t.id,
                    'date_str': d.strftime('%d.%m.%Y'),
                    'raw_date': d
                }
                add_to_group(card, d)

    # 4. Патенты HR
    if current_user.role in ['admin', 'user2', 'executive']:
        patents = PatentPeriod.query.filter_by(is_current=True, status='active').all()
        for p in patents:
            if p.end_date:
                days_left = (p.end_date - today).days
                if days_left <= 14:
                    card = {
                        'id': f'pat_{p.id}',
                        'type': 'patent',
                        'title': 'Истекает патент',
                        'text': f'{p.employee.name} (осталось {days_left} дн.)',
                        'url': url_for('hr.foreign_employee_card', employee_id=p.employee.id),
                        'date_str': p.end_date.strftime('%d.%m.%Y'),
                        'raw_date': p.end_date
                    }
                    add_to_group(card, p.end_date, is_forced_urgent=(days_left<=7))

    # 5. ЗАДАЧИ ИЗ TELEGRAM (Умные)
    from sqlalchemy import or_, and_
    
    target_roles = [current_user.role]
    
    # Запрос: Показываем задачи, где я указан ЛИЧНО (assignee_id == my_id)
    # ИЛИ (никому лично не назначено И моя роль подходит)
    # ИЛИ (я Админ и вижу всё)
    if current_user.role == 'admin':
        query = TgTask.query.filter(TgTask.status == 'new')
    else:
        query = TgTask.query.filter(
            TgTask.status == 'new',
            or_(
                TgTask.assignee_id == current_user.id,
                and_(TgTask.assignee_id.is_(None), TgTask.assignee_role.in_(target_roles))
            )
        )
        
    tg_tasks = query.all()
    
    for t in tg_tasks:
        d = t.deadline or today
        card = {
            'id': f'tgtask_{t.id}',
            'type': 'tg_task',
            'title': t.title,
            'details': t.details,
            'task_id': t.id,
            'assignee_name': t.assignee.username if t.assignee else None,
            'date_str': d.strftime('%d.%m.%Y'),
            'raw_date': d
        }
        add_to_group(card, d)
        groups['today']['cards'][-1]['color'] = 'info' if card in groups['today']['cards'] else 'info'

    # Сортировка внутри папок по дате и удаление пустых папок
    for g in groups.values():
        g['cards'].sort(key=lambda x: x.get('raw_date', today))
        
    feed_groups = [g for g in sorted(groups.values(), key=lambda x: x['order']) if g['cards']]
    
    return render_template('feed.html', feed_groups=feed_groups)

@bp.route('/api/feed/dismiss/<card_id>', methods=['POST'])
def dismiss_card(card_id):
    return ""

@bp.route('/api/feed/complete_task', methods=['POST'])
@login_required
def complete_task():
    task_id = request.form.get('task_id')
    fact_qty = int(request.form.get('fact_qty', 0))
    
    task = DiggingTask.query.get(task_id)
    if task and task.status == 'pending' and fact_qty > 0:
        log = DiggingLog(
            date=msk_today(), order_item_id=task.order_item_id, plant_id=task.item.plant_id,
            size_id=task.item.size_id, field_id=task.item.field_id, year=task.item.year,
            user_id=current_user.id, quantity=fact_qty, status='pending' 
        )
        db.session.add(log)
        task.status = 'done'
        task.item.order.refresh_status_by_dug()
        db.session.commit()
        
        return f"""
        <div class="card border-0 shadow-sm mb-3 bg-success bg-opacity-10 text-center py-3 fade-me-in">
            <h5 class="text-success m-0"><i class="fas fa-check-circle"></i> Принято {fact_qty} шт!</h5>
        </div>
        """
    return "Ошибка ввода", 400

@bp.route('/api/feed/complete_tg_task', methods=['POST'])
@login_required
def complete_tg_task():
    task_id = request.form.get('task_id')
    task = TgTask.query.get(task_id)
    if task and task.status == 'new':
        task.status = 'done'
        db.session.commit()
        return f"""
        <div class="card border-0 shadow-sm mb-3 bg-primary bg-opacity-10 text-center py-3 fade-me-in">
            <h6 class="text-primary m-0"><i class="fas fa-check-double"></i> Задача выполнена!</h6>
        </div>
        """
    return "Ошибка", 400

# ========================================================
# WEBHOOK ТЕЛЕГРАМ БОТА ДЛЯ ПОСТАНОВКИ ЗАДАЧ ОТ РУКОВОДИТЕЛЯ
# ========================================================
@bp.route('/api/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """
    Сюда Telegram присылает сообщения. 
    Мы ловим те, где есть упоминание бота (например @FloraFlow)
    и отправляем в УМНЫЙ AI АГЕНТ (с инструментами) для глубокого разбора.
    """
    data = request.json
    if not data or 'message' not in data:
        return jsonify({'status': 'ok'}) 
        
    msg = data['message']
    text = msg.get('text', '')
    
    is_private = msg.get('chat', {}).get('type') == 'private'
    
    if is_private or text.startswith('@FloraFlow') or 'заказ' in text.lower() or 'копать' in text.lower():
        
        sender = msg.get('from', {}).get('first_name', 'Руководитель')
        
        try:
            from app.ai_tools_agent import process_telegram_message_with_ai
            
            result_msg = process_telegram_message_with_ai(text, sender)
            print(f"AI Result: {result_msg}")
            
        except Exception as e:
            print(f"Ошибка AI Webhook (Tools): {e}")
            new_task = TgTask(raw_text=text, title="Сообщение из чата (Сбой)", details=text, assignee_role='user', sender_name=sender)
            db.session.add(new_task)
            db.session.commit()

    return jsonify({'status': 'ok'})

@bp.route('/uploads/<path:filename>')
def serve_uploaded_file(filename):
    try:
        response = send_from_directory(
            current_app.config['UPLOAD_FOLDER'],
            filename,
            max_age=60 * 60 * 24 * 30,
        )
        response.headers['Cache-Control'] = 'public, max-age=2592000, immutable'
        return response
    except (OSError, FileNotFoundError):
        return "", 404

@bp.route('/admin/backup_db')
@login_required
def backup_db():
    if current_user.role != 'admin': 
        return redirect(url_for('main.index'))
    
    db_path = current_app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
    return send_file(db_path, as_attachment=True, download_name="backup.db")

@bp.route('/sw.js')
def service_worker():
    response = send_file('static/sw.js', mimetype='application/javascript')
    response.headers['Cache-Control'] = 'no-cache'
    return response

@bp.route('/manifest.json')
def manifest():
    return send_file('static/manifest.json', mimetype='application/json')

@bp.route('/static/icon-192.png')
def app_icon():
    try:
        return send_file('static/icon-192.png')
    except (OSError, FileNotFoundError):
        return "", 404

@bp.route('/guide')
@login_required
def guide():
    return render_template('guide.html')

@bp.route('/offline')
def offline():
    return render_template('offline.html')

@bp.route('/api/cache-manifest')
@login_required
def cache_manifest():
    last_log = ActionLog.query.order_by(ActionLog.id.desc()).first()
    last_log_id = last_log.id if last_log else 0
    data_version = f"u{current_user.id}_v{last_log_id}"

    urls = [
        url_for('orders.orders_list'),
        url_for('orders.order_create'),
        url_for('stock.stock_report'),
        url_for('stock.documents'),
        url_for('directory.directory'),
        url_for('main.guide')
    ]

    last_orders = Order.query.filter(Order.is_deleted == False).order_by(Order.date.desc()).limit(50).all()
    for o in last_orders:
        urls.append(url_for('orders.order_detail', order_id=o.id))

    if current_user.role in ['admin', 'executive']:
        urls.extend([
            url_for('finance.reports_reconciliation'),
            url_for('finance.reports_turnover'),
            url_for('finance.expenses', tab='invoices'),
            url_for('finance.expenses'),
            url_for('finance.budget'),
            url_for('finance.cost_report'),
            url_for('finance.reports_financial'),
            url_for('finance.reports_margin'),
            url_for('finance.reports_investor'),
            url_for('finance.reports_projects'),
            url_for('finance.reports_calculator'),
            url_for('crm.crm_price_calculator'),
            url_for('crm.crm_client_analytics'),
            url_for('auth.users_manage'),
            url_for('stock.logs')
        ])

    if current_user.role in ['admin', 'user2']:
        urls.extend([
            url_for('hr.personnel')
        ])

    return jsonify({
        'version': data_version,
        'urls': urls
    })