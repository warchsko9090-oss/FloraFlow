import os
from flask import Blueprint, redirect, url_for, send_file, send_from_directory, current_app, render_template, jsonify
from flask_login import current_user, login_required
from app.models import Order, ActionLog

bp = Blueprint('main', __name__)

from datetime import timedelta
from sqlalchemy import func
from app.models import db, Order, PaymentInvoice, Document, PatentPeriod, OrderItem, DiggingTask, DiggingLog
from app.utils import msk_today

@bp.route('/')
def index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.login'))
        
    today = msk_today()
    feed_cards = []

    # ==========================================
    # 1. КАРТОЧКИ ДЛЯ РУКОВОДИТЕЛЯ И АДМИНА
    # ==========================================
    if current_user.role in ['admin', 'executive']:
        invoices = PaymentInvoice.query.filter(PaymentInvoice.status != 'paid').all()
        for inv in invoices:
            remaining = inv.amount - sum(e.amount for e in inv.expenses)
            if remaining > 0:
                is_urgent = inv.priority == 'high' or (inv.due_date and (inv.due_date - today).days <= 3)
                feed_cards.append({
                    'id': f'inv_{inv.id}',
                    'type': 'invoice',
                    'title': 'Счет на оплату',
                    'text': f'{inv.item.name if inv.item else "Без статьи"}: {inv.original_name}',
                    'amount': remaining,
                    'is_urgent': is_urgent,
                    'url': url_for('finance.expenses', invoice_id=inv.id)
                })

    # ==========================================
    # 2. КАРТОЧКИ ДЛЯ МЕНЕДЖЕРА И АДМИНА
    # ==========================================
    if current_user.role in ['admin', 'user']:
        drafts = Document.query.filter_by(doc_type='client_draft').all()
        for d in drafts:
            feed_cards.append({
                'id': f'draft_{d.id}',
                'type': 'draft',
                'title': 'Новая заявка с сайта',
                'text': f'Требует подтверждения менеджером.',
                'date': d.date.strftime('%d.%m.%Y'),
                'is_urgent': True,
                'url': url_for('orders.client_draft_detail', doc_id=d.id)
            })

    # ==========================================
    # 3. КАРТОЧКИ ДЛЯ БРИГАДИРА И АДМИНА (НОВЫЕ ЗАДАНИЯ)
    # ==========================================
    if current_user.role in ['admin', 'brigadier']:
        # Ищем задания на сегодня и просроченные
        tasks = DiggingTask.query.filter(DiggingTask.status == 'pending', DiggingTask.planned_date <= today).all()
        for t in tasks:
            is_urgent = t.planned_date < today
            feed_cards.append({
                'id': f'task_{t.id}',
                'type': 'task',
                'title': f'Выкопка: {t.item.order.client.name}',
                'text': f'{t.item.plant.name} ({t.item.size.name} | Поле {t.item.field.name}). Заказ #{t.item.order_id}',
                'qty': t.planned_qty,
                'comment': t.comment,
                'is_urgent': is_urgent,
                'date_str': 'ПРОСРОЧЕНО' if is_urgent else 'СЕГОДНЯ',
                'task_id': t.id
            })

    # ==========================================
    # 4. КАРТОЧКИ ДЛЯ КАДРОВ (HR)
    # ==========================================
    if current_user.role in ['admin', 'user2', 'executive']:
        patents = PatentPeriod.query.filter_by(is_current=True, status='active').all()
        for p in patents:
            if p.end_date:
                days_left = (p.end_date - today).days
                if days_left <= 14:
                    feed_cards.append({
                        'id': f'pat_{p.id}',
                        'type': 'patent',
                        'title': 'Истекает патент',
                        'text': f'{p.employee.name} (осталось {days_left} дн.)',
                        'is_urgent': days_left <= 7,
                        'url': url_for('hr.foreign_employee_card', employee_id=p.employee.id)
                    })

    feed_cards.sort(key=lambda x: (not x.get('is_urgent', False), x['title']))
    return render_template('feed.html', feed_cards=feed_cards)

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
        # 1. Записываем факт выкопки в журнал
        log = DiggingLog(
            date=msk_today(),
            order_item_id=task.order_item_id,
            plant_id=task.item.plant_id,
            size_id=task.item.size_id,
            field_id=task.item.field_id,
            year=task.item.year,
            user_id=current_user.id,
            quantity=fact_qty,
            status='pending' 
        )
        db.session.add(log)
        
        # 2. Закрываем задание
        task.status = 'done'
        
        # 3. Пересчитываем статус заказа
        task.item.order.refresh_status_by_dug()
        
        db.session.commit()
        
        return f"""
        <div class="card border-0 shadow-sm mb-3 bg-success bg-opacity-10 text-center py-3 fade-me-in">
            <h5 class="text-success m-0"><i class="fas fa-check-circle"></i> Принято {fact_qty} шт!</h5>
        </div>
        """
    return "Ошибка ввода", 400

# ВАЖНО: <path:filename> позволяет читать файлы из папок (например photo/plant_1/1.jpg)
@bp.route('/uploads/<path:filename>')
def serve_uploaded_file(filename):
    try:
        response = send_from_directory(
            current_app.config['UPLOAD_FOLDER'],
            filename,
            max_age=60 * 60 * 24 * 30,  # 30 days browser cache for media
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
    
    # Путь к БД берем из конфига
    db_path = current_app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
    return send_file(db_path, as_attachment=True, download_name="backup.db")

@bp.route('/sw.js')
def service_worker():
    # Отдаем файл sw.js из папки static, но с правильным типом данных
    response = send_file('static/sw.js', mimetype='application/javascript')
    # Запрещаем кэшировать сам файл скрипта, чтобы обновления логики применялись сразу
    response.headers['Cache-Control'] = 'no-cache'
    return response

@bp.route('/manifest.json')
def manifest():
    # PWA манифест
    return send_file('static/manifest.json', mimetype='application/json')

@bp.route('/static/icon-192.png')
def app_icon():
    # Иконка PWA
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

# --- API ДЛЯ ПРОГРЕВА КЭША (Новое) ---
@bp.route('/api/cache-manifest')
@login_required
def cache_manifest():
    """Возвращает версию БД и список URL для кэширования"""
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