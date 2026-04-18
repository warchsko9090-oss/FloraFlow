import os
from flask import Blueprint, redirect, url_for, send_file, send_from_directory, current_app, render_template, jsonify
from flask_login import current_user, login_required
from app.models import Order, ActionLog

bp = Blueprint('main', __name__)

@bp.route('/')
def index():
    # Если пользователь вошел
    if current_user.is_authenticated: 
        # Руководителя отправляем сразу в Счета (новая объединенная вкладка)
        if current_user.role == 'executive':
            return redirect(url_for('finance.expenses', tab='invoices'))
        
        # Бригадиру/Кадровику сразу на Кадры (часы - вкладка Табель)
        if current_user.role in ['brigadier', 'user2']:
            return redirect(url_for('hr.personnel', tab='timesheet'))

        # Всех остальных - в Заказы
        return redirect(url_for('orders.orders_list'))
        
    # Если нет -> на страницу входа
    return redirect(url_for('auth.login'))

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