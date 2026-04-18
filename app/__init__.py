import os
from datetime import datetime, timedelta
from flask import Flask
from werkzeug.exceptions import RequestEntityTooLarge
from flask_login import LoginManager
from app.models import db, User
from . import main, auth, directory, orders, stock, finance, hr, crm, chat # <--- Добавили chat
import sqlite3 # <--- Добавлено
from sqlalchemy import event # <--- Добавлено
from sqlalchemy.engine import Engine # <--- Добавлено

login_manager = LoginManager()
login_manager.login_view = 'auth.login'

# --- ИСПРАВЛЕНИЕ ДЛЯ РУССКОГО ПОИСКА В SQLITE ---
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
        # Регистрируем python-функцию lower для SQL, чтобы работало lower('Сосна') == 'сосна'
        dbapi_connection.create_function("lower", 1, lambda x: x.lower() if x else None)

def create_app():
    app = Flask(__name__)

    # --- КОНФИГУРАЦИЯ ---
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'my_secret_key_123')
    app.config['SESSION_COOKIE_HTTPONLY'] = True 
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax' 
    # Лимит загрузок (по умолчанию 64 MB, можно переопределить env MAX_UPLOAD_MB)
    max_upload_mb = int(os.environ.get('MAX_UPLOAD_MB', '64'))
    app.config['MAX_CONTENT_LENGTH'] = max_upload_mb * 1024 * 1024

    # Настройка путей (Amvera vs Локально)
    if os.path.exists('/data'):
        db_path = os.path.join('/data', 'nursery.db')
        app.config['UPLOAD_FOLDER'] = '/data'
    else:
        basedir = os.path.abspath(os.path.dirname(__file__))
        project_dir = os.path.dirname(basedir)
        db_path = os.path.join(project_dir, 'nursery.db')
        
        static_folder = os.path.join(basedir, 'static')
        if not os.path.exists(static_folder):
            os.makedirs(static_folder)
        app.config['UPLOAD_FOLDER'] = static_folder

    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)
    login_manager.init_app(app)

    # --- РЕГИСТРАЦИЯ БЛЮПРИНТОВ ---
    # Важно: все эти файлы (main.py, auth.py и т.д.) должны лежать в папке app/
    from . import main, auth, directory, orders, stock, finance, hr, crm, public_client_api
    
    app.register_blueprint(main.bp)
    app.register_blueprint(auth.bp)
    app.register_blueprint(directory.bp)
    app.register_blueprint(orders.bp)
    app.register_blueprint(stock.bp)
    app.register_blueprint(finance.bp)
    app.register_blueprint(hr.bp)
    app.register_blueprint(crm.bp)
    app.register_blueprint(chat.bp)
    app.register_blueprint(public_client_api.bp)
    
    from . import digging
    app.register_blueprint(digging.bp)

    # --- ФИЛЬТРЫ ---
    @app.template_filter('money')
    def format_money(value):
        if value is None: return ""
        try: return "{:,.2f} ₽".format(float(value)).replace(',', ' ')
        except (ValueError, TypeError): return str(value)

    @app.template_filter('money_int')
    def format_money_int(value):
        if value is None: return ""
        try: return "{:,.0f} ₽".format(float(value)).replace(',', ' ')
        except (ValueError, TypeError): return str(value)

    @app.template_filter('house_number')
    def house_number_filter(address):
        if not address: return ""
        import re
        m = re.search(r'\bд(?:ом)?\.?\s*(\d[\dа-яА-Яa-zA-Z/]*)', address)
        if m: return m.group(1)
        parts = [p.strip() for p in address.split(',')]
        for part in reversed(parts):
            if re.match(r'^\d{5,6}$', part.strip()):
                continue
            nums = re.findall(r'\d[\dа-яА-Яa-zA-Z/]*', part)
            if nums: return nums[-1]
        return parts[-1].strip() if parts else address

    @app.template_filter('dateru')
    def dateru(value):
        if value is None: return ""
        if isinstance(value, str): return value 
        return value.strftime('%d.%m.%Y')

    @app.template_filter('year_class')
    def year_class_filter(year):
        if not year: return "secondary"
        try:
            current = (datetime.utcnow() + timedelta(hours=3)).year
            diff = current - int(year)
            if diff <= 1: return "success"
            if diff <= 3: return "warning"
            return "danger"
        except (ValueError, TypeError): return "secondary"

    @app.template_filter('highlight_menu')
    def highlight_menu_filter(text):
        if not text: return ""
        # Список слов из меню для подсветки
        keywords = {
            'Заказы': 'text-primary fw-bold',
            'CRM': 'text-info fw-bold',
            'Склад': 'text-success fw-bold',
            'Производство': 'text-warning fw-bold',
            'Финансы': 'text-danger fw-bold',
            'Кадры': 'text-secondary fw-bold',
            'Настройки': 'text-dark fw-bold'
        }
        import re
        # Проходимся по тексту и оборачиваем ключевые слова в span с классом
        for word, classes in keywords.items():
            # Замена с учетом регистра, ищем слово целиком или как часть фразы
            pattern = re.compile(re.escape(word), re.IGNORECASE)
            # При замене используем оригинальное слово из словаря, чтобы регистр был красивым
            text = pattern.sub(f'<span class="{classes}">{word}</span>', text)
        return text

    @app.errorhandler(RequestEntityTooLarge)
    def handle_large_upload(_e):
        from flask import request, flash, redirect, url_for
        flash(f'Файл слишком большой. Лимит: {max_upload_mb} MB.')
        # Возвращаем на страницу, с которой пришли, если это возможно.
        return redirect(request.referrer or url_for('directory.directory'))

    # --- СОЗДАНИЕ БД ---
    with app.app_context():
        db.create_all()

        # Safely create missing indexes on existing tables (SQLite)
        import sqlite3
        db_path = app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            idx_stmts =[
                "CREATE INDEX IF NOT EXISTS idx_docrow_document ON document_row (document_id)",
                "CREATE INDEX IF NOT EXISTS idx_expense_date_budget ON expense (date, budget_item_id)",
                "CREATE INDEX IF NOT EXISTS idx_expense_employee ON expense (employee_id)",
                "CREATE INDEX IF NOT EXISTS idx_expense_invoice ON expense (invoice_id)",
                "CREATE INDEX IF NOT EXISTS idx_timelog_date ON time_log (date)",
                "ALTER TABLE time_log ADD COLUMN is_day_off BOOLEAN DEFAULT 0",
                "ALTER TABLE expense ADD COLUMN target_month INTEGER",
                "ALTER TABLE expense ADD COLUMN target_year INTEGER",
                "ALTER TABLE \"order\" ADD COLUMN is_barter BOOLEAN DEFAULT 0",
                "ALTER TABLE payment ADD COLUMN payment_type VARCHAR(20) DEFAULT 'cashless'",
                "ALTER TABLE expense ADD COLUMN barter_order_id INTEGER",
                "ALTER TABLE client ADD COLUMN fixed_balance NUMERIC(15, 2)",
                "ALTER TABLE client ADD COLUMN fixed_balance_date DATE",
            ]
            for stmt in idx_stmts:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass
            conn.commit()
            conn.close()
        except Exception:
            pass

        # Run ad-hoc schema migrations for backward compatibility
        from app.digging import ensure_digging_table_exists, ensure_payment_file_column
        try:
            ensure_digging_table_exists()
            ensure_payment_file_column()
        except Exception:
            pass

        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin')
            db.session.add(u)
            if not User.query.filter_by(username='user1').first(): 
                u1 = User(username='user1', role='user'); u1.set_password('user1'); db.session.add(u1)
            if not User.query.filter_by(username='user2').first(): 
                u2 = User(username='user2', role='user2'); u2.set_password('user2'); db.session.add(u2)
            db.session.commit()

    return app

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))