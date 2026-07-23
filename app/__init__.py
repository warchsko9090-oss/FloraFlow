import os
import logging
from datetime import datetime, timedelta
from flask import Flask
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.middleware.proxy_fix import ProxyFix
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
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            # WAL — конкурентные чтения и устойчивость к падениям воркеров gunicorn.
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=5000")
        except Exception:
            pass
        cursor.close()
        # Регистрируем python-функцию lower для SQL, чтобы работало lower('Сосна') == 'сосна'
        dbapi_connection.create_function("lower", 1, lambda x: x.lower() if x else None)


def _is_behind_proxy():
    """Определяем, работает ли приложение за reverse-proxy (Amvera, Nginx и т.п.)."""
    if os.environ.get('BEHIND_PROXY', '').lower() in ('1', 'true', 'yes'):
        return True
    # Признаки Amvera / облачного окружения
    if os.environ.get('AMVERA') or os.path.exists('/data'):
        return True
    return False


def create_app():
    app = Flask(__name__)

    # --- ЛОГИ В STDOUT (Amvera/gunicorn подхватят) ---
    logging.basicConfig(
        level=os.environ.get('LOG_LEVEL', 'INFO').upper(),
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s'
    )

    # --- КОНФИГУРАЦИЯ ---
    secret = os.environ.get('SECRET_KEY', 'my_secret_key_123')
    if secret == 'my_secret_key_123' and os.environ.get('FLASK_ENV') != 'development':
        app.logger.warning('SECRET_KEY не задан через окружение — используется дефолт. Установите переменную SECRET_KEY в Amvera.')
    app.config['SECRET_KEY'] = secret
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    # За HTTPS-прокси (Amvera) — безопасная кука и https-ссылки в url_for(_external=True)
    if _is_behind_proxy():
        app.config['SESSION_COOKIE_SECURE'] = True
        app.config['PREFERRED_URL_SCHEME'] = 'https'
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # Лимит загрузок (по умолчанию 64 MB, можно переопределить env MAX_UPLOAD_MB)
    max_upload_mb = int(os.environ.get('MAX_UPLOAD_MB', '64'))
    app.config['MAX_CONTENT_LENGTH'] = max_upload_mb * 1024 * 1024

    # --- WEB PUSH (VAPID) ---
    app.config['VAPID_PUBLIC_KEY'] = os.environ.get('VAPID_PUBLIC_KEY', '').strip()
    app.config['VAPID_PRIVATE_KEY'] = os.environ.get('VAPID_PRIVATE_KEY', '').strip()
    app.config['VAPID_CLAIM_EMAIL'] = (
        os.environ.get('VAPID_CLAIM_EMAIL', '').strip()
        or 'mailto:admin@floraflowerp.local'
    )

    # --- БАЗА ДАННЫХ ---
    db_url = os.environ.get('DATABASE_URL', '').strip()
    if db_url:
        if db_url.startswith('postgres://'):
            db_url = 'postgresql://' + db_url[len('postgres://'):]
        app.config['SQLALCHEMY_DATABASE_URI'] = db_url
        if os.path.isdir('/data'):
            app.config['UPLOAD_FOLDER'] = '/data'
        else:
            basedir = os.path.abspath(os.path.dirname(__file__))
            static_folder = os.path.join(basedir, 'static')
            os.makedirs(static_folder, exist_ok=True)
            app.config['UPLOAD_FOLDER'] = static_folder
    else:
        if os.path.isdir('/data'):
            try:
                os.makedirs('/data', exist_ok=True)
            except Exception:
                pass
            db_path = os.path.join('/data', 'nursery.db')
            app.config['UPLOAD_FOLDER'] = '/data'
        else:
            basedir = os.path.abspath(os.path.dirname(__file__))
            project_dir = os.path.dirname(basedir)
            db_path = os.path.join(project_dir, 'nursery.db')
            static_folder = os.path.join(basedir, 'static')
            os.makedirs(static_folder, exist_ok=True)
            app.config['UPLOAD_FOLDER'] = static_folder
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,
        'pool_recycle': 300,
    }

    db.init_app(app)
    login_manager.init_app(app)

    # --- РЕГИСТРАЦИЯ БЛЮПРИНТОВ ---
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

    from . import production
    app.register_blueprint(production.bp)

    from . import vium as vium_module
    app.register_blueprint(vium_module.bp)

    from . import shop_admin
    app.register_blueprint(shop_admin.bp)

    from . import push as push_module
    app.register_blueprint(push_module.bp)
    push_module.register_push_hooks(app)

    SHOP_MANAGER_ALLOWED_PREFIXES = (
        'auth.', 'orders.', 'shop_admin.', 'public_client_api.', 'push.',
    )
    SHOP_MANAGER_ALLOWED_EXACT = {
        'static',
        'stock.stock_report', 'stock.stock_report_export', 'stock.changelog',
        'directory.directory',
        'main.serve_uploaded_file', 'main.manifest', 'main.app_icon',
        'main.service_worker', 'main.offline',
    }

    @app.before_request
    def _restrict_shop_manager():
        from flask import request, redirect, url_for
        from flask_login import current_user
        if not getattr(current_user, 'is_authenticated', False):
            return None
        if (getattr(current_user, 'role', None) or '') != 'shop_manager':
            return None
        ep = request.endpoint or ''
        if ep in SHOP_MANAGER_ALLOWED_EXACT:
            return None
        if any(ep.startswith(p) for p in SHOP_MANAGER_ALLOWED_PREFIXES):
            return None
        return redirect(url_for('orders.orders_list'))

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
        for word, classes in keywords.items():
            pattern = re.compile(re.escape(word), re.IGNORECASE)
            text = pattern.sub(f'<span class="{classes}">{word}</span>', text)
        return text

    @app.errorhandler(RequestEntityTooLarge)
    def handle_large_upload(_e):
        from flask import request, flash, redirect, url_for
        flash(f'Файл слишком большой. Лимит: {max_upload_mb} MB.')
        return redirect(request.referrer or url_for('directory.directory'))

    # --- СОЗДАНИЕ БД ---
    with app.app_context():
        import os as _os
        import tempfile as _tempfile
        _lock_path = _os.path.join(_tempfile.gettempdir(), 'ff_db_create_all.lock')
        _lock_fh = None
        try:
            _lock_fh = open(_lock_path, 'w')
            try:
                import fcntl
                fcntl.flock(_lock_fh, fcntl.LOCK_EX)
            except Exception:
                pass
            try:
                db.create_all()
            except Exception as e:
                app.logger.error(f'db.create_all() упал: {e}')
        finally:
            try:
                if _lock_fh is not None:
                    try:
                        import fcntl
                        fcntl.flock(_lock_fh, fcntl.LOCK_UN)
                    except Exception:
                        pass
                    _lock_fh.close()
            except Exception:
                pass

        try:
            from app.db_migrations import ensure_legacy_schema
            ensure_legacy_schema(app.logger)
        except Exception as e:
            app.logger.warning(f'legacy schema migration skipped: {e}')

        try:
            from app.digging import ensure_digging_table_exists, ensure_payment_file_column
            ensure_digging_table_exists()
            ensure_payment_file_column()
            from app.shop_on_request import ensure_shop_on_request_nullable
            ensure_shop_on_request_nullable()
            from app.services import ensure_numbered_container_yards
            ensure_numbered_container_yards()
        except Exception as e:
            app.logger.warning(f'ensure_* migrations skipped: {e}')

        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin')
            db.session.add(u)
            if not User.query.filter_by(username='user1').first():
                u1 = User(username='user1', role='user'); u1.set_password('user1'); db.session.add(u1)
            if not User.query.filter_by(username='user2').first():
                u2 = User(username='user2', role='user2'); u2.set_password('user2'); db.session.add(u2)
            db.session.commit()

        try:
            from app.shop_contacts import seed_default_shop_contacts
            seed_default_shop_contacts(db.session)
        except Exception as e:
            app.logger.warning(f'shop contacts seed skipped: {e}')

        try:
            from app.shop_contacts_page import seed_contacts_page_defaults, ensure_contacts_page_columns
            ensure_contacts_page_columns()
            seed_contacts_page_defaults(db.session)
            from app.shop_landing_page import seed_landing_page_defaults, ensure_landing_page_columns
            ensure_landing_page_columns()
            seed_landing_page_defaults(db.session)
        except Exception as e:
            app.logger.warning(f'shop contacts page seed skipped: {e}')

    try:
        from app.scheduler import init_scheduler
        init_scheduler(app)
    except Exception:
        app.logger.exception('init_scheduler failed')

    return app

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))
