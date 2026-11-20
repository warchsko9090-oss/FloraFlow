import os
import tempfile


def _bootstrap_floraflow1_demo_env():
    """FloraFlow1 (Amvera, SQLite /data, без DATABASE_URL) — демо рядом с продом ff-m1.

    Прод всегда с DATABASE_URL/Postgres и TG_BOT_TOKEN — сюда не попадает.
    """
    if not (os.environ.get('AMVERA') or os.path.isdir('/data')):
        return
    if os.environ.get('DATABASE_URL') or os.environ.get('TG_BOT_TOKEN'):
        return
    defaults = {
        'DEMO_MODE': '1',
        'DISABLE_SCHEDULER': '1',
        'DISABLE_AI_SEED': '1',
        'TG_USE_POLLING': '0',
        'SECRET_KEY': 'demo-floraflow-client-preview',
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


_bootstrap_floraflow1_demo_env()

from app import create_app

app = create_app()


def _seed_lock_path():
    """Persistent lock на Amvera (/data), иначе temp — как для db.create_all."""
    for candidate in ('/data', app.config.get('UPLOAD_FOLDER'), tempfile.gettempdir()):
        if candidate and os.path.isdir(candidate):
            return os.path.join(candidate, '.ff_ai_seed.lock')
    return os.path.join(tempfile.gettempdir(), '.ff_ai_seed.lock')


def _run_ai_seed_once():
    """Сидинг при старте воркера: один процесс на деплой, остальные пропускают."""
    if os.environ.get('DISABLE_AI_SEED', '').lower() in ('1', 'true', 'yes'):
        app.logger.info('AI seed отключён (DISABLE_AI_SEED)')
        return

    try:
        from train_ai import seed_knowledge
    except Exception as e:
        app.logger.info('train_ai.py не подключен (%s) — авто-обучение пропущено', e)
        return

    lock_path = _seed_lock_path()
    lock_fh = None
    acquired = False
    try:
        lock_fh = open(lock_path, 'a+')
        try:
            import fcntl
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except (ImportError, BlockingIOError, OSError):
            app.logger.info('AI seed: другой воркер уже выполняет или выполнил сидинг — пропуск')
            return

        with app.app_context():
            added = seed_knowledge()
            if added:
                app.logger.info('AI seed: добавлено %s новых примеров', added)
            else:
                app.logger.info('AI seed: новых примеров нет, пропуск записи')
    except Exception as e:
        app.logger.warning('Ошибка при запуске обучения: %s', e)
    finally:
        if lock_fh is not None:
            try:
                if acquired:
                    import fcntl
                    fcntl.flock(lock_fh, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                lock_fh.close()
            except Exception:
                pass


_run_ai_seed_once()


def _run_demo_seed_once():
    """Amvera/локальный DEMO_MODE: синтетика в пустую БД + дедлайны на сегодня."""
    if (os.environ.get('DEMO_MODE') or '').strip().lower() not in ('1', 'true', 'yes'):
        return
    lock_path = _seed_lock_path().replace('.ff_ai_seed.lock', '.ff_demo_seed.lock')
    lock_fh = None
    acquired = False
    try:
        lock_fh = open(lock_path, 'a+')
        try:
            import fcntl
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except (ImportError, BlockingIOError, OSError):
            # Windows / другой воркер — пробуем без блокировки один раз
            acquired = True
        if not acquired:
            return
        with app.app_context():
            from tools.demo_seed import ensure_demo_seeded
            stats = ensure_demo_seeded()
            if stats:
                app.logger.info('DEMO_MODE seed: %s', stats)
            else:
                app.logger.info('DEMO_MODE: база уже заполнена / дедлайны обновлены')
    except Exception as e:
        app.logger.warning('DEMO_MODE seed failed: %s', e)
    finally:
        if lock_fh is not None:
            try:
                if acquired:
                    import fcntl
                    fcntl.flock(lock_fh, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                lock_fh.close()
            except Exception:
                pass


_run_demo_seed_once()


if __name__ == '__main__':
    # Локальный запуск (python run.py). В проде работает gunicorn -c gunicorn.conf.py run:app.
    app.run(host='0.0.0.0', port=5000, debug=True)
