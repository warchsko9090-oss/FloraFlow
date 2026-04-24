"""Фоновый планировщик задач для Flask+gunicorn.

Запускает ежедневный скан аномалий в 9:00 по МСК. Реализация:
- APScheduler BackgroundScheduler стартует в каждом воркере gunicorn
  (мы не можем рассчитывать на `preload_app=True`, т.к. с ним SQLite
  соединения плохо наследуются от мастера).
- Чтобы разные воркеры не гонялись одним заданием одновременно, перед
  выполнением берётся файловый лок на persistent-томе (`/data/...`
  или, локально, рядом с БД). Сам `ensure_daily_scan()` идемпотентен,
  поэтому двойное срабатывание не приведёт к дубликатам — это просто
  дополнительная защита от лишней работы и потенциальных SQLite-гонок.
- Управление: переменная окружения `DISABLE_SCHEDULER=1` отключает
  планировщик (полезно на локалке, когда два разработчика запускают
  отдельные инстансы).
"""
from __future__ import annotations

import os
import time
import logging
from datetime import datetime

_logger = logging.getLogger(__name__)

# Единственный экземпляр на процесс — чтобы `init_scheduler` был идемпотентным.
_scheduler = None


def _lock_dir(app):
    """Где хранить lock-файлы. Отдаём предпочтение persistent-тому Amvera."""
    for candidate in ('/data', app.config.get('UPLOAD_FOLDER'), os.getcwd()):
        if candidate and os.path.isdir(candidate):
            return candidate
    return os.getcwd()


def _acquire_file_lock(path, ttl_seconds=3600):
    """Атомарно создаёт lock-файл. Возвращает True если лок захвачен.
    Если файл старее ttl_seconds — считает его протухшим и перехватывает.
    """
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f'{os.getpid()}@{datetime.utcnow().isoformat()}'.encode())
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        try:
            mtime = os.path.getmtime(path)
            if time.time() - mtime > ttl_seconds:
                os.remove(path)
                return _acquire_file_lock(path, ttl_seconds)
        except FileNotFoundError:
            return _acquire_file_lock(path, ttl_seconds)
        except Exception:
            pass
        return False


def _release_file_lock(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception:
        _logger.warning('failed to remove lock %s', path, exc_info=True)


def _run_daily_anomaly_scan(app):
    """Обёртка с app-контекстом и файловым локом — дёргается cron-триггером."""
    lock_path = os.path.join(_lock_dir(app), '.anomaly_scan.lock')
    if not _acquire_file_lock(lock_path):
        _logger.info('daily anomaly scan: lock held by another worker, skip')
        return
    try:
        with app.app_context():
            try:
                from app.anomaly_engine import ensure_daily_scan
                result = ensure_daily_scan()
                _logger.info('daily anomaly scan (scheduled): %s', result)
            except Exception:
                _logger.exception('daily anomaly scan (scheduled) failed')
    finally:
        _release_file_lock(lock_path)


def init_scheduler(app):
    """Инициализирует глобальный планировщик. Вызывается из create_app()."""
    global _scheduler
    if _scheduler is not None:
        return _scheduler  # уже поднят в этом процессе
    if os.environ.get('DISABLE_SCHEDULER', '').lower() in ('1', 'true', 'yes'):
        app.logger.info('scheduler disabled via DISABLE_SCHEDULER env var')
        return None
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except Exception as e:  # noqa: BLE001
        app.logger.warning('APScheduler не установлен (%s) — автоскан отключён', e)
        return None

    try:
        # Europe/Moscow = UTC+3, без летнего времени
        trigger = CronTrigger(hour=9, minute=0, timezone='Europe/Moscow')
    except Exception:
        # Фоллбэк — если tzdata/pytz отсутствует, используем UTC 06:00 (= 9:00 МСК)
        trigger = CronTrigger(hour=6, minute=0)

    sched = BackgroundScheduler(
        daemon=True,
        job_defaults={
            'coalesce': True,          # если процессор был занят — запускаем один раз, не очередью
            'max_instances': 1,        # не запускаем параллельно с собой
            'misfire_grace_time': 3600,  # если воркер проспал на час — всё ещё запустить
        },
    )
    sched.add_job(
        func=_run_daily_anomaly_scan,
        args=[app],
        trigger=trigger,
        id='daily_anomaly_scan',
        replace_existing=True,
    )
    try:
        sched.start()
        _scheduler = sched
        # Покажем в логах, когда ближайший запуск — удобно проверить сразу
        # после деплоя.
        try:
            job = sched.get_job('daily_anomaly_scan')
            app.logger.info(
                'scheduler started (pid=%s), next daily_anomaly_scan at %s',
                os.getpid(), getattr(job, 'next_run_time', None),
            )
        except Exception:
            app.logger.info('scheduler started (pid=%s)', os.getpid())
    except Exception:
        app.logger.exception('scheduler start failed')
        return None
    return _scheduler
