"""Long-polling Telegram updates.

Amvera's inbound webhook from Telegram times out (Connection timed out),
while outbound api.telegram.org works. So on Amvera we deleteWebhook and
pull getUpdates from one worker.
"""
from __future__ import annotations

import os
import tempfile
import threading
import time


def _should_poll() -> bool:
    flag = (os.environ.get('TG_USE_POLLING') or '').strip().lower()
    if flag in ('0', 'false', 'no'):
        return False
    if flag in ('1', 'true', 'yes'):
        return True
    return bool(os.environ.get('AMVERA') or os.path.isdir('/data'))


def _paths():
    base = '/data' if os.path.isdir('/data') else tempfile.gettempdir()
    return (
        os.path.join(base, 'tg_poll.lock'),
        os.path.join(base, 'tg_updates_offset.txt'),
    )


def _read_offset(path):
    try:
        with open(path, encoding='utf-8') as fh:
            return int((fh.read() or '0').strip() or '0')
    except Exception:
        return 0


def _write_offset(path, offset):
    try:
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(str(int(offset)))
    except Exception:
        pass


def start_telegram_poller(app):
    if not _should_poll():
        return

    lock_path, offset_path = _paths()

    def run():
        lock_fh = None
        try:
            lock_fh = open(lock_path, 'w')
            import fcntl
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as exc:
            app.logger.info('Telegram poller not started on this worker: %s', exc)
            if lock_fh:
                try:
                    lock_fh.close()
                except Exception:
                    pass
            return

        from app.telegram import delete_webhook, get_updates

        with app.app_context():
            ok, msg = delete_webhook(drop_pending=False)
            app.logger.info('Telegram polling: deleteWebhook %s %s', ok, msg)

        app.logger.info('Telegram polling started (getUpdates)')
        offset = _read_offset(offset_path)
        while True:
            try:
                updates = get_updates(offset or None, timeout=25)
                if not updates:
                    continue
                from app.main import process_telegram_update
                with app.app_context():
                    for upd in updates:
                        offset = int(upd.get('update_id') or 0) + 1
                        _write_offset(offset_path, offset)
                        process_telegram_update(upd)
            except Exception as exc:
                app.logger.warning('Telegram poller: %s', exc)
                time.sleep(3)

    threading.Thread(target=run, daemon=True, name='tg-poller').start()
