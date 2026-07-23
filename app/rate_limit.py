"""Простой лимит запросов по IP (на процесс gunicorn). Защита от ботов и флуда форм."""
from __future__ import annotations

import threading
import time
from functools import wraps

from flask import abort, has_app_context, request


_lock = threading.Lock()
_hits: dict[str, list[float]] = {}


def client_ip() -> str:
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    return request.remote_addr or 'unknown'


def _prune(now: float, window: float) -> None:
    if len(_hits) < 5000:
        return
    stale = [k for k, ts in _hits.items() if not ts or now - ts[-1] > window]
    for k in stale:
        _hits.pop(k, None)


def check_rate_limit(scope: str, max_requests: int, window_seconds: int) -> bool:
    """True если лимит ещё не исчерпан и запрос учтён."""
    if not has_app_context():
        return True
    now = time.time()
    key = f'{scope}:{client_ip()}'
    with _lock:
        _prune(now, window_seconds)
        bucket = [t for t in _hits.get(key, []) if now - t < window_seconds]
        if len(bucket) >= max_requests:
            return False
        bucket.append(now)
        _hits[key] = bucket
    return True


def rate_limit(scope: str, max_requests: int, window_seconds: int):
    """Декоратор: не более max_requests за window_seconds с одного IP."""

    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not check_rate_limit(scope, max_requests, window_seconds):
                abort(429)
            return view(*args, **kwargs)

        return wrapped

    return decorator
