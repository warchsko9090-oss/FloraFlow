"""Файловый + in-memory кэш тяжёлых отчётов (себестоимость, остатки).

Ночной прогрев и кнопка «Обновить» живут рядом с APScheduler.
Ключи — sha1 от имени + аргументов; значения — pickle.
"""
from __future__ import annotations

import hashlib
import logging
import os
import pickle
import threading
import time
from typing import Any, Callable, Optional

_logger = logging.getLogger(__name__)

_MEM: dict[str, tuple[float, Any]] = {}
_MEM_LOCK = threading.Lock()
_DISK_LOCK = threading.Lock()

# 12 часов — хватает до ночного прогрева; принудительный refresh сбрасывает.
DEFAULT_TTL_SEC = 12 * 3600


def _cache_root() -> str:
    for candidate in ('/data/report_cache',):
        try:
            os.makedirs(candidate, exist_ok=True)
            if os.path.isdir(candidate) and os.access(candidate, os.W_OK):
                return candidate
        except OSError:
            continue
    try:
        from flask import current_app
        base = current_app.config.get('UPLOAD_FOLDER') or os.getcwd()
    except Exception:
        base = os.getcwd()
    path = os.path.join(base, 'report_cache')
    os.makedirs(path, exist_ok=True)
    return path


def make_cache_key(name: str, *args, **kwargs) -> str:
    raw = repr((name, args, tuple(sorted(kwargs.items()))))
    digest = hashlib.sha1(raw.encode('utf-8', errors='replace')).hexdigest()[:28]
    safe_name = ''.join(c if c.isalnum() or c in '-_' else '_' for c in (name or 'x'))[:40]
    return f'{safe_name}__{digest}'


def _path_for(key: str) -> str:
    return os.path.join(_cache_root(), f'{key}.pkl')


def cache_get(key: str, ttl: int = DEFAULT_TTL_SEC) -> Optional[Any]:
    now = time.time()
    with _MEM_LOCK:
        hit = _MEM.get(key)
        if hit is not None:
            ts, val = hit
            if now - ts <= ttl:
                return val
            _MEM.pop(key, None)
    path = _path_for(key)
    try:
        st = os.stat(path)
        if now - st.st_mtime > ttl:
            return None
        with _DISK_LOCK:
            with open(path, 'rb') as f:
                val = pickle.load(f)
        with _MEM_LOCK:
            _MEM[key] = (st.st_mtime, val)
        return val
    except FileNotFoundError:
        return None
    except Exception:
        _logger.exception('report_cache read failed key=%s', key)
        return None


def cache_set(key: str, value: Any) -> None:
    now = time.time()
    with _MEM_LOCK:
        _MEM[key] = (now, value)
    path = _path_for(key)
    tmp = path + '.tmp'
    try:
        with _DISK_LOCK:
            with open(tmp, 'wb') as f:
                pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, path)
    except Exception:
        _logger.exception('report_cache write failed key=%s', key)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


def clear_report_cache(prefix: Optional[str] = None) -> int:
    """Сбрасывает memory + disk. prefix — начало имени ключа (до __)."""
    removed = 0
    with _MEM_LOCK:
        if prefix:
            for k in [k for k in _MEM if k.startswith(prefix)]:
                _MEM.pop(k, None)
                removed += 1
        else:
            removed += len(_MEM)
            _MEM.clear()
    root = _cache_root()
    try:
        for name in os.listdir(root):
            if not name.endswith('.pkl'):
                continue
            if prefix and not name.startswith(prefix):
                continue
            try:
                os.remove(os.path.join(root, name))
                removed += 1
            except OSError:
                pass
    except OSError:
        pass
    _logger.info('report_cache cleared prefix=%s removed≈%s', prefix, removed)
    return removed


def cached_report(
    name: str,
    fn: Callable[..., Any],
    *args,
    force: bool = False,
    ttl: int = DEFAULT_TTL_SEC,
    **kwargs,
) -> Any:
    key = make_cache_key(name, *args, **kwargs)
    if not force:
        hit = cache_get(key, ttl=ttl)
        if hit is not None:
            return hit
    t0 = time.perf_counter()
    value = fn(*args, **kwargs)
    cache_set(key, value)
    _logger.info(
        'report_cache MISS %s key=%s took=%.2fs',
        name, key, time.perf_counter() - t0,
    )
    return value


def warm_heavy_reports() -> dict:
    """Прогрев ключевых срезов для Партнёра / маржи / себестоимости / остатков."""
    from app.utils import msk_now, msk_today
    from app.services import calculate_cost_data, get_detailed_stock_at_year_end

    year = msk_now().year
    stats = {'cost': [], 'stock_ye': [], 'stock_report': None}
    for y in (year, year - 1, year - 2):
        if y < 2017:
            continue
        try:
            calculate_cost_data(y, force_refresh=True)
            stats['cost'].append(y)
        except Exception:
            _logger.exception('warm cost_data year=%s', y)
        try:
            get_detailed_stock_at_year_end(y, force_refresh=True)
            stats['stock_ye'].append(y)
        except Exception:
            _logger.exception('warm stock_ye year=%s', y)

    try:
        from app.models import Plant, Size, Field
        from app.stock import build_stock_report_data
        from app.utils import natural_key

        end_date = msk_today()
        all_plants = sorted(Plant.query.all(), key=lambda x: x.name)
        all_sizes = sorted(Size.query.all(), key=natural_key)
        all_fields = sorted(Field.query.all(), key=natural_key)
        cache_name = (
            f'stock_report|product|{end_date.isoformat()}'
            f'|f()|p()|sauto|y()'
        )

        def _build():
            return build_stock_report_data(
                'product',
                end_date,
                selected_fields=None,
                selected_plants=None,
                selected_sizes=None,
                selected_years=None,
                all_plants=all_plants,
                all_sizes=all_sizes,
                all_fields=all_fields,
            )

        cached_report(cache_name, _build, force=True)
        stats['stock_report'] = end_date.isoformat()
    except Exception:
        _logger.exception('warm stock_report failed')
    return stats
