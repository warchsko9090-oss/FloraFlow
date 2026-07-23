"""Фоновое слайд-шоу витрины (data-sidebar) — общая логика для /shop и главной."""
from __future__ import annotations

import os

from app.photo_storage import SIDEBAR_REL_DIR, list_sidebar_files
from app.shop_images import SHOP_IMG_BG, shop_image_url

SIDEBAR_SLIDE_INTERVAL_MS = 7000


def get_sidebar_slide_urls(width: int = SHOP_IMG_BG) -> list[str]:
    """Список URL слайдов. На shop-контейнере файлы на ERP — запрашиваем через ERP_PUBLIC_BASE_URL."""
    from flask import current_app

    root = current_app.config['UPLOAD_FOLDER']
    local_files = list_sidebar_files(root)
    if local_files:
        return [
            shop_image_url(f'{SIDEBAR_REL_DIR}/{name}', width)
            for name in local_files
        ]

    erp = (os.environ.get('ERP_PUBLIC_BASE_URL') or '').strip().rstrip('/')
    if erp:
        import requests

        try:
            r = requests.get(f'{erp}/public/client/sidebar-slides', timeout=15)
            if r.ok:
                data = r.json()
                return [u for u in (data.get('items') or []) if u]
        except Exception:
            pass
    return []


def sidebar_slides_payload(width: int = SHOP_IMG_BG) -> dict:
    return {
        'status': 'ok',
        'items': get_sidebar_slide_urls(width),
        'interval_ms': SIDEBAR_SLIDE_INTERVAL_MS,
    }
