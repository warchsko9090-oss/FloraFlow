"""Токены доступа к чувствительным shop-ресурсам (КП PDF и т.п.)."""
from __future__ import annotations

import hashlib
import hmac

from flask import current_app


def shop_kp_access_token(doc_id: int) -> str:
    secret = (current_app.config.get('SECRET_KEY') or 'change-me').encode('utf-8')
    digest = hmac.new(secret, f'kp:{int(doc_id)}'.encode(), hashlib.sha256).hexdigest()
    return digest[:32]


def verify_shop_kp_access(doc_id: int, token: str | None) -> bool:
    given = (token or '').strip()
    if not given:
        return False
    expected = shop_kp_access_token(doc_id)
    return hmac.compare_digest(given, expected)
