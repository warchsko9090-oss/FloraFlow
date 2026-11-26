"""Режим резервной витрины: тот же UI, без остатков / КП / заявок."""
from __future__ import annotations

import os


def shop_readonly_enabled() -> bool:
    return (os.environ.get("SHOP_READONLY") or "").strip().lower() in ("1", "true", "yes")


READONLY_BANNER = (
    "Резервный режим: наличие уточняйте у менеджера. "
    "Онлайн-заказ и заявки временно недоступны."
)


def readonly_forbidden_html(message: str | None = None):
    from flask import make_response

    text = message or "В резервном режиме заказ и заявки недоступны. Свяжитесь с менеджером."
    body = (
        "<!DOCTYPE html><html lang=ru><meta charset=utf-8>"
        f"<title>Временно недоступно</title><body style='font-family:serif;padding:2rem'>"
        f"<h1>Княжество</h1><p>{text}</p>"
        "<p><a href='/shop'>В каталог</a> · <a href='/shop/contacts'>Контакты</a></p>"
        "</body></html>"
    )
    return make_response(body, 403, {"Content-Type": "text/html; charset=utf-8"})
