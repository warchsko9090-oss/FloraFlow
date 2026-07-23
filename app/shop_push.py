"""Web Push операторам ERP при заявках с сайта (КП, по запросу, главная)."""

from __future__ import annotations

import threading
from typing import Iterable

from flask import current_app

from app.models import User
from app.push import is_push_configured, send_push_to_users

ROLES_KP_DRAFT = ('admin', 'user')
ROLES_ON_REQUEST = ('admin', 'user', 'executive')


def _user_ids_for_roles(roles: Iterable[str]) -> list[int]:
    role_list = [r for r in roles if r]
    if not role_list:
        return []
    return [u.id for u in User.query.filter(User.role.in_(role_list)).all()]


def _fmt_money(amount) -> str:
    try:
        n = int(round(float(amount or 0)))
    except (TypeError, ValueError):
        n = 0
    return f'{n:,}'.replace(',', ' ')


def _send_push_async(app, user_ids: list[int], payload: dict) -> None:
    """Не блокируем ответ витрины — отправка в фоне."""

    def runner():
        with app.app_context():
            try:
                if not is_push_configured():
                    return
                sent = send_push_to_users(user_ids, payload)
                if sent:
                    app.logger.info('Shop push: %s устройств', sent)
            except Exception as e:
                try:
                    app.logger.warning('Shop push failed: %s', e)
                except Exception:
                    pass

    threading.Thread(target=runner, name='shop-push', daemon=True).start()


def notify_kp_draft(payload: dict, doc_id: int) -> bool:
    """Push о новом client_draft (КП с /shop)."""
    if not is_push_configured():
        return False
    user_ids = _user_ids_for_roles(ROLES_KP_DRAFT)
    if not user_ids:
        return False

    customer = (payload.get('customer_name') or '').strip() or 'Клиент'
    body = f'{customer} · итого {_fmt_money(payload.get("total_sum"))} ₽'
    push_payload = {
        'title': f'Новый запрос КП №{int(doc_id)}',
        'body': body[:300],
        'url': f'/orders/client_draft/{int(doc_id)}',
        'tag': f'kp-draft-{int(doc_id)}',
        'task_id': 0,
    }
    app = current_app._get_current_object()
    _send_push_async(app, user_ids, push_payload)
    return True


def notify_on_request(row, source: str = 'shop') -> bool:
    """Push о ShopOnRequest (витрина или главная)."""
    if not is_push_configured():
        return False
    user_ids = _user_ids_for_roles(ROLES_ON_REQUEST)
    if not user_ids:
        return False

    source_label = 'Витрина /shop' if source == 'shop' else 'Главная'
    plant_name = 'Главная: прайс'
    if getattr(row, 'plant', None) and row.plant:
        plant_name = (row.plant.name or '').strip() or plant_name
    customer = (getattr(row, 'customer_name', '') or '').strip() or 'Клиент'
    req_id = int(getattr(row, 'id', 0) or 0)

    push_payload = {
        'title': f'Новая заявка №{req_id}',
        'body': f'{source_label}: {customer} · {plant_name}'[:300],
        'url': f'/orders/shop_on_request/{req_id}',
        'tag': f'shop-on-request-{req_id}',
        'task_id': 0,
    }
    app = current_app._get_current_object()
    _send_push_async(app, user_ids, push_payload)
    return True
