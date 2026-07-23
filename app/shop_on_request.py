"""Заявки «По запросу» с витрины /shop."""

from __future__ import annotations

from flask_login import current_user
from sqlalchemy import inspect, text

from app.models import Document, Plant, ShopOnRequest, ShopOnRequestLog, Size, db
from app.shop_catalog import build_visible_catalog_items, sort_catalog_items
from app.utils import msk_now


def ensure_shop_on_request_nullable():
    """Разрешает заявки без plant_id/size_id (форма с главной страницы)."""
    insp = inspect(db.engine)
    if not insp.has_table('shop_on_request'):
        return
    cols = {c['name']: c for c in insp.get_columns('shop_on_request')}
    plant_col = cols.get('plant_id')
    if not plant_col or plant_col.get('nullable'):
        return
    if db.engine.dialect.name == 'postgresql':
        db.session.execute(text('ALTER TABLE shop_on_request ALTER COLUMN plant_id DROP NOT NULL'))
        db.session.execute(text('ALTER TABLE shop_on_request ALTER COLUMN size_id DROP NOT NULL'))
        db.session.commit()


def _catalog_on_request_map():
    return {
        (item['plant_id'], item['size_id']): item
        for item in sort_catalog_items(build_visible_catalog_items(apply_shop_prices=True))
        if item.get('on_request')
    }


def validate_on_request_submission(plant_id, size_id, customer_name, phone, message):
    """Проверяет данные заявки. Возвращает (ok, error_message, catalog_item)."""
    try:
        plant_id = int(plant_id)
        size_id = int(size_id)
    except (TypeError, ValueError):
        return False, 'Некорректная позиция', None

    if not Plant.query.get(plant_id) or not Size.query.get(size_id):
        return False, 'Позиция не найдена', None

    cat_item = _catalog_on_request_map().get((plant_id, size_id))
    if not cat_item:
        return False, 'Позиция недоступна для заявки «По запросу»', None

    name = (customer_name or '').strip()
    phone_val = (phone or '').strip()
    msg = (message or '').strip()

    if len(name) < 2:
        return False, 'Укажите имя или название компании', None
    if len(name) > 200:
        return False, 'Слишком длинное имя', None
    if len(phone_val) < 5:
        return False, 'Укажите телефон', None
    if len(phone_val) > 80:
        return False, 'Слишком длинный телефон', None
    if len(msg) < 3:
        return False, 'Напишите сообщение менеджеру', None
    if len(msg) > 2000:
        return False, 'Сообщение слишком длинное', None

    return True, '', cat_item


def _append_log(request_row, action, old_status, new_status, comment=None, user_id=None):
    db.session.add(ShopOnRequestLog(
        request_id=request_row.id,
        action=action,
        user_id=user_id,
        comment=(comment or '').strip() or None,
        old_status=old_status,
        new_status=new_status,
    ))


def create_on_request(plant_id, size_id, customer_name, phone, message):
    ensure_shop_on_request_nullable()
    ok, err, _cat = validate_on_request_submission(plant_id, size_id, customer_name, phone, message)
    if not ok:
        raise ValueError(err)

    row = ShopOnRequest(
        plant_id=int(plant_id),
        size_id=int(size_id),
        customer_name=(customer_name or '').strip(),
        phone=(phone or '').strip(),
        message=(message or '').strip(),
        status=ShopOnRequest.STATUS_NEW,
    )
    db.session.add(row)
    db.session.flush()
    _append_log(row, 'created', None, ShopOnRequest.STATUS_NEW)
    return row


def validate_landing_inquiry(customer_name, phone, message=''):
    name = (customer_name or '').strip()
    phone_val = (phone or '').strip()
    msg = (message or '').strip() or 'Запрос прайса и консультации с главной страницы knyajestvo.ru'

    if len(name) < 2:
        return False, 'Укажите имя или название компании', ''
    if len(name) > 200:
        return False, 'Слишком длинное имя', ''
    if len(phone_val) < 5:
        return False, 'Укажите телефон', ''
    if len(phone_val) > 80:
        return False, 'Слишком длинный телефон', ''
    if len(msg) > 2000:
        return False, 'Сообщение слишком длинное', ''
    return True, '', msg


def create_landing_inquiry(customer_name, phone, message=''):
    ensure_shop_on_request_nullable()
    ok, err, msg = validate_landing_inquiry(customer_name, phone, message)
    if not ok:
        raise ValueError(err)

    row = ShopOnRequest(
        plant_id=None,
        size_id=None,
        customer_name=(customer_name or '').strip(),
        phone=(phone or '').strip(),
        message=msg,
        status=ShopOnRequest.STATUS_NEW,
    )
    db.session.add(row)
    db.session.flush()
    _append_log(row, 'created', None, ShopOnRequest.STATUS_NEW)
    return row


def review_on_request(request_id, action, manager_comment=''):
    """Согласовать или отклонить заявку. Возвращает обновлённую запись."""
    row = ShopOnRequest.query.get_or_404(request_id)
    if row.status != ShopOnRequest.STATUS_NEW:
        raise ValueError('Заявка уже обработана')

    comment = (manager_comment or '').strip()
    user_id = current_user.id if current_user and current_user.is_authenticated else None
    old_status = row.status

    if action == 'approve':
        row.status = ShopOnRequest.STATUS_APPROVED
        row.manager_comment = comment or None
        row.reviewed_by_user_id = user_id
        row.reviewed_at = msk_now()
        _append_log(row, 'approved', old_status, row.status, comment, user_id)
    elif action == 'reject':
        if len(comment) < 2:
            raise ValueError('Укажите причину отказа')
        row.status = ShopOnRequest.STATUS_REJECTED
        row.manager_comment = comment
        row.reviewed_by_user_id = user_id
        row.reviewed_at = msk_now()
        _append_log(row, 'rejected', old_status, row.status, comment, user_id)
    else:
        raise ValueError('Неизвестное действие')

    return row


def count_new_on_requests():
    return ShopOnRequest.query.filter_by(status=ShopOnRequest.STATUS_NEW).count()


def count_dashboard_site_alerts(role: str) -> int:
    """Новые заявки с витрины для бейджа дашборда (как в ленте index())."""
    total = 0
    if role in ('admin', 'user', 'executive'):
        total += count_new_on_requests()
    if role in ('admin', 'user'):
        total += Document.query.filter_by(doc_type='client_draft').count()
    return total
