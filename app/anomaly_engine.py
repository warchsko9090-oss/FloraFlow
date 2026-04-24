"""Оркестратор аномалий: запускает детекторы и синхронизирует результат
с TgTask. Идемпотентен — можно вызывать сколько угодно раз в день, карточки
не будут дублироваться (dedup_key).

Логика:
  1. Собираем текущие аномалии через `anomaly_rules.run_all_detectors()`.
  2. Для каждой — ищем открытый TgTask с таким dedup_key.
     - Если нашли: обновляем last_seen_at и details (поля могли измениться).
     - Если не нашли: создаём новый TgTask, first_seen_at = last_seen_at = сейчас.
  3. Все ещё-открытые аномальные TgTask, которых СЕЙЧАС в списке нет, закрываем
     (status='done', completed_at=now) — аномалия сама себя решила.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, date

from flask import current_app
from sqlalchemy import or_

from app.models import db, TgTask
from app.utils import msk_now, msk_today
from app.anomaly_rules import run_all_detectors


# При каком action_type храним аномалии — отдельный тип, чтобы не путаться
# с задачами из Telegram.
ANOMALY_ACTION_TYPE = 'anomaly'


def _tg_notify_new_anomaly(anomaly):
    """Шлёт в рабочий TG-чат уведомление при ПЕРВОМ появлении конкретной
    аномалии. Вызывается из ветки создания в `run_daily_scan`, чтобы не
    спамить повторными сообщениями в последующие сутки (дедуп по dedup_key).

    На данный момент — только для 'stale_reserved' (резерв без движения 14 дн.)
    по запросу продавцов. Остальные аномалии остаются только в ленте дашборда.
    """
    kind = anomaly.get('kind')
    if kind != 'stale_reserved':
        return

    try:
        from app import telegram as tg
    except Exception:
        return

    payload = anomaly.get('payload') or {}
    order_id = payload.get('order_id')
    client = payload.get('client_name') or '—'
    days = payload.get('days_stale')
    total = payload.get('total_sum')

    base_url = (os.environ.get('APP_BASE_URL') or '').strip().rstrip('/')
    link = f'{base_url}/order/{order_id}' if base_url and order_id else f'/order/{order_id}'

    lines = [
        f'⚠️ <b>Заказ №{order_id}</b> в резерве {days} дн. без движения.',
        f'Клиент: <b>{client}</b>' + (f' · сумма {total:,.0f} ₽'.replace(',', ' ') if total else ''),
        'Нужно связаться с клиентом и уточнить актуальность заказа.',
        f'Открыть: {link}',
    ]
    try:
        tg.send_message('\n'.join(lines), chat_type='orders')
    except Exception as exc:
        try:
            current_app.logger.warning('stale_reserved TG notify failed: %s', exc)
        except Exception:
            pass


def _serialize_roles(roles):
    """Роли у TgTask хранятся одной строкой. Если нужно несколько — склеим
    через запятую, а при чтении разберём."""
    if not roles:
        return None
    return ','.join(roles) if isinstance(roles, (list, tuple)) else str(roles)


def run_daily_scan():
    """Синхронизировать аномалии с TgTask. Возвращает dict со сводкой."""
    now = msk_now()
    today = msk_today()

    detected, detector_errors = run_all_detectors(today, collect_errors=True)
    detected_map = {a['dedup_key']: a for a in detected}

    # Существующие открытые аномальные карточки
    existing = TgTask.query.filter(
        TgTask.action_type == ANOMALY_ACTION_TYPE,
        TgTask.status == 'new',
        TgTask.dedup_key.isnot(None),
    ).all()
    existing_map = {t.dedup_key: t for t in existing}

    created = 0
    updated = 0
    resolved = 0

    # 1) Создание / обновление
    newly_created_anomalies = []
    for key, a in detected_map.items():
        card = existing_map.get(key)
        payload_json = json.dumps(a.get('payload') or {}, ensure_ascii=False)
        if card is None:
            card = TgTask(
                raw_text=f'[anomaly:{a["kind"]}]',
                title=a['title'],
                details=a['details'],
                action_type=ANOMALY_ACTION_TYPE,
                action_payload=payload_json,
                status='new',
                # Первая встречная — обе метки равны.
                first_seen_at=now,
                last_seen_at=now,
                dedup_key=key,
                severity=a.get('severity', 'info'),
                assignee_role=_serialize_roles(a.get('roles') or ['admin', 'executive']),
                sender_name='system.anomaly',
                source='anomaly',
            )
            db.session.add(card)
            created += 1
            newly_created_anomalies.append(a)
        else:
            # Обновляем текст и last_seen — «жива» аномалия.
            card.title = a['title']
            card.details = a['details']
            card.action_payload = payload_json
            card.severity = a.get('severity', card.severity or 'info')
            card.last_seen_at = now
            updated += 1

    # 2) Закрытие исчезнувших
    for key, card in existing_map.items():
        if key in detected_map:
            continue
        card.status = 'done'
        card.completed_at = now
        # completed_by_id оставляем None — это автоматическое закрытие.
        resolved += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception(f'anomaly scan commit failed: {e}')
        return {'ok': False, 'error': str(e)}

    # После успешного коммита — шлём TG-уведомления по свежесозданным аномалиям.
    # Если TG отвалится — это не должно ронять скан.
    for a in newly_created_anomalies:
        try:
            _tg_notify_new_anomaly(a)
        except Exception:
            try:
                current_app.logger.exception('TG notify for new anomaly failed')
            except Exception:
                pass

    return {
        'ok': True,
        'created': created,
        'updated': updated,
        'resolved': resolved,
        'total_detected': len(detected_map),
        'scan_at': now.isoformat(),
        'detector_errors': detector_errors,
    }


def sync_price_anomaly_for_order(order_id):
    """Синхронная проверка цен конкретного заказа (после create/edit).

    В отличие от `run_daily_scan`, трогает ровно одну карточку с
    dedup_key=order_no_price:order={id}: создаёт/обновляет, если у заказа
    есть позиции без цены, либо закрывает, если цены проставлены. Остальные
    аномалии не затрагивает.

    Вызывается из orders.py после коммита, поэтому всегда вешается в try/except —
    никогда не ломает основной поток.
    """
    if not order_id:
        return
    try:
        from app.anomaly_rules import detect_orders_missing_price
        now = msk_now()
        today = msk_today()

        found = detect_orders_missing_price(today=today, order_id=order_id) or []
        dedup_key = f'order_no_price:order={order_id}'
        card = TgTask.query.filter(
            TgTask.action_type == ANOMALY_ACTION_TYPE,
            TgTask.dedup_key == dedup_key,
            TgTask.status == 'new',
        ).first()

        if found:
            a = found[0]
            payload_json = json.dumps(a.get('payload') or {}, ensure_ascii=False)
            if card is None:
                db.session.add(TgTask(
                    raw_text=f'[anomaly:{a["kind"]}]',
                    title=a['title'],
                    details=a['details'],
                    action_type=ANOMALY_ACTION_TYPE,
                    action_payload=payload_json,
                    status='new',
                    first_seen_at=now,
                    last_seen_at=now,
                    dedup_key=dedup_key,
                    severity=a.get('severity', 'warning'),
                    assignee_role=_serialize_roles(a.get('roles') or ['admin', 'executive', 'user']),
                    sender_name='system.anomaly',
                    source='anomaly',
                ))
            else:
                card.title = a['title']
                card.details = a['details']
                card.action_payload = payload_json
                card.severity = a.get('severity', card.severity or 'warning')
                card.last_seen_at = now
        else:
            # Цены теперь в порядке — закрываем карточку, если была.
            if card is not None:
                card.status = 'done'
                card.completed_at = now

        db.session.commit()
    except Exception as e:  # noqa: BLE001 — не роняем основной поток
        db.session.rollback()
        try:
            current_app.logger.warning('sync_price_anomaly_for_order(%s) failed: %s', order_id, e)
        except Exception:
            pass


def ensure_daily_scan():
    """Запускает скан максимум раз в сутки. Проверка идёт через «последний
    last_seen_at среди аномалий» — если он свежее, чем граница (пн=сегодня),
    считаем, что скан сегодня уже проходил.

    Безопасно: если скана сегодня не было — запустим; если был — no-op.
    """
    today = msk_today()
    last = db.session.query(db.func.max(TgTask.last_seen_at)).filter(
        TgTask.action_type == ANOMALY_ACTION_TYPE,
    ).scalar()
    if last is not None:
        last_date = last.date() if hasattr(last, 'date') else last
        if last_date >= today:
            return None  # уже сканировали сегодня
    return run_daily_scan()


def days_since_first_seen(card):
    """Сколько дней длится аномалия (для UI «идёт 3-ю неделю»)."""
    if not card.first_seen_at:
        return 0
    delta = msk_now() - card.first_seen_at
    return max(0, delta.days)
