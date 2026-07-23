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

from app.models import db, TgTask, OrderItem
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


def sync_year_mismatch_for_order(order_id):
    """Синхронная проверка год/остаток конкретного заказа после правок.

    На каждую активную позицию заказа отдельная аномалия (3 возможных kind).
    Карточки идемпотентны, dedup_key=`{kind}:item={item_id}`. Если позиция
    «починилась» — старая карточка закрывается.
    """
    if not order_id:
        return
    try:
        from app.anomaly_rules import detect_year_mismatch_for_order
        now = msk_now()
        found = detect_year_mismatch_for_order(order_id) or []
        new_by_item = {}
        for a in found:
            payload = a.get('payload') or {}
            item_id = payload.get('item_id')
            if item_id:
                new_by_item[item_id] = a

        items = OrderItem.query.filter_by(order_id=order_id).all()
        kinds = ('year_no_batch', 'year_overcommit', 'year_no_stock_anywhere')
        for it in items:
            new_a = new_by_item.get(it.id)
            for k in kinds:
                key = f'{k}:item={it.id}'
                card = TgTask.query.filter(
                    TgTask.action_type == ANOMALY_ACTION_TYPE,
                    TgTask.dedup_key == key,
                    TgTask.status == 'new',
                ).first()
                if new_a and new_a.get('kind') == k:
                    payload_json = json.dumps(
                        new_a.get('payload') or {},
                        ensure_ascii=False, default=str,
                    )
                    if card is None:
                        db.session.add(TgTask(
                            raw_text=f'[anomaly:{k}]',
                            title=new_a['title'],
                            details=new_a['details'],
                            action_type=ANOMALY_ACTION_TYPE,
                            action_payload=payload_json,
                            status='new',
                            first_seen_at=now,
                            last_seen_at=now,
                            dedup_key=key,
                            severity=new_a.get('severity', 'warning'),
                            assignee_role=_serialize_roles(new_a.get('roles') or ['admin', 'executive']),
                            sender_name='system.anomaly',
                            source='anomaly',
                        ))
                    else:
                        card.title = new_a['title']
                        card.details = new_a['details']
                        card.action_payload = payload_json
                        card.severity = new_a.get('severity', card.severity or 'warning')
                        card.last_seen_at = now
                else:
                    if card is not None:
                        card.status = 'done'
                        card.completed_at = now
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            current_app.logger.warning(
                'sync_year_mismatch_for_order(%s) failed: %s', order_id, e
            )
        except Exception:
            pass


_INVOICE_PSEUDO = {'HISTORY', 'IMPORT'}


def _order_has_invoice(order):
    """True, если у заказа выставлен «настоящий» счёт. Служебные значения
    HISTORY/IMPORT не считаются (они присваиваются ghost-заказам)."""
    if not order:
        return False
    inv = (order.invoice_number or '').strip()
    if not inv:
        return False
    if inv.upper() in _INVOICE_PSEUDO:
        return False
    return True


def check_digging_started_without_invoice(order_id):
    """Триггер: бригадир начал копать заказ, у которого ещё нет счёта.

    Идемпотентно — на заказ создаётся одна карточка (dedup_key с order_id).
    При появлении счёта или отмене заказа карточка автоматически закрывается.
    Получатели:
      • admin + executive (по роли);
      • менеджер, создавший заказ (по assignee_id, если Order.created_by_user_id
        известен и относится к роли user/admin/executive — иначе пропускаем
        персональную привязку, остаётся только показ по ролям).
    """
    if not order_id:
        return
    try:
        from app.models import Order

        order = Order.query.get(order_id)
        if not order or order.is_deleted:
            return

        dedup_key = f'digging_no_invoice:order={order_id}'
        card = TgTask.query.filter(
            TgTask.action_type == ANOMALY_ACTION_TYPE,
            TgTask.dedup_key == dedup_key,
        ).first()

        # Условие «не требуется уведомление» — счёт выставлен или заказ закрыт.
        if (
            _order_has_invoice(order)
            or order.status in ('canceled', 'ghost')
        ):
            if card and card.status == 'new':
                card.status = 'done'
                card.completed_at = msk_now()
                db.session.commit()
            return

        if card and card.status == 'new':
            # Уже создана — просто обновим last_seen_at.
            card.last_seen_at = msk_now()
            db.session.commit()
            return
        if card and card.status == 'done':
            # Уже закрыта руками админа — не воскрешаем.
            return

        # Создаём новую карточку.
        client_name = (order.client.name if order.client else '—')
        try:
            total = float(order.total_sum or 0)
        except Exception:
            total = 0.0

        details = (
            f'По заказу №{order_id} ({client_name}) бригадир начал выкопку, '
            f'но в заказе не указан счёт. '
        )
        if total:
            details += f'Сумма заказа {total:,.0f} ₽. '.replace(',', ' ')
        details += 'Нужно выставить счёт клиенту до отгрузки.'

        # Менеджер заказа — по возможности.
        assignee_id = None
        try:
            uid = order.created_by_user_id
            if uid:
                from app.models import User
                u = User.query.get(uid)
                if u and u.role in ('admin', 'executive', 'user'):
                    assignee_id = u.id
        except Exception:
            assignee_id = None

        roles = ['admin', 'executive', 'user']
        now = msk_now()
        task = TgTask(
            raw_text=f'[anomaly:digging_no_invoice]',
            title=f'Заказ №{order_id} копают без счёта',
            details=details,
            action_type=ANOMALY_ACTION_TYPE,
            action_payload=json.dumps({'order_id': order_id}, ensure_ascii=False),
            status='new',
            first_seen_at=now,
            last_seen_at=now,
            dedup_key=dedup_key,
            severity='warning',
            assignee_id=assignee_id,
            assignee_role=_serialize_roles(roles),
            sender_name='system.anomaly',
            source='anomaly',
        )
        db.session.add(task)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            current_app.logger.warning(
                'check_digging_started_without_invoice(%s) failed: %s', order_id, e
            )
        except Exception:
            pass


def sync_recount_anomaly_for_doc(doc_id):
    """Синхронная проверка карточки пересчёта (Document doc_type='field_recount')
    на аномальные расхождения. Создаёт/обновляет/закрывает TgTask по двум
    видам:
      - recount_asymmetry — асимметрия плюсов и минусов в одной карточке;
      - recount_volume_high — объём правок к остатку поля (по полю).
    Все аномалии связаны с одним документом, dedup_key включает doc.id, поэтому
    повторное сохранение карточки админом обновляет ту же запись (не плодит
    дубли).
    """
    if not doc_id:
        return
    try:
        from app.anomaly_rules import detect_recount_anomaly_for_doc

        now = msk_now()
        found = detect_recount_anomaly_for_doc(doc_id) or []
        new_by_key = {a['dedup_key']: a for a in found}

        # Все ранее существующие открытые карточки для ЭТОГО документа —
        # ищем по префиксам ключей.
        prefixes = (
            f'recount_asymmetry:doc={doc_id}',
            f'recount_volume_high:doc={doc_id}:',
        )
        existing = TgTask.query.filter(
            TgTask.action_type == ANOMALY_ACTION_TYPE,
            TgTask.status == 'new',
            or_(
                TgTask.dedup_key == prefixes[0],
                TgTask.dedup_key.like(prefixes[1] + '%'),
            ),
        ).all()
        existing_map = {t.dedup_key: t for t in existing}

        for key, a in new_by_key.items():
            payload_json = json.dumps(a.get('payload') or {}, ensure_ascii=False, default=str)
            card = existing_map.get(key)
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
                    dedup_key=key,
                    severity=a.get('severity', 'warning'),
                    assignee_role=_serialize_roles(a.get('roles') or ['admin', 'executive']),
                    sender_name='system.anomaly',
                    source='anomaly',
                ))
            else:
                card.title = a['title']
                card.details = a['details']
                card.action_payload = payload_json
                card.severity = a.get('severity', card.severity or 'warning')
                card.last_seen_at = now

        # Закрываем те, которых в новой выдаче нет (документ переутвердили
        # без перекоса).
        for key, card in existing_map.items():
            if key in new_by_key:
                continue
            card.status = 'done'
            card.completed_at = now

        db.session.commit()
    except Exception as e:
        db.session.rollback()
        try:
            current_app.logger.warning(
                'sync_recount_anomaly_for_doc(%s) failed: %s', doc_id, e
            )
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
