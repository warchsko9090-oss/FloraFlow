"""Web Push (VAPID) — серверная часть.

Что здесь:
  • Blueprint `/api/push/...` — выдача публичного VAPID-ключа, подписка/отписка
    устройства пользователя (PWA).
  • Helper `send_push_for_tg_task(task)` — отправляет push конкретной TgTask
    всем подходящим подписанным устройствам (по assignee_id и/или ролям).
  • CLI: `python -m app.push generate` печатает свежую пару VAPID-ключей.

Push отключается «мягко»: если в env не заданы VAPID_PUBLIC_KEY /
VAPID_PRIVATE_KEY, серверные эндпоинты по-прежнему отвечают 200 (с пустым
ключом — фронт это проверяет), а `send_push_for_tg_task` тихо ничего не
отправляет. Это нужно для безопасного выката: ключи можно выпустить и
прописать в Amvera уже после деплоя кода.
"""

from __future__ import annotations

import json
import os
import sys
import base64
import threading
from datetime import datetime
from typing import Iterable, Optional

from flask import Blueprint, current_app, jsonify, request
from flask_login import current_user, login_required

from app.models import db, PushSubscription, User, TgTask
from app.utils import msk_now


bp = Blueprint('push', __name__)


# --- утилиты конфигурации ----------------------------------------------------

def _vapid_public_key() -> str:
    return (current_app.config.get('VAPID_PUBLIC_KEY') or '').strip()


def _vapid_private_key() -> str:
    raw = (current_app.config.get('VAPID_PRIVATE_KEY') or '').strip()
    if not raw:
        return ''
    # Часто в env переводы строк заэкранированы как \n — pywebpush ждёт реальные.
    if '\\n' in raw and 'BEGIN' in raw:
        raw = raw.replace('\\n', '\n')
    return raw


def _vapid_claim() -> dict:
    email = (current_app.config.get('VAPID_CLAIM_EMAIL') or 'mailto:admin@example.com').strip()
    if not email.startswith('mailto:'):
        email = 'mailto:' + email
    return {'sub': email}


def is_push_configured() -> bool:
    return bool(_vapid_public_key() and _vapid_private_key())


# --- роуты подписки ----------------------------------------------------------

@bp.route('/api/push/vapid_key', methods=['GET'])
def api_push_vapid_key():
    """Возвращает публичный VAPID-ключ для подписки на фронте.
    Если ключ не задан — отдаём пустую строку, фронт пропускает шаг подписки.
    """
    return jsonify({'key': _vapid_public_key()})


@bp.route('/api/push/subscribe', methods=['POST'])
@login_required
def api_push_subscribe():
    """Принимает PushSubscription (endpoint, keys.p256dh, keys.auth) от
    устройства и сохраняет её в БД (привязка к текущему пользователю).
    Если такой endpoint уже есть — обновляем владельца и last_seen_at.
    """
    data = request.get_json(silent=True) or {}
    endpoint = (data.get('endpoint') or '').strip()
    keys = data.get('keys') or {}
    p256dh = (keys.get('p256dh') or '').strip()
    auth = (keys.get('auth') or '').strip()
    if not endpoint or not p256dh or not auth:
        return jsonify({'status': 'error', 'message': 'invalid subscription'}), 400

    ua = (request.headers.get('User-Agent') or '')[:255]
    now = msk_now()

    sub = PushSubscription.query.filter_by(endpoint=endpoint).first()
    if sub is None:
        sub = PushSubscription(
            user_id=current_user.id,
            endpoint=endpoint,
            p256dh=p256dh,
            auth=auth,
            user_agent=ua,
            created_at=now,
            last_seen_at=now,
            failed_count=0,
        )
        db.session.add(sub)
    else:
        sub.user_id = current_user.id
        sub.p256dh = p256dh
        sub.auth = auth
        sub.user_agent = ua
        sub.last_seen_at = now
        sub.failed_count = 0
    db.session.commit()
    return jsonify({'status': 'ok', 'id': sub.id})


@bp.route('/api/push/test', methods=['POST', 'GET'])
@login_required
def api_push_test():
    """Шлёт тестовый push на все подписки текущего пользователя.
    Полезно для диагностики: видно сразу, сколько устройств получило."""
    if not is_push_configured():
        return jsonify({'status': 'error', 'message': 'vapid not configured', 'sent': 0}), 200
    payload = {
        'title': 'Тестовое уведомление',
        'body': 'Это проверочный push от сервера.',
        'url': '/',
        'tag': f'test-{current_user.id}-{int(msk_now().timestamp())}',
        'task_id': 0,
    }
    sent = send_push_to_users([current_user.id], payload)
    subs = PushSubscription.query.filter_by(user_id=current_user.id).count()
    return jsonify({'status': 'ok', 'sent': sent, 'subscriptions': subs})


@bp.route('/api/push/unsubscribe', methods=['POST'])
@login_required
def api_push_unsubscribe():
    data = request.get_json(silent=True) or {}
    endpoint = (data.get('endpoint') or '').strip()
    if not endpoint:
        return jsonify({'status': 'error', 'message': 'no endpoint'}), 400
    sub = PushSubscription.query.filter_by(endpoint=endpoint).first()
    if sub:
        db.session.delete(sub)
        db.session.commit()
    return jsonify({'status': 'ok'})


# --- отправка push -----------------------------------------------------------

def _role_match(card_role: str, user_role: str) -> bool:
    """Совместимо с _role_match из main.py: assignee_role хранится одной
    строкой, в ней может быть несколько ролей через запятую."""
    if not card_role:
        return False
    parts = [p.strip() for p in card_role.split(',') if p.strip()]
    return user_role in parts


def _collect_target_user_ids(task: TgTask) -> set[int]:
    """Берём всех адресатов задачи: персональный assignee_id + все
    пользователи, чья роль входит в assignee_role.
    """
    user_ids: set[int] = set()
    if task.assignee_id:
        user_ids.add(int(task.assignee_id))
    role_csv = (task.assignee_role or '').strip()
    if role_csv:
        roles = [r.strip() for r in role_csv.split(',') if r.strip()]
        if roles:
            users = User.query.filter(User.role.in_(roles)).all()
            for u in users:
                user_ids.add(u.id)
    return user_ids


def _build_payload(task: TgTask) -> dict:
    """Готовим JSON-payload для service worker. Лёгкий: title + body + url.
    Текст обрезаем, чтобы не упереться в лимит Apple Push (~4 KB)."""
    title = (task.title or 'Новая задача')[:100]
    body = (task.details or task.raw_text or '')[:300]
    url = '/'  # дефолт — открыть приложение
    try:
        # Если внутри action_payload есть ссылка на заказ/документ — используем.
        ap = json.loads(task.action_payload or '{}')
        if isinstance(ap, dict):
            if ap.get('order_id'):
                url = f"/order/{ap['order_id']}"
            elif ap.get('doc_id'):
                url = f"/document/edit/{ap['doc_id']}"
    except Exception:
        pass
    return {
        'title': title,
        'body': body,
        'url': url,
        'tag': task.dedup_key or f'task-{task.id}',
        'task_id': task.id,
    }


def send_push_to_users(user_ids: Iterable[int], payload: dict, ttl: int = 3600) -> int:
    """Отправляет push на все подписки указанных пользователей.
    Возвращает количество успешно отправленных. При ошибке 410/404
    (подписка истекла) удаляем её из БД.
    """
    if not is_push_configured():
        return 0
    user_ids = [int(x) for x in user_ids if x]
    if not user_ids:
        return 0
    try:
        from pywebpush import webpush, WebPushException
    except Exception as e:
        try:
            current_app.logger.warning('pywebpush not available: %s', e)
        except Exception:
            pass
        return 0

    subs = PushSubscription.query.filter(PushSubscription.user_id.in_(user_ids)).all()
    if not subs:
        return 0

    private_key = _vapid_private_key()
    claims = _vapid_claim()
    sent = 0
    to_delete: list[int] = []
    data_str = json.dumps(payload, ensure_ascii=False)

    for sub in subs:
        sub_info = {
            'endpoint': sub.endpoint,
            'keys': {'p256dh': sub.p256dh, 'auth': sub.auth},
        }
        try:
            webpush(
                subscription_info=sub_info,
                data=data_str,
                vapid_private_key=private_key,
                vapid_claims=dict(claims),
                ttl=ttl,
            )
            sent += 1
            sub.last_seen_at = msk_now()
            sub.failed_count = 0
        except WebPushException as e:
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            if status in (404, 410):
                # Подписка протухла — удалим.
                to_delete.append(sub.id)
            else:
                sub.failed_count = (sub.failed_count or 0) + 1
                if sub.failed_count >= 5:
                    to_delete.append(sub.id)
                try:
                    current_app.logger.warning(
                        'webpush failed for sub=%s status=%s: %s', sub.id, status, e
                    )
                except Exception:
                    pass
        except Exception as e:
            try:
                current_app.logger.warning('webpush unexpected error: %s', e)
            except Exception:
                pass
            sub.failed_count = (sub.failed_count or 0) + 1
            if sub.failed_count >= 5:
                to_delete.append(sub.id)

    if to_delete:
        PushSubscription.query.filter(PushSubscription.id.in_(to_delete)).delete(
            synchronize_session=False
        )
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
    return sent


def send_push_for_tg_task(task: Optional[TgTask]) -> int:
    """Главный публичный helper для триггеров TgTask."""
    if not task:
        return 0
    if not is_push_configured():
        return 0
    try:
        targets = _collect_target_user_ids(task)
        if not targets:
            return 0
        return send_push_to_users(targets, _build_payload(task))
    except Exception as e:
        try:
            current_app.logger.warning('send_push_for_tg_task failed: %s', e)
        except Exception:
            pass
        return 0


# --- Авто-хуки на создание TgTask -------------------------------------------
#
# Вместо ручной правки десятка мест, где создаются TgTask, вешаем глобальный
# слушатель: фиксируем id новых задач в локальной очереди транзакции, а после
# успешного commit пушим их. Сбой push никогда не ломает основной поток.

from sqlalchemy import event as _sa_event

_PENDING_PUSH_KEY = '_pending_push_task_ids'


def _on_tgtask_after_insert(mapper, connection, target):
    try:
        sess = db.session  # текущая сессия Flask-SQLAlchemy
        if sess is None:
            return
        # Запоминаем ID задачи в самой сессии — чтобы достать после commit.
        bucket = sess.info.setdefault(_PENDING_PUSH_KEY, [])
        # ID может быть пока None для autoincrement — добавим объект, а после
        # коммита возьмём его id (объект уже будет иметь PK).
        bucket.append(target)
    except Exception:
        pass


def _push_background_runner(app, task_ids):
    """Запущен в отдельном демон-потоке. Создаёт свой app-context, поэтому
    flask-sqlalchemy выдаст этой ветке новую сессию (а не ту, что в состоянии
    'committed' у изначального запроса). Здесь уже можно спокойно читать
    TgTask, отправлять webpush и коммитить вспомогательные обновления
    (last_seen_at / удаление протухших подписок).
    """
    try:
        with app.app_context():
            try:
                tasks = TgTask.query.filter(TgTask.id.in_(task_ids)).all()
                for t in tasks:
                    try:
                        if t.status not in (None, 'new'):
                            continue
                        send_push_for_tg_task(t)
                    except Exception:
                        try:
                            app.logger.warning('push: send for task %s failed', getattr(t, 'id', '?'))
                        except Exception:
                            pass
            except Exception as e:
                try:
                    app.logger.warning('push background sender failed: %s', e)
                except Exception:
                    pass
            finally:
                try:
                    db.session.remove()
                except Exception:
                    pass
    except Exception:
        pass


def _on_session_after_commit(session):
    bucket = session.info.pop(_PENDING_PUSH_KEY, None)
    if not bucket:
        return
    try:
        from flask import has_app_context
        if not has_app_context():
            return
    except Exception:
        return
    if not is_push_configured():
        return
    # PK уже проставлен после flush/commit, но сами объекты по-прежнему
    # привязаны к "только что закоммиченной" сессии — поэтому работаем
    # только со списком id, а тяжёлый запрос делаем в фоне.
    ids = []
    for obj in bucket:
        try:
            if obj.id:
                ids.append(int(obj.id))
        except Exception:
            continue
    if not ids:
        return
    try:
        app = current_app._get_current_object()
    except Exception:
        return
    try:
        threading.Thread(
            target=_push_background_runner,
            args=(app, ids),
            name='webpush-sender',
            daemon=True,
        ).start()
    except Exception as e:
        try:
            current_app.logger.warning('push hook spawn failed: %s', e)
        except Exception:
            pass


def register_push_hooks(app):
    """Регистрирует SQLAlchemy-слушатели один раз при старте приложения."""
    try:
        # Чтобы не подписаться дважды при перезагрузках reloader-ом.
        if getattr(app, '_push_hooks_registered', False):
            return
        _sa_event.listen(TgTask, 'after_insert', _on_tgtask_after_insert)
        _sa_event.listen(db.session, 'after_commit', _on_session_after_commit)
        app._push_hooks_registered = True
    except Exception as e:
        try:
            app.logger.warning('register_push_hooks failed: %s', e)
        except Exception:
            pass


# --- CLI ---------------------------------------------------------------------

def _print_keypair():
    """Печатает свежую пару VAPID-ключей в формате для env."""
    try:
        from py_vapid import Vapid01 as Vapid
    except Exception:
        from py_vapid import Vapid  # старые версии
    v = Vapid()
    v.generate_keys()
    # private — в base64url из raw 32 байт; public — 65 байт uncompressed.
    priv_pem = v.private_pem().decode('utf-8')
    # public_key — uncompressed P-256 (65 байт), кодируем в base64url без padding.
    try:
        pub_raw = v.public_key.public_bytes(
            __import__('cryptography').hazmat.primitives.serialization.Encoding.X962,
            __import__('cryptography').hazmat.primitives.serialization.PublicFormat.UncompressedPoint,
        )
    except Exception:
        # Fallback: публичный ключ берём из метода py_vapid
        pub_raw = base64.urlsafe_b64decode(v.public_key_urlsafe_base64() + '==')
    pub_b64 = base64.urlsafe_b64encode(pub_raw).rstrip(b'=').decode('ascii')

    # Приватный для pywebpush — путь к PEM или сам PEM-стрингом. Удобнее — pem.
    print('VAPID_PUBLIC_KEY=' + pub_b64)
    print('VAPID_PRIVATE_KEY=' + priv_pem.replace('\n', '\\n'))
    print('# Подсказка: VAPID_PRIVATE_KEY можно положить в env в одну строку')
    print('# с экранированными переводами (\\n), либо использовать base64-PEM.')


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'generate':
        _print_keypair()
    else:
        print('usage: python -m app.push generate')
