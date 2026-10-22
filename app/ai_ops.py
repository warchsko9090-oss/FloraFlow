"""AI Ops — пилот для Vlad: мульти-чат, сводка, задачи и черновики документов.

Включается env AI_OPS_ENABLED (по умолчанию on). UI/API только для is_ai_ops_pilot.
"""
from __future__ import annotations

import html
import json
import os
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from functools import wraps

from flask import (
    Blueprint, current_app, flash, jsonify, redirect, render_template,
    request, url_for,
)
from flask_login import current_user, login_required
from werkzeug.utils import secure_filename

from app.models import (
    Client, Document, OpsActionDraft, OpsChatMessage, OpsDigestLog, Order,
    Payment, Plant, PriceHistory, Size, TgTask, User, db,
)
from app.utils import msk_now, msk_today

bp = Blueprint('ai_ops', __name__, url_prefix='/ops')

_DEFAULT_DIGEST_IDS = '8512060616'
_VLAD_TG_ID = '8512060616'
_MAX_TRANSCRIPT_CHARS = 90000
_TG_MSG_LIMIT = 3900

CHANNEL_ENV = {
    'sales': 'TG_CHAT_ID_ORDERS',
    'hr': 'TG_CHAT_ID_HR',
    'production': 'TG_CHAT_ID_PRODUCTION',
    'itr': 'TG_CHAT_ID_ITR',
}
CHANNEL_LABELS = {
    'sales': 'Продажи',
    'hr': 'Кадры',
    'production': 'Производство',
    'itr': 'ИТР',
}
ACTION_TYPES = (
    'payment_confirm',
    'shipment_confirm',
    'order_create',
    'order_qty_change',
    'price_change',
)


def ai_ops_enabled() -> bool:
    raw = (os.environ.get('AI_OPS_ENABLED') or '1').strip().lower()
    return raw not in ('0', 'false', 'no', 'off')


def is_ai_ops_pilot(user) -> bool:
    if not ai_ops_enabled():
        return False
    if not user or not getattr(user, 'is_authenticated', False):
        return False
    if (getattr(user, 'username', None) or '').strip().lower() == 'vlad':
        return True
    return str(getattr(user, 'telegram_id', '') or '') == _VLAD_TG_ID


def pilot_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not is_ai_ops_pilot(current_user):
            flash('Раздел доступен только в пилоте AI Ops.', 'warning')
            return redirect(url_for('main.index'))
        return view(*args, **kwargs)
    return wrapped


def resolve_vlad_user() -> User | None:
    u = User.query.filter(db.func.lower(User.username) == 'vlad').first()
    if u:
        return u
    return User.query.filter_by(telegram_id=_VLAD_TG_ID).first()


def digest_recipient_ids() -> list[str]:
    raw = (os.environ.get('TG_SALES_DIGEST_IDS') or _DEFAULT_DIGEST_IDS).strip()
    ids = [p.strip() for p in raw.replace(';', ',').split(',') if p.strip()]
    if _VLAD_TG_ID not in ids:
        ids.append(_VLAD_TG_ID)
    return ids


def _channel_chat_id(channel: str) -> str:
    from app.telegram import _resolve_chat_id
    env_key = CHANNEL_ENV.get(channel)
    if not env_key:
        return ''
    return _resolve_chat_id(env_key) or ''


def channel_for_chat_id(chat_id: str) -> str | None:
    chat_id = str(chat_id or '')
    if not chat_id:
        return None
    for ch in CHANNEL_ENV:
        if _channel_chat_id(ch) == chat_id:
            return ch
    return None


def _tg_date_to_dt(unix_ts) -> datetime | None:
    if not unix_ts:
        return None
    try:
        return datetime.utcfromtimestamp(int(unix_ts)) + timedelta(hours=3)
    except Exception:
        return None


def _sender_label(msg: dict) -> str:
    sender = msg.get('from') or {}
    first = sender.get('first_name') or ''
    username = (sender.get('username') or '').lstrip('@')
    if first and username:
        return f'{first} (@{username})'
    return first or (f'@{username}' if username else 'unknown')


def _media_placeholder(msg: dict) -> str:
    kinds = []
    if msg.get('photo'):
        kinds.append('фото')
    if msg.get('document'):
        kinds.append('документ')
    if msg.get('video') or msg.get('video_note'):
        kinds.append('видео')
    if msg.get('voice') or msg.get('audio'):
        kinds.append('аудио')
    if msg.get('sticker'):
        kinds.append('стикер')
    if not kinds:
        return ''
    return '[' + ', '.join(kinds) + ']'


def ingest_ops_message(msg: dict, channel: str | None = None) -> dict:
    """Сохраняет сообщение в OpsChatMessage. Не бросает наружу."""
    if not ai_ops_enabled():
        return {'ok': True, 'status': 'disabled'}
    try:
        chat = msg.get('chat') or {}
        tg_chat_id = str(chat.get('id') or '')
        tg_message_id = int(msg.get('message_id') or 0)
        if not tg_chat_id or not tg_message_id:
            return {'ok': False, 'error': 'empty'}

        ch = channel or channel_for_chat_id(tg_chat_id)
        if not ch:
            return {'ok': True, 'status': 'skip_other_chat'}

        text = (msg.get('text') or msg.get('caption') or '').strip()
        if not text:
            text = _media_placeholder(msg)
        if not text:
            return {'ok': True, 'status': 'empty_skip'}

        sender = msg.get('from') or {}
        msg_dt = _tg_date_to_dt(msg.get('date')) or msk_now()
        label = _sender_label(msg)
        sender_tg_id = str(sender.get('id') or '') or None
        is_bot = bool(sender.get('is_bot'))

        existing = OpsChatMessage.query.filter_by(
            tg_chat_id=tg_chat_id, tg_message_id=tg_message_id,
        ).first()
        if existing is not None:
            if existing.raw_text != text[:8000]:
                existing.raw_text = text[:8000]
                existing.updated_at = msk_now()
                db.session.commit()
                return {'ok': True, 'status': 'updated', 'id': existing.id, 'channel': ch}
            return {'ok': True, 'status': 'dup', 'id': existing.id, 'channel': ch}

        row = OpsChatMessage(
            channel=ch,
            tg_chat_id=tg_chat_id,
            tg_message_id=tg_message_id,
            tg_date=msg_dt,
            raw_text=text[:8000],
            sender_name=label[:150],
            sender_tg_id=sender_tg_id,
            is_bot=is_bot,
        )
        db.session.add(row)
        db.session.commit()
        return {'ok': True, 'status': 'stored', 'id': row.id, 'channel': ch}
    except Exception as exc:
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            current_app.logger.exception('ai_ops.ingest_ops_message failed')
        except Exception:
            pass
        return {'ok': False, 'error': str(exc)}


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, 0, 0, 0)
    end = start + timedelta(days=1)
    return start, end


def _load_day_messages(day: date, channel: str | None = None) -> list[OpsChatMessage]:
    start, end = _day_bounds(day)
    q = OpsChatMessage.query.filter(
        OpsChatMessage.tg_date >= start,
        OpsChatMessage.tg_date < end,
    )
    if channel:
        q = q.filter(OpsChatMessage.channel == channel)
    return q.order_by(
        OpsChatMessage.channel.asc(),
        OpsChatMessage.tg_date.asc(),
        OpsChatMessage.id.asc(),
    ).all()


def _build_channel_transcripts(rows: list[OpsChatMessage]) -> dict[str, str]:
    by_ch: dict[str, list[str]] = {}
    for r in rows:
        t = r.tg_date.strftime('%H:%M') if r.tg_date else '??:??'
        who = r.sender_name or 'unknown'
        if r.is_bot:
            who = f'{who} [bot]'
        body = (r.raw_text or '').replace('\n', ' ').strip()
        by_ch.setdefault(r.channel or 'sales', []).append(f'{t} {who}: {body}')
    out = {}
    for ch, lines in by_ch.items():
        text = '\n'.join(lines)
        if len(text) > _MAX_TRANSCRIPT_CHARS // 4:
            text = text[-(_MAX_TRANSCRIPT_CHARS // 4):]
        out[ch] = text
    return out


_DIGEST_SYSTEM = (
    'Ты — помощник руководителя питомника. Делаешь дневную сводку по нескольким '
    'рабочим чатам и выделяешь кандидатов на действия в ERP. Не выдумывай.'
)

_DIGEST_PROMPT = (
    'Сделай оперативную сводку на русском за {day} по каналам ниже.\n\n'
    '1) Текстовая сводка: секции по каналам (Продажи / Кадры / Производство / ИТР), '
    'короткие «•» пункты, пустые секции пропускай. До ~2000 символов.\n'
    '2) Затем отдельной строкой ровно маркер <<<ACTIONS>>> и JSON-массив кандидатов:\n'
    '[{{"type":"payment_confirm|shipment_confirm|order_create|order_qty_change|price_change",'
    '"entities":{{"order_id":null,"client":null,"amount":null,"plant":null,"size":null,"qty":null,"price":null}},'
    '"quote":"...","confidence":0.0-1.0,"channel":"sales|hr|production|itr"}}]\n'
    'Если кандидатов нет — пустой массив [].\n'
    'Только факты из текста. Игнор болтовни про логины/стикеры.\n\n'
    'Сообщений: {message_count}\n\n'
    '{transcript_block}'
)


def _digest_provider_order() -> list[str]:
    mode = (os.environ.get('SALES_DIGEST_PROVIDER') or 'auto').strip().lower()
    has_gemini = bool((os.environ.get('GEMINI_API_KEY') or '').strip())
    has_groq = bool((os.environ.get('GROQ_API_KEY') or '').strip())
    if mode == 'gemini':
        order = ['gemini', 'groq']
    elif mode == 'groq':
        order = ['groq', 'gemini']
    else:
        order = ['gemini', 'groq'] if has_gemini else ['groq', 'gemini']
    out = []
    for name in order:
        if name == 'gemini' and has_gemini and name not in out:
            out.append(name)
        if name == 'groq' and has_groq and name not in out:
            out.append(name)
    return out


def _llm_raw(prompt: str) -> str:
    providers = _digest_provider_order()
    errors = []
    for name in providers:
        try:
            if name == 'gemini':
                from app.gemini_util import gemini_generate_text
                text = gemini_generate_text(
                    prompt=prompt,
                    system=_DIGEST_SYSTEM,
                    temperature=0.2,
                    max_output_tokens=3072,
                    timeout_sec=60.0,
                )
            else:
                from groq import Groq
                from app.groq_util import groq_model_text
                client = Groq(api_key=(os.environ.get('GROQ_API_KEY') or '').strip())
                resp = client.chat.completions.create(
                    model=groq_model_text(),
                    messages=[
                        {'role': 'system', 'content': _DIGEST_SYSTEM},
                        {'role': 'user', 'content': prompt},
                    ],
                    temperature=0.2,
                )
                text = (resp.choices[0].message.content or '').strip()
            if text:
                return text
            errors.append(f'{name}: empty')
        except Exception as exc:
            errors.append(f'{name}: {str(exc)[:160]}')
            try:
                current_app.logger.exception('ai_ops llm %s failed', name)
            except Exception:
                pass
    return ''


def _fallback_multi_summary(day: date, by_ch: dict[str, str], message_count: int) -> str:
    parts = [f'Сводка AI Ops за {day.isoformat()} ({message_count} сообщ.)', '[авто без LLM]']
    keys = (
        'заказ', 'отгруз', 'оплат', 'счёт', 'счет', 'выкоп', 'копк', 'кадр',
        'отпуск', 'больнич', 'производ', 'план', 'час', 'бригад',
    )
    for ch, transcript in by_ch.items():
        bullets = []
        for line in (transcript or '').splitlines():
            bl = line.lower()
            if any(k in bl for k in keys):
                body = line.split(':', 1)[-1].strip()[:180]
                if body:
                    bullets.append(f'• {body}')
            if len(bullets) >= 6:
                break
        if bullets:
            parts.append(f'\n{CHANNEL_LABELS.get(ch, ch)}:')
            parts.extend(bullets)
    if len(parts) <= 2:
        parts.append('\nЗа день нет явных операционных пунктов.')
    return '\n'.join(parts)


def _parse_llm_digest(raw: str) -> tuple[str, list[dict]]:
    text = (raw or '').strip()
    actions: list[dict] = []
    summary = text
    marker = '<<<ACTIONS>>>'
    if marker in text:
        summary, _, rest = text.partition(marker)
        summary = summary.strip()
        rest = rest.strip()
        m = re.search(r'\[[\s\S]*\]', rest)
        if m:
            try:
                parsed = json.loads(m.group(0))
                if isinstance(parsed, list):
                    actions = [a for a in parsed if isinstance(a, dict)]
            except Exception:
                actions = []
    else:
        m = re.search(r'\[[\s\S]*\]\s*$', text)
        if m:
            try:
                parsed = json.loads(m.group(0))
                if isinstance(parsed, list):
                    actions = [a for a in parsed if isinstance(a, dict)]
                    summary = text[:m.start()].strip()
            except Exception:
                pass
    return summary, actions


def _heuristic_actions(by_ch: dict[str, str]) -> list[dict]:
    """Простые кандидаты без LLM — оплаты/отгрузки по ключевым словам."""
    out = []
    order_re = re.compile(r'(?:заказ|#)\s*(\d{2,5})', re.I)
    amount_re = re.compile(r'(\d[\d\s]{2,})(?:\s*(?:р|руб|₽))?', re.I)
    for ch, transcript in by_ch.items():
        for line in (transcript or '').splitlines():
            bl = line.lower()
            quote = line.split(':', 1)[-1].strip()[:300]
            oid = None
            m = order_re.search(line)
            if m:
                try:
                    oid = int(m.group(1))
                except Exception:
                    oid = None
            amount = None
            am = amount_re.search(line)
            if am:
                try:
                    amount = float(am.group(1).replace(' ', '').replace('\xa0', ''))
                except Exception:
                    amount = None
            if any(k in bl for k in ('оплат', 'перевод', 'платёж', 'платеж', 'пп ')):
                out.append({
                    'type': 'payment_confirm',
                    'entities': {'order_id': oid, 'amount': amount, 'client': None},
                    'quote': quote,
                    'confidence': 0.45,
                    'channel': ch,
                })
            elif any(k in bl for k in ('отгруз', 'погруз', 'накладн')):
                out.append({
                    'type': 'shipment_confirm',
                    'entities': {'order_id': oid},
                    'quote': quote,
                    'confidence': 0.4,
                    'channel': ch,
                })
            elif any(k in bl for k in ('новый заказ', 'сделай заказ', 'оформи заказ')):
                out.append({
                    'type': 'order_create',
                    'entities': {'client': None},
                    'quote': quote,
                    'confidence': 0.35,
                    'channel': ch,
                })
            elif any(k in bl for k in ('количеств', 'шт вместо', 'qty', 'поменяй qty')):
                out.append({
                    'type': 'order_qty_change',
                    'entities': {'order_id': oid, 'qty': None},
                    'quote': quote,
                    'confidence': 0.35,
                    'channel': ch,
                })
            elif any(k in bl for k in ('цен', 'прайс', 'измени цен')):
                out.append({
                    'type': 'price_change',
                    'entities': {'plant': None, 'size': None, 'price': amount},
                    'quote': quote,
                    'confidence': 0.35,
                    'channel': ch,
                })
            if len(out) >= 20:
                return out
    return out


def _stable_entity_key(action_type: str, entities: dict) -> str:
    entities = entities or {}
    if action_type == 'payment_confirm':
        oid = entities.get('order_id')
        if oid:
            return f'order={oid}'
        client = (entities.get('client') or '').strip().lower()
        amount = entities.get('amount')
        return f'client={client or "?"}:amount={amount or "?"}'
    if action_type == 'shipment_confirm':
        oid = entities.get('order_id')
        return f'order={oid or "?"}'
    if action_type == 'order_create':
        client = (entities.get('client') or '').strip().lower()[:40]
        return f'client={client or "?"}:day={msk_today().isoformat()}'
    if action_type == 'order_qty_change':
        oid = entities.get('order_id')
        plant = (entities.get('plant') or '').strip().lower()[:30]
        return f'order={oid or "?"}:plant={plant or "?"}'
    if action_type == 'price_change':
        plant = (entities.get('plant') or '').strip().lower()[:30]
        size = (entities.get('size') or '').strip().lower()[:20]
        return f'plant={plant or "?"}:size={size or "?"}'
    return f'raw={hash(json.dumps(entities, sort_keys=True, default=str)) % 10**8}'


def _find_order(entities: dict) -> Order | None:
    oid = entities.get('order_id')
    if oid:
        try:
            o = Order.query.get(int(oid))
            if o and not getattr(o, 'is_deleted', False):
                return o
        except Exception:
            pass
    client_name = (entities.get('client') or '').strip()
    if client_name:
        c = Client.query.filter(Client.name.ilike(f'%{client_name}%')).first()
        if c:
            q = (
                Order.query.filter_by(client_id=c.id)
                .filter(Order.is_deleted.is_(False))
                .order_by(Order.id.desc())
            )
            return q.first()
    return None


def _order_paid_enough(order: Order, amount: float | None) -> bool:
    if not order:
        return False
    try:
        paid = float(order.paid_sum or 0)
        total = float(order.total_sum or 0)
    except Exception:
        paid = 0.0
        total = 0.0
    if total > 0 and paid + 0.5 >= total:
        return True
    if amount and amount > 0:
        # уже есть платёж близкой суммы за 14 дней
        since = msk_today() - timedelta(days=14)
        for p in (order.payments or []):
            try:
                if p.date and p.date < since:
                    continue
                if abs(float(p.amount or 0) - float(amount)) < 1.0:
                    return True
            except Exception:
                continue
    return False


def _shipment_exists(order: Order) -> bool:
    if not order:
        return False
    if (order.status or '') == 'shipped':
        return True
    return Document.query.filter_by(order_id=order.id, doc_type='shipment').first() is not None


def _similar_order_today(entities: dict) -> bool:
    client_name = (entities.get('client') or '').strip()
    if not client_name:
        return False
    c = Client.query.filter(Client.name.ilike(f'%{client_name}%')).first()
    if not c:
        return False
    start, end = _day_bounds(msk_today())
    exists = (
        Order.query.filter_by(client_id=c.id)
        .filter(Order.is_deleted.is_(False))
        .filter(Order.date >= start, Order.date < end)
        .first()
    )
    return exists is not None


def _qty_already_matches(order: Order, entities: dict) -> bool:
    if not order:
        return False
    want = entities.get('qty')
    plant_name = (entities.get('plant') or '').strip().lower()
    if want is None:
        return False
    try:
        want_i = int(want)
    except Exception:
        return False
    for it in (order.items or []):
        pname = (it.plant.name if it.plant else '').lower()
        if plant_name and plant_name not in pname:
            continue
        if int(it.quantity or 0) == want_i:
            return True
    return False


def _price_already_matches(entities: dict) -> bool:
    plant_name = (entities.get('plant') or '').strip()
    size_name = (entities.get('size') or '').strip()
    price = entities.get('price')
    if not plant_name or price is None:
        return False
    plant = Plant.query.filter(Plant.name.ilike(f'%{plant_name}%')).first()
    if not plant:
        return False
    size = None
    if size_name:
        size = Size.query.filter(Size.name.ilike(f'%{size_name}%')).first()
    q = PriceHistory.query.filter_by(plant_id=plant.id)
    if size:
        q = q.filter_by(size_id=size.id)
    row = q.order_by(PriceHistory.id.desc()).first()
    if not row:
        return False
    try:
        return abs(float(row.price or 0) - float(price)) < 0.5
    except Exception:
        return False


def _should_skip_action(action_type: str, entities: dict, order: Order | None) -> tuple[bool, str]:
    if action_type == 'payment_confirm':
        if order and _order_paid_enough(order, entities.get('amount')):
            return True, 'already_paid'
        return False, ''
    if action_type == 'shipment_confirm':
        if order and _shipment_exists(order):
            return True, 'already_shipped'
        return False, ''
    if action_type == 'order_create':
        if _similar_order_today(entities):
            return True, 'similar_order_today'
        return False, ''
    if action_type == 'order_qty_change':
        if order and _qty_already_matches(order, entities):
            return True, 'qty_matches'
        return False, ''
    if action_type == 'price_change':
        if _price_already_matches(entities):
            return True, 'price_matches'
        return False, ''
    return False, ''


def _title_for_action(action_type: str, entities: dict, order: Order | None) -> str:
    labels = {
        'payment_confirm': 'Провести оплату',
        'shipment_confirm': 'Подтвердить отгрузку',
        'order_create': 'Создать заказ',
        'order_qty_change': 'Изменить количество',
        'price_change': 'Изменить цену',
    }
    base = labels.get(action_type, action_type)
    if order:
        client = order.client.name if order.client else ''
        return f'{base}: заказ #{order.id}' + (f' · {client}' if client else '')
    if entities.get('client'):
        return f'{base}: {entities.get("client")}'
    return base


def reconcile_and_upsert_tasks(actions: list[dict], day: date | None = None) -> dict:
    """Создаёт/обновляет TgTask + OpsActionDraft по кандидатам. Без спама."""
    day = day or msk_today()
    vlad = resolve_vlad_user()
    created = updated = skipped = closed = 0
    now = msk_now()
    seen_keys: set[str] = set()

    open_tasks = TgTask.query.filter(
        TgTask.source == 'ai_ops',
        TgTask.status.in_(['new', 'snoozed']),
        TgTask.dedup_key.isnot(None),
    ).all()
    by_key = {t.dedup_key: t for t in open_tasks if t.dedup_key}

    for raw in actions or []:
        action_type = (raw.get('type') or '').strip()
        if action_type not in ACTION_TYPES:
            continue
        entities = raw.get('entities') or {}
        if not isinstance(entities, dict):
            entities = {}
        stable = _stable_entity_key(action_type, entities)
        dedup_key = f'ai_ops:{action_type}:{stable}'
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        order = _find_order(entities)
        skip, reason = _should_skip_action(action_type, entities, order)
        if skip:
            existing = by_key.get(dedup_key)
            if existing and existing.status in ('new', 'snoozed'):
                existing.status = 'done'
                existing.completed_at = now
                existing.last_seen_at = now
                closed += 1
            skipped += 1
            continue

        quote = (raw.get('quote') or '')[:2000]
        conf = raw.get('confidence')
        try:
            conf_d = Decimal(str(conf)) if conf is not None else None
        except (InvalidOperation, ValueError):
            conf_d = None
        channel = (raw.get('channel') or 'sales')[:20]
        title = _title_for_action(action_type, entities, order)[:200]
        details = quote or title
        payload = {
            'action_type': action_type,
            'entities': entities,
            'order_id': order.id if order else None,
            'client_id': order.client_id if order else None,
            'channel': channel,
            'quote': quote,
            'confidence': float(conf_d) if conf_d is not None else None,
            'digest_date': day.isoformat(),
        }

        task = by_key.get(dedup_key)
        if task is not None:
            task.last_seen_at = now
            task.details = details[:4000]
            task.title = title
            task.action_payload = json.dumps(payload, ensure_ascii=False)
            if task.status == 'snoozed' and task.deadline and task.deadline <= day:
                task.status = 'new'
            draft = OpsActionDraft.query.filter_by(task_id=task.id).order_by(
                OpsActionDraft.id.desc()
            ).first()
            if draft and draft.status == 'pending':
                draft.payload_json = json.dumps(payload, ensure_ascii=False)
                draft.quote = quote
                draft.order_id = order.id if order else draft.order_id
                draft.client_id = order.client_id if order else draft.client_id
                draft.confidence = conf_d
                draft.updated_at = now
            updated += 1
            continue

        task = TgTask(
            raw_text=quote or title,
            title=title,
            details=details[:4000],
            assignee_id=vlad.id if vlad else None,
            assignee_role='admin' if not vlad else None,
            deadline=day,
            action_type=action_type,
            action_payload=json.dumps(payload, ensure_ascii=False),
            status='new',
            sender_name='AI Ops',
            source='ai_ops',
            dedup_key=dedup_key,
            first_seen_at=now,
            last_seen_at=now,
            severity='warning' if action_type == 'payment_confirm' else 'info',
        )
        db.session.add(task)
        db.session.flush()
        draft = OpsActionDraft(
            task_id=task.id,
            action_type=action_type,
            status='pending',
            payload_json=json.dumps(payload, ensure_ascii=False),
            channel=channel,
            quote=quote,
            confidence=conf_d,
            order_id=order.id if order else None,
            client_id=order.client_id if order else None,
        )
        db.session.add(draft)
        by_key[dedup_key] = task
        created += 1

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception('ai_ops.reconcile commit failed')
        return {'ok': False, 'created': 0, 'updated': 0, 'skipped': 0, 'closed': 0}

    return {
        'ok': True,
        'created': created,
        'updated': updated,
        'skipped': skipped,
        'closed': closed,
    }


def _format_tg_html(day: date, summary: str, message_count: int) -> str:
    title = f'<b>AI Ops сводка</b> — {html.escape(day.isoformat())}'
    meta = f'<i>{message_count} сообщ. (sales/hr/prod/itr)</i>'
    clean = html.escape(summary).replace('\n', '\n')
    return f'{title}\n{meta}\n\n{clean}'


def _split_chunks(text: str, limit: int = _TG_MSG_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks = []
    rest = text
    while rest:
        if len(rest) <= limit:
            chunks.append(rest)
            break
        cut = rest.rfind('\n', 0, limit)
        if cut < limit // 3:
            cut = limit
        chunks.append(rest[:cut])
        rest = rest[cut:].lstrip('\n')
    return chunks


def run_ops_digest(
    *,
    day: date | None = None,
    force: bool = False,
    send: bool = True,
) -> dict:
    """Дневная мульти-сводка + extract действий → reconcile задач."""
    if not ai_ops_enabled():
        return {'ok': True, 'status': 'disabled'}

    day = day or msk_today()
    existing = OpsDigestLog.query.filter_by(digest_date=day).first()
    if existing and not force:
        return {
            'ok': True,
            'status': 'already_sent',
            'digest_date': day.isoformat(),
            'message_count': existing.message_count,
        }

    rows = _load_day_messages(day)
    by_ch = _build_channel_transcripts(rows)
    message_count = len(rows)

    if not rows:
        summary = f'За {day.isoformat()} в логе AI Ops пусто.'
        actions: list[dict] = []
    else:
        blocks = []
        for ch in ('sales', 'hr', 'production', 'itr'):
            if ch in by_ch and by_ch[ch].strip():
                blocks.append(
                    f'=== {CHANNEL_LABELS.get(ch, ch)} ===\n{by_ch[ch]}'
                )
        transcript_block = '\n\n'.join(blocks)
        prompt = _DIGEST_PROMPT.format(
            day=day.isoformat(),
            message_count=message_count,
            transcript_block=transcript_block,
        )
        raw = _llm_raw(prompt)
        if raw:
            summary, actions = _parse_llm_digest(raw)
        else:
            summary = _fallback_multi_summary(day, by_ch, message_count)
            actions = _heuristic_actions(by_ch)

        if not actions:
            actions = _heuristic_actions(by_ch)

    # В reconcile — контекст открытых задач уже внутри; вчерашний дайджест
    # можно подмешать в details при необходимости.
    yesterday = OpsDigestLog.query.filter_by(
        digest_date=day - timedelta(days=1)
    ).first()
    if yesterday and yesterday.actions_json:
        try:
            prev = json.loads(yesterday.actions_json) or []
            # лёгкий контекст: не создаём заново, только для логов
            _ = prev
        except Exception:
            pass

    reconcile = reconcile_and_upsert_tasks(actions, day=day)

    recipients = digest_recipient_ids()
    sent_ok, sent_err = [], []
    if send and recipients:
        from app.telegram import send_chat_message
        payload = _format_tg_html(day, summary, message_count)
        for chat_id in recipients:
            for i, chunk in enumerate(_split_chunks(payload)):
                prefix = f'(часть {i + 1})\n' if i else ''
                ok, err = send_chat_message(chat_id, prefix + chunk)
                if ok:
                    sent_ok.append(chat_id)
                else:
                    sent_err.append(f'{chat_id}: {err}')
                    break

    actions_json = json.dumps(actions, ensure_ascii=False)[:50000]
    if existing:
        existing.message_count = message_count
        existing.summary_text = summary[:20000]
        existing.actions_json = actions_json
        existing.recipient_ids = ','.join(recipients)
        existing.sent_at = msk_now()
    else:
        db.session.add(OpsDigestLog(
            digest_date=day,
            message_count=message_count,
            summary_text=summary[:20000],
            actions_json=actions_json,
            recipient_ids=','.join(recipients),
            sent_at=msk_now(),
        ))
    db.session.commit()

    return {
        'ok': not sent_err,
        'status': 'sent' if sent_ok and not sent_err else ('partial' if sent_ok else 'built'),
        'digest_date': day.isoformat(),
        'message_count': message_count,
        'actions': len(actions),
        'reconcile': reconcile,
        'recipients_ok': sorted(set(sent_ok)),
        'errors': sent_err,
    }


def draft_payload(draft: OpsActionDraft) -> dict:
    try:
        return json.loads(draft.payload_json or '{}') or {}
    except Exception:
        return {}


def get_draft_for_task(task_id: int) -> OpsActionDraft | None:
    return (
        OpsActionDraft.query.filter_by(task_id=task_id)
        .order_by(OpsActionDraft.id.desc())
        .first()
    )


def post_payment_draft(
    draft: OpsActionDraft,
    *,
    amount: float,
    payment_type: str,
    pay_date: date,
    comment: str,
    file_storage=None,
    user_id: int | None = None,
) -> dict:
    order = draft.order or (Order.query.get(draft.order_id) if draft.order_id else None)
    if not order:
        pl = draft_payload(draft)
        order = _find_order(pl.get('entities') or {})
    if not order:
        return {'ok': False, 'error': 'Заказ не найден'}

    if _order_paid_enough(order, amount):
        draft.status = 'skipped'
        if draft.task_id:
            t = TgTask.query.get(draft.task_id)
            if t and t.status in ('new', 'snoozed'):
                t.status = 'done'
                t.completed_at = msk_now()
                t.completed_by_id = user_id
        db.session.commit()
        return {'ok': True, 'status': 'skipped_already_paid', 'order_id': order.id}

    p = Payment(
        order_id=order.id,
        amount=Decimal(str(amount)),
        date=pay_date,
        payment_type=payment_type or 'cashless',
        comment=(comment or 'AI Ops')[:200],
    )
    db.session.add(p)
    db.session.flush()

    if file_storage and getattr(file_storage, 'filename', None):
        try:
            upload_root = current_app.config.get('UPLOAD_FOLDER') or os.path.join(
                current_app.root_path, 'uploads'
            )
            pay_dir = os.path.join(upload_root, 'payments')
            os.makedirs(pay_dir, exist_ok=True)
            fname = secure_filename(file_storage.filename)
            fname = f'pay_{p.id}_{fname}'
            fpath = os.path.join(pay_dir, fname)
            file_storage.save(fpath)
            p.file_path = fpath
        except Exception:
            current_app.logger.exception('ai_ops payment file save failed')

    draft.status = 'posted'
    draft.payment_id = p.id
    draft.order_id = order.id
    draft.posted_at = msk_now()
    draft.posted_by_id = user_id
    if draft.task_id:
        t = TgTask.query.get(draft.task_id)
        if t:
            t.status = 'done'
            t.completed_at = msk_now()
            t.completed_by_id = user_id

    db.session.commit()
    return {'ok': True, 'status': 'posted', 'payment_id': p.id, 'order_id': order.id}


def snooze_task(task: TgTask, until: date | None = None) -> None:
    until = until or (msk_today() + timedelta(days=1))
    task.status = 'snoozed'
    task.deadline = until
    task.updated_at = msk_now()
    db.session.commit()


# ---------- HTTP routes ----------

@bp.route('/calendar')
@login_required
@pilot_required
def ops_calendar():
    """Оболочка календаря: v1 — дубль плана выкопки."""
    from app.digging import build_planning_weeks

    today = msk_today()
    center = today - timedelta(days=today.weekday())
    weeks_before = int(request.args.get('wb', 2) or 2)
    weeks_after = int(request.args.get('wa', 4) or 4)
    weeks_data = build_planning_weeks(
        center, weeks_before=weeks_before, weeks_after=weeks_after,
    )
    open_ops = TgTask.query.filter(
        TgTask.source == 'ai_ops',
        TgTask.status.in_(['new', 'snoozed']),
    ).order_by(TgTask.deadline.asc(), TgTask.id.desc()).limit(40).all()
    return render_template(
        'ops/calendar.html',
        weeks_data=weeks_data,
        center_monday=center,
        today=today,
        open_ops=open_ops,
        weeks_before=weeks_before,
        weeks_after=weeks_after,
    )


@bp.route('/api/draft/<int:draft_id>')
@login_required
@pilot_required
def api_draft_get(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    pl = draft_payload(draft)
    return jsonify({
        'id': draft.id,
        'action_type': draft.action_type,
        'status': draft.status,
        'order_id': draft.order_id,
        'client_id': draft.client_id,
        'payload': pl,
        'quote': draft.quote,
        'file_path': bool(draft.file_path),
    })


@bp.route('/api/task/<int:task_id>/snooze', methods=['POST'])
@login_required
@pilot_required
def api_task_snooze(task_id):
    task = TgTask.query.get_or_404(task_id)
    if task.source != 'ai_ops':
        return 'forbidden', 403
    snooze_task(task)
    return """
    <div class="card border-0 shadow-sm mb-3 bg-secondary bg-opacity-10 text-center py-3">
        <h6 class="text-secondary m-0"><i class="fas fa-moon me-1"></i> На завтра</h6>
    </div>
    """


@bp.route('/api/draft/<int:draft_id>/post_payment', methods=['POST'])
@login_required
@pilot_required
def api_post_payment(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    if draft.action_type != 'payment_confirm':
        return jsonify({'ok': False, 'error': 'wrong type'}), 400
    if draft.status != 'pending':
        return jsonify({'ok': False, 'error': 'already processed'}), 400

    try:
        amount = float(str(request.form.get('amount') or '0').replace(',', '.').replace(' ', ''))
    except Exception:
        amount = 0.0
    if amount <= 0:
        flash('Укажите сумму оплаты', 'danger')
        return redirect(request.referrer or url_for('main.index'))

    pay_date_raw = request.form.get('date') or msk_today().isoformat()
    try:
        pay_date = datetime.strptime(pay_date_raw, '%Y-%m-%d').date()
    except Exception:
        pay_date = msk_today()

    result = post_payment_draft(
        draft,
        amount=amount,
        payment_type=request.form.get('payment_type') or 'cashless',
        pay_date=pay_date,
        comment=request.form.get('comment') or '',
        file_storage=request.files.get('file'),
        user_id=current_user.id,
    )
    if request.headers.get('HX-Request'):
        if result.get('ok'):
            return """
            <div class="card border-0 shadow-sm mb-3 bg-success bg-opacity-10 text-center py-3">
                <h6 class="text-success m-0"><i class="fas fa-check-double"></i> Оплата проведена</h6>
            </div>
            """
        return f'<div class="alert alert-danger">{result.get("error")}</div>', 400
    if result.get('ok'):
        flash('Оплата проведена', 'success')
    else:
        flash(result.get('error') or 'Ошибка', 'danger')
    return redirect(request.referrer or url_for('main.index'))


@bp.route('/api/draft/<int:draft_id>/ack', methods=['POST'])
@login_required
@pilot_required
def api_draft_ack(draft_id):
    """Фаза 2: отметить черновик обработанным вручную (отгрузка/заказ/qty/цена)."""
    draft = OpsActionDraft.query.get_or_404(draft_id)
    draft.status = 'posted'
    draft.posted_at = msk_now()
    draft.posted_by_id = current_user.id
    if draft.task_id:
        t = TgTask.query.get(draft.task_id)
        if t and t.status in ('new', 'snoozed'):
            t.status = 'done'
            t.completed_at = msk_now()
            t.completed_by_id = current_user.id
    db.session.commit()
    return """
    <div class="card border-0 shadow-sm mb-3 bg-primary bg-opacity-10 text-center py-3">
        <h6 class="text-primary m-0"><i class="fas fa-check"></i> Готово</h6>
    </div>
    """


@bp.route('/api/run_digest', methods=['POST'])
@login_required
@pilot_required
def api_run_digest():
    force = request.form.get('force') in ('1', 'true', 'yes')
    result = run_ops_digest(force=force, send=True)
    return jsonify(result)
