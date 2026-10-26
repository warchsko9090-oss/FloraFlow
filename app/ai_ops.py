"""AI Ops — пилот для Vlad: мульти-чат, сводка, задачи и черновики документов.

Включается env AI_OPS_ENABLED (по умолчанию on). UI/API только для is_ai_ops_pilot.
"""
from __future__ import annotations

import hashlib
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
    'purchase_request',
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
        # Голос / аудио / видео / кружок — расшифровка речи (Groq→Gemini)
        if msg.get('voice') or msg.get('audio') or msg.get('video') or msg.get('video_note'):
            try:
                from app.telegram import message_text_with_voice
                text = message_text_with_voice(msg, text) or text
            except Exception:
                current_app.logger.exception('ai_ops speech STT failed')
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
            # Не затираем уже расшифрованную речь плейсхолдером при повторном апдейте
            new_text = text[:8000]
            old = existing.raw_text or ''
            _speech_prefixes = ('[голос]', '[видео]', '[кружок]')
            _placeholders = (
                '[аудио]', '[видео]', '[аудио, стикер]', '[стикер, аудио]',
                '[видео, стикер]',
            )
            if old.startswith(_speech_prefixes) and new_text in _placeholders:
                return {'ok': True, 'status': 'dup', 'id': existing.id, 'channel': ch}
            if existing.raw_text != new_text:
                existing.raw_text = new_text
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
    '   Если одна и та же тема (заказ / оплата / отгрузка) всплыла в нескольких чатах — '
    'в сводке один пункт с пометкой каналов (напр. «ИТР+Производство»), без дублей.\n'
    '2) Затем отдельной строкой ровно маркер <<<ACTIONS>>> и JSON-массив кандидатов:\n'
    '[{{"type":"payment_confirm|shipment_confirm|order_create|order_qty_change|price_change|purchase_request",'
    '"entities":{{"order_id":null,"client":null,"amount":null,"plant":null,"size":null,"qty":null,"price":null,'
    '"items":[{{"text":"название","qty":"1"}}],"title":null}},'
    '"quote":"...","confidence":0.0-1.0,"channel":"sales|hr|production|itr",'
    '"channels":["itr","production"]}}]\n'
    'КРИТИЧНО про ACTIONS:\n'
    '- Одна крупица факта = один элемент массива, даже если её повторили/дополнили в других чатах.\n'
    '- В quote склей цитаты из всех каналов: «[ИТР] …; [Производство] …».\n'
    '- В channels перечисли все затронутые каналы; channel = основной (где впервые или полнее).\n'
    '- Речь из голосовых/видео/кружков помечена [голос]/[видео]/[кружок] — '
    'это полноценный текст, учитывай.\n'
    '- purchase_request: список закупок/заявка «что купить» (часто производство). '
    'В entities.items — нормализованные позиции (text, qty опц.), без выдуманных строк. '
    'Один список = один action, даже если нумерация 1,2,3…\n'
    '- Если кандидатов нет — пустой массив [].\n'
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
        # Нумерованный список закупок (часто производство)
        purchase = _heuristic_purchase_list(ch, transcript)
        if purchase:
            out.append(purchase)
    return out


def _normalize_purchase_items(raw_items) -> list[dict]:
    """items → [{id, text, qty, done}] без пустых."""
    out: list[dict] = []
    seen: set[str] = set()
    for i, it in enumerate(raw_items or []):
        if isinstance(it, str):
            text = it.strip()
            qty = None
            done = False
            iid = None
        elif isinstance(it, dict):
            text = (it.get('text') or it.get('name') or it.get('title') or '').strip()
            qty = it.get('qty') if it.get('qty') not in (None, '') else it.get('quantity')
            done = bool(it.get('done'))
            iid = it.get('id')
        else:
            continue
        if not text:
            continue
        key = re.sub(r'\s+', ' ', text.lower())[:120]
        if key in seen:
            continue
        seen.add(key)
        out.append({
            'id': str(iid or f'i{len(out) + 1}'),
            'text': text[:300],
            'qty': (str(qty)[:40] if qty not in (None, '') else None),
            'done': done,
        })
        if len(out) >= 40:
            break
    return out


def _merge_purchase_items(old_items, new_items) -> list[dict]:
    """Склеивает позиции, сохраняя галочки done."""
    old = _normalize_purchase_items(old_items)
    new = _normalize_purchase_items(new_items)
    by_key: dict[str, dict] = {}
    for it in old:
        by_key[re.sub(r'\s+', ' ', it['text'].lower())[:120]] = dict(it)
    for it in new:
        k = re.sub(r'\s+', ' ', it['text'].lower())[:120]
        if k in by_key:
            prev = by_key[k]
            if it.get('qty') and not prev.get('qty'):
                prev['qty'] = it['qty']
            # галочки админа не сбрасываем; True с любой стороны сохраняем
            prev['done'] = bool(prev.get('done') or it.get('done'))
        else:
            by_key[k] = dict(it)
            by_key[k]['done'] = bool(it.get('done'))
    merged = list(by_key.values())
    for i, it in enumerate(merged):
        it['id'] = str(it.get('id') or f'i{i + 1}')
    return merged[:40]


def _heuristic_purchase_list(channel: str, transcript: str) -> dict | None:
    """Ищет нумерованный список (1, … / 1. …) — заявка на закупку."""
    lines = (transcript or '').splitlines()
    item_re = re.compile(
        r'^\s*(?:\d{1,2})\s*[.,):\-–—]\s*(.+)$'
    )
    # Также «1, текст» как у Бехруза
    item_re2 = re.compile(r'^\s*(\d{1,2})\s*,\s*(.+)$')
    collected: list[str] = []
    start_idx = None
    for idx, line in enumerate(lines):
        body = line.split(':', 1)[-1].strip()
        m = item_re.match(body) or item_re2.match(body)
        if m:
            text = (m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1)).strip()
            text = re.sub(r'\s+', ' ', text)
            if len(text) >= 3:
                if start_idx is None:
                    start_idx = idx
                collected.append(text[:300])
            continue
        # разрыв списка
        if collected and len(collected) >= 2:
            break
        if collected and not m:
            collected = []
            start_idx = None

    if len(collected) < 2:
        return None
    # Контекст: закуп/нужно рядом или канал production
    window = '\n'.join(lines[max(0, (start_idx or 0) - 4):(start_idx or 0) + 1]).lower()
    buyish = any(
        k in window for k in ('закуп', 'купи', 'нужн', 'надо ', 'список', 'прин')
    )
    if channel != 'production' and not buyish:
        return None
    items = _normalize_purchase_items([{'text': t} for t in collected])
    return {
        'type': 'purchase_request',
        'entities': {
            'items': items,
            'title': 'Заявка на закупку',
            'day': msk_today().isoformat(),
        },
        'quote': '\n'.join(f'{i + 1}. {t}' for i, t in enumerate(collected))[:1500],
        'confidence': 0.55 if buyish else 0.4,
        'channel': channel or 'production',
    }


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
    if action_type == 'purchase_request':
        # Одна открытая заявка на день (склейка дописок в тот же чеклист)
        day = (entities.get('day') or msk_today().isoformat())[:10]
        return f'day={day}'
    return f'raw={hash(json.dumps(entities, sort_keys=True, default=str)) % 10**8}'


def _channel_label(ch: str | None) -> str:
    return CHANNEL_LABELS.get((ch or '').strip(), ch or '?')


def _merge_quote(prev: str, new: str, channel: str | None = None) -> str:
    """Склеивает цитаты из разных чатов без дублей."""
    prev = (prev or '').strip()
    new = (new or '').strip()
    if not new:
        return prev[:2000]
    tagged = new
    if channel and not new.startswith('['):
        tagged = f'[{_channel_label(channel)}] {new}'
    if not prev:
        return tagged[:2000]
    if new in prev or tagged in prev:
        return prev[:2000]
    return f'{prev}\n{tagged}'.strip()[:2000]


def _normalize_and_merge_actions(actions: list[dict]) -> list[dict]:
    """Одна крупица факта = один action: резолвим заказ и склеиваем чаты.

    ИТР «по заказу #12» + Производство «дополнили» → один payment/shipment.
    """
    buckets: dict[str, dict] = {}
    order_cache: dict[str, Order | None] = {}

    for raw in actions or []:
        if not isinstance(raw, dict):
            continue
        action_type = (raw.get('type') or '').strip()
        if action_type not in ACTION_TYPES:
            continue
        entities = raw.get('entities') or {}
        if not isinstance(entities, dict):
            entities = {}
        else:
            entities = dict(entities)

        if action_type == 'purchase_request':
            entities['items'] = _normalize_purchase_items(entities.get('items'))
            entities['day'] = (entities.get('day') or msk_today().isoformat())[:10]
            if not entities.get('title'):
                entities['title'] = 'Заявка на закупку'
            if not entities['items']:
                continue

        # Резолв заказа до ключа — иначе дубли «клиент+сумма» и «order=#» не схлопнутся
        cache_key = json.dumps(
            {'oid': entities.get('order_id'), 'c': entities.get('client')},
            sort_keys=True, default=str,
        )
        if cache_key not in order_cache:
            order_cache[cache_key] = _find_order(entities)
        order = order_cache[cache_key]
        if order:
            entities['order_id'] = order.id
            if order.client and not entities.get('client'):
                entities['client'] = order.client.name

        stable = _stable_entity_key(action_type, entities)
        key = f'{action_type}:{stable}'
        channel = (raw.get('channel') or '')[:20]
        channels_in = raw.get('channels')
        ch_list: list[str] = []
        if isinstance(channels_in, list):
            ch_list = [str(c)[:20] for c in channels_in if c]
        if channel and channel not in ch_list:
            ch_list.append(channel)

        quote = (raw.get('quote') or '')[:2000]
        conf = raw.get('confidence')
        try:
            conf_f = float(conf) if conf is not None else 0.0
        except (TypeError, ValueError):
            conf_f = 0.0

        if key in buckets:
            prev = buckets[key]
            prev_ch = list(prev.get('channels') or [])
            # При появлении второго канала — пометим исходную цитату источником
            prev_quote = prev.get('quote') or ''
            if (
                len(prev_ch) == 1
                and quote
                and prev_quote
                and not prev_quote.startswith('[')
            ):
                prev_quote = f'[{_channel_label(prev_ch[0])}] {prev_quote}'
            prev['quote'] = _merge_quote(prev_quote, quote, channel)
            for c in ch_list:
                if c and c not in prev_ch:
                    prev_ch.append(c)
            prev['channels'] = prev_ch
            if conf_f > float(prev.get('confidence') or 0):
                prev['confidence'] = conf_f
            # itr приоритетнее как источник «главного» канала
            if channel and (
                not prev.get('channel')
                or (channel == 'itr' and prev.get('channel') != 'itr')
            ):
                prev['channel'] = channel
            pe = prev.get('entities') or {}
            if action_type == 'purchase_request':
                pe['items'] = _merge_purchase_items(pe.get('items'), entities.get('items'))
                pe['day'] = entities.get('day') or pe.get('day')
                if entities.get('title') and not pe.get('title'):
                    pe['title'] = entities['title']
            else:
                for k, v in entities.items():
                    if v is not None and v != '' and (pe.get(k) is None or pe.get(k) == ''):
                        pe[k] = v
            prev['entities'] = pe
            continue

        buckets[key] = {
            'type': action_type,
            'entities': entities,
            'quote': quote,
            'confidence': conf_f,
            'channel': channel or (ch_list[0] if ch_list else 'sales'),
            'channels': ch_list,
        }

    return list(buckets.values())


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


def _shipment_fully_done(order: Order) -> bool:
    """Skip только если заказ уже полностью отгружен."""
    if not order:
        return False
    if (order.status or '') == 'shipped':
        return True
    items = list(order.items or [])
    if not items:
        return False
    return all(int(i.shipped_quantity or 0) >= int(i.quantity or 0) for i in items)


def _resolve_plant(name: str | None) -> Plant | None:
    name = (name or '').strip()
    if not name:
        return None
    return Plant.query.filter(Plant.name.ilike(f'%{name}%')).order_by(Plant.id.asc()).first()


def _resolve_size(name: str | None) -> Size | None:
    name = (name or '').strip()
    if not name:
        return None
    return Size.query.filter(Size.name.ilike(f'%{name}%')).order_by(Size.id.asc()).first()


def _resolve_client(name: str | None) -> Client | None:
    name = (name or '').strip()
    if not name:
        return None
    return Client.query.filter(Client.name.ilike(f'%{name}%')).order_by(Client.id.asc()).first()


def _unallocated_field_id() -> int | None:
    from app.models import Field
    f = Field.query.filter(Field.name.ilike('%не распредел%')).first()
    if f:
        return f.id
    f = Field.query.order_by(Field.id.asc()).first()
    return f.id if f else None


def _mark_draft_done(draft: OpsActionDraft, *, user_id: int | None, extra: dict | None = None) -> None:
    draft.status = 'posted'
    draft.posted_at = msk_now()
    draft.posted_by_id = user_id
    if extra:
        for k, v in extra.items():
            if hasattr(draft, k):
                setattr(draft, k, v)
    if draft.task_id:
        t = TgTask.query.get(draft.task_id)
        if t and t.status in ('new', 'snoozed'):
            t.status = 'done'
            t.completed_at = msk_now()
            t.completed_by_id = user_id


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
        if order and _shipment_fully_done(order):
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
    if action_type == 'purchase_request':
        items = _normalize_purchase_items(entities.get('items'))
        if not items:
            return True, 'empty_purchase'
        return False, ''
    return False, ''


def _title_for_action(action_type: str, entities: dict, order: Order | None) -> str:
    labels = {
        'payment_confirm': 'Провести оплату',
        'shipment_confirm': 'Подтвердить отгрузку',
        'order_create': 'Создать заказ',
        'order_qty_change': 'Изменить количество',
        'price_change': 'Изменить цену',
        'purchase_request': 'Заявка на закупку',
    }
    base = labels.get(action_type, action_type)
    if action_type == 'purchase_request':
        items = _normalize_purchase_items(entities.get('items'))
        n = len(items)
        done = sum(1 for x in items if x.get('done'))
        title = entities.get('title') or base
        return f'{title}: {done}/{n}' if n else str(title)
    if order:
        client = order.client.name if order.client else ''
        return f'{base}: заказ #{order.id}' + (f' · {client}' if client else '')
    if entities.get('client'):
        return f'{base}: {entities.get("client")}'
    return base


def reconcile_and_upsert_tasks(actions: list[dict], day: date | None = None) -> dict:
    """Создаёт/обновляет TgTask + OpsActionDraft по кандидатам. Без спама.

    Перед записью склеивает кросс-канальные дубли в один action.
    """
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

    for raw in _normalize_and_merge_actions(actions):
        action_type = (raw.get('type') or '').strip()
        if action_type not in ACTION_TYPES:
            continue
        entities = raw.get('entities') or {}
        if not isinstance(entities, dict):
            entities = {}
        entities = dict(entities)
        if action_type == 'purchase_request':
            entities['items'] = _normalize_purchase_items(entities.get('items'))
            entities['day'] = (entities.get('day') or day.isoformat())[:10]
            if not entities.get('title'):
                entities['title'] = 'Заявка на закупку'

        order = _find_order(entities)
        if order:
            entities['order_id'] = order.id

        stable = _stable_entity_key(action_type, entities)
        dedup_key = f'ai_ops:{action_type}:{stable}'
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

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
        channels = raw.get('channels') if isinstance(raw.get('channels'), list) else []
        channels = [str(c)[:20] for c in channels if c]
        if channel and channel not in channels:
            channels.append(channel)
        title = _title_for_action(action_type, entities, order)[:200]
        if len(channels) > 1:
            ch_hint = '+'.join(_channel_label(c) for c in channels[:4])
            if ch_hint and ch_hint not in title:
                title = f'{title} · {ch_hint}'[:200]
        details = quote or title
        if action_type == 'purchase_request':
            details = '\n'.join(
                f'{"✓" if it.get("done") else "○"} {it.get("text")}'
                + (f' ({it["qty"]})' if it.get('qty') else '')
                for it in (entities.get('items') or [])
            ) or details
        payload = {
            'action_type': action_type,
            'entities': entities,
            'items': entities.get('items') if action_type == 'purchase_request' else None,
            'order_id': order.id if order else None,
            'client_id': order.client_id if order else None,
            'channel': channel,
            'channels': channels,
            'quote': quote,
            'confidence': float(conf_d) if conf_d is not None else None,
            'digest_date': day.isoformat(),
        }

        task = by_key.get(dedup_key)
        if task is not None:
            task.last_seen_at = now
            draft = OpsActionDraft.query.filter_by(task_id=task.id).order_by(
                OpsActionDraft.id.desc()
            ).first()
            if action_type == 'purchase_request' and draft and draft.status == 'pending':
                old_pl = draft_payload(draft)
                old_items = (
                    old_pl.get('items')
                    or (old_pl.get('entities') or {}).get('items')
                    or []
                )
                entities['items'] = _merge_purchase_items(old_items, entities.get('items'))
                payload['items'] = entities['items']
                payload['entities'] = entities
                title = _title_for_action(action_type, entities, order)[:200]
                details = '\n'.join(
                    f'{"✓" if it.get("done") else "○"} {it.get("text")}'
                    + (f' ({it["qty"]})' if it.get('qty') else '')
                    for it in entities['items']
                ) or details
            merged_details = (
                details if action_type == 'purchase_request'
                else _merge_quote(task.details or '', quote, channel)
            )
            task.details = (merged_details or details)[:4000]
            task.title = title
            task.action_payload = json.dumps(payload, ensure_ascii=False)
            if task.status == 'snoozed' and task.deadline and task.deadline <= day:
                task.status = 'new'
            if draft and draft.status == 'pending':
                draft.payload_json = json.dumps(payload, ensure_ascii=False)
                draft.quote = _merge_quote(draft.quote or '', quote, channel)
                draft.order_id = order.id if order else draft.order_id
                draft.client_id = (
                    order.client_id if order else draft.client_id
                )
                draft.confidence = conf_d
                draft.updated_at = now
                if channels:
                    # channel колонка — «главный»; полный список в payload
                    draft.channel = channel or draft.channel
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
            severity=(
                'warning' if action_type in ('payment_confirm', 'purchase_request')
                else 'info'
            ),
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
    order_id: int | None = None,
) -> dict:
    order = None
    if order_id:
        order = Order.query.get(int(order_id))
        if order and getattr(order, 'is_deleted', False):
            order = None
    if not order:
        order = draft.order or (Order.query.get(draft.order_id) if draft.order_id else None)
    if not order:
        pl = draft_payload(draft)
        order = _find_order(pl.get('entities') or {})
    if not order:
        return {'ok': False, 'error': 'Выберите заказ'}
    # Зафиксируем выбор в черновике (на случай повторного открытия)
    draft.order_id = order.id
    if order.client_id:
        draft.client_id = order.client_id

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


def _draft_order(draft: OpsActionDraft) -> Order | None:
    order = draft.order or (Order.query.get(draft.order_id) if draft.order_id else None)
    if order:
        return order
    pl = draft_payload(draft)
    return _find_order(pl.get('entities') or {})


def post_shipment_draft(draft: OpsActionDraft, *, user_id: int | None = None) -> dict:
    """Отгрузить весь остаток по заказу (как order_ship по каждой позиции)."""
    order = _draft_order(draft)
    if not order:
        return {'ok': False, 'error': 'Заказ не найден'}
    if _shipment_fully_done(order):
        draft.status = 'skipped'
        if draft.task_id:
            t = TgTask.query.get(draft.task_id)
            if t and t.status in ('new', 'snoozed'):
                t.status = 'done'
                t.completed_at = msk_now()
                t.completed_by_id = user_id
        db.session.commit()
        return {'ok': True, 'status': 'skipped_already_shipped', 'order_id': order.id}

    from app.utils import get_or_create_stock
    from app.digging import clear_plan_after_ship

    shipped_rows = 0
    doc = Document(
        doc_type='shipment',
        user_id=user_id,
        order_id=order.id,
        comment=f'AI Ops отгрузка {order.id}',
        date=msk_now(),
    )
    db.session.add(doc)
    db.session.flush()

    for item in (order.items or []):
        left = int(item.quantity or 0) - int(item.shipped_quantity or 0)
        if left <= 0:
            continue
        db.session.add(DocumentRow(
            document_id=doc.id,
            plant_id=item.plant_id,
            size_id=item.size_id,
            field_from_id=item.field_id,
            year=item.year,
            quantity=left,
        ))
        from app.utils import field_skips_stock_check
        if not field_skips_stock_check(item.field_id):
            stock = get_or_create_stock(item.plant_id, item.size_id, item.field_id, item.year)
            stock.quantity = int(stock.quantity or 0) - left
        item.shipped_quantity = int(item.shipped_quantity or 0) + left
        shipped_rows += 1

    if shipped_rows == 0:
        db.session.rollback()
        return {'ok': False, 'error': 'Нечего отгружать'}

    if not order.project_id:
        try:
            from app.finance import resolve_project_id_for_yard_fields
            linked = resolve_project_id_for_yard_fields(
                [it.field_id for it in (order.items or []) if it.field_id]
            )
            if linked:
                order.project_id = linked
        except Exception:
            pass

    if (order.status or '') == 'reserved':
        order.status = 'in_progress'
    if all(int(i.shipped_quantity or 0) >= int(i.quantity or 0) for i in (order.items or [])):
        order.status = 'shipped'

    try:
        clear_plan_after_ship(order)
    except Exception:
        current_app.logger.exception('ai_ops clear_plan_after_ship')

    _mark_draft_done(draft, user_id=user_id, extra={
        'order_id': order.id,
        'document_id': doc.id,
    })
    db.session.commit()
    return {
        'ok': True,
        'status': 'posted',
        'order_id': order.id,
        'document_id': doc.id,
        'lines': shipped_rows,
    }


def post_order_create_draft(
    draft: OpsActionDraft,
    *,
    client_id: int,
    plant_id: int | None = None,
    size_id: int | None = None,
    qty: int | None = None,
    price: float | None = None,
    field_id: int | None = None,
    year: int | None = None,
    user_id: int | None = None,
) -> dict:
    client = Client.query.get(client_id)
    if not client:
        return {'ok': False, 'error': 'Клиент не найден'}

    entities = (draft_payload(draft).get('entities') or {})
    if _similar_order_today({'client': client.name}):
        draft.status = 'skipped'
        if draft.task_id:
            t = TgTask.query.get(draft.task_id)
            if t and t.status in ('new', 'snoozed'):
                t.status = 'done'
                t.completed_at = msk_now()
                t.completed_by_id = user_id
        db.session.commit()
        return {'ok': True, 'status': 'skipped_similar_today'}

    from app.models import OrderItem

    o = Order(
        client_id=client.id,
        date=msk_now(),
        status='reserved',
        created_by_user_id=user_id,
    )
    db.session.add(o)
    db.session.flush()

    if plant_id and size_id and qty and int(qty) > 0:
        fid = field_id or _unallocated_field_id()
        yr = year or msk_today().year
        try:
            from app.utils import get_actual_price
            from app.shop_prices import order_default_price
            wholesale = float(get_actual_price(plant_id, size_id, fid) or 0)
            p = float(price) if price is not None else float(
                order_default_price(plant_id, size_id, wholesale) or wholesale or 0
            )
        except Exception:
            p = float(price or 0)
        db.session.add(OrderItem(
            order_id=o.id,
            plant_id=int(plant_id),
            size_id=int(size_id),
            field_id=fid,
            year=int(yr),
            quantity=int(qty),
            price=p,
        ))
        draft.plant_id = int(plant_id)
        draft.size_id = int(size_id)

    # сохранить client из формы в payload
    entities['client'] = client.name
    entities['client_id'] = client.id
    draft.payload_json = json.dumps({
        **draft_payload(draft),
        'entities': entities,
    }, ensure_ascii=False)

    _mark_draft_done(draft, user_id=user_id, extra={
        'order_id': o.id,
        'client_id': client.id,
    })
    db.session.commit()
    return {'ok': True, 'status': 'posted', 'order_id': o.id, 'client_id': client.id}


def post_qty_change_draft(
    draft: OpsActionDraft,
    *,
    qty: int,
    item_id: int | None = None,
    user_id: int | None = None,
) -> dict:
    order = _draft_order(draft)
    if not order:
        return {'ok': False, 'error': 'Заказ не найден'}
    if qty < 0:
        return {'ok': False, 'error': 'Количество не может быть < 0'}

    entities = draft_payload(draft).get('entities') or {}
    item = None
    if item_id:
        from app.models import OrderItem
        item = OrderItem.query.filter_by(id=item_id, order_id=order.id).first()
    if item is None:
        plant_name = (entities.get('plant') or '').strip().lower()
        for it in (order.items or []):
            pname = (it.plant.name if it.plant else '').lower()
            if plant_name and plant_name not in pname:
                continue
            item = it
            break
        if item is None and order.items:
            item = order.items[0]
    if item is None:
        return {'ok': False, 'error': 'Позиция заказа не найдена'}

    if int(item.quantity or 0) == int(qty):
        draft.status = 'skipped'
        if draft.task_id:
            t = TgTask.query.get(draft.task_id)
            if t and t.status in ('new', 'snoozed'):
                t.status = 'done'
                t.completed_at = msk_now()
                t.completed_by_id = user_id
        db.session.commit()
        return {'ok': True, 'status': 'skipped_qty_matches', 'order_id': order.id}

    shipped = int(item.shipped_quantity or 0)
    if qty < shipped:
        return {'ok': False, 'error': f'Нельзя меньше уже отгруженного ({shipped})'}

    old_qty = int(item.quantity or 0)
    item.quantity = int(qty)
    try:
        from app.models import OrderItemHistory
        db.session.add(OrderItemHistory(
            order_id=order.id,
            order_item_id=item.id,
            action_type='qty_change',
            before_quantity=old_qty,
            after_quantity=int(qty),
            delta_quantity=int(qty) - old_qty,
            changed_by_user_id=user_id,
            created_at=msk_now(),
        ))
    except Exception:
        current_app.logger.exception('ai_ops qty history')

    _mark_draft_done(draft, user_id=user_id, extra={'order_id': order.id})
    db.session.commit()
    return {
        'ok': True,
        'status': 'posted',
        'order_id': order.id,
        'item_id': item.id,
        'old_qty': old_qty,
        'new_qty': int(qty),
    }


def post_price_change_draft(
    draft: OpsActionDraft,
    *,
    plant_id: int,
    size_id: int,
    price: float,
    user_id: int | None = None,
) -> dict:
    if price < 0:
        return {'ok': False, 'error': 'Цена не может быть < 0'}
    from app.stock import apply_price_for_plant_size

    # apply_price_for_plant_size уже коммитит — сначала проверим skip
    entities = {'plant': None, 'size': None, 'price': price}
    plant = Plant.query.get(plant_id)
    size = Size.query.get(size_id)
    if plant:
        entities['plant'] = plant.name
    if size:
        entities['size'] = size.name
    if _price_already_matches(entities):
        draft.status = 'skipped'
        if draft.task_id:
            t = TgTask.query.get(draft.task_id)
            if t and t.status in ('new', 'snoozed'):
                t.status = 'done'
                t.completed_at = msk_now()
                t.completed_by_id = user_id
        db.session.commit()
        return {'ok': True, 'status': 'skipped_price_matches'}

    draft_id = draft.id
    result = apply_price_for_plant_size(plant_id, size_id, price, source='ai_ops')
    if not result.get('ok'):
        return {'ok': False, 'error': result.get('message') or 'Не удалось сменить цену'}

    # apply_price коммитит сам — перечитаем draft и закроем
    draft = OpsActionDraft.query.get(draft_id)
    if draft:
        draft.plant_id = plant_id
        draft.size_id = size_id
        _mark_draft_done(draft, user_id=user_id)
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            current_app.logger.exception('ai_ops price draft commit')
            return {'ok': False, 'error': 'Цена применена, но черновик не закрылся'}
    return {
        'ok': True,
        'status': 'posted',
        'plant_id': plant_id,
        'size_id': size_id,
        'price': price,
        'detail': result.get('message'),
    }


def enrich_ops_draft_card(dr: OpsActionDraft, payload_order_id=None) -> dict:
    """Поля для шаблона карточки дашборда."""
    pl = draft_payload(dr)
    ent = pl.get('entities') or {}
    order = dr.order or (Order.query.get(dr.order_id) if dr.order_id else None)
    if order is None and payload_order_id:
        order = Order.query.get(payload_order_id)

    plant = None
    size = None
    if dr.plant_id:
        plant = Plant.query.get(dr.plant_id)
    elif ent.get('plant'):
        plant = _resolve_plant(ent.get('plant'))
    if dr.size_id:
        size = Size.query.get(dr.size_id)
    elif ent.get('size'):
        size = _resolve_size(ent.get('size'))

    client = None
    if dr.client_id:
        client = Client.query.get(dr.client_id)
    elif ent.get('client'):
        client = _resolve_client(ent.get('client'))
    elif order and order.client:
        client = order.client

    items_preview = []
    if order:
        for it in (order.items or [])[:8]:
            left = int(it.quantity or 0) - int(it.shipped_quantity or 0)
            items_preview.append({
                'id': it.id,
                'plant': it.plant.name if it.plant else '—',
                'size': it.size.name if it.size else '—',
                'qty': int(it.quantity or 0),
                'shipped': int(it.shipped_quantity or 0),
                'left': left,
            })

    clients_opts = []
    if dr.action_type == 'order_create':
        q = Client.query.order_by(Client.name.asc()).limit(80)
        if client:
            clients_opts = [{'id': client.id, 'name': client.name}]
            for c in q:
                if c.id != client.id:
                    clients_opts.append({'id': c.id, 'name': c.name})
                    if len(clients_opts) >= 40:
                        break
        else:
            clients_opts = [{'id': c.id, 'name': c.name} for c in q.limit(40)]

    plants_opts, sizes_opts = [], []
    if dr.action_type in ('order_create', 'price_change'):
        plants_opts = [
            {'id': p.id, 'name': p.name}
            for p in Plant.query.order_by(Plant.name.asc()).limit(80)
        ]
        sizes_opts = [
            {'id': s.id, 'name': s.name}
            for s in Size.query.order_by(Size.name.asc()).limit(80)
        ]

    orders_opts = []
    preferred_oid = (order.id if order else None) or dr.order_id or payload_order_id
    if dr.action_type == 'payment_confirm':
        orders_opts = _orders_opts_for_payment(
            client=client,
            preferred_order_id=preferred_oid,
        )

    channels = pl.get('channels') if isinstance(pl.get('channels'), list) else []
    if not channels and dr.channel:
        channels = [dr.channel]

    result = {
        'id': dr.id,
        'action_type': dr.action_type,
        'order_id': preferred_oid,
        'amount': ent.get('amount'),
        'qty': ent.get('qty'),
        'price': ent.get('price'),
        'plant_id': plant.id if plant else None,
        'plant_name': plant.name if plant else (ent.get('plant') or ''),
        'size_id': size.id if size else None,
        'size_name': size.name if size else (ent.get('size') or ''),
        'client_id': client.id if client else None,
        'client_name': client.name if client else (ent.get('client') or ''),
        'quote': dr.quote,
        'channel': dr.channel,
        'channels': channels,
        'channels_label': '+'.join(_channel_label(c) for c in channels) if channels else '',
        'items_preview': items_preview,
        'clients_opts': clients_opts,
        'plants_opts': plants_opts,
        'sizes_opts': sizes_opts,
        'orders_opts': orders_opts,
        'suggest_cash': ('нал' in (dr.quote or '').lower()),
        'purchase_items': [],
        'purchase_done': 0,
        'purchase_total': 0,
        'purchase_title': '',
    }
    if dr.action_type == 'purchase_request':
        items = _normalize_purchase_items(pl.get('items') or ent.get('items'))
        result['purchase_items'] = items
        result['purchase_total'] = len(items)
        result['purchase_done'] = sum(1 for x in items if x.get('done'))
        result['purchase_title'] = (
            ent.get('title') or pl.get('title') or 'Заявка на закупку'
        )
    return result


def _persist_purchase_items(draft: OpsActionDraft, items: list[dict]) -> None:
    """Пишет чеклист в payload + обновляет заголовок/детали задачи."""
    items = _normalize_purchase_items(items)
    pl = draft_payload(draft)
    ent = dict(pl.get('entities') or {})
    ent['items'] = items
    if not ent.get('day'):
        ent['day'] = msk_today().isoformat()
    if not ent.get('title'):
        ent['title'] = 'Заявка на закупку'
    pl['entities'] = ent
    pl['items'] = items
    draft.payload_json = json.dumps(pl, ensure_ascii=False)
    draft.updated_at = msk_now()
    if draft.task_id:
        t = TgTask.query.get(draft.task_id)
        if t:
            t.title = _title_for_action('purchase_request', ent, None)[:200]
            t.details = '\n'.join(
                f'{"✓" if it.get("done") else "○"} {it.get("text")}'
                + (f' ({it["qty"]})' if it.get('qty') else '')
                for it in items
            )[:4000]
            t.action_payload = json.dumps(pl, ensure_ascii=False)
            t.last_seen_at = msk_now()


def toggle_purchase_item(
    draft: OpsActionDraft,
    *,
    item_id: str,
    done: bool | None = None,
) -> dict:
    if draft.action_type != 'purchase_request':
        return {'ok': False, 'error': 'Не заявка на закупку'}
    if draft.status != 'pending':
        return {'ok': False, 'error': 'Черновик уже закрыт'}
    pl = draft_payload(draft)
    items = _normalize_purchase_items(
        pl.get('items') or (pl.get('entities') or {}).get('items')
    )
    found = False
    for it in items:
        if str(it.get('id')) == str(item_id):
            it['done'] = (not it.get('done')) if done is None else bool(done)
            found = True
            break
    if not found:
        return {'ok': False, 'error': 'Позиция не найдена'}
    _persist_purchase_items(draft, items)
    db.session.commit()
    done_n = sum(1 for x in items if x.get('done'))
    return {
        'ok': True,
        'items': items,
        'done': done_n,
        'total': len(items),
        'all_done': bool(items) and done_n == len(items),
    }


def complete_purchase_draft(
    draft: OpsActionDraft,
    *,
    user_id: int | None = None,
) -> dict:
    if draft.action_type != 'purchase_request':
        return {'ok': False, 'error': 'Не заявка на закупку'}
    if draft.status != 'pending':
        return {'ok': False, 'error': 'Уже закрыто'}
    _mark_draft_done(draft, user_id=user_id)
    db.session.commit()
    return {'ok': True}


def _orders_opts_for_payment(
    *,
    client: Client | None = None,
    preferred_order_id=None,
) -> list[dict]:
    """Активные заказы с долгом для выбора в карточке оплаты."""
    active = ('reserved', 'in_progress', 'ready', 'shipped')
    q = (
        Order.query
        .filter(
            Order.is_deleted.is_(False),
            Order.status.in_(active),
        )
        .order_by(Order.id.desc())
    )
    if client is not None:
        q = q.filter(Order.client_id == client.id)
    rows = q.limit(80).all()

    preferred = None
    if preferred_order_id:
        try:
            preferred = Order.query.get(int(preferred_order_id))
        except Exception:
            preferred = None

    opts: list[dict] = []
    seen: set[int] = set()

    def _add(o: Order):
        if not o or o.id in seen:
            return
        try:
            paid = float(o.paid_sum or 0)
            total = float(o.total_sum or 0)
        except Exception:
            paid, total = 0.0, 0.0
        debt = total - paid
        # Полностью оплаченные — только если это уже выбранный заказ
        if debt <= 0.5 and (preferred is None or o.id != preferred.id):
            return
        cname = o.client.name if o.client else '—'
        debt_s = f'{debt:,.0f}'.replace(',', ' ')
        opts.append({
            'id': o.id,
            'label': f'#{o.id} · {cname} · долг {debt_s} ₽',
            'debt': debt,
        })
        seen.add(o.id)

    if preferred:
        _add(preferred)
    for o in rows:
        _add(o)
        if len(opts) >= 40:
            break

    # Если по клиенту пусто — расширяем на всех с долгом
    if len(opts) <= 1 and client is not None:
        for o in (
            Order.query
            .filter(Order.is_deleted.is_(False), Order.status.in_(active))
            .order_by(Order.id.desc())
            .limit(60)
            .all()
        ):
            _add(o)
            if len(opts) >= 40:
                break

    return opts


# ---------- HTTP routes ----------

@bp.route('/calendar')
@login_required
@pilot_required
def ops_calendar():
    """Календарь Ops: выкопка/отгрузки + сроки патентов и регистраций ИН."""
    from app.digging import build_planning_weeks
    from app.hr import attach_hr_events_to_weeks

    today = msk_today()
    center = today - timedelta(days=today.weekday())
    weeks_before = int(request.args.get('wb', 2) or 2)
    # шире вперёд — патенты/регистрации часто на 1–3 месяца
    weeks_after = int(request.args.get('wa', 12) or 12)
    weeks_data = build_planning_weeks(
        center, weeks_before=weeks_before, weeks_after=weeks_after,
    )
    first_monday = center - timedelta(weeks=weeks_before)
    last_sunday = center + timedelta(weeks=weeks_after, days=6)
    hr_events = attach_hr_events_to_weeks(weeks_data, first_monday, last_sunday)
    upcoming_hr = [e for e in hr_events if e['date'] >= today][:30]

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
        upcoming_hr=upcoming_hr,
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

    order_id = None
    raw_oid = (request.form.get('order_id') or '').strip()
    if raw_oid:
        try:
            order_id = int(raw_oid)
        except Exception:
            order_id = None

    result = post_payment_draft(
        draft,
        amount=amount,
        payment_type=request.form.get('payment_type') or 'cashless',
        pay_date=pay_date,
        comment=request.form.get('comment') or '',
        file_storage=request.files.get('file'),
        user_id=current_user.id,
        order_id=order_id,
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


def _hx_ok(title: str, *, success: bool = True) -> str:
    color = 'success' if success else 'primary'
    return f"""
    <div class="card border-0 shadow-sm mb-3 bg-{color} bg-opacity-10 text-center py-3">
        <h6 class="text-{color} m-0"><i class="fas fa-check-double"></i> {html.escape(title)}</h6>
    </div>
    """


def _hx_err(msg: str, code: int = 400):
    return f'<div class="alert alert-danger mb-0">{html.escape(msg or "Ошибка")}</div>', code


@bp.route('/api/draft/<int:draft_id>/post_shipment', methods=['POST'])
@login_required
@pilot_required
def api_post_shipment(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    if draft.action_type != 'shipment_confirm' or draft.status != 'pending':
        return _hx_err('Черновик недоступен')
    result = post_shipment_draft(draft, user_id=current_user.id)
    if request.headers.get('HX-Request'):
        if result.get('ok'):
            return _hx_ok(f'Отгружено · заказ #{result.get("order_id")}')
        return _hx_err(result.get('error') or 'Ошибка')
    return jsonify(result), (200 if result.get('ok') else 400)


@bp.route('/api/draft/<int:draft_id>/post_order', methods=['POST'])
@login_required
@pilot_required
def api_post_order(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    if draft.action_type != 'order_create' or draft.status != 'pending':
        return _hx_err('Черновик недоступен')
    try:
        client_id = int(request.form.get('client_id') or 0)
    except Exception:
        client_id = 0
    plant_id = request.form.get('plant_id') or None
    size_id = request.form.get('size_id') or None
    qty = request.form.get('qty') or None
    price = request.form.get('price') or None
    try:
        plant_id = int(plant_id) if plant_id else None
        size_id = int(size_id) if size_id else None
        qty = int(qty) if qty not in (None, '') else None
        price = float(str(price).replace(',', '.')) if price not in (None, '') else None
    except Exception:
        return _hx_err('Проверьте поля позиции')
    result = post_order_create_draft(
        draft,
        client_id=client_id,
        plant_id=plant_id,
        size_id=size_id,
        qty=qty,
        price=price,
        user_id=current_user.id,
    )
    if request.headers.get('HX-Request'):
        if result.get('ok'):
            oid = result.get('order_id')
            return _hx_ok(f'Заказ создан' + (f' #{oid}' if oid else ''))
        return _hx_err(result.get('error') or 'Ошибка')
    return jsonify(result), (200 if result.get('ok') else 400)


@bp.route('/api/draft/<int:draft_id>/post_qty', methods=['POST'])
@login_required
@pilot_required
def api_post_qty(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    if draft.action_type != 'order_qty_change' or draft.status != 'pending':
        return _hx_err('Черновик недоступен')
    try:
        qty = int(request.form.get('qty') or -1)
        item_id = int(request.form.get('item_id') or 0) or None
    except Exception:
        return _hx_err('Некорректное количество')
    result = post_qty_change_draft(
        draft, qty=qty, item_id=item_id, user_id=current_user.id,
    )
    if request.headers.get('HX-Request'):
        if result.get('ok'):
            return _hx_ok(f'Qty: {result.get("old_qty")} → {result.get("new_qty")}')
        return _hx_err(result.get('error') or 'Ошибка')
    return jsonify(result), (200 if result.get('ok') else 400)


@bp.route('/api/draft/<int:draft_id>/post_price', methods=['POST'])
@login_required
@pilot_required
def api_post_price(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    if draft.action_type != 'price_change' or draft.status != 'pending':
        return _hx_err('Черновик недоступен')
    try:
        plant_id = int(request.form.get('plant_id') or 0)
        size_id = int(request.form.get('size_id') or 0)
        price = float(str(request.form.get('price') or '0').replace(',', '.').replace(' ', ''))
    except Exception:
        return _hx_err('Проверьте растение / размер / цену')
    if not plant_id or not size_id:
        return _hx_err('Укажите растение и размер')
    result = post_price_change_draft(
        draft, plant_id=plant_id, size_id=size_id, price=price, user_id=current_user.id,
    )
    if request.headers.get('HX-Request'):
        if result.get('ok'):
            return _hx_ok(f'Цена {price:g} ₽ применена')
        return _hx_err(result.get('error') or 'Ошибка')
    return jsonify(result), (200 if result.get('ok') else 400)


@bp.route('/api/draft/<int:draft_id>/ack', methods=['POST'])
@login_required
@pilot_required
def api_draft_ack(draft_id):
    """Ручное закрытие без проведения документа (fallback)."""
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
    return _hx_ok('Отмечено вручную', success=True)


def _render_purchase_checklist(draft: OpsActionDraft, *, card_id: str | None = None) -> str:
    enriched = enrich_ops_draft_card(draft)
    return render_template(
        'ops/_purchase_checklist.html',
        draft=enriched,
        card_id=card_id or '',
    )


@bp.route('/api/draft/<int:draft_id>/purchase_toggle', methods=['POST'])
@login_required
@pilot_required
def api_purchase_toggle(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    item_id = (request.form.get('item_id') or '').strip()
    if not item_id:
        return _hx_err('Нет item_id')
    done_raw = request.form.get('done')
    done = None
    if done_raw is not None and str(done_raw) != '':
        done = str(done_raw).lower() in ('1', 'true', 'yes', 'on')
    result = toggle_purchase_item(draft, item_id=item_id, done=done)
    if not result.get('ok'):
        return _hx_err(result.get('error') or 'Ошибка')
    if request.headers.get('HX-Request'):
        card_id = (request.form.get('card_id') or '').strip() or None
        return _render_purchase_checklist(draft, card_id=card_id)
    return jsonify(result)


@bp.route('/api/draft/<int:draft_id>/purchase_done', methods=['POST'])
@login_required
@pilot_required
def api_purchase_done(draft_id):
    draft = OpsActionDraft.query.get_or_404(draft_id)
    result = complete_purchase_draft(draft, user_id=current_user.id)
    if request.headers.get('HX-Request'):
        if result.get('ok'):
            return _hx_ok('Закупка закрыта')
        return _hx_err(result.get('error') or 'Ошибка')
    return jsonify(result), (200 if result.get('ok') else 400)


@bp.route('/api/run_digest', methods=['POST'])
@login_required
@pilot_required
def api_run_digest():
    force = request.form.get('force') in ('1', 'true', 'yes')
    result = run_ops_digest(force=force, send=True)
    return jsonify(result)


@bp.route('/api/demo_seed', methods=['GET', 'POST'])
@login_required
@pilot_required
def api_demo_seed():
    """Пилот: создать демо-карточки всех типов для UI-проверки (без проведения)."""
    # Берём незакрытый заказ; для оплаты предпочтём с долгом
    sample_order = None
    unpaid_order = None
    for o in Order.query.filter(Order.is_deleted.is_(False)).order_by(Order.id.desc()).limit(80):
        if (o.status or '') in ('canceled', 'ghost'):
            continue
        if sample_order is None and (o.status or '') != 'shipped':
            sample_order = o
        try:
            if unpaid_order is None and float(o.total_sum or 0) > float(o.paid_sum or 0) + 0.5:
                if (o.status or '') != 'shipped':
                    unpaid_order = o
        except Exception:
            pass
        if sample_order and unpaid_order:
            break
    pay_order = unpaid_order or sample_order
    plant = Plant.query.order_by(Plant.id.asc()).first()
    size = Size.query.order_by(Size.id.asc()).first()
    client = sample_order.client if sample_order and sample_order.client else Client.query.first()

    actions = [
        {
            'type': 'payment_confirm',
            'entities': {
                'order_id': pay_order.id if pay_order else None,
                'amount': 1000,
                'client': (pay_order.client.name if pay_order and pay_order.client else None)
                or (client.name if client else None),
            },
            'quote': 'Демо: проверить оплату',
            'confidence': 0.99,
            'channel': 'sales',
        },
        {
            'type': 'shipment_confirm',
            'entities': {'order_id': sample_order.id if sample_order else None},
            'quote': 'Демо: отгрузка',
            'confidence': 0.99,
            'channel': 'sales',
        },
        {
            'type': 'order_create',
            'entities': {'client': (client.name if client else 'Демо') + ' AIOpsDemo'},
            'quote': 'Демо: новый заказ',
            'confidence': 0.99,
            'channel': 'sales',
        },
        {
            'type': 'order_qty_change',
            'entities': {
                'order_id': sample_order.id if sample_order else None,
                'qty': 5,
                'plant': plant.name if plant else None,
            },
            'quote': 'Демо: qty',
            'confidence': 0.99,
            'channel': 'sales',
        },
        {
            'type': 'price_change',
            'entities': {
                'plant': plant.name if plant else None,
                'size': size.name if size else None,
                'price': 777,
            },
            'quote': 'Демо: цена',
            'confidence': 0.99,
            'channel': 'sales',
        },
        {
            'type': 'purchase_request',
            'entities': {
                'title': 'Заявка на закупку',
                'day': msk_today().isoformat(),
                'items': [
                    {'text': 'комбинезон для химии', 'qty': '1'},
                    {'text': 'весы маленькие для удобрений', 'qty': '1'},
                    {'text': 'перчатки резиновые', 'qty': '3 пары'},
                    {'text': 'фонарик налобный', 'qty': '3'},
                    {'text': 'цепь для пилы', 'qty': '1'},
                    {'text': 'замок', 'qty': '4'},
                ],
            },
            'quote': (
                '1, комбинезон для химии\n'
                '2, весы маленькие для удобрений\n'
                '3, перчатки 3 пары резиновые\n'
                '4, фонарик 3 шт налобный\n'
                '5, 1 цепь для пилы\n'
                '6, замок 4 шт'
            ),
            'confidence': 0.99,
            'channel': 'production',
        },
    ]
    # уникальные ключи на день — force через суффикс quote в entities day
    for a in actions:
        a['entities'] = dict(a.get('entities') or {})
        a['entities']['_demo'] = msk_today().isoformat()

    # для order_create dedup использует client+day — суффикс в имени уже есть
    result = reconcile_and_upsert_tasks(actions, day=msk_today())
    return jsonify({
        'ok': True,
        'reconcile': result,
        'sample_order_id': sample_order.id if sample_order else None,
        'hint': 'Откройте дашборд — карточки AI Ops. Проведение документов на проде делайте осознанно.',
    })
