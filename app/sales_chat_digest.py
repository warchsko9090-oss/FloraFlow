"""Агент саммари чата продаж (свободный текст Telegram).

Поток:
    каждое сообщение из TG_CHAT_ID_ORDERS
        -> ingest_message()  # сырой лог SalesChatMessage
    ежедневно 20:00 МСК
        -> run_sales_chat_digest()
        -> LLM по тексту дня
        -> личка бота получателям (TG_SALES_DIGEST_IDS)

Group Privacy у бота должна быть OFF (как для чата расходов), иначе
обычные сообщения группы не придут на webhook.
"""
from __future__ import annotations

import html
import os
from datetime import date, datetime, timedelta

from flask import current_app

from app.models import db, SalesChatMessage, SalesChatDigestLog
from app.utils import msk_now, msk_today

# Получатель по умолчанию — личный tg id владельца
_DEFAULT_DIGEST_IDS = '8512060616'
_MAX_TRANSCRIPT_CHARS = 90000
_TG_MSG_LIMIT = 3900


def orders_chat_id() -> str:
    return (
        (os.environ.get('TG_CHAT_ID_ORDERS') or '').strip()
        or (os.environ.get('TG_CHAT_ID') or '').strip()
    )


def digest_recipient_ids() -> list[str]:
    raw = (os.environ.get('TG_SALES_DIGEST_IDS') or _DEFAULT_DIGEST_IDS).strip()
    return [p.strip() for p in raw.replace(';', ',').split(',') if p.strip()]


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


def ingest_message(msg: dict) -> dict:
    """Сохраняет сырое сообщение чата продаж. Не бросает наружу."""
    try:
        chat = msg.get('chat') or {}
        tg_chat_id = str(chat.get('id') or '')
        tg_message_id = int(msg.get('message_id') or 0)
        if not tg_chat_id or not tg_message_id:
            return {'ok': False, 'error': 'empty'}

        expected = orders_chat_id()
        if expected and tg_chat_id != expected:
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

        existing = SalesChatMessage.query.filter_by(
            tg_chat_id=tg_chat_id, tg_message_id=tg_message_id,
        ).first()
        if existing is not None:
            # edited_message — обновляем текст
            if existing.raw_text != text[:8000]:
                existing.raw_text = text[:8000]
                existing.updated_at = msk_now()
                db.session.commit()
                return {'ok': True, 'status': 'updated', 'id': existing.id}
            return {'ok': True, 'status': 'dup', 'id': existing.id}

        row = SalesChatMessage(
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
        return {'ok': True, 'status': 'stored', 'id': row.id}
    except Exception as exc:
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            current_app.logger.exception('sales_chat_digest.ingest_message failed')
        except Exception:
            pass
        return {'ok': False, 'error': str(exc)}


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, 0, 0, 0)
    end = start + timedelta(days=1)
    return start, end


def _load_day_messages(day: date) -> list[SalesChatMessage]:
    start, end = _day_bounds(day)
    chat_id = orders_chat_id()
    q = SalesChatMessage.query.filter(
        SalesChatMessage.tg_date >= start,
        SalesChatMessage.tg_date < end,
    )
    if chat_id:
        q = q.filter(SalesChatMessage.tg_chat_id == chat_id)
    return q.order_by(SalesChatMessage.tg_date.asc(), SalesChatMessage.id.asc()).all()


def _build_transcript(rows: list[SalesChatMessage]) -> str:
    lines = []
    for r in rows:
        t = r.tg_date.strftime('%H:%M') if r.tg_date else '??:??'
        who = r.sender_name or 'unknown'
        if r.is_bot:
            who = f'{who} [bot]'
        body = (r.raw_text or '').replace('\n', ' ').strip()
        lines.append(f'{t} {who}: {body}')
    text = '\n'.join(lines)
    if len(text) <= _MAX_TRANSCRIPT_CHARS:
        return text
    # хвост дня важнее начала при обрезке
    return text[-_MAX_TRANSCRIPT_CHARS:]


def _llm_summarize(day: date, transcript: str, message_count: int) -> str:
    api_key = (os.environ.get('GROQ_API_KEY') or '').strip()
    if not api_key:
        return _fallback_summary(day, transcript, message_count, reason='нет GROQ_API_KEY')

    from groq import Groq
    from app.groq_util import groq_model_text

    prompt = (
        'Ты — помощник руководителя питомника. Ниже переписка чата продаж за один день '
        '(свободный текст сотрудников и бота).\n'
        'Сделай краткое саммари на русском для лички руководителя.\n\n'
        'Структура (пропусти пустые блоки):\n'
        '1) Копка / выкопка — старт, факт, договорённости\n'
        '2) Оплаты / деньги — кто, сколько, по какому заказу/клиенту (если сказано)\n'
        '3) Отгрузки / логистика\n'
        '4) Заказы / клиенты / срочность\n'
        '5) Прочее важное и хвосты (незакрытые вопросы)\n\n'
        'Правила: только факты из переписки, не выдумывай; даты/суммы/имена сохраняй; '
        'без воды и вступлений; маркеры списка «•»; максимум ~2500 символов.\n\n'
        f'Дата: {day.isoformat()}, сообщений: {message_count}\n\n'
        f'Переписка:\n{transcript}'
    )
    try:
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model=groq_model_text(),
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.2,
        )
        text = (resp.choices[0].message.content or '').strip()
        if not text:
            return _fallback_summary(day, transcript, message_count, reason='пустой ответ LLM')
        return text
    except Exception as exc:
        try:
            current_app.logger.exception('sales_chat_digest LLM failed')
        except Exception:
            pass
        return _fallback_summary(day, transcript, message_count, reason=str(exc)[:200])


def _fallback_summary(day: date, transcript: str, message_count: int, reason: str = '') -> str:
    head = f'Итоги чата продаж за {day.isoformat()} ({message_count} сообщ.)'
    if reason:
        head += f'\n[авто-выжимка без LLM: {reason}]'
    # последние ~25 строк как «сырой» хвост
    lines = transcript.splitlines()
    tail = lines[-25:] if len(lines) > 25 else lines
    body = '\n'.join(tail) if tail else '(пусто)'
    return f'{head}\n\n{body}'


def _format_tg_html(day: date, summary: str, message_count: int) -> str:
    title = f'<b>Итоги чата продаж</b> — {html.escape(day.isoformat())}'
    meta = f'<i>{message_count} сообщ. за день</i>'
    # убираем опасные теги от модели, оставляем простой текст + переносы
    clean = html.escape(summary)
    clean = clean.replace('\n', '\n')
    # лёгкая разметка: строки начинающиеся с «•» уже ок
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


def run_sales_chat_digest(
    *,
    day: date | None = None,
    force: bool = False,
    send: bool = True,
) -> dict:
    """Собирает саммари за день МСК и шлёт в личку. force — повторная отправка."""
    day = day or msk_today()
    existing = SalesChatDigestLog.query.filter_by(digest_date=day).first()
    if existing and not force:
        return {
            'ok': True,
            'status': 'already_sent',
            'digest_date': day.isoformat(),
            'message_count': existing.message_count,
        }

    rows = _load_day_messages(day)
    if not rows:
        summary = f'За {day.isoformat()} в логе чата продаж пусто (бот ещё не накопил сообщения или день тихий).'
        message_count = 0
    else:
        transcript = _build_transcript(rows)
        message_count = len(rows)
        summary = _llm_summarize(day, transcript, message_count)

    recipients = digest_recipient_ids()
    sent_ok = []
    sent_err = []
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

    if existing:
        existing.message_count = message_count
        existing.summary_text = summary[:20000]
        existing.recipient_ids = ','.join(recipients)
        existing.sent_at = msk_now()
    else:
        db.session.add(SalesChatDigestLog(
            digest_date=day,
            message_count=message_count,
            summary_text=summary[:20000],
            recipient_ids=','.join(recipients),
            sent_at=msk_now(),
        ))
    db.session.commit()

    return {
        'ok': not sent_err,
        'status': 'sent' if sent_ok and not sent_err else ('partial' if sent_ok else 'built'),
        'digest_date': day.isoformat(),
        'message_count': message_count,
        'recipients_ok': sorted(set(sent_ok)),
        'errors': sent_err,
    }
