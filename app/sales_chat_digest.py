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


_DIGEST_SYSTEM = (
    'Ты — помощник руководителя питомника. Пишешь короткую выжимку дня из чата продаж '
    'в личку: что сделать / что уже решено. Не цитируй переписку целиком.'
)

_DIGEST_PROMPT = (
    'Сделай оперативную выжимку на русском по переписке чата продаж за день.\n\n'
    'Формат — короткие пункты «•», без вступлений и без дословных цитат.\n'
    'Пиши как поручения/статусы, например:\n'
    '• Владу — проверить заказ N, провести оплату / сверку\n'
    '• Отгрузка 14.10 заказ 287 (вечер) — Княжество → Выборгский р-н; '
    'данные водителя в чате, Алексей принял\n'
    '• Машина от Сингаевской завтра утром — накладная без цен; подтверждено\n\n'
    'Блоки (пустые пропускай):\n'
    '1) Контроль / кому что сделать\n'
    '2) Отгрузки и логистика (дата/время, заказ, направление; '
    'паспорт/права/госномер НЕ переписывай — только «данные водителя есть / связались»)\n'
    '3) Оплаты и деньги\n'
    '4) Копка / выкопка\n'
    '5) Хвосты и незакрытое\n\n'
    'Игнор: болтовня про скачать приложение/логин, стикеры, пустые «[фото]/«[аудио]» '
    'без смысла, повторы «Принял» без новой информации.\n'
    'Только факты из текста; не выдумывай; до ~1800 символов.\n\n'
    'Дата: {day}, сообщений: {message_count}\n\n'
    'Переписка:\n{transcript}'
)


def _digest_provider_order() -> list[str]:
    """Порядок LLM для дайджеста.

    SALES_DIGEST_PROVIDER=auto|gemini|groq
    auto: при наличии GEMINI_API_KEY сначала Gemini (Groq с Miami часто 403),
    иначе Groq; второй провайдер — запасной.
    """
    mode = (os.environ.get('SALES_DIGEST_PROVIDER') or 'auto').strip().lower()
    has_gemini = bool((os.environ.get('GEMINI_API_KEY') or '').strip())
    has_groq = bool((os.environ.get('GROQ_API_KEY') or '').strip())
    if mode == 'gemini':
        order = ['gemini', 'groq']
    elif mode == 'groq':
        order = ['groq', 'gemini']
    else:
        # auto
        order = ['gemini', 'groq'] if has_gemini else ['groq', 'gemini']
    out = []
    for name in order:
        if name == 'gemini' and has_gemini and name not in out:
            out.append(name)
        if name == 'groq' and has_groq and name not in out:
            out.append(name)
    return out


def _summarize_via_groq(prompt: str) -> str:
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
    return (resp.choices[0].message.content or '').strip()


def _summarize_via_gemini(prompt: str) -> str:
    from app.gemini_util import gemini_generate_text

    return gemini_generate_text(
        prompt=prompt,
        system=_DIGEST_SYSTEM,
        temperature=0.2,
        max_output_tokens=2048,
        timeout_sec=55.0,
    )


def _llm_summarize(day: date, transcript: str, message_count: int) -> str:
    prompt = _DIGEST_PROMPT.format(
        day=day.isoformat(),
        message_count=message_count,
        transcript=transcript,
    )
    providers = _digest_provider_order()
    if not providers:
        return _fallback_summary(
            day, transcript, message_count,
            reason='нет GROQ_API_KEY и GEMINI_API_KEY',
        )

    errors: list[str] = []
    for name in providers:
        try:
            if name == 'gemini':
                text = _summarize_via_gemini(prompt)
            else:
                text = _summarize_via_groq(prompt)
            if text:
                return text
            errors.append(f'{name}: пустой ответ')
        except Exception as exc:
            errors.append(f'{name}: {str(exc)[:160]}')
            try:
                current_app.logger.exception('sales_chat_digest %s failed', name)
            except Exception:
                pass

    return _fallback_summary(
        day, transcript, message_count,
        reason='; '.join(errors)[:220] if errors else 'LLM недоступен',
    )


def _is_noise_line(body: str) -> bool:
    b = (body or '').strip().lower()
    if not b:
        return True
    if b in ('[фото]', '[аудио]', '[видео]', '[стикер]', '[документ]', '[фото, аудио]'):
        return True
    if b in ('принял', 'принято', 'ок', 'ok', '👍', '☝️'):
        return True
    noise_bits = (
        'скачать приложение', 'под полем «пароль»', 'под полем "пароль"',
        'новая ссылка', 'старые приложения', 'логин и пароль',
        'для тупых', 'не будем хламить', 'в личные сообщение', 'в личные сообщения',
        'читайте внимательно', 'учи как', 'это наша программа',
    )
    return any(x in b for x in noise_bits)


def _looks_like_id_dump(body: str) -> bool:
    """Паспорт / ВУ / длинные реквизиты водителя — в выжимку не тащим."""
    b = (body or '').lower()
    keys = ('паспорт', 'выдан', 'в/у', 'вод. удостов', 'гос.номер', 'госномер', 'мс')
    hits = sum(1 for k in keys if k in b)
    return hits >= 2 or ('паспорт' in b and len(body) > 120)


def _is_business_line(body: str) -> bool:
    b = (body or '').lower()
    keys = (
        'заказ', 'отгруз', 'оплат', 'счёт', 'счет', 'накладн', 'машин', 'газел',
        'погруз', 'выгруз', 'копк', 'выкоп', 'логист', 'достав', 'завтра', 'утро',
        'вечер', 'час', 'накл', 'без цен', 'княжеств', 'сингаев',
    )
    return any(k in b for k in keys)


def _shorten_logistics(body: str) -> str:
    """Оставляет суть отгрузки без паспортных хвостов."""
    import re
    text = (body or '').strip()
    bl = text.lower()

    # Сообщение почти целиком — паспорт/ВУ: не цитируем
    if _looks_like_id_dump(text) and not any(
        k in bl for k in ('отгруз', 'заказ', 'погруз', 'выгруз', 'накладн', 'завтра', 'машин')
    ):
        # Попробуем вытащить ФИО в начале («Изотов Александр…»)
        cut_at = len(text)
        for marker in ('Паспорт', 'паспорт', 'В/У', 'в/у', 'тел:', 'тел ', 'авто:'):
            idx = text.find(marker)
            if 0 <= idx < cut_at:
                cut_at = idx
        head = text[:cut_at].strip(' ;,')
        name_m = re.match(
            r'^([А-ЯЁA-Z][а-яёa-z\-]+(?:\s+[А-ЯЁA-Z][а-яёa-z\-]+){1,3})',
            head,
        )
        name = (name_m.group(1) if name_m else '').strip()
        if name:
            return f'Данные водителя в чате ({name})'
        return 'Данные водителя в чате'

    # обрезка после типичного начала паспортного блока
    for marker in ('Паспорт', 'паспорт', 'в/у', 'В/У', 'Выдан:', 'выдан:'):
        idx = text.find(marker)
        if idx > 30:
            text = text[:idx].rstrip(' ;,')
            break
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) > 220:
        text = text[:217].rstrip() + '…'
    if _looks_like_id_dump(body) or 'паспорт' in bl:
        if 'данные водителя' not in text.lower():
            text = (text + '; данные водителя в чате').strip('; ')
    return text

def _fallback_summary(day: date, transcript: str, message_count: int, reason: str = '') -> str:
    """Эвристическая выжимка без LLM — без дословного дампа чата."""
    head = f'Итоги чата продаж за {day.isoformat()} ({message_count} сообщ.)'
    if reason:
        head += f'\n[авто-выжимка без LLM]'

    logistics: list[str] = []
    money: list[str] = []
    dig: list[str] = []
    other: list[str] = []
    acks: list[str] = []

    for raw in (transcript or '').splitlines():
        if ':' not in raw:
            continue
        # "HH:MM Name: body"
        try:
            _time, rest = raw.split(' ', 1)
            who, body = rest.split(':', 1)
        except ValueError:
            continue
        who = who.strip()
        body = body.strip()
        bl = body.lower()
        if bl in ('принял', 'принято') or bl.startswith('принял'):
            acks.append(who.split('(')[0].strip() or who)
            continue
        if _is_noise_line(body):
            continue
        if not _is_business_line(body) and not _looks_like_id_dump(body):
            continue

        is_log = any(k in bl for k in (
            'отгруз', 'машин', 'газел', 'погруз', 'выгруз', 'накладн', 'логист',
            'достав', 'завтра', 'час',
        )) or _looks_like_id_dump(body)
        is_pay = any(k in bl for k in ('оплат', 'счёт', 'счет', 'деньг', 'перевод'))
        is_dig = any(k in bl for k in ('копк', 'выкоп'))

        if is_log:
            logistics.append(_shorten_logistics(body))
        elif is_pay:
            money.append(body[:200])
        elif is_dig:
            dig.append(body[:200])
        else:
            other.append(body[:200])

    bullets: list[str] = []
    if logistics:
        bullets.append('Отгрузки / логистика:')
        for item in logistics[:8]:
            bullets.append(f'• {item}')
        if acks:
            uniq = []
            for a in acks:
                if a and a not in uniq:
                    uniq.append(a)
            bullets.append(f'• Подтвердили: {", ".join(uniq[:5])}')
    if money:
        bullets.append('Оплаты / деньги:')
        for item in money[:6]:
            bullets.append(f'• {item}')
    if dig:
        bullets.append('Копка / выкопка:')
        for item in dig[:6]:
            bullets.append(f'• {item}')
    if other:
        bullets.append('Прочее по заказам:')
        for item in other[:6]:
            bullets.append(f'• {item}')

    if not bullets:
        body = (
            'За день нет явных операционных пунктов '
            '(или чат был про доступы/технику без заказов и отгрузок).'
        )
    else:
        body = '\n'.join(bullets)

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
