"""Антиспам заявок с сайта: ловушка, эвристика, ИИ. Спам не пишется в БД и не уходит менеджеру."""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import time
from dataclasses import dataclass

from flask import current_app, has_app_context


_CYR = re.compile(r'[А-Яа-яЁё]')
_CJK = re.compile(r'[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]')
_CAMEL = re.compile(r'^[A-Z][a-z]{2,14}[A-Z][a-z]{2,16}$')
_LATIN_ONE = re.compile(r"^[A-Za-z][A-Za-z._\-]{4,28}$")
_DIGITS = re.compile(r'\d')

_SPAM_WORDS = (
    'azino', 'casino', 'crypto', 'bitcoin', 'viagra', 'cialis', 'backlink',
    'seo ', 'seo-', 'forex', 'binance', 'telegram bot', 'xxx', 'porn',
    'saya ', 'ingin ', 'harga', 'anda.', 'bonjour', 'guten tag', 'kaufen',
    'prezzo', 'precio', 'klik di sini', 'cheap promo',
)

_PLANT_HINTS = (
    'туя', 'ель', 'сосна', 'клён', 'клен', 'липа', 'дуб', 'гортенз', 'сажен',
    'дерев', 'питомник', 'ландшафт', 'хвой', 'кустар', 'изгород', 'газон',
    'thuja', 'picea', 'pinus', 'acer', 'tilia', 'hydrangea', 'seedling',
    'tree', 'shrub', 'nursery', 'landscape', 'plant', 'растене', 'растени',
    'прайс', 'консультац', 'перезвон', 'нужно', 'хочу', 'заказ', 'шт',
)


@dataclass(frozen=True)
class SpamVerdict:
    drop: bool
    reason: str
    score: int
    via: str


def antispam_enabled() -> bool:
    flag = (os.environ.get('SHOP_ANTISPAM') or '1').strip().lower()
    return flag not in ('0', 'false', 'no', 'off')


def _secret() -> bytes:
    if has_app_context():
        return (current_app.config.get('SECRET_KEY') or 'change-me').encode('utf-8')
    return (os.environ.get('SECRET_KEY') or 'change-me').encode('utf-8')


def issue_form_token() -> str:
    ts = str(int(time.time()))
    sig = hmac.new(_secret(), ts.encode(), hashlib.sha256).hexdigest()[:20]
    return f'{ts}.{sig}'


def parse_form_token(token: str) -> tuple[bool, float | None]:
    raw = (token or '').strip()
    if '.' not in raw:
        return False, None
    ts_s, sig = raw.split('.', 1)
    try:
        ts = int(ts_s)
    except ValueError:
        return False, None
    expect = hmac.new(_secret(), ts_s.encode(), hashlib.sha256).hexdigest()[:20]
    if not hmac.compare_digest(sig, expect):
        return False, None
    return True, float(ts)


def _norm(text) -> str:
    return (text or '').strip()


def _phone_digits(phone: str) -> str:
    return ''.join(_DIGITS.findall(phone or ''))


def heuristic_score(name: str, phone: str, message: str) -> tuple[int, list[str]]:
    name = _norm(name)
    message = _norm(message)
    blob = f'{name} {message}'.lower()
    score = 0
    reasons: list[str] = []

    if any(w in blob for w in _SPAM_WORDS):
        score += 4
        reasons.append('spam-words')
    if _CJK.search(name + message):
        score += 4
        reasons.append('cjk')
    if _CAMEL.match(name or ''):
        score += 2
        reasons.append('camel-name')
    elif _LATIN_ONE.match(name or '') and not _CYR.search(name):
        score += 1
        reasons.append('latin-one-word')

    if message and not _CYR.search(message) and not any(h in blob for h in _PLANT_HINTS):
        # Латиница без растений: типичный шаблон бота «hi I want your price».
        letters = re.sub(r'[^a-zA-Z]', '', message)
        if len(letters) >= 12:
            score += 3
            reasons.append('latin-message')

    digits = _phone_digits(phone)
    if len(digits) < 10 or len(digits) > 12:
        score += 1
        reasons.append('phone')

    if _CYR.search(name):
        score -= 2
        reasons.append('cyr-name')
    if _CYR.search(message):
        score -= 2
        reasons.append('cyr-msg')
    if any(h in blob for h in _PLANT_HINTS):
        score -= 2
        reasons.append('plants')
    if digits.startswith(('7', '8', '9')) and 10 <= len(digits) <= 11:
        score -= 1
        reasons.append('ru-phone')

    return score, reasons


def _ai_classify(name: str, phone: str, message: str) -> tuple[bool | None, str]:
    api_key = (os.environ.get('GROQ_API_KEY') or '').strip()
    if not api_key:
        return None, 'no-key'
    try:
        from groq import Groq

        timeout = float(os.environ.get('SHOP_ANTISPAM_AI_TIMEOUT', '4'))
        model = os.environ.get('GROQ_MODEL', 'llama-3.3-70b-versatile')
        client = Groq(api_key=api_key, timeout=timeout)
        prompt = (
            'Питомник «Княжество» (Тульская область, Россия). '
            'Продаём взрослые растения ландшафтным бюро и частным заказчикам РФ/СНГ.\n'
            'Определи, спам ли заявка с сайта. spam=true только для ботов, рекламы, '
            'казино/SEO/крипты и шаблонных иностранных «узнать вашу цену» без растений.\n'
            'Не спам: короткое «перезвоните», заявка без комментария, имя латиницей '
            'у живого человека, английский про trees/thuja/landscape, реальный заказ.\n'
            'Ответь только JSON: {"spam": true/false, "confidence": 0.0-1.0, "reason": "кратко"}\n\n'
            f'Имя: {name}\nТелефон: {phone}\nСообщение: {message or "—"}\n'
        )
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={'type': 'json_object'},
            messages=[
                {'role': 'system', 'content': 'Отвечай только валидным JSON.'},
                {'role': 'user', 'content': prompt},
            ],
        )
        import json as _json
        raw = (resp.choices[0].message.content or '{}').strip()
        data = _json.loads(raw)
        spam = bool(data.get('spam'))
        conf = float(data.get('confidence') or 0)
        reason = str(data.get('reason') or 'ai')[:180]
        if spam and conf >= 0.72:
            return True, f'ai:{reason}'
        if (not spam) and conf >= 0.55:
            return False, f'ai-ham:{reason}'
        return None, f'ai-weak:{reason}'
    except Exception as exc:
        return None, f'ai-error:{type(exc).__name__}'


def inspect_inquiry(
    customer_name: str,
    phone: str,
    message: str = '',
    *,
    honeypot: str = '',
    form_token: str = '',
    source: str = 'landing',
) -> SpamVerdict:
    """drop=True — не писать в БД и не слать почту/телеграм. Клиенту всё равно «успех»."""
    if not antispam_enabled():
        return SpamVerdict(False, 'off', 0, 'off')

    if _norm(honeypot):
        return SpamVerdict(True, 'honeypot', 99, 'honeypot')

    ok_token, started = parse_form_token(form_token)
    now = time.time()
    if not ok_token:
        # Живая форма всегда кладёт токен. Пустой/битый — почти наверняка бот.
        return SpamVerdict(True, 'bad-token', 8, 'token')
    age = now - started
    if age < 1.2:
        return SpamVerdict(True, 'too-fast', 8, 'token')
    if age > 60 * 60 * 24 * 2:
        return SpamVerdict(True, 'token-stale', 6, 'token')

    score, reasons = heuristic_score(customer_name, phone, message)
    if score >= 3:
        return SpamVerdict(True, ','.join(reasons) or 'heuristic', score, 'heuristic')
    if score <= 0:
        return SpamVerdict(False, ','.join(reasons) or 'ham', score, 'heuristic')

    ai_spam, ai_reason = _ai_classify(customer_name, phone, message)
    if ai_spam is True:
        return SpamVerdict(True, ai_reason, score, 'ai')
    if ai_spam is False:
        return SpamVerdict(False, ai_reason, score, 'ai')
    # ИИ недоступен/неуверен: при спорном score не режем живых клиентов.
    if score >= 2:
        return SpamVerdict(True, f'{",".join(reasons)};{ai_reason}', score, 'heuristic')
    return SpamVerdict(False, f'{",".join(reasons)};{ai_reason}', score, 'pass')


def log_drop(verdict: SpamVerdict, source: str, name: str, phone: str) -> None:
    if not has_app_context():
        return
    current_app.logger.info(
        'shop antispam drop source=%s via=%s reason=%s score=%s name=%s phone=%s',
        source, verdict.via, verdict.reason, verdict.score,
        (name or '')[:40], (phone or '')[:20],
    )
