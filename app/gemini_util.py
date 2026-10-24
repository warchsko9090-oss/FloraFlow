"""Минимальный клиент Google Gemini (Generative Language API).

Без отдельного SDK — через requests (уже в requirements).
Ключ: GEMINI_API_KEY. Модель: GEMINI_MODEL / GEMINI_MODEL_TEXT.
"""
from __future__ import annotations

import os
from typing import Iterable

import requests

_DEFAULT_MODELS = (
    'gemini-3.8-flash',
    'gemini-3-flash-preview',
    'gemini-flash-latest',
    'gemini-3.5-flash',
)


def gemini_api_key() -> str:
    return (os.environ.get('GEMINI_API_KEY') or '').strip()


def gemini_models_for_text() -> list[str]:
    preferred = (
        (os.environ.get('GEMINI_MODEL_TEXT') or '').strip()
        or (os.environ.get('GEMINI_MODEL') or '').strip()
    )
    out: list[str] = []
    if preferred:
        out.append(preferred)
    for name in _DEFAULT_MODELS:
        if name not in out:
            out.append(name)
    return out


def gemini_generate_text(
    *,
    prompt: str,
    system: str = '',
    temperature: float = 0.2,
    max_output_tokens: int = 2048,
    timeout_sec: float = 60.0,
    models: Iterable[str] | None = None,
) -> str:
    """generateContent. Пробует модели по очереди при 404/503."""
    api_key = gemini_api_key()
    if not api_key:
        raise RuntimeError('GEMINI_API_KEY не задан')

    # systemInstruction + user content
    body: dict = {
        'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
        'generationConfig': {
            'temperature': temperature,
            'maxOutputTokens': max_output_tokens,
        },
    }
    if system.strip():
        body['systemInstruction'] = {'parts': [{'text': system.strip()}]}

    last_err: Exception | None = None
    for model in (list(models) if models is not None else gemini_models_for_text()):
        url = (
            f'https://generativelanguage.googleapis.com/v1beta/models/'
            f'{model}:generateContent'
        )
        try:
            resp = requests.post(
                url,
                params={'key': api_key},
                json=body,
                timeout=timeout_sec,
            )
            if resp.status_code in (404, 503):
                last_err = RuntimeError(f'{model}: HTTP {resp.status_code} {resp.text[:180]}')
                continue
            if resp.status_code >= 400:
                raise RuntimeError(f'{model}: HTTP {resp.status_code} {resp.text[:300]}')
            data = resp.json()
            cands = data.get('candidates') or []
            if not cands:
                last_err = RuntimeError(f'{model}: пустой candidates')
                continue
            parts = ((cands[0].get('content') or {}).get('parts')) or []
            text = ''.join((p.get('text') or '') for p in parts).strip()
            if text:
                return text
            last_err = RuntimeError(f'{model}: пустой текст')
        except requests.RequestException as exc:
            last_err = exc
            continue
    raise RuntimeError(str(last_err) if last_err else 'Gemini: все модели недоступны')


_NO_SPEECH_TOKEN = '__NO_SPEECH__'

# Типичные «фантазии» модели на полной тишине (не путать с паузами в живой речи).
_STT_SILENCE_HALLUCINATIONS = (
    'привет, как дела',
    'привет как дела',
    'что делаешь',
    'здравствуйте',
    'добрый день',
    'hello',
    'how are you',
)


def _wav_near_silent(file_bytes: bytes, *, peak_threshold: int = 80) -> bool:
    """True, если WAV почти без сигнала (тестовая тишина / пустой микрофон)."""
    import struct
    import wave
    from io import BytesIO

    try:
        with wave.open(BytesIO(file_bytes), 'rb') as w:
            n = w.getnframes()
            if n <= 0:
                return True
            raw = w.readframes(n)
            width = w.getsampwidth()
        if width != 2 or len(raw) < 4:
            return False
        samples = struct.unpack('<' + 'h' * (len(raw) // 2), raw)
        peak = max(abs(s) for s in samples) if samples else 0
        return peak < peak_threshold
    except Exception:
        return False


def _sanitize_stt_transcript(text: str, *, near_silent: bool = False) -> str:
    t = (text or '').strip()
    if not t:
        return ''
    if _NO_SPEECH_TOKEN in t.replace(' ', ''):
        return ''
    low = t.lower().strip(' .!?…')
    # На заведомой тишине отсекаем короткие «приветствия-галлюцинации»
    if near_silent:
        return ''
    if len(low) <= 40 and any(h in low for h in _STT_SILENCE_HALLUCINATIONS):
        # Без операционных маркеров — скорее фантазия на шуме/тишине
        ops = ('заказ', 'оплат', 'отгруз', 'патент', 'руб', '₽', '#', 'выкоп', 'бригад')
        if not any(k in low for k in ops):
            return ''
    return t


def gemini_transcribe_audio(
    file_bytes: bytes,
    *,
    mime_type: str = 'audio/ogg',
    language_hint: str = 'ru',
    timeout_sec: float = 60.0,
) -> str:
    """Speech→text через Gemini (inline audio). Пустая строка при ошибке / нет речи."""
    import base64

    api_key = gemini_api_key()
    if not api_key or not file_bytes:
        return ''

    mime = (mime_type or 'audio/ogg').strip() or 'audio/ogg'
    near_silent = mime in ('audio/wav', 'audio/x-wav') and _wav_near_silent(file_bytes)
    if near_silent:
        return ''

    b64 = base64.b64encode(file_bytes).decode('ascii')
    is_video = (mime or '').startswith('video/')
    source = 'видео (только речь с аудиодорожки)' if is_video else 'аудио'
    prompt = (
        f'Задача: точная расшифровка речи из {source} (speech-to-text).\n'
        'Правила:\n'
        '1) Передай только то, что реально сказано. Не описывай картинку, '
        'не додумывай, не придумывай приветствия и диалоги.\n'
        '2) Короткие паузы и молчание внутри фразы — просто пропускай, текст не выдумывай.\n'
        '3) Если в записи нет речи (тишина, шум, музыка без слов) — ответь ровно '
        f'{_NO_SPEECH_TOKEN} и ничего больше.\n'
        '4) Без кавычек, без пояснений, без «вот расшифровка».\n'
        f'Язык: преимущественно {language_hint or "ru"}.'
    )

    body = {
        'contents': [{
            'role': 'user',
            'parts': [
                {'text': prompt},
                {'inlineData': {'mimeType': mime, 'data': b64}},
            ],
        }],
        'generationConfig': {
            'temperature': 0.0,
            'maxOutputTokens': 2048,
        },
    }

    last_err: Exception | None = None
    for model in gemini_models_for_text():
        url = (
            f'https://generativelanguage.googleapis.com/v1beta/models/'
            f'{model}:generateContent'
        )
        try:
            resp = requests.post(
                url,
                params={'key': api_key},
                json=body,
                timeout=timeout_sec,
            )
            if resp.status_code in (404, 503):
                last_err = RuntimeError(f'{model}: HTTP {resp.status_code}')
                continue
            if resp.status_code >= 400:
                last_err = RuntimeError(f'{model}: HTTP {resp.status_code} {resp.text[:200]}')
                continue
            data = resp.json()
            cands = data.get('candidates') or []
            if not cands:
                last_err = RuntimeError(f'{model}: empty candidates')
                continue
            parts = ((cands[0].get('content') or {}).get('parts')) or []
            text = ''.join((p.get('text') or '') for p in parts).strip()
            return _sanitize_stt_transcript(text, near_silent=False)
        except requests.RequestException as exc:
            last_err = exc
            continue
    try:
        from flask import current_app
        current_app.logger.warning('gemini_transcribe_audio failed: %s', last_err)
    except Exception:
        pass
    return ''
