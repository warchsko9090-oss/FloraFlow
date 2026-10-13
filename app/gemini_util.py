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
