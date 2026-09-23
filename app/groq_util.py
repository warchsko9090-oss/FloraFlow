"""Общие настройки Groq: модель по умолчанию после deprecation Llama 3.3."""
from __future__ import annotations

import os

# Groq снял llama-3.3-70b-versatile (16.08.2026) для free/dev.
# Рекомендуемая замена: openai/gpt-oss-120b. Переопределяется GROQ_MODEL.
_DEFAULT_GROQ_MODEL = 'openai/gpt-oss-120b'
_LEGACY_MODELS = {
    'llama-3.3-70b-versatile',
    'llama-3.1-8b-instant',
    'llama-3.1-70b-versatile',
}


def groq_model(default: str | None = None) -> str:
    raw = (os.environ.get('GROQ_MODEL') or '').strip()
    if not raw or raw in _LEGACY_MODELS:
        return default or _DEFAULT_GROQ_MODEL
    return raw
