"""Модели Groq по задачам (после deprecation Llama 3.x / Scout).

Переопределение через env (см. groq_model_for). Устаревшие имена
из env игнорируются и заменяются дефолтом задачи.
"""
from __future__ import annotations

import os

# Текст / JSON: счета, ВиУМ, дайджест, AI-чат
_DEFAULT_TEXT = 'openai/gpt-oss-120b'
# Быстрая классификация: статья бюджета, антиспам
_DEFAULT_CLASSIFY = 'openai/gpt-oss-20b'
# Фото чеков / выписок / реквизитов
_DEFAULT_VISION = 'qwen/qwen3.6-27b'
# CRM: анализ рынка с web search
_DEFAULT_CRM = 'groq/compound-beta'

_LEGACY_MODELS = {
    'llama-3.3-70b-versatile',
    'llama-3.1-8b-instant',
    'llama-3.1-70b-versatile',
    'meta-llama/llama-4-scout-17b-16e-instruct',
    'llama-3.3-70b-specdec',
}

_TASK_DEFAULTS = {
    'text': _DEFAULT_TEXT,
    'classify': _DEFAULT_CLASSIFY,
    'vision': _DEFAULT_VISION,
    'crm': _DEFAULT_CRM,
    'chat': _DEFAULT_TEXT,
}

# Порядок env: специфичный ключ задачи → общий GROQ_MODEL (кроме vision/crm)
_TASK_ENV = {
    'text': ('GROQ_MODEL_TEXT', 'GROQ_MODEL'),
    'classify': ('GROQ_MODEL_CLASSIFY', 'GROQ_MODEL'),
    'vision': ('GROQ_VISION_MODEL',),
    'crm': ('GROQ_MODEL_CRM',),
    'chat': ('GROQ_MODEL_CHAT', 'GROQ_MODEL'),
}


def _resolve(raw: str | None, fallback: str) -> str:
    name = (raw or '').strip()
    if not name or name in _LEGACY_MODELS:
        return fallback
    return name


def groq_model_for(task: str, default: str | None = None) -> str:
    """task: text | classify | vision | crm | chat."""
    key = (task or 'text').strip().lower()
    fallback = default or _TASK_DEFAULTS.get(key, _DEFAULT_TEXT)
    for env_name in _TASK_ENV.get(key, ('GROQ_MODEL',)):
        val = os.environ.get(env_name)
        if (val or '').strip():
            return _resolve(val, fallback)
    return fallback


def groq_model(default: str | None = None) -> str:
    """Совместимость: общая текстовая модель."""
    return groq_model_for('text', default=default)


def groq_model_text() -> str:
    return groq_model_for('text')


def groq_model_classify() -> str:
    return groq_model_for('classify')


def groq_model_vision() -> str:
    return groq_model_for('vision')


def groq_model_crm() -> str:
    return groq_model_for('crm')


def groq_model_chat() -> str:
    return groq_model_for('chat')
