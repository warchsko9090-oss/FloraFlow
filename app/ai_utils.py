import os
import requests
import json

API_URL = "https://api-inference.huggingface.co/models/sentence-transformers/all-MiniLM-L6-v2"

HF_TOKEN = os.environ.get("HF_TOKEN", "")

def _get_headers():
    return {"Authorization": f"Bearer {HF_TOKEN}"} if HF_TOKEN else {}

def query_huggingface(payload):
    """Отправляет запрос к API HuggingFace"""
    try:
        response = requests.post(API_URL, headers=_get_headers(), json=payload)
        return response.json()
    except Exception:
        return None

def find_best_match(user_query, knowledge_base):
    """
    user_query: текст вопроса
    knowledge_base: список объектов из БД
    """
    if not user_query or not knowledge_base:
        return None

    # Подготавливаем список вопросов из базы
    db_questions = [item.question for item in knowledge_base]

    # Формируем запрос: "Сравни user_query с каждым из db_questions"
    payload = {
        "inputs": {
            "source_sentence": user_query,
            "sentences": db_questions
        }
    }

    # Отправляем в облако
    output = query_huggingface(payload)

    # Проверяем ответ (это должен быть список чисел-оценок)
    if not output or not isinstance(output, list):
        return None

    # Ищем самую высокую оценку
    # output будет выглядеть как [0.1, 0.95, 0.3 ...]
    max_score = -1
    best_idx = -1

    for i, score in enumerate(output):
        if score > max_score:
            max_score = score
            best_idx = i

    # Порог уверенности (40%)
    if max_score < 0.4:
        return None

    return knowledge_base[best_idx]