"""Локальные end-to-end тесты Telegram-вебхука.

Что имитирует:
    • реальную супергруппу «Жемчужниково продажи» (chat_id = -5256333216);
    • отправителей — админов К (@Kirill_8988) и В (@warchesko);
    • обязательное @упоминание бота @FloraFlovvBot.

Как запускать:
    1) Локально поднять сервер: `python run.py` (слушает :5000).
    2) В отдельном окне: `python test_bot.py`.

В продакшен-сценарии (whitelist в env) этот же вебхук примет только сообщения
из TG_ALLOWED_CHAT_IDS / TG_CHAT_ID_* и только от TG_ALLOWED_SENDERS.
"""

import requests
import time

WEBHOOK_URL = "http://127.0.0.1:5000/api/telegram/webhook"

# Идентификаторы супергрупп из разговора с пользователем.
CHAT_PRODAZHI = -5256333216   # «Жемчужниково продажи»
CHAT_PROIZVODSTVO = -5198757991  # «Жемчужниково производство»

# Разрешённые отправители (whitelist).
SENDER_K = {"id": 111, "first_name": "Кирилл", "username": "Kirill_8988"}
SENDER_V = {"id": 222, "first_name": "Владислав", "username": "warchesko"}

# Пример «левого» пользователя — должен быть отсечён whitelist'ом.
SENDER_STRANGER = {"id": 999, "first_name": "Левый", "username": "some_other_guy"}

scenarios = [
    {
        "name": "1. Создание заказа (относительная дата) — от K, продажи",
        "chat_id": CHAT_PRODAZHI, "chat_title": "Жемчужниково продажи",
        "sender": SENDER_K,
        "text": "@FloraFlovvBot @Aleksei Приветствую! У нас новый клиент - ИП Леонидов. Свяжись с ним завтра и сделай заказ. Предоплата 100% по счету."
    },
    {
        "name": "2. Копка (дедлайн числом) — от V, производство",
        "chat_id": CHAT_PROIZVODSTVO, "chat_title": "Жемчужниково производство",
        "sender": SENDER_V,
        "text": "@FloraFlovvBot Заказ Овчинникова номер 2 оплачен полностью. @Behruz завтра начинаем копать со спиреи, надо закончить до 15."
    },
    {
        "name": "3. Отгрузка (точная дата, падеж «Алексею»)",
        "chat_id": CHAT_PRODAZHI, "chat_title": "Жемчужниково продажи",
        "sender": SENDER_V,
        "text": "@FloraFlovvBot Алексею — машину для Хвои заказал на 28.04, не менее 7 т и желательно 8м. Подготовь отгрузку."
    },
    {
        "name": "4. Оплата (без даты) — @warchesko",
        "chat_id": CHAT_PRODAZHI, "chat_title": "Жемчужниково продажи",
        "sender": SENDER_V,
        "text": "@FloraFlovvBot Зеленый самурай, заказ оплачен полностью. Алексей, отметь в базе без пп."
    },
    {
        "name": "5. Поступление / Прикоп ОКС — падеж «Бехрузу»",
        "chat_id": CHAT_PROIZVODSTVO, "chat_title": "Жемчужниково производство",
        "sender": SENDER_K,
        "text": "@FloraFlovvBot Из Тольятти выехал ОКС. Бехрузу надо принять и укрыть в 32 доме. Сделай до пятницы."
    },
    {
        "name": "6. Без упоминания бота — должен быть проигнорирован",
        "chat_id": CHAT_PROIZVODSTVO, "chat_title": "Жемчужниково производство",
        "sender": SENDER_K,
        "text": "Саженцы пересчитали. Завтра вытаскивать и прикапывать по поступлению до 20 числа!"
    },
    {
        "name": "7. Левый отправитель — whitelist должен отсечь",
        "chat_id": CHAT_PRODAZHI, "chat_title": "Жемчужниково продажи",
        "sender": SENDER_STRANGER,
        "text": "@FloraFlovvBot @Aleksei попробую пролезть без прав"
    },
    {
        "name": "8. Левый чат — должен быть проигнорирован",
        "chat_id": -1001234567890, "chat_title": "Сторонний чат",
        "sender": SENDER_V,
        "text": "@FloraFlovvBot @Aleksei тест из чужого чата"
    },
    {
        # text_mention: у Бехруза нет @username, но К ткнул на него через "@" в UI.
        # В таком случае Telegram присылает entity type=text_mention с user.id.
        # Наш вебхук подменит «Бехруз» на «@behruz», если в TG_USER_ID_MAP указана пара.
        "name": "9. text_mention (без @) — Бехруз без username",
        "chat_id": CHAT_PROIZVODSTVO, "chat_title": "Жемчужниково производство",
        "sender": SENDER_K,
        "text": "@FloraFlovvBot Бехруз, завтра вытаскиваем спирею до 25 числа",
        "entities": [
            {"type": "text_mention", "offset": 14, "length": 6,
             "user": {"id": 70000001, "first_name": "Бехруз", "is_bot": False}},
        ],
    },
]


def _build_payload(scenario):
    message = {
        "message_id": int(time.time()) % 10**6,
        "text": scenario["text"],
        "chat": {
            "id": scenario["chat_id"],
            "title": scenario["chat_title"],
            "type": "supergroup",
        },
        "from": {**scenario["sender"], "is_bot": False},
        "date": int(time.time()),
    }
    if scenario.get("entities"):
        message["entities"] = scenario["entities"]
    return {
        "update_id": int(time.time() * 1000) % 10**9,
        "message": message,
    }


def run_tests():
    print("Запуск тестирования Telegram-вебхука...\n")
    for i, s in enumerate(scenarios, 1):
        print(f"[{i}/{len(scenarios)}] {s['name']}")
        print(f"  chat: {s['chat_title']} ({s['chat_id']})")
        print(f"  from: @{s['sender'].get('username')}")
        print(f"  text: {s['text']}")
        try:
            r = requests.post(WEBHOOK_URL, json=_build_payload(s), timeout=30)
            print(f"  -> {r.status_code} {r.text.strip()[:200]}")
        except Exception as e:
            print(f"  -> connection error: {e} (сервер запущен?)")
        print("-" * 60)

        # Groq free-tier rate limit — даём передышку между AI-вызовами.
        if i < len(scenarios):
            time.sleep(6)

    print("\nГотово. Проверь:")
    print("  • Дашборд Алексея (role=user) и Бехруза (user2) — должны появиться задачи.")
    print("  • GET /api/telegram/_debug под админом — увидишь stage для каждого сценария.")


if __name__ == "__main__":
    run_tests()