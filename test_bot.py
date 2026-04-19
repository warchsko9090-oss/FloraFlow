import requests
# Можешь менять текст тут и запускать скрипт: python test_bot.py
text = "@FloraFlow @Behruz надо вытаскивать и прикапывать саженцы по поступлению. Сделай до конца сегодняшнего дня!"

data = {
    "message": {
        "text": text,
        "chat": {"type": "group"},
        "from": {"first_name": "Босс (Тест)"}
    }
}
r = requests.post("http://127.0.0.1:5000/api/telegram/webhook", json=data)
print(r.status_code, r.text)