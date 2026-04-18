# Настройка напоминаний по патентам

Этот модуль отправляет уведомления в Telegram за 14 и 7 дней до окончания патента.

## 1. Переменные окружения

Обязательно задайте:

- `TG_BOT_TOKEN` - токен Telegram-бота
- `PATENT_REMINDER_TOKEN` - секрет для вызова endpoint из cron

Рекомендуется:

- `TG_CHAT_ID_PATENTS` - отдельный чат для напоминаний по патентам

Если `TG_CHAT_ID_PATENTS` не задан, используется `TG_CHAT_ID_HR`, затем `TG_CHAT_ID`.

## 2. Endpoint для запуска

- URL: `/patents/reminders/run`
- Методы: `GET` или `POST`
- Авторизация: токен в query или в header

Примеры:

```bash
curl "https://YOUR_DOMAIN/patents/reminders/run?token=YOUR_SECRET"
```

или

```bash
curl -H "X-Reminder-Token: YOUR_SECRET" "https://YOUR_DOMAIN/patents/reminders/run"
```

## 3. Планировщик (cron)

Настройте внешний cron-сервис на ежедневный вызов URL.

Пример расписания:

- Каждый день в 09:00 по Москве

Важно: API-ключ Amvera не требуется. Нужен только URL приложения и `PATENT_REMINDER_TOKEN`.
