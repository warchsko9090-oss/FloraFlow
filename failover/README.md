# Failover: дом → Amvera-заглушка

Если домашний сервер (электричество/интернет) недоступен:

1. Внешний сторож (GitHub Action каждые 10 мин) видит down  
2. Будит Amvera-проект **kdoor** (прокси → дом; при падении — `SHOP_READONLY` витрина 1:1)  
3. Меняет DNS `A @` и `A www` на IP Amvera (оба на один IP)  
4. Пишет админу в Telegram  
5. Когда дом снова жив — DNS обратно, Amvera усыпляет

Админ чинит дом спокойно: клиенты на `knyajestvo.ru` видят ту же витрину без шт/КП/заявок.

## Файлы

| Путь | Назначение |
|---|---|
| `watch.py` | Сторож: health / DNS / Amvera / TG |
| `regru_dns.py` | Reg.ru API2 смена A |
| `amvera_scale.py` | Amvera MCP `scaleProject` |
| `../amvera.door.yml` + `../wsgi_door.py` | Деплой kdoor (прокси + readonly shop) |
| `../.github/workflows/failover-watch.yml` | Внешний cron |
| `../scripts/export-shop-failover-snapshot.py` | Снимок SQLite + media в `/data` |

## Разовый сетап

### 1. Amvera kdoor

Деплой корня FloraFlow с `amvera.door.yml` → проект `kdoor`.

Переменные (см. комментарии в `amvera.door.yml`):

```
UPSTREAM_ORIGIN=http://95.31.41.191
UPSTREAM_HOST=knyajestvo.ru
SHOP_READONLY=1
SHOP_ONLY=1
DATABASE_URL=sqlite:////data/failover.db
UPLOAD_FOLDER=/data
SECRET_KEY=...
DISABLE_SCHEDULER=1
```

Снимок: `python scripts/export-shop-failover-snapshot.py` → `/data/failover.db` + media.

### 2. DNS (A `@` + `www`)

Failover снимает **дубликаты A `@`** и ставит один A на целевой IP.
`www`: если уже `CNAME → knyajestvo.ru.` — не трогаем (следует за apex).
Если `www` — A-запись — ставим тот же IP, что и `@`.

Важно: два A `@` (дом + Amvera) ломают failover — клиент может попасть на мёртвый дом.

### 2b. Штатные записи

- `A @` → дом `95.31.41.191` (или Amvera при failover), **одна** запись  
- `CNAME www` → `knyajestvo.ru.` (предпочтительно) **или** `A www` = тот же IP  
- TTL зоны **300** (быстрее переключение):

```bash
python failover/watch.py --set-ttl 300
```

### 3. Reg.ru API

В ЛК Reg.ru: API-пароль + **whitelist IP**.  
Для GitHub Actions IP плавающие — варианты:

- whitelist известных диапазонов GitHub, или  
- self-hosted runner с белым IP, или  
- маленький всегда-он VPS только под сторож  

Без whitelist DNS-шаг не пройдёт (в TG придёт ошибка).

### 4. GitHub Secrets

`HOME_IP`, `FAILOVER_IP`, `REGRU_USERNAME`, `REGRU_PASSWORD`,  
`AMVERA_TOKEN`, `AMVERA_PROJECT_ID`, `TG_BOT_TOKEN`, `TG_CHAT_ID`  
(+ опционально `AMVERA_MCP_URL`, `TG_API_BASE`, `FAILOVER_DOMAIN`)

Actions → `failover-watch` → Run workflow (сначала с Dry-run).

### 5. Снимок витрины (цены + фото + код)

**Сейчас на Amvera пусто, пока дом был выключен** — снимок не заливался. После старта дома:

1. В Amvera kdoor → env: `FAILOVER_SYNC_TOKEN=<длинный секрет>` (тот же, что в `failover-sync.env`).
2. На домашнем ПК:

```powershell
mkdir $env:USERPROFILE\.floraflow -Force
copy scripts\failover-sync.env.example $env:USERPROFILE\.floraflow\failover-sync.env
# вписать FAILOVER_SYNC_TOKEN

# разовый прогон (дом должен отвечать / БД доступна)
.\scripts\sync-failover-weekly.ps1

# каждое воскресенье 00:00
.\scripts\sync-failover-weekly.ps1 -RegisterSchedule
```

Что делает weekly:
- экспорт БД (цены/остатки для витрины) + все фото → `MANIFEST.json` (sha256) + `snapshot.tar.gz`
- проверка целостности SQLite + хешей файлов
- POST на kdoor `/internal/failover-sync` → `/data`
- опционально `PUSH_CODE=1` — git push кода в kdoor

## Ручные команды

```bash
cd failover
copy .env.example .env   # заполнить

python watch.py --dry-run
python watch.py --force-failover
python watch.py --force-home
python watch.py --set-ttl 300
```

## Пороги

- `FAIL_THRESHOLD=2` — два подряд fail (≈20 мин при cron */10) до failover  
- `OK_THRESHOLD=2` — два подряд ok до возврата домой  

Так короткий морг дома не дёргает DNS.
