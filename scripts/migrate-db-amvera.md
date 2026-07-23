# Миграция FloraFlow → новый PostgreSQL Amvera (меньший диск)

Операционный runbook. Код приложения менять не нужно — только `DATABASE_URL`.

## Цель

| | Старый | Новый |
|---|---|---|
| Проект | `floraflowerp-db` | `floraflowerp-db2` |
| Диск | 25 ГБ (нельзя уменьшить) | **15 ГБ** |
| Данные | ~16 МБ логика + ~10 ГБ WAL | только логика через `pg_dump` |

**Важно:** `DATABASE_URL` один и тот же у **FloraFlowERP** и **knyajestvo**.

---

## A. Подготовка (без простоя)

### A1. Бэкап Amvera

1. Amvera → PostgreSQL → `floraflowerp-db` → **Бекапы** → **Создать бэкап**.
2. Дождаться успеха.

### A2. Внешний доступ (POSTGRES-домен)

На **старой** БД: **Домены** → добавить домен типа **POSTGRES** (если ещё нет).  
Записать host вида: `floraflowerp-db-….db-msk0.amvera.tech`

То же сделать после создания **новой** БД.

### A3. Инструмент переноса на ПК

Достаточно Python из проекта (**без установки PostgreSQL**):

```powershell
cd "C:\Users\warch\Downloads\floraflowerp-code (7)"
pip install -r requirements.txt
python scripts\migrate_pg_python.py --help
```

Опционально (если хотите классический `pg_dump`):  
`.\scripts\migrate-db-to-new-cluster.ps1 -Action check-tools`

### A4. Выписать старый DATABASE_URL

Amvera → приложение **FloraFlowERP** → Переменные → скопировать `DATABASE_URL`.  
Проверить, что у **knyajestvo** тот же URL (или тот же хост/БД).

---

## B. Создать новый кластер

1. Amvera → PostgreSQL → **Создать базу данных**
2. Имя проекта: `floraflowerp-db2`
3. Тариф: Начальный Плюс (или Plus CPU)
4. Диск: **15 ГБ**
5. Реплик: **1**
6. Имя БД: `FloraFlow`
7. Пользователь + пароль — сохранить
8. **Активировать Superuser** — да (удобно для restore)
9. Дождаться **Запущено**, реплики **1 / 1**
10. **Конфигурация → Postgresql**:
    - `wal_keep_size` = **512**
    - `max_slot_wal_keep_size` = **1024**
    - `max_connections` = **100**
    - **Сохранить**
11. **Домены** → POSTGRES-домен для внешней заливки дампа
12. Вкладка **Инфо** → скопировать внутренний RW-хост  
    (`amvera-…-cnpg-floraflowerp-db2-rw`) — он пойдёт в `DATABASE_URL` приложений

Собрать новый URL:

```text
postgresql://USER:PASSWORD@INTERNAL_RW_HOST:5432/FloraFlow?sslmode=require
```

(если в старом URL не было `sslmode` — скопируйте query-часть один в один со старого.)

Для дампа с ПК используйте **внешний** POSTGRES-host, для приложений Amvera — **внутренний** RW.

---

## C. Окно простоя (dump → restore)

### C1. Пауза приложений

Amvera → поставить на паузу / остановить:

1. `FloraFlowERP`
2. `knyajestvo`

### C2. Копирование (рекомендуется)

Подставьте **внешние** POSTGRES-хосты (не внутренние `amvera-…-cnpg-…-rw` — с ПК они недоступны):

```powershell
cd "C:\Users\warch\Downloads\floraflowerp-code (7)"

$old = "postgresql://USER:PASS@OLD_EXTERNAL_HOST:5432/FloraFlow?sslmode=require"
$new = "postgresql://USER:PASS@NEW_EXTERNAL_HOST:5432/FloraFlow?sslmode=require"

# одним шагом: схема + данные + сверка COUNT
python scripts\migrate_pg_python.py copy --old $old --new $new
```

Пока в конце не будет `VERIFY OK` — **URL не переключать**.

Дополнительно можно сохранить локальный SQL-дамп:

```powershell
python scripts\migrate_pg_python.py dump --url $old --file backups\floraflow.sql
```

---

## D. Переключение

1. Внутренний URL новой БД (с вкладки Инфо) вставить в переменную `DATABASE_URL`:
   - проект **FloraFlowERP**
   - проект **knyajestvo**
2. Запустить оба приложения.
3. Проверить:
   - логин в ERP
   - заказы / склад
   - витрину shop
   - `/admin/db-health` — логический размер ~МБ, `wal_keep_size` ≈ 512

Показать URL ещё раз:

```powershell
.\scripts\migrate-db-to-new-cluster.ps1 -Action print-url -NewUrl "postgresql://..."
```

---

## E. Удаление старого (через 24–48 часов)

См. [decommission-old-db.md](decommission-old-db.md).

Пока старый кластер жив — можно откатить `DATABASE_URL` обратно за 2 минуты.

---

## Откат

1. Пауза приложений.
2. Вернуть старый `DATABASE_URL` в ERP + shop.
3. Запустить приложения.
4. Новый `floraflowerp-db2` можно удалить позже.
