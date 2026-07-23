# Деплой: 2 репозитория + 2 контейнера Amvera

Схема «понятно и раздельно»:

```
GitHub FloraFlow          GitHub knyajestvo
       │                         │
       │ push 1                  │ push 2
       ▼                         ▼
 Amvera FloraFlowERP        Amvera knyajestvo
 (ERP + БД)                 (knyajestvo.ru)
```

---

## 1. Репозитории на GitHub

| Репозиторий | URL | Amvera-проект |
|-------------|-----|---------------|
| **FloraFlow** | `github.com/warchsko9090-oss/FloraFlow` | FloraFlowERP |
| **knyajestvo** | `github.com/warchsko9090-oss/knyajestvo` | knyajestvo |

### ERP (уже есть)

Локально — **корень проекта** (папка с `app/`, `run.py`, `amvera.yml`).

Remote уже настроен: `https://github.com/warchsko9090-oss/FloraFlow.git`

Папка `knyajestvo-code/` **не входит** в ERP-репо (см. `.gitignore`).

### Shop (создать один раз)

1. GitHub → **New repository** → имя: `knyajestvo` (Private).
2. Локально:

```powershell
cd "C:\Users\warch\Downloads\floraflowerp-code (7)\knyajestvo-code"

git init
git add .
git commit -m "initial shop"
git branch -M master
git remote add origin https://github.com/warchsko9090-oss/knyajestvo.git
git push -u origin master
```

3. Один раз указать путь к ERP (если shop и ERP не в monorepo):

```powershell
copy tools\erp-path.local.example tools\erp-path.local
# отредактируйте erp-path.local — путь к корню FloraFlow
```

---

## 2. Amvera

### FloraFlowERP

| Поле | Значение |
|------|----------|
| Git | `warchsko9090-oss/FloraFlow` |
| Ветка | `master` |
| Корень | `.` |
| Конфиг | `amvera.yml` из корня |

### knyajestvo

| Поле | Значение |
|------|----------|
| Git | `warchsko9090-oss/knyajestvo` |
| Ветка | `master` |
| Корень | `.` |
| Конфиг | `amvera.yml` |

Переменные shop (Amvera → knyajestvo → Переменные):

```
DATABASE_URL=...          # скопировать из FloraFlowERP
ERP_PUBLIC_BASE_URL=https://floraflowerp-warchesko.amvera.io
SECRET_KEY=...
BEHIND_PROXY=1
```

---

## 3. Ежедневный workflow

### Я правлю только ERP (админка, остатки, shop-admin)

```powershell
cd "C:\Users\warch\Downloads\floraflowerp-code (7)"
.\scripts\deploy-erp.ps1 "правки shop-admin"
```

### Я правлю код витрины / PDF / каталог (файлы в `app/`)

Нужны **оба** push — shop использует **копию** `app/`:

```powershell
# 1. ERP
cd "C:\Users\warch\Downloads\floraflowerp-code (7)"
.\scripts\deploy-erp.ps1 "правки каталога"

# 2. Shop (sync + push)
cd knyajestvo-code
.\scripts\deploy-shop.ps1 "sync app из ERP"
```

### Я правлю только shop-обёртку (`shop_site/`, `wsgi.py`)

```powershell
cd knyajestvo-code
.\scripts\deploy-shop.ps1 "правки wsgi"
```

---

## 4. Проверка после деплоя

- ERP: `https://floraflowerp-warchesko.amvera.io`
- Shop: `https://knyajestvo.ru/shop`
- PDF: `https://knyajestvo.ru/shop/stock.pdf`
- Health shop: `https://knyajestvo.ru/health`

---

## 5. Локальная разработка

```powershell
# Терминал 1 — ERP
cd "C:\Users\warch\Downloads\floraflowerp-code (7)"
python run.py

# Терминал 2 — Shop
cd knyajestvo-code
$env:ERP_PUBLIC_BASE_URL='http://127.0.0.1:5000'
python run.py
```

Shop: http://127.0.0.1:5001/shop

---

## 6. Важно

- **Два push** — нормально, так и задумано.
- После правок в `app/` shop **не обновится**, пока не запустите `sync_erp_bundle.py` и push в knyajestvo.
- `tools/erp-path.local` — только на вашем ПК, в Git не попадает.
- Миграции БД и `/shop-admin` — только на ERP.
