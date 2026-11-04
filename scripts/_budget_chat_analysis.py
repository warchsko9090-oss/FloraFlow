# -*- coding: utf-8 -*-
"""Сверка: экспорт Telegram + монитор чата расходов (prod) + регистр Expense.

ЗП в чате часто одной суммой (итог/часть долга), в ERP — по сотрудникам из табеля.
Правки в ERP не вносятся — только Excel.
"""
from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

CHAT_HTML = Path(r"C:\Users\warch\Downloads\Telegram Desktop\ChatExport_2026-11-03\messages.html")
OUT_DIR = Path(r"C:\Users\warch\Documents\FloraFlow\backups\budget-review-2026-11")
COMMENTS_XLSX = OUT_DIR / "budget-articles-comments.xlsx"
YEAR = 2026
NEXT_YEAR = 2027

AMOUNT_RE = re.compile(
    r"(?<![.\d])(\d{1,3}(?:[ \u00a0]\d{3})+|\d+)(?:[.,](\d{1,2}))?\s*(?:₽|руб\.?|р\.?|р\b)?",
    re.I,
)
SKIP_TEXT = re.compile(r"(тест маршрута|chat_id:|тип:\s*expenses)", re.I)
MAX_SANE = Decimal("5000000")

_PAYROLL_RE = re.compile(
    r"(?i)(?:\bзп\b|зарплат|аванс|выдал\w*\s+б[еэ]хруз|б[еэ]хруз\w*\s+зп|"
    r"рабоч\w+|людям|бригад|патент\w*\s+выдано|офиц\w*\s+зп)"
)
_MONTH_STEMS = [
    (re.compile(r"(?i)январ"), 1), (re.compile(r"(?i)феврал"), 2),
    (re.compile(r"(?i)март"), 3), (re.compile(r"(?i)апрел"), 4),
    (re.compile(r"(?i)ма[йя]"), 5), (re.compile(r"(?i)июн"), 6),
    (re.compile(r"(?i)июл"), 7), (re.compile(r"(?i)август"), 8),
    (re.compile(r"(?i)сентябр"), 9), (re.compile(r"(?i)октябр"), 10),
    (re.compile(r"(?i)ноябр"), 11), (re.compile(r"(?i)декабр"), 12),
]
_YEAR_RE = re.compile(r"(20\d{2})")

SALARY_CODES = {"10", "10.1", "10.2", "10.3", "10.4", "10.5"}

RULES: list[tuple[tuple[str, ...], str]] = [
    (("гсм", "бензин", "дизель", "солярк", "заправк", "азс", "топлив"), "7.1"),
    (("электроэнерг", "электрич", "кватт", "квт"), "9"),
    (("торф", "грунт", "субстрат"), "6"),
    (("удобрен", "осмокот", "базакот", "пестицид", "фунгицид", "гербицид"), "6.1"),
    (("сетк", "мешковин", "спанбонд", "агроткан", "агроволокн"), "6.2"),
    (("капельн", "капельник"), "5.1"),
    (("труба", "пнд", "пвх", "фитинг", "муфт", "тройник", "полив"), "5.2"),
    (("насос",), "5.3"),
    (("грядк", "посев"), "5"),
    (("инструмент", "секатор", "кусторез", "триммер", "бензопил"), "4"),
    (("лопат", "грабл", "тачк"), "4.1"),
    (("мотоблок",), "8"),
    (("запчаст", "фильтр", "масло", "трактор"), "8.1"),
    (("чоп", "охран"), "13"),
    (("найм", "экскаватор", "кран", "манипулятор", "аренда техник"), "12"),
    (("хозрасход", "туалетн", "салфетк", "порошок", "моющее"), "7"),
    (("кофе", "интернет", "офис", "картридж", "канцел"), "2"),
    (("щебень", "отсыпк", "дорог"), "1"),
    (("забор", "столб", "колючк"), "1.1"),
    (("растен", "саженц", "окс"), "3"),
    (("представит", "командировоч", "гостиниц"), "14"),
]


def load_db_url() -> str:
    for line in Path(os.environ["USERPROFILE"], ".floraflow", "backup-pg.env").read_text(
        encoding="utf-8"
    ).splitlines():
        if line.startswith("FLORAFLOW_BACKUP_DATABASE_URL="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("no db url")


def D(x) -> Decimal:
    try:
        return Decimal(str(x or 0))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def amounts_close(a, b) -> bool:
    aa, bb = D(a), D(b)
    if aa == bb:
        return True
    return abs(aa - bb) <= max(Decimal("100"), aa * Decimal("0.015"))


def parse_amount(text: str) -> Decimal | None:
    cands = []
    for m in AMOUNT_RE.finditer((text or "").replace("\xa0", " ")):
        whole = m.group(1).replace(" ", "").replace("\xa0", "")
        frac = m.group(2) or "0"
        try:
            val = Decimal(f"{whole}.{frac}")
        except Exception:
            continue
        if val < 100 or val > MAX_SANE:
            continue
        if 1900 <= val <= 2100 and frac == "0":
            continue
        cands.append(val)
    return max(cands) if cands else None


def is_payroll(text: str) -> bool:
    return bool(_PAYROLL_RE.search(text or ""))


def parse_payroll_month_year(text: str, ref: date) -> tuple[int, int]:
    month = None
    for rx, num in _MONTH_STEMS:
        if rx.search(text or ""):
            month = num
            break
    year = ref.year
    ym = _YEAR_RE.search(text or "")
    if ym:
        year = int(ym.group(1))
    elif month and month > ref.month + 1:
        year = ref.year - 1
    return (month or ref.month), year


def norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"[^a-zа-я0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def token_set(s: str) -> set[str]:
    stop = {
        "на", "по", "за", "и", "в", "с", "к", "от", "для", "руб", "рублей", "р",
        "шт", "нал", "безнал", "оплатил", "оплата", "перевод", "чек",
    }
    return {t for t in norm(s).split() if len(t) > 2 and t not in stop}


def suggest_article(text: str, code_to_item: dict) -> tuple[str | None, str]:
    t = " " + norm(text) + " "
    if any(k in t for k in ("зп", "зарплат", "аванс")):
        if "влад" in t and "офиц" not in t:
            return "10.2", "зп/аванс Влад"
        if "алексе" in t or "менеджер" in t:
            return "10", "зп менеджер/Алексей"
        if "бухгалтер" in t:
            return "10.1", "зп бухгалтер"
        if any(k in t for k in ("рабоч", "бехруз", "бэхруз", "людям", "бригад")):
            return "10.3", "зп рабочие (итог в чате)"
        if "офиц" in t:
            return "10.4", "зп офиц."
    if "патент" in t:
        return "11.2", "патент/оформление"
    if any(k in t for k in (" горш", " c5", " c3", " c7", " с5", " с3", " с7")):
        if "15" in code_to_item:
            return "15", "контейнеры"
    for keys, code in RULES:
        if any(k in t for k in keys) and code in code_to_item:
            return code, f"правило:{keys[0]}"
    return None, ""


def _nk_code(c: str):
    return [int(x) if x.isdigit() else x.lower() for x in re.split(r"(\d+)", str(c or ""))]


def load_prior_article_comments(path: Path) -> dict[str, dict]:
    """Сохраняем ваши комментарии при пересборке (ключ = код или __new__N)."""
    if not path.exists():
        return {}
    try:
        wb = load_workbook(path, data_only=False)
    except Exception:
        return {}
    if "Статьи — комментарии" not in wb.sheetnames:
        return {}
    ws = wb["Статьи — комментарии"]
    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {h: i for i, h in enumerate(headers) if h}
    out: dict[str, dict] = {}
    new_i = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or all(v is None or str(v).strip() == "" for v in row):
            continue
        code = row[idx["Код ERP"]] if "Код ERP" in idx else row[0]
        code = str(code).strip() if code is not None else ""
        def cell(name, default=""):
            if name not in idx:
                return default
            v = row[idx[name]]
            return "" if v is None else v

        payload = {
            "how": cell("Как я понимаю статью"),
            "action": cell("Что сделать"),
            "new_code": cell("Новый код"),
            "new_name": cell("Новое / уточнённое название"),
            "merge_to": cell("Куда перенести / с чем слить"),
            "notes": cell("Примеры / заметки"),
            "name_hint": cell("Статья (как в ERP)"),
        }
        if not any(str(payload[k]).strip() for k in ("how", "action", "new_code", "new_name", "merge_to", "notes")):
            # пустая строка-шаблон без правок — не сохраняем
            if not code or code.startswith("("):
                continue
        if not code or code.startswith("("):
            key = f"__new__{new_i}"
            new_i += 1
            out[key] = payload
        else:
            out[code] = payload
    return out


def write_articles_comments(items, fact_by_code: dict, path: Path) -> Path:
    """Рабочий лист: вы пишете понимание статей и предлагаете новые — потом пересоберём бюджет."""
    prior = load_prior_article_comments(path)
    wb = Workbook()

    ws0 = wb.active
    ws0.title = "Как заполнять"
    for line in [
        "Лист «Статьи — комментарии» — ваш черновик структуры бюджета.",
        "1) В колонке «Как я понимаю статью» напишите своими словами, что туда входит.",
        "2) «Что сделать»: оставить / переименовать / слить / разбить / убрать / новая.",
        "3) Новые статьи — внизу блока «НОВЫЕ СТАТЬИ» (пустые строки) или с действием «новая».",
        "4) Fact 2026 — справочно из ERP; при пересборке скрипта ваши тексты сохраняются.",
        "5) Когда заполните — скажите, перераспределим бюджет 2027 по вашей карте статей.",
        "Файл отдельный: budget-articles-comments.xlsx (не затирается листом «Бюджет 2027»).",
    ]:
        ws0.append([line])
    ws0.column_dimensions["A"].width = 110

    headers = [
        "Код ERP",
        "Статья (как в ERP)",
        "Fact 2026",
        "Как я понимаю статью",
        "Что сделать",
        "Новый код",
        "Новое / уточнённое название",
        "Куда перенести / с чем слить",
        "Примеры / заметки",
    ]
    ws = wb.create_sheet("Статьи — комментарии", 0)
    ws.append(headers)
    style_header(ws)
    fill_new = PatternFill("solid", fgColor="FFF8E1")
    fill_hint = PatternFill("solid", fgColor="E8F5E9")

    usable = [it for it in items if str(it["code"]) != "ARCH.ADJ"]
    usable = sorted(usable, key=lambda it: _nk_code(str(it["code"])))

    for it in usable:
        code = str(it["code"])
        prev = prior.get(code, {})
        ws.append([
            code,
            it["name"],
            float(fact_by_code.get(code, Decimal(0))),
            prev.get("how", ""),
            prev.get("action", ""),
            prev.get("new_code", ""),
            prev.get("new_name", ""),
            prev.get("merge_to", ""),
            prev.get("notes", ""),
        ])
        for col in range(4, 10):
            ws.cell(ws.max_row, col).fill = fill_hint

    # разделитель + сохранённые ранее «новые» + запас пустых строк
    ws.append([])
    sep = ws.max_row
    ws.append(["(новые)", "↓ НОВЫЕ СТАТЬИ — допишите ниже", "", "", "новая", "", "", "", ""])
    for col in range(1, 10):
        ws.cell(ws.max_row, col).font = Font(bold=True)
        ws.cell(ws.max_row, col).fill = fill_new

    for key, prev in prior.items():
        if not str(key).startswith("__new__"):
            continue
        ws.append([
            prev.get("new_code") or "",
            prev.get("name_hint") or prev.get("new_name") or "",
            "",
            prev.get("how", ""),
            prev.get("action") or "новая",
            prev.get("new_code", ""),
            prev.get("new_name", ""),
            prev.get("merge_to", ""),
            prev.get("notes", ""),
        ])
        for col in range(1, 10):
            ws.cell(ws.max_row, col).fill = fill_new

    for _ in range(12):
        ws.append(["", "", "", "", "новая", "", "", "", ""])
        for col in range(1, 10):
            ws.cell(ws.max_row, col).fill = fill_new

    # подсказка по действиям справа
    ws.column_dimensions["D"].width = 42
    ws.column_dimensions["E"].width = 16
    ws.column_dimensions["G"].width = 32
    ws.column_dimensions["H"].width = 28
    ws.column_dimensions["I"].width = 36
    autosize(ws)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:I{max(sep - 1, 2)}"

    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


def parse_chat_html(path: Path) -> list[dict]:
    soup = BeautifulSoup(path.read_text(encoding="utf-8"), "lxml")
    rows, last_from, current_day = [], None, None
    for el in soup.select("div.history > div.message"):
        classes = el.get("class") or []
        if "service" in classes:
            det = el.select_one("div.body.details")
            if det:
                try:
                    current_day = datetime.strptime(
                        det.get_text(" ", strip=True), "%d %B %Y"
                    ).date()
                except Exception:
                    pass
            continue
        if "default" not in classes:
            continue
        fn = el.select_one("div.from_name")
        if fn:
            last_from = fn.get_text(" ", strip=True)
        text_el = el.select_one("div.text")
        if not text_el:
            continue
        text = text_el.get_text("\n", strip=True)
        if not text or SKIP_TEXT.search(text):
            continue
        dt_el = el.select_one("div.pull_right.date.details")
        title = dt_el.get("title") if dt_el else None
        msg_dt = None
        if title:
            try:
                msg_dt = datetime.strptime(title.split(" GMT")[0], "%d.%m.%Y %H:%M:%S")
            except Exception:
                pass
        day = msg_dt.date() if msg_dt else current_day
        rows.append({
            "source": "export",
            "msg_id": el.get("id"),
            "date": day,
            "from": last_from or "?",
            "text": text,
            "amount": parse_amount(text),
            "payroll": is_payroll(text),
        })
    return rows


def fetch_all(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT id, code, name,
               COALESCE(is_amortization,false) AS is_amortization,
               COALESCE(is_container_purchase,false) AS is_container_purchase
        FROM budget_item ORDER BY id
        """
    )
    items = cur.fetchall()
    cur.execute(
        """
        SELECT e.id, e.date, e.amount, e.description, e.payment_type,
               e.budget_item_id, e.employee_id, e.target_month, e.target_year,
               e.project_id, bi.code AS bi_code, bi.name AS bi_name,
               emp.name AS emp_name, p.name AS project_name
        FROM expense e
        LEFT JOIN budget_item bi ON bi.id = e.budget_item_id
        LEFT JOIN employee emp ON emp.id = e.employee_id
        LEFT JOIN project p ON p.id = e.project_id
        WHERE EXTRACT(YEAR FROM e.date) = %s OR e.target_year = %s
        ORDER BY e.date, e.id
        """,
        (YEAR, YEAR),
    )
    expenses = cur.fetchall()
    cur.execute(
        """
        SELECT id, tg_message_id, tg_date, raw_text, sender_name,
               parsed_amount, parsed_description, parsed_payment_type,
               suggested_budget_item_id, status, expense_id, task_id,
               created_at
        FROM chat_expense_message
        WHERE coalesce(tg_date, created_at) >= %s
          AND coalesce(tg_date, created_at) < %s
        ORDER BY coalesce(tg_date, created_at), id
        """,
        (date(YEAR, 1, 1), date(YEAR + 1, 1, 1)),
    )
    chat_db = cur.fetchall()
    cur.execute(
        """
        SELECT year, month, budget_item_id, amount, bi.code, bi.name
        FROM budget_plan bp
        JOIN budget_item bi ON bi.id = bp.budget_item_id
        WHERE year = %s
        """,
        (YEAR,),
    )
    plans = cur.fetchall()
    return items, expenses, chat_db, plans


def payroll_rows_for(expenses, month: int, year: int, payment_type: str | None = None):
    out = []
    for e in expenses:
        if not e["employee_id"]:
            continue
        tm, ty = e["target_month"], e["target_year"]
        if tm and ty:
            if int(tm) != month or int(ty) != year:
                continue
        else:
            if not e["date"] or e["date"].month != month or e["date"].year != year:
                continue
        if payment_type in ("cash", "cashless") and e["payment_type"] != payment_type:
            continue
        out.append(e)
    return out


def find_payroll_cover(amount, text, ref_date, expenses, payment_type=None):
    """ЗП-итог в чате ↔ разбивка по сотрудникам в ERP."""
    if amount is None or not is_payroll(text):
        return None
    month, year = parse_payroll_month_year(text, ref_date or date.today())
    rows = payroll_rows_for(expenses, month, year, payment_type)
    if not rows and payment_type:
        rows = payroll_rows_for(expenses, month, year, None)
    if not rows:
        return {"kind": "payroll_wait", "month": month, "year": year, "total": 0, "count": 0, "lines": []}

    # 1) одна строка ≈ сумма чата
    for exp in rows:
        if amounts_close(exp["amount"], amount):
            return {
                "kind": "line",
                "month": month,
                "year": year,
                "total": D(exp["amount"]),
                "count": 1,
                "lines": [exp],
                "expense": exp,
            }

    # 2) вся котловая сумма месяца ≈ чат
    total = sum((D(x["amount"]) for x in rows), Decimal(0))
    if amounts_close(total, amount):
        return {
            "kind": "month_total",
            "month": month,
            "year": year,
            "total": total,
            "count": len(rows),
            "lines": rows,
            "expense": rows[0],
        }

    # 3) сумма выплат в день сообщения (±1) — типичная «выдал Бехрузу 400»
    if ref_date:
        day_rows = [
            x for x in rows
            if x["date"] and abs((x["date"] - ref_date).days) <= 1
        ]
        day_total = sum((D(x["amount"]) for x in day_rows), Decimal(0))
        if day_rows and amounts_close(day_total, amount):
            return {
                "kind": "payout_day",
                "month": month,
                "year": year,
                "total": day_total,
                "count": len(day_rows),
                "lines": day_rows,
                "expense": day_rows[0],
            }

        # 4) часть долга: чат ≤ сумме месяца и близко к сумме за ±3 дня вокруг даты
        window = [
            x for x in rows
            if x["date"] and abs((x["date"] - ref_date).days) <= 3
        ]
        win_total = sum((D(x["amount"]) for x in window), Decimal(0))
        if window and amounts_close(win_total, amount):
            return {
                "kind": "payout_window",
                "month": month,
                "year": year,
                "total": win_total,
                "count": len(window),
                "lines": window,
                "expense": window[0],
            }
        # 5) чат — часть месячного долга (сумма месяца больше, разница разумная)
        if total > 0 and amount <= total + Decimal("100"):
            # если сумма чата составляет >= 40% месяца или >= 50к — считаем покрытием-частью
            if amount >= Decimal("50000") or (total > 0 and amount / total >= Decimal("0.35")):
                return {
                    "kind": "month_partial",
                    "month": month,
                    "year": year,
                    "total": total,
                    "count": len(rows),
                    "lines": rows,
                    "expense": rows[0],
                    "chat_share": float(amount / total) if total else 0,
                }

    return {
        "kind": "payroll_unmatched",
        "month": month,
        "year": year,
        "total": total,
        "count": len(rows),
        "lines": rows,
    }


def match_single_expense(amount, text, day, expenses, used_ids: set):
    if amount is None or not day:
        return None, "нет суммы/даты"
    tokens = token_set(text)
    best, best_score = None, 0
    for e in expenses:
        if e["id"] in used_ids or e["employee_id"]:
            # персональные ЗП не матчим как обычный расход 1:1 с итогами
            continue
        if abs(D(e["amount"]) - amount) > max(Decimal("1"), amount * Decimal("0.02")):
            continue
        if not e["date"] or abs((e["date"] - day).days) > 5:
            continue
        overlap = len(tokens & token_set(e["description"] or ""))
        score = 100 - abs((e["date"] - day).days) * 5 + overlap * 10
        if abs(D(e["amount"]) - amount) < Decimal("0.01"):
            score += 20
        if score > best_score:
            best_score, best = score, e
    if best and best_score >= 70:
        return best, f"score={best_score}"
    return None, "не найдено"


def style_header(ws, row=1):
    fill = PatternFill("solid", fgColor="1F4E3D")
    font = Font(color="FFFFFF", bold=True)
    for cell in ws[row]:
        cell.fill = fill
        cell.font = font
        cell.alignment = Alignment(wrap_text=True, vertical="center")


def autosize(ws, max_width=46):
    for col in ws.columns:
        letter = get_column_letter(col[0].column)
        width = 10
        for cell in col[:100]:
            width = min(max_width, max(width, len(str(cell.value or "")) + 2))
        ws.column_dimensions[letter].width = width


def write_table(ws, headers, rows, keys):
    ws.append(headers)
    style_header(ws)
    thin = Border(
        left=Side(style="thin", color="D0D0D0"),
        right=Side(style="thin", color="D0D0D0"),
        top=Side(style="thin", color="D0D0D0"),
        bottom=Side(style="thin", color="D0D0D0"),
    )
    for r in rows:
        vals = []
        for k in keys:
            v = r.get(k)
            if isinstance(v, (date, datetime)):
                v = v.isoformat() if not isinstance(v, datetime) else v.isoformat(sep=" ", timespec="minutes")
            elif isinstance(v, Decimal):
                v = float(v)
            vals.append(v)
        ws.append(vals)
        for cell in ws[ws.max_row]:
            cell.border = thin
            cell.alignment = Alignment(wrap_text=True, vertical="top")
    autosize(ws)
    ws.auto_filter.ref = ws.dimensions
    ws.freeze_panes = "A2"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Parse HTML export…")
    export_rows = parse_chat_html(CHAT_HTML)
    print(f"  export texts: {len(export_rows)}, with amount: {sum(1 for r in export_rows if r['amount'])}")

    print("Load prod…")
    conn = psycopg2.connect(load_db_url())
    items, expenses, chat_db, plans = fetch_all(conn)
    conn.close()
    code_to_item = {str(i["code"]): i for i in items}
    id_to_item = {i["id"]: i for i in items}
    print(f"  expenses={len(expenses)}, chat_monitor={len(chat_db)}, items={len(items)}")

    # --- классификация монитора чата (как tg-chat-gap / расходный отчёт) ---
    monitor_rows = []
    for row in chat_db:
        text = row["parsed_description"] or row["raw_text"] or ""
        day = (row["tg_date"].date() if row["tg_date"] else None) or (
            row["created_at"].date() if row["created_at"] else None
        )
        amount = D(row["parsed_amount"]) if row["parsed_amount"] is not None else parse_amount(text)
        status = row["status"]
        cover = None
        erp = None
        verdict = status
        detail = ""

        if row["expense_id"]:
            erp = next((e for e in expenses if e["id"] == row["expense_id"]), None)
            verdict = "linked"
            detail = f"expense #{row['expense_id']}"
        elif amount and is_payroll(text):
            cover = find_payroll_cover(amount, text, day, expenses, row["parsed_payment_type"])
            if cover and cover["kind"] in ("line", "month_total", "payout_day", "payout_window", "month_partial"):
                verdict = f"payroll:{cover['kind']}"
                detail = (
                    f"{cover['month']:02d}.{cover['year']}: "
                    f"{int(cover['total'])} ₽ / {cover['count']} стр."
                )
                if cover.get("chat_share"):
                    detail += f" (чат≈{cover['chat_share']:.0%} месяца)"
                erp = cover.get("expense")
            elif cover and cover["kind"] == "payroll_wait":
                verdict = "payroll_wait"
                detail = f"ожидаем разбивку {cover['month']:02d}.{cover['year']}"
            elif cover:
                verdict = "payroll_gap"
                detail = (
                    f"в ERP за {cover['month']:02d}.{cover['year']} "
                    f"{int(cover['total'])} ₽ / {cover['count']} стр., чат {int(amount)} ₽"
                )
        elif amount and day:
            exp, why = match_single_expense(amount, text, day, expenses, set())
            if exp:
                verdict = "matched_fuzzy"
                detail = why
                erp = exp
            elif status in ("pending", "unparseable", "rejected"):
                verdict = status
            else:
                verdict = status or "missing"

        sug_code, sug_why = suggest_article(text, code_to_item)
        monitor_rows.append({
            "id": row["id"],
            "date": day,
            "amount": float(amount) if amount else None,
            "text": (text or "")[:500],
            "from": row["sender_name"],
            "status_db": status,
            "verdict": verdict,
            "detail": detail,
            "payroll": is_payroll(text),
            "erp_id": erp["id"] if erp else None,
            "erp_code": erp["bi_code"] if erp else None,
            "erp_article": erp["bi_name"] if erp else None,
            "erp_emp": erp["emp_name"] if erp else None,
            "suggest_code": sug_code,
            "suggest_article": code_to_item.get(sug_code, {}).get("name") if sug_code else None,
            "payment": row["parsed_payment_type"],
        })

    # --- export HTML: то, чего нет в мониторе (по сумме+дню±1) ---
    mon_keys = set()
    for m in monitor_rows:
        if m["date"] and m["amount"]:
            mon_keys.add((m["date"].isoformat(), round(float(m["amount"]))))

    export_extra = []
    used_exp = set()
    for c in export_rows:
        if not c["amount"] or not c["date"]:
            continue
        key = (c["date"].isoformat(), round(float(c["amount"])))
        # уже есть в мониторе?
        near = any(
            abs((c["date"] - (m["date"] or c["date"])).days) <= 1
            and m["amount"] and amounts_close(m["amount"], c["amount"])
            for m in monitor_rows
        )
        if near:
            continue
        if c["payroll"]:
            cover = find_payroll_cover(c["amount"], c["text"], c["date"], expenses, None)
            if cover and cover["kind"] in ("line", "month_total", "payout_day", "payout_window", "month_partial"):
                export_extra.append({
                    **c,
                    "verdict": f"payroll:{cover['kind']}",
                    "detail": f"{cover['month']:02d}.{cover['year']}: {int(cover['total'])}₽ / {cover['count']} стр.",
                    "erp_id": cover["expense"]["id"] if cover.get("expense") else None,
                })
                continue
            export_extra.append({
                **c,
                "verdict": cover["kind"] if cover else "payroll_gap",
                "detail": (
                    f"ERP {cover['month']:02d}.{cover['year']}: {int(cover['total'])}₽"
                    if cover else "нет разбивки"
                ),
                "erp_id": None,
            })
            continue
        exp, why = match_single_expense(c["amount"], c["text"], c["date"], expenses, used_exp)
        if exp:
            used_exp.add(exp["id"])
            export_extra.append({**c, "verdict": "matched_fuzzy", "detail": why, "erp_id": exp["id"]})
        else:
            sug_code, sug_why = suggest_article(c["text"], code_to_item)
            export_extra.append({
                **c,
                "verdict": "missing",
                "detail": why,
                "erp_id": None,
                "suggest_code": sug_code,
                "suggest_article": code_to_item.get(sug_code, {}).get("name") if sug_code else None,
            })

    # ЗП лист: итоги из монитора+export payroll
    zp_rows = []
    for m in monitor_rows:
        if not m["payroll"] or not m["amount"]:
            continue
        zp_rows.append({
            "date": m["date"],
            "amount_chat": m["amount"],
            "text": m["text"],
            "source": "монитор чата",
            "verdict": m["verdict"],
            "detail": m["detail"],
            "erp_code": m["erp_code"],
        })
    for c in export_extra:
        if not c.get("payroll"):
            continue
        zp_rows.append({
            "date": c["date"],
            "amount_chat": float(c["amount"]),
            "text": c["text"],
            "source": "экспорт HTML (нет в мониторе)",
            "verdict": c["verdict"],
            "detail": c.get("detail"),
            "erp_code": None,
        })

    # месячная сводка ЗП ERP
    zp_month = defaultdict(lambda: {"cash": Decimal(0), "cashless": Decimal(0), "n": 0, "emps": set()})
    for e in expenses:
        if not e["employee_id"]:
            continue
        if e["target_month"] and e["target_year"]:
            key = (int(e["target_year"]), int(e["target_month"]))
        elif e["date"]:
            key = (e["date"].year, e["date"].month)
        else:
            continue
        zp_month[key]["n"] += 1
        zp_month[key]["emps"].add(e["employee_id"])
        if e["payment_type"] == "cash":
            zp_month[key]["cash"] += D(e["amount"])
        else:
            zp_month[key]["cashless"] += D(e["amount"])

    # не в ERP: не payroll и не matched
    truly_missing = [
        m for m in monitor_rows
        if m["verdict"] in ("pending", "missing", "rejected", "unparseable")
        or (m["verdict"] == "payroll_gap")
    ]
    truly_missing_non_zp = [m for m in truly_missing if not m["payroll"] and m["verdict"] != "unparseable"]
    export_missing = [c for c in export_extra if c["verdict"] in ("missing", "payroll_gap", "payroll_unmatched")]

    # перепривязка — только не-ЗП matched с другой статьёй
    rebind = []
    for m in monitor_rows:
        if m["payroll"] or not m["erp_id"]:
            continue
        if not m["suggest_code"] or not m["erp_code"]:
            continue
        if str(m["suggest_code"]) == str(m["erp_code"]):
            continue
        if str(m["erp_code"]) in SALARY_CODES and str(m["suggest_code"]) in SALARY_CODES:
            continue
        rebind.append(m)

    # --- Excel review ---
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    ws.append(["Сверка расходов: чат Telegram + монитор (расходный отчёт) + ERP"])
    ws.append([f"Экспорт: {CHAT_HTML.name}"])
    ws.append([f"Собрано: {datetime.now().strftime('%d.%m.%Y %H:%M')}"])
    ws.append([])
    ws.append(["Показатель", "Значение"])
    ws.append(["Сообщений в мониторе чата (prod) за 2026", len(chat_db)])
    ws.append(["  imported/matched", sum(1 for m in monitor_rows if m["verdict"] in ("linked", "matched_fuzzy") or m["status_db"] in ("imported", "matched"))])
    ws.append(["  payroll покрыто разбивкой", sum(1 for m in monitor_rows if str(m["verdict"]).startswith("payroll:") )])
    ws.append(["  payroll ждём/разрыв", sum(1 for m in monitor_rows if m["verdict"] in ("payroll_wait", "payroll_gap"))])
    ws.append(["  pending / rejected / unparseable", sum(1 for m in monitor_rows if m["status_db"] in ("pending", "rejected", "unparseable"))])
    ws.append(["Экспорт HTML сверх монитора", len(export_extra)])
    ws.append(["Кандидаты «нет в ERP» (не ЗП)", len(truly_missing_non_zp)])
    ws.append(["Кандидаты перепривязки (не ЗП)", len(rebind)])
    ws.append([])
    ws.append(["Как читать ЗП"])
    ws.append(["В чате «400 000р зп рабочие» — это ИТОГ (или часть долга за месяц), а не одна проводка."])
    ws.append(["В ERP та же сумма разнесена по сотрудникам (expense.employee_id + target_month) по табелю."])
    ws.append(["Лист «ЗП чат vs табель» показывает покрытие: month_total / payout_day / month_partial."])
    ws.append(["Такие строки НЕ считаются «упущенными расходами»."])
    ws.append([])
    ws.append(["Монитор чата = то, что ERP уже видит из Telegram (раздел Счета и расходы → анализ чата / tg-chat-gap)."])
    style_header(ws, 5)
    autosize(ws)

    ws = wb.create_sheet("ЗП чат vs табель")
    write_table(
        ws,
        ["Дата чата", "Сумма в чате", "Источник", "Вердикт", "Деталь (ERP)", "Текст", "Код ERP"],
        sorted(zp_rows, key=lambda r: r["date"] or date(1900, 1, 1), reverse=True),
        ["date", "amount_chat", "source", "verdict", "detail", "text", "erp_code"],
    )

    ws = wb.create_sheet("ЗП ERP по месяцам")
    ws.append(["Год", "Месяц", "Строк", "Сотрудников", "Нал", "Безнал", "Итого"])
    style_header(ws)
    for (y, m) in sorted(zp_month.keys()):
        b = zp_month[(y, m)]
        total = b["cash"] + b["cashless"]
        ws.append([y, m, b["n"], len(b["emps"]), float(b["cash"]), float(b["cashless"]), float(total)])
    autosize(ws)

    ws = wb.create_sheet("Монитор чата (расходный)")
    write_table(
        ws,
        [
            "id", "Дата", "Сумма", "Статус БД", "Вердикт сверки", "Деталь",
            "ЗП?", "Код ERP", "Статья ERP", "Сотрудник", "Предложение", "Текст",
        ],
        monitor_rows,
        [
            "id", "date", "amount", "status_db", "verdict", "detail",
            "payroll", "erp_code", "erp_article", "erp_emp", "suggest_code", "text",
        ],
    )

    ws = wb.create_sheet("Не в ERP (не ЗП)")
    write_table(
        ws,
        ["Дата", "Сумма", "Статус", "Вердикт", "Предл. код", "Предл. статья", "Текст"],
        truly_missing_non_zp,
        ["date", "amount", "status_db", "verdict", "suggest_code", "suggest_article", "text"],
    )

    ws = wb.create_sheet("Экспорт без монитора")
    write_table(
        ws,
        ["Дата", "Сумма", "ЗП?", "Вердикт", "Деталь", "Предл. код", "Текст", "От кого"],
        export_extra,
        ["date", "amount", "payroll", "verdict", "detail", "suggest_code", "text", "from"],
    )

    ws = wb.create_sheet("Перепривязка")
    write_table(
        ws,
        ["Дата", "Сумма", "Сейчас код", "Сейчас статья", "Предл. код", "Предл. статья", "ERP id", "Текст", "Действие"],
        rebind,
        ["date", "amount", "erp_code", "erp_article", "suggest_code", "suggest_article", "erp_id", "text"],
    )

    review_path = OUT_DIR / f"rashody-chat-vs-prod-{YEAR}.xlsx"
    wb.save(review_path)
    print("Wrote", review_path)

    # --- Budget 2027: fact ERP; не дублировать payroll totals из чата ---
    fact_by_code = defaultdict(lambda: Decimal(0))
    for e in expenses:
        fact_by_code[str(e["bi_code"] or "UNASSIGNED")] += D(e["amount"])
    plan_by_code = defaultdict(lambda: Decimal(0))
    for p in plans:
        plan_by_code[str(p["code"])] += D(p["amount"])

    # упущенное только non-payroll missing
    missed_by_code = defaultdict(lambda: Decimal(0))
    for m in truly_missing_non_zp:
        if m.get("suggest_code") and m.get("amount"):
            missed_by_code[str(m["suggest_code"])] += D(m["amount"])
    for c in export_missing:
        if c.get("payroll"):
            continue
        if c.get("suggest_code") and c.get("amount"):
            missed_by_code[str(c["suggest_code"])] += D(c["amount"])

    def growth_for(code, name):
        n = norm(name)
        if code.startswith("10") or code.startswith("11") or "зарплат" in n or "налог" in n:
            return Decimal("1.10")
        if code in ("9", "9.1") or "электрич" in n:
            return Decimal("1.12")
        if code == "7.1" or "гсм" in n:
            return Decimal("1.10")
        if code_to_item.get(code, {}).get("is_amortization"):
            return Decimal("1.00")
        return Decimal("1.08")

    wb2 = Workbook()
    ws = wb2.active
    ws.title = "Бюджет 2027"
    ws.append([
        "Код", "Статья", "Fact 2026", "Plan 2026", "Упущено (не ЗП)",
        "База", "Коэфф.", "План 2027", "Аморт.", "Контейнеры", "Комментарий",
    ])
    style_header(ws)
    total_2027 = Decimal(0)
    rows_out = []
    for it in items:
        code = str(it["code"])
        if code == "ARCH.ADJ":
            continue
        fact = fact_by_code.get(code, Decimal(0))
        plan = plan_by_code.get(code, Decimal(0))
        miss = missed_by_code.get(code, Decimal(0))
        base = fact + miss
        if base <= 0 and plan <= 0:
            continue
        if base <= 0:
            base = plan
        if plan > 0 and fact > 0 and not it["is_amortization"]:
            base = max(fact + miss, (fact + plan) / 2)
        g = growth_for(code, it["name"])
        if code == "UNASSIGNED":
            proposed, comment = Decimal(0), "Служебная — разносить, в план не класть"
        elif it["is_amortization"]:
            proposed = (fact + miss).quantize(Decimal("1"))
            comment = "Капекс без авто-роста"
            g = Decimal("1")
        elif it["is_container_purchase"]:
            proposed = ((fact + miss) * Decimal("1.05")).quantize(Decimal("1"))
            comment = "Контейнеры вне общего себеса полей; +5%"
        else:
            proposed = (base * g).quantize(Decimal("1"))
            comment = "Опер. база×коэфф."
            if miss:
                comment += f" +упущено не-ЗП ~{int(miss)}₽"
            if code in SALARY_CODES:
                comment = "ФОТ: только факт ERP (разбивка по людям), чат-итоги не дублируем"
                proposed = (fact * Decimal("1.10")).quantize(Decimal("1"))
                g = Decimal("1.10")
        rows_out.append({
            "code": code, "name": it["name"], "fact": float(fact), "plan": float(plan),
            "miss": float(miss), "base": float(base), "g": float(g),
            "y": float(proposed),
            "amort": "да" if it["is_amortization"] else "",
            "cont": "да" if it["is_container_purchase"] else "",
            "c": comment,
        })
        total_2027 += proposed

    # одна новая статья при необходимости
    if sum(1 for m in truly_missing_non_zp if not m.get("suggest_code") and (m.get("amount") or 0) >= 3000) >= 5:
        rows_out.append({
            "code": "7.2", "name": "Доставки и транспортные услуги (разовые)",
            "fact": 0, "plan": 0, "miss": 0, "base": 150000, "g": 1,
            "y": 150000, "amort": "", "cont": "",
            "c": "НОВАЯ: если доставки сейчас в Хозрасходах/ГСМ",
        })
        total_2027 += Decimal(150000)

    def nk(c):
        return [int(x) if x.isdigit() else x.lower() for x in re.split(r"(\d+)", c["code"])]

    for r in sorted(rows_out, key=nk):
        ws.append([r["code"], r["name"], r["fact"], r["plan"], r["miss"], r["base"], r["g"], r["y"], r["amort"], r["cont"], r["c"]])
    ws.append([])
    ws.append(["", "ИТОГО 2027", "", "", "", "", "", float(total_2027)])
    autosize(ws)
    ws.freeze_panes = "A2"

    ws = wb2.create_sheet("Помесячно 2027")
    ws.append(["Код", "Статья"] + [f"{m:02d}" for m in range(1, 13)] + ["Год"])
    style_header(ws)
    for r in sorted(rows_out, key=nk):
        year_amt = Decimal(str(r["y"]))
        if year_amt <= 0:
            continue
        if r["amort"]:
            months = [0] * 12
            months[2] = float((year_amt * Decimal("0.5")).quantize(Decimal("1")))
            months[8] = float(year_amt) - months[2]
        else:
            base_m = (year_amt / 12).quantize(Decimal("1"))
            months = [float(base_m)] * 12
            months[11] += float(year_amt) - sum(months)
        ws.append([r["code"], r["name"], *months, float(year_amt)])
    autosize(ws)

    ws = wb2.create_sheet("Пояснения")
    for line in [
        "ЗП в бюджете 2027 = факт ERP × 1.10. Крупные суммы из чата («400 тр рабочим») не прибавляются сверху — они уже разнесены по сотрудникам.",
        "Сверка ЗП: лист «ЗП чат vs табель» в первом файле.",
        "Упущено — только не-зарплатные позиции без проводки в expense.",
        "Монитор чата = ChatExpenseMessage (тот же контур, что анализ чата расходов в ERP).",
        "Правки в базу из этих файлов не применялись.",
    ]:
        ws.append([line])
    ws.column_dimensions["A"].width = 110

    budget_path = OUT_DIR / f"budget-{NEXT_YEAR}-draft.xlsx"
    wb2.save(budget_path)
    print("Wrote", budget_path)

    comments_path = write_articles_comments(items, fact_by_code, COMMENTS_XLSX)
    print("Wrote", comments_path)
    print("total 2027", float(total_2027))
    print(
        "payroll covered",
        sum(1 for m in monitor_rows if str(m["verdict"]).startswith("payroll:")),
        "missing non-zp",
        len(truly_missing_non_zp),
    )


if __name__ == "__main__":
    main()
