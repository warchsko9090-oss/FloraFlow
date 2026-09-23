"""AI-монитор чата «Расходы Жемчужниково».

Подключается к существующему Telegram-webhook: каждое новое сообщение из чата
с `chat_id == TG_CHAT_ID_EXPENSES` прогоняется через парсер, сверяется с
таблицей `Expense` и, если записи нет, кладётся карточкой на дашборд админу.
Подтверждения админа обучают классификатор статей бюджета (таблица
`ChatExpenseAlias`).

Поток:
    TG message
        |
        v
    ingest_message(msg)          # сохраняем ChatExpenseMessage
        |
        v
    parse_expense_text()         # regex: amount + desc + cash/cashless
        |
        v
    find_duplicate_expense()     # есть ли в БД уже такой расход?
        |                |
        | yes            | no
        v                v
    status=matched    classify_budget_item()  # alias -> Groq
     реакция '✅'          |
                           v
                    create TgTask  + карточка на дашборде

Логика идемпотентна: уникальный индекс (tg_chat_id, tg_message_id) в
`ChatExpenseMessage` защищает от повторной обработки при ретраях вебхука.


НАСТРОЙКА НА СТОРОНЕ TELEGRAM / ПРОДА
=====================================

1. В BotFather (`@BotFather`):
   `/mybots` -> выбрать `@FloraFlovvBot`
     -> Bot Settings -> Group Privacy -> **Turn OFF**
   Без этого бот увидит в чате только сообщения с @упоминанием и
   webhook не получит текст обычных расходов.

2. Добавить `@FloraFlovvBot` администратором в чат
   «Расходы Жемчужниково». Достаточно роли без прав на кик/модерацию —
   главное, чтобы он читал сообщения (см. п.1).

3. Выставить переменные окружения на Amvera:
     TG_CHAT_ID_EXPENSES  — id чата «Расходы Жемчужниково»
                            (можно получить в любом TG-клиенте: ссылка на
                             чат в web.telegram.org содержит `#-100...`,
                             супергруппам id начинается с `-100`).
     TG_ALLOWED_CHAT_IDS  — в список добавить то же значение, чтобы
                            webhook не отрезал чат на фазе whitelist.
     GROQ_API_KEY         — если нужен LLM-классификатор статей бюджета
                            (опционально; без него остаётся только
                             обучающаяся таблица алиасов).

4. Убедиться, что `TG_BOT_TOKEN` уже задан (общий для всех чатов).

5. После деплоя: перейти в чат и написать тестовое сообщение формата
   «1р - тест. нал» — в дашборде админа должна появиться карточка
   «Расход из ТГ», в TG на сообщении — реакция ✅ (для дублей) или
   задача останется в фиде до подтверждения.

6. Авто-режим (Phase 2): пока выключен. Ключи оставлены для будущего
   `EXPENSE_AUTO_APPLY=1` + `EXPENSE_AUTO_APPLY_MIN_HITS=N` — когда
   таблица `ChatExpenseAlias` наберёт уверенности.
"""
from __future__ import annotations

import json
import os
import re
import traceback
from datetime import datetime, timedelta, date
from decimal import Decimal, InvalidOperation

from flask import current_app
from sqlalchemy import and_, extract, func, or_

from app.models import (
    db, Expense, BudgetItem, TgTask, User, PaymentInvoice,
    ChatExpenseMessage, ChatExpenseAlias,
)
from app.utils import msk_now, msk_today, MONTH_NAMES


# ---------------------------------------------------------------------------
# ПАРСЕР
# ---------------------------------------------------------------------------

# «4700р - оплата трактора на погрузку. нал»
#  ^^^^  ^                              ^^^^^
#  сумма разделитель        описание    тип оплаты

# Сумма: цифры с возможными пробелами/точками-разделителями, затем маркер
# валюты («р», «р.», «руб», «рубл.», «₽»). Пример: «19 913р», «1.500руб», «3 550р».
_AMOUNT_RE = re.compile(
    r"""^\s*
        (?P<num>\d[\d\s\.\,]*?)           # цифры (возможны пробелы/точки/запятые)
        \s*
        (?:р\.?|руб(?:\.|лей|ля)?|₽)     # р / р. / руб / руб. / рублей / ₽
        \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Тип оплаты: ищем отдельно «безнал» (и его формы) и «нал». ВНИМАНИЕ к порядку:
# 'безнал' содержит 'нал', поэтому сначала пробуем 'безнал*'.
_PAYMENT_CASHLESS_RE = re.compile(r"\bбезнал\w*\b", re.IGNORECASE)
_PAYMENT_CASH_RE = re.compile(r"\bнал\w*\b", re.IGNORECASE)

# Для пост-чистки описания вырезаем типичный «хвост» с типом оплаты.
_PAYMENT_TRIM_RE = re.compile(
    r"[\s\.,:;—–\-]*\b(безнал\w*|нал\w*)\b[\s\.]*$",
    re.IGNORECASE,
)

_PAYROLL_RE = re.compile(
    r"(?i)(?<![а-яa-z])(з/?п|зарплат\w*|аванс\w*)(?![а-яa-z])",
)

_MONTH_STEMS = (
    (re.compile(r'(?i)январ'), 1),
    (re.compile(r'(?i)феврал'), 2),
    (re.compile(r'(?i)март'), 3),
    (re.compile(r'(?i)апрел'), 4),
    (re.compile(r'(?i)\bма[йяе]\b'), 5),
    (re.compile(r'(?i)июн'), 6),
    (re.compile(r'(?i)июл'), 7),
    (re.compile(r'(?i)август'), 8),
    (re.compile(r'(?i)сентябр'), 9),
    (re.compile(r'(?i)октябр'), 10),
    (re.compile(r'(?i)ноябр'), 11),
    (re.compile(r'(?i)декабр'), 12),
)
_YEAR_RE = re.compile(r'\b(20\d{2})\b')


def _clean_amount(raw: str) -> Decimal | None:
    """«19 913» / «1.500» / «2,500» → Decimal. Если пусто/невалид — None."""
    if not raw:
        return None
    cleaned = re.sub(r"[\s]", "", raw)
    # Если видим и «.» и «,» — считаем «.» разделителем тысяч (РФ запись).
    if "." in cleaned and "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    # Убираем точки-разделители тысяч вида «1.500», если их несколько или
    # цифр после последней точки больше 2 — значит это не копейки.
    if cleaned.count(".") >= 1:
        parts = cleaned.split(".")
        if len(parts) > 2 or (len(parts[-1]) != 2):
            cleaned = "".join(parts)
    try:
        value = Decimal(cleaned)
        if value <= 0:
            return None
        return value
    except (InvalidOperation, ValueError):
        return None


def parse_expense_text(text: str) -> dict | None:
    """Разбирает сообщение. Возвращает `None`, если не удалось распознать
    сумму (тогда считаем это не расходом).

    Пример результата:
        {'amount': Decimal('4700'),
         'description': 'оплата трактора на погрузку',
         'payment_type': 'cash'}
    """
    if not text:
        return None
    first_line = text.split("\n", 1)[0].strip()

    m = _AMOUNT_RE.match(first_line)
    if not m:
        return None
    amount = _clean_amount(m.group("num"))
    if amount is None:
        return None

    # Всё после суммы → хвост, из которого вытащим тип оплаты.
    tail = first_line[m.end():]

    # Убираем ведущие «-», «—», «:», пробелы.
    tail = re.sub(r"^[\s\-\—\–\:\.]+", "", tail)

    # Тип оплаты (безнал важнее нал).
    payment_type = None
    if _PAYMENT_CASHLESS_RE.search(tail):
        payment_type = "cashless"
    elif _PAYMENT_CASH_RE.search(tail):
        payment_type = "cash"

    # Описание = хвост без финального «. нал»/«. безнал» и висячих точек.
    description = _PAYMENT_TRIM_RE.sub("", tail).strip(" .,-—–:\t")

    # Если описание пустое (бывает, когда сообщение — чистая «сумма. нал»),
    # пишем плейсхолдер — админ сам уточнит при подтверждении.
    if not description:
        description = "Без назначения"

    return {
        "amount": amount,
        "description": description[:500],
        "payment_type": payment_type,
    }


# ---------------------------------------------------------------------------
# НОРМАЛИЗАЦИЯ И ОБУЧАЮЩИЕ АЛИАСЫ
# ---------------------------------------------------------------------------

def _normalize_alias_key(description: str, max_words: int = 4) -> str:
    """Приводит описание к «ключу алиаса»: lowercase, без пунктуации,
    первые `max_words` слов длиной > 2 символов.

    Пример: «оплата трактора на погрузку» → «оплата трактора погрузку»
    """
    if not description:
        return ""
    txt = description.lower()
    txt = re.sub(r"[^\w\s]+", " ", txt, flags=re.UNICODE)
    tokens = [t for t in txt.split() if len(t) > 2]
    return " ".join(tokens[:max_words])


def _bump_alias(alias_key: str, budget_item_id: int, user_id: int | None = None):
    """Инкрементирует/создаёт запись в `ChatExpenseAlias`. Не коммитит —
    вызывающий код обязан сделать `db.session.commit()` сам (обычно вместе
    с созданием Expense)."""
    if not alias_key or not budget_item_id:
        return
    row = ChatExpenseAlias.query.filter_by(
        alias_key=alias_key, budget_item_id=budget_item_id,
    ).first()
    now = msk_now()
    if row is None:
        db.session.add(ChatExpenseAlias(
            alias_key=alias_key,
            budget_item_id=budget_item_id,
            created_by_user_id=user_id,
            hit_count=1,
            last_used_at=now,
        ))
    else:
        row.hit_count = (row.hit_count or 0) + 1
        row.last_used_at = now


# ---------------------------------------------------------------------------
# КЛАССИФИКАТОР СТАТЕЙ БЮДЖЕТА
# ---------------------------------------------------------------------------

def _top_alias_for(description: str) -> ChatExpenseAlias | None:
    """Ищем лучший алиас: по точному `alias_key`, затем по «первому слову»
    (на случай, если бот уже видел более короткое описание).
    """
    key = _normalize_alias_key(description)
    if not key:
        return None
    # Полное совпадение ключа → берём с максимальным hit_count.
    row = (
        ChatExpenseAlias.query.filter_by(alias_key=key)
        .order_by(ChatExpenseAlias.hit_count.desc())
        .first()
    )
    if row:
        return row
    # Fallback: первое значимое слово.
    first = key.split(" ", 1)[0] if key else ""
    if len(first) >= 4:
        return (
            ChatExpenseAlias.query.filter(
                ChatExpenseAlias.alias_key.like(f"{first}%")
            )
            .order_by(ChatExpenseAlias.hit_count.desc())
            .first()
        )
    return None


def _llm_classify(description: str) -> int | None:
    """Спрашиваем у Groq, какая статья бюджета ближе к описанию. Возвращает
    `budget_item_id` или None. Любые ошибки — молча, классификатор НЕ
    обязан работать; админ при подтверждении всё равно сам выберет статью.
    """
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        return None
    items = BudgetItem.query.order_by(BudgetItem.name).all()
    if not items:
        return None
    try:
        from groq import Groq
        from app.groq_util import groq_model_classify
        client = Groq(api_key=api_key, timeout=10)

        catalog = "\n".join(
            f"{it.id}|{it.code or ''}|{it.name}" for it in items
        )
        prompt = (
            "Ты — бухгалтер. На вход — короткое описание расхода "
            "(русский, с разговорными сокращениями) и список статей "
            "бюджета в формате 'id|код|название'.\n"
            "Верни ТОЛЬКО число — id подходящей статьи. Никаких слов, "
            "никаких пояснений. Если подходящей статьи точно нет — "
            "верни 0.\n\n"
            f"Описание: {description}\n\n"
            f"Статьи:\n{catalog}"
        )
        resp = client.chat.completions.create(
            model=groq_model_classify(),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=8,
        )
        raw = (resp.choices[0].message.content or "").strip()
        m = re.search(r"\d+", raw)
        if not m:
            return None
        val = int(m.group(0))
        if val <= 0:
            return None
        # Удостоверимся, что возвращённый id реально существует в каталоге.
        if not any(it.id == val for it in items):
            return None
        return val
    except Exception:
        traceback.print_exc()
        return None


def classify_budget_item(description: str) -> tuple[int | None, str]:
    """Возвращает `(budget_item_id, source)`:
        source = 'alias' | 'llm' | 'none'
    """
    alias = _top_alias_for(description)
    if alias is not None:
        return alias.budget_item_id, "alias"
    llm_id = _llm_classify(description)
    if llm_id:
        return llm_id, "llm"
    return None, "none"


# ---------------------------------------------------------------------------
# ДЕДУП ПРОТИВ EXPENSE
# ---------------------------------------------------------------------------

def find_duplicate_expense(
    amount: Decimal,
    description: str,
    ref_date: date,
    days_window: int = 3,
    fuzzy_threshold: int = 70,
) -> Expense | None:
    """Пытаемся найти уже проведённый в БД расход, соответствующий
    сообщению. Критерии:
      • одна и та же сумма (строгое сравнение Decimal);
      • дата расхода в пределах `days_window` от даты сообщения;
      • fuzzy-совпадение описания ≥ `fuzzy_threshold` (rapidfuzz).
    Если rapidfuzz недоступен — сравниваем по пересечению значимых слов.
    """
    if amount is None:
        return None
    lo = ref_date - timedelta(days=days_window)
    hi = ref_date + timedelta(days=days_window)
    candidates = Expense.query.filter(
        Expense.amount == amount,
        Expense.date >= lo,
        Expense.date <= hi,
    ).all()
    if not candidates:
        return None

    norm_desc = _normalize_alias_key(description, max_words=10)

    try:
        from rapidfuzz import fuzz
        best = None
        best_score = 0
        for c in candidates:
            other = _normalize_alias_key(c.description or "", max_words=10)
            score = fuzz.token_set_ratio(norm_desc, other) if norm_desc and other else 0
            if score > best_score:
                best_score = score
                best = c
        if best is not None and best_score >= fuzzy_threshold:
            return best
        # Если описание пустое у обеих сторон, но суммы совпали — считаем
        # это дублем (редкий случай «перевёл 1000р. нал» без назначения).
        if not norm_desc:
            return candidates[0]
        return None
    except Exception:
        # Fallback без rapidfuzz: хотя бы одно общее слово длиной > 3.
        a = set(norm_desc.split())
        for c in candidates:
            b = set(_normalize_alias_key(c.description or "").split())
            if a & b:
                return c
        return candidates[0] if not a else None


def find_matching_invoice(amount: Decimal, description: str, fuzzy_threshold: int = 55):
    """Неоплаченный счёт с близкой суммой и похожим назначением."""
    if amount is None:
        return None
    try:
        amount = Decimal(str(amount))
    except (InvalidOperation, TypeError, ValueError):
        return None
    unpaid = PaymentInvoice.query.filter(PaymentInvoice.status == 'new').all()
    if not unpaid:
        return None
    delta = max(Decimal('100'), (amount * Decimal('0.05')))
    close = []
    for inv in unpaid:
        try:
            inv_amt = Decimal(str(inv.amount or 0))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if abs(inv_amt - amount) <= delta:
            close.append(inv)
    if not close:
        return None
    if len(close) == 1 and not (description or '').strip():
        return close[0]
    hay = _normalize_alias_key(description or '', max_words=12)
    try:
        from rapidfuzz import fuzz
        best, best_score = None, 0
        for inv in close:
            text = ' '.join(filter(None, [
                inv.summary, inv.comment, inv.original_name,
            ]))
            other = _normalize_alias_key(text, max_words=12)
            score = fuzz.token_set_ratio(hay, other) if hay and other else 40
            if score > best_score:
                best_score, best = score, inv
        if best is not None and (best_score >= fuzzy_threshold or (len(close) == 1 and best_score >= 35)):
            return best
        return close[0] if len(close) == 1 else None
    except Exception:
        return close[0]


def is_payroll_chat_text(text: str | None) -> bool:
    return bool(_PAYROLL_RE.search(text or ''))


def parse_payroll_month_year(text: str | None, ref_date: date | None = None) -> tuple[int, int]:
    ref = ref_date or msk_today()
    blob = text or ''
    month = None
    for rx, num in _MONTH_STEMS:
        if rx.search(blob):
            month = num
            break
    year = ref.year
    ym = _YEAR_RE.search(blob)
    if ym:
        year = int(ym.group(1))
    elif month and month > ref.month + 1:
        year = ref.year - 1
    return (month or ref.month), year


def _amounts_close(a, b) -> bool:
    try:
        aa = Decimal(str(a or 0))
        bb = Decimal(str(b or 0))
    except (InvalidOperation, TypeError, ValueError):
        return False
    if aa == bb:
        return True
    return abs(aa - bb) <= max(Decimal('100'), aa * Decimal('0.015'))


def _payroll_expenses(month: int, year: int, payment_type: str | None = None) -> list[Expense]:
    q = Expense.query.filter(Expense.employee_id.isnot(None))
    q = q.filter(or_(
        and_(Expense.target_month == month, Expense.target_year == year),
        and_(
            Expense.target_month.is_(None),
            extract('month', Expense.date) == month,
            extract('year', Expense.date) == year,
        ),
    ))
    if payment_type in ('cash', 'cashless'):
        q = q.filter(Expense.payment_type == payment_type)
    return q.all()


def find_payroll_cover(
    amount: Decimal,
    description: str,
    ref_date: date,
    payment_type: str | None = None,
) -> dict | None:
    """ЗП в чате одной суммой, в ERP — по сотрудникам из табеля."""
    if amount is None or not is_payroll_chat_text(description):
        return None
    month, year = parse_payroll_month_year(description, ref_date)
    rows = _payroll_expenses(month, year, payment_type)
    if not rows:
        rows = _payroll_expenses(month, year, None)
    if not rows:
        return None
    for exp in rows:
        if _amounts_close(exp.amount, amount):
            return {'kind': 'line', 'expense': exp, 'month': month, 'year': year}
    total = sum((Decimal(str(x.amount or 0)) for x in rows), Decimal(0))
    if _amounts_close(total, amount):
        return {
            'kind': 'batch',
            'expense': rows[0],
            'month': month,
            'year': year,
            'total': total,
            'count': len(rows),
        }
    return None


# ---------------------------------------------------------------------------
# ТОЧКА ВХОДА ИЗ WEBHOOK
# ---------------------------------------------------------------------------

def _tg_date_to_dt(unix_ts) -> datetime | None:
    if not unix_ts:
        return None
    try:
        return datetime.utcfromtimestamp(int(unix_ts)) + timedelta(hours=3)
    except Exception:
        return None


def _sender_label(msg: dict) -> str:
    sender = msg.get("from") or {}
    first = sender.get("first_name") or ""
    username = (sender.get("username") or "").lstrip("@")
    if first and username:
        return f"{first} (@{username})"
    return first or (f"@{username}" if username else "unknown")


def ingest_message(msg: dict) -> dict:
    """Обрабатывает одно сообщение из чата расходов. Возвращает dict с
    результатом (для логов/тестов). Никогда не кидает — при ошибке пишет
    в current_app.logger и возвращает {'ok': False, 'error': ...}.

    Аргумент `msg` — то, что Telegram кладёт в ключ `message` (или
    `edited_message` / `channel_post`) в update-пейлоаде.
    """
    try:
        text = (msg.get("text") or msg.get("caption") or "").strip()
        chat = msg.get("chat") or {}
        tg_chat_id = str(chat.get("id") or "")
        tg_message_id = int(msg.get("message_id") or 0)
        if not tg_chat_id or not tg_message_id:
            return {"ok": False, "error": "empty"}

        sender = msg.get("from") or {}
        if sender.get("is_bot"):
            return {"ok": True, "status": "bot_skip"}

        # Идемпотентность: если это сообщение уже видели — ничего не делаем.
        existing = ChatExpenseMessage.query.filter_by(
            tg_chat_id=tg_chat_id, tg_message_id=tg_message_id,
        ).first()
        if existing is not None:
            return {"ok": True, "status": existing.status, "chat_expense_id": existing.id}

        from app.bank_slip import extract_telegram_media, ingest_expenses_chat_media
        media = extract_telegram_media(msg)
        if media:
            bank = ingest_expenses_chat_media(msg, media)
            if bank.get("handled"):
                return bank

        if not text:
            return {"ok": False, "error": "empty"}

        parsed = parse_expense_text(text)
        msg_dt = _tg_date_to_dt(msg.get("date")) or msk_now()
        sender = _sender_label(msg)

        row = ChatExpenseMessage(
            tg_chat_id=tg_chat_id,
            tg_message_id=tg_message_id,
            tg_date=msg_dt,
            raw_text=text[:4000],
            sender_name=sender,
            status="pending",
        )

        if parsed is None:
            # Не смогли распознать сумму — не расход. Сохраняем запись,
            # чтобы не пытаться обрабатывать снова, но задач не создаём.
            row.status = "unparseable"
            db.session.add(row)
            db.session.commit()
            return {"ok": True, "status": "unparseable"}

        row.parsed_amount = parsed["amount"]
        row.parsed_description = parsed["description"]
        row.parsed_payment_type = parsed["payment_type"]

        payroll = find_payroll_cover(
            parsed["amount"],
            parsed["description"] or text,
            msg_dt.date(),
            parsed["payment_type"],
        )
        if is_payroll_chat_text(parsed["description"] or text):
            if payroll:
                row.status = "matched"
                row.expense_id = payroll["expense"].id if payroll.get("expense") else None
                db.session.add(row)
                db.session.commit()
                _safe_react(tg_chat_id, tg_message_id, None)
                return {
                    "ok": True,
                    "status": "payroll_matched",
                    "chat_expense_id": row.id,
                }
            row.status = "payroll"
            db.session.add(row)
            db.session.commit()
            _safe_react(tg_chat_id, tg_message_id, None)
            return {"ok": True, "status": "payroll", "chat_expense_id": row.id}

        # 1) Уже есть такой расход в БД — ставим реакцию и закрываем.
        dup = find_duplicate_expense(
            parsed["amount"], parsed["description"], msg_dt.date()
        )
        if dup is not None:
            row.status = "matched"
            row.expense_id = dup.id
            db.session.add(row)
            db.session.commit()
            _safe_react(tg_chat_id, tg_message_id, None)
            return {"ok": True, "status": "matched", "expense_id": dup.id}

        # 1.5) Похоже на неоплаченный счёт — не создаём обычный расход сразу.
        inv_hit = find_matching_invoice(parsed["amount"], parsed["description"])
        if inv_hit is not None:
            row.status = "invoice_match"
            row.matched_invoice_id = inv_hit.id
            if inv_hit.budget_item_id:
                row.suggested_budget_item_id = inv_hit.budget_item_id
            task = _create_task_for_chat_expense(row, source="invoice")
            db.session.add(row)
            db.session.flush()
            if task is not None:
                task.action_payload = json.dumps({
                    "chat_expense_id": row.id,
                    "invoice_id": inv_hit.id,
                    "url": f"/expenses/chat/{row.id}",
                    "amount": str(parsed["amount"]),
                    "description": parsed["description"],
                    "payment_type": parsed["payment_type"],
                    "suggested_budget_item_id": row.suggested_budget_item_id,
                    "classifier_source": "invoice",
                    "tg_chat_id": tg_chat_id,
                    "tg_message_id": tg_message_id,
                    "sender": sender,
                }, ensure_ascii=False)
                db.session.add(task)
                db.session.flush()
                row.task_id = task.id
            db.session.commit()
            return {
                "ok": True, "status": "invoice_match",
                "chat_expense_id": row.id,
                "invoice_id": inv_hit.id,
            }

        # 2) Нет дубля — подсказываем статью бюджета и создаём TgTask.
        suggested_id, source = classify_budget_item(parsed["description"])
        row.suggested_budget_item_id = suggested_id

        task = _create_task_for_chat_expense(row, source=source)
        db.session.add(row)
        db.session.flush()  # чтобы row.id появился
        if task is not None:
            task.action_payload = json.dumps({
                "chat_expense_id": row.id,
                "url": f"/expenses/chat/{row.id}",
                "amount": str(parsed["amount"]),
                "description": parsed["description"],
                "payment_type": parsed["payment_type"],
                "suggested_budget_item_id": suggested_id,
                "classifier_source": source,
                "tg_chat_id": tg_chat_id,
                "tg_message_id": tg_message_id,
                "sender": sender,
            }, ensure_ascii=False)
            db.session.add(task)
            db.session.flush()
            row.task_id = task.id
        db.session.commit()
        return {
            "ok": True, "status": "pending",
            "chat_expense_id": row.id,
            "suggested_budget_item_id": suggested_id,
            "classifier_source": source,
        }
    except Exception as exc:
        db.session.rollback()
        try:
            current_app.logger.exception("expense_chat.ingest_message failed")
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}


def _create_task_for_chat_expense(row: ChatExpenseMessage, source: str) -> TgTask | None:
    """Собирает TgTask для фида админа. Не добавляет в сессию (см. ingest_message).

    Дедуп через `dedup_key='chat_expense:msg=<id>'` — на случай повторных
    прогонов одного и того же `ChatExpenseMessage` (не должно происходить,
    но стратегически страхуемся).
    """
    now = msk_now()
    if source == "miniapp" and row.parsed_amount is not None:
        title = f"Быстрый расход: {row.parsed_amount:.0f} ₽"
        details_parts = [
            f"<b>{row.parsed_amount:.0f} ₽</b> — {row.parsed_description}",
        ]
    elif row.parsed_amount is not None:
        title = f"Расход из ТГ: {row.parsed_amount:.0f} ₽"
        details_parts = [
            f"<b>{row.parsed_amount:.0f} ₽</b> — {row.parsed_description}",
        ]
    else:
        title = "Чек из ТГ: разобрать вручную"
        details_parts = [
            row.parsed_description or "Фото чека, сумму не разобрали.",
        ]
    if row.parsed_payment_type:
        details_parts.append(
            "наличные" if row.parsed_payment_type == "cash" else "безнал"
        )
    if row.sender_name:
        details_parts.append(f"От: {row.sender_name}")
    if source == "miniapp":
        details_parts.append("Источник: Mini App · быстрый расход")
    else:
        details_parts.append("Источник: ТГ-чат «Расходы Жемчужниково»")
    if source == "invoice":
        details_parts.append("Похоже на неоплаченный счёт — подтвердите в Mini App «Счета».")
    elif source == "receipt":
        details_parts.append("Фото чека сохранено в базе — откройте вложение на карточке.")
    elif source == "miniapp":
        has_file = bool(row.raw_text and '[файл]' in row.raw_text)
        details_parts.append(
            "Файл прикреплён — откройте вложение на карточке."
            if has_file else
            "Проверьте статью и проведите расход."
        )
    elif source == "alias":
        details_parts.append("Подсказка статьи — из обучения по прошлым подтверждениям.")
    elif source == "llm":
        details_parts.append("Подсказка статьи — от AI-классификатора.")
    else:
        details_parts.append("Подсказку статьи не определили — выберите вручную.")

    return TgTask(
        raw_text=row.raw_text[:4000] if row.raw_text else "",
        title=title,
        details="\n".join(details_parts),
        action_type="chat_expense",
        status="new",
        # Расход видят и админ, и руководитель: один из них обычно
        # и подтверждает/отклоняет. Формат CSV — такой же, как у аномалий,
        # _role_match в main.py умеет его ловить.
        assignee_role="admin,executive",
        sender_name=row.sender_name or "tg.expenses",
        source="chat_expense",
        dedup_key=f"chat_expense:msg={row.tg_chat_id}:{row.tg_message_id}",
        first_seen_at=now,
        last_seen_at=now,
        severity="warning",
    )


def ingest_miniapp_quick_expense(
    user: User,
    *,
    amount: Decimal,
    summary: str,
    payment_type: str = 'cashless',
    file_bytes: bytes | None = None,
    filename: str | None = None,
) -> dict:
    """Быстрый расход из Mini App: карточка админу как у AI-агента чата расходов.

    Expense не создаём — админ подтвердит статью на дашборде.
    """
    from app.bank_slip import file_hash, BankSlip

    summary = (summary or '').strip()[:500]
    if not summary:
        return {'ok': False, 'error': 'need_summary'}
    if amount is None or amount <= 0:
        return {'ok': False, 'error': 'bad_amount'}
    ptype = 'cash' if payment_type == 'cash' else 'cashless'
    sender = (getattr(user, 'username', None) or f'user#{user.id}')[:150]
    tg_chat_id = f'miniapp:{user.id}'
    # Уникальный message_id в пределах пользователя
    base = int(msk_now().timestamp() * 1000) % 1_000_000_000
    tg_message_id = base
    for _ in range(20):
        exists = ChatExpenseMessage.query.filter_by(
            tg_chat_id=tg_chat_id, tg_message_id=tg_message_id,
        ).first()
        if not exists:
            break
        tg_message_id = (tg_message_id + 1) % 1_000_000_000

    slip_id = None
    has_file = bool(file_bytes)
    if file_bytes:
        digest = file_hash(file_bytes)
        slip = BankSlip.query.filter_by(file_hash=digest).first()
        if not slip:
            slip = BankSlip(
                file_hash=digest,
                original_name=(filename or 'receipt.jpg')[:255],
                file_blob=file_bytes,
                source='miniapp',
                kind='receipt',
                tg_chat_id=tg_chat_id[:64],
                tg_message_id=tg_message_id,
                created_by_user_id=user.id,
            )
            db.session.add(slip)
            db.session.flush()
        slip_id = slip.id

    suggested_id, clf_source = classify_budget_item(summary)
    raw = summary
    if has_file:
        raw = f'{summary}\n[файл]'

    row = ChatExpenseMessage(
        tg_chat_id=tg_chat_id,
        tg_message_id=tg_message_id,
        tg_date=msk_now(),
        raw_text=raw[:4000],
        sender_name=sender,
        status='pending',
        parsed_amount=amount,
        parsed_description=summary,
        parsed_payment_type=ptype,
        suggested_budget_item_id=suggested_id,
    )
    db.session.add(row)
    db.session.flush()

    task = _create_task_for_chat_expense(row, source='miniapp')
    if task is not None:
        db.session.add(task)
        db.session.flush()
        task.action_payload = json.dumps({
            'chat_expense_id': row.id,
            'slip_id': slip_id,
            'url': f'/expenses/chat/{row.id}',
            'amount': str(amount),
            'description': summary,
            'payment_type': ptype,
            'suggested_budget_item_id': suggested_id,
            'classifier_source': clf_source or 'miniapp',
            'tg_chat_id': tg_chat_id,
            'tg_message_id': tg_message_id,
            'sender': sender,
            'file_url': f'/api/bank-slips/{slip_id}/file' if slip_id else '',
        }, ensure_ascii=False)
        row.task_id = task.id

    db.session.commit()
    return {
        'ok': True,
        'chat_expense_id': row.id,
        'task_id': row.task_id,
        'slip_id': slip_id,
        'suggested_budget_item_id': suggested_id,
        'amount': float(amount),
        'summary': summary,
        'payment_type': ptype,
        'sender': sender,
        'has_file': has_file,
        'file_bytes': file_bytes if has_file else None,
        'filename': (filename or 'receipt.jpg') if has_file else None,
    }


def _safe_react(chat_id, message_id, emoji="✅"):
    """Ставит реакцию или снимает её (emoji=None / ''). Не роняет ingest."""
    try:
        from app import telegram as tg
        tg.set_reaction(chat_id, message_id, emoji or '')
    except Exception:
        try:
            current_app.logger.warning("set_reaction failed", exc_info=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# ОПЕРАЦИИ АДМИНА (вызываются из HTTP-роутов /api/expenses/chat/<id>/*)
# ---------------------------------------------------------------------------

def confirm_chat_expense(
    msg_id: int,
    user: User,
    budget_item_id: int | None = None,
    expense_date: date | None = None,
) -> tuple[bool, str]:
    """Админ подтвердил, что это реальный расход. Создаём Expense в БД,
    обучаем классификатор (алиас), закрываем TgTask, ставим реакцию.
    Возвращает (ok, message).
    """
    row = ChatExpenseMessage.query.get(msg_id)
    if row is None:
        return False, "not_found"
    if row.status in ("imported", "rejected"):
        return False, f"already_{row.status}"
    if row.parsed_amount is None:
        return False, "unparseable"

    final_item_id = budget_item_id or row.suggested_budget_item_id
    if not final_item_id:
        return False, "budget_item_required"
    item = BudgetItem.query.get(final_item_id)
    if item is None:
        return False, "budget_item_not_found"

    use_date = expense_date or (row.tg_date.date() if row.tg_date else msk_today())
    payment_type = row.parsed_payment_type or "cashless"

    try:
        expense = Expense(
            date=use_date,
            budget_item_id=final_item_id,
            description=row.parsed_description or row.raw_text[:500],
            amount=row.parsed_amount,
            payment_type=payment_type,
            invoice_id=row.matched_invoice_id,
        )
        db.session.add(expense)
        db.session.flush()

        row.expense_id = expense.id
        row.status = "imported"
        row.suggested_budget_item_id = final_item_id

        if row.matched_invoice_id:
            inv = PaymentInvoice.query.get(row.matched_invoice_id)
            if inv is not None and inv.status == "new":
                inv.status = "paid"
                try:
                    from app.vium_inbox import maybe_enqueue as _vium_enqueue
                    _vium_enqueue(inv)
                except Exception:
                    current_app.logger.exception("vium_inbox after chat-invoice match")

        # Обучаем классификатор.
        alias_key = _normalize_alias_key(row.parsed_description or "")
        if alias_key:
            _bump_alias(alias_key, final_item_id, user_id=getattr(user, "id", None))

        # Закрываем TgTask, если он был.
        if row.task_id:
            task = TgTask.query.get(row.task_id)
            if task is not None and task.status != "done":
                task.status = "done"
                task.completed_at = msk_now()
                task.completed_by_id = getattr(user, "id", None)

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        try:
            current_app.logger.exception("confirm_chat_expense failed")
        except Exception:
            pass
        return False, str(exc)

    _safe_react(row.tg_chat_id, row.tg_message_id, None)
    return True, "ok"


def reject_chat_expense(msg_id: int, user: User) -> tuple[bool, str]:
    """Админ пометил «не расход» / дубль, импортировать не нужно.
    Закрываем TgTask, ставим реакцию-крестик (❌), alias НЕ трогаем.
    """
    row = ChatExpenseMessage.query.get(msg_id)
    if row is None:
        return False, "not_found"
    if row.status in ("imported", "rejected"):
        return False, f"already_{row.status}"

    try:
        desc = row.parsed_description or row.raw_text or ''
        if is_payroll_chat_text(desc):
            row.status = "payroll"
        else:
            row.status = "rejected"
        if row.task_id:
            task = TgTask.query.get(row.task_id)
            if task is not None and task.status != "done":
                task.status = "done"
                task.completed_at = msk_now()
                task.completed_by_id = getattr(user, "id", None)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        try:
            current_app.logger.exception("reject_chat_expense failed")
        except Exception:
            pass
        return False, str(exc)

    if row.status == "payroll":
        _safe_react(row.tg_chat_id, row.tg_message_id, None)
    else:
        _safe_react(row.tg_chat_id, row.tg_message_id, "👀")
    return True, "ok"


def reclassify_chat_expense(
    msg_id: int,
    user: User,
    budget_item_id: int,
) -> tuple[bool, str]:
    """Админ меняет подсказанную статью (до подтверждения). Сохраняет
    выбор в `suggested_budget_item_id` — чтобы при нажатии «Подтвердить»
    Expense ушёл с этой статьёй.
    """
    row = ChatExpenseMessage.query.get(msg_id)
    if row is None:
        return False, "not_found"
    if row.status in ("imported", "rejected"):
        return False, f"already_{row.status}"
    item = BudgetItem.query.get(budget_item_id)
    if item is None:
        return False, "budget_item_not_found"
    try:
        row.suggested_budget_item_id = budget_item_id
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return False, str(exc)
    return True, "ok"


def telegram_message_url(chat_id, message_id) -> str | None:
    if not chat_id or not message_id:
        return None
    s = str(chat_id).strip()
    if s.startswith('-100'):
        return f'https://t.me/c/{s[4:]}/{int(message_id)}'
    if s.startswith('-'):
        return f'https://t.me/c/{s.lstrip("-")}/{int(message_id)}'
    return None


def _row_day(row: ChatExpenseMessage) -> date | None:
    if row.tg_date:
        return row.tg_date.date() if hasattr(row.tg_date, 'date') else row.tg_date
    if row.created_at:
        return row.created_at.date() if hasattr(row.created_at, 'date') else row.created_at
    return None


def _expense_still_there(row: ChatExpenseMessage) -> Expense | None:
    if not row.expense_id:
        return None
    return Expense.query.get(row.expense_id)


def classify_chat_row_vs_db(row: ChatExpenseMessage) -> dict:
    """Есть ли уже расход в ERP по этому сообщению чата."""
    day = _row_day(row) or msk_today()
    desc = row.parsed_description or row.raw_text or ''
    linked = _expense_still_there(row)
    if linked is not None:
        return {
            'in_db': True,
            'reason': 'linked',
            'expense': linked,
            'invoice': None,
        }
    if row.parsed_amount is not None:
        dup = find_duplicate_expense(
            Decimal(str(row.parsed_amount)),
            desc,
            day,
        )
        if dup is not None:
            return {'in_db': True, 'reason': 'matched_now', 'expense': dup, 'invoice': None}
        pay = find_payroll_cover(
            Decimal(str(row.parsed_amount)),
            desc,
            day,
            row.parsed_payment_type,
        )
        if pay is not None:
            return {
                'in_db': True,
                'reason': 'payroll_split',
                'expense': pay.get('expense'),
                'invoice': None,
            }
        if is_payroll_chat_text(desc):
            return {'in_db': False, 'reason': 'payroll_wait', 'expense': None, 'invoice': None}
    inv = None
    if row.matched_invoice_id:
        inv = PaymentInvoice.query.get(row.matched_invoice_id)
        if inv and (inv.expenses or []):
            return {
                'in_db': True,
                'reason': 'invoice_expenses',
                'expense': inv.expenses[0],
                'invoice': inv,
            }
    if row.status == 'rejected':
        return {'in_db': False, 'reason': 'rejected', 'expense': None, 'invoice': inv}
    if row.parsed_amount is None:
        return {'in_db': False, 'reason': 'unparseable', 'expense': None, 'invoice': inv}
    if row.status == 'invoice_match':
        return {'in_db': False, 'reason': 'invoice_match', 'expense': None, 'invoice': inv}
    return {'in_db': False, 'reason': row.status or 'pending', 'expense': None, 'invoice': inv}


def _close_chat_task(row: ChatExpenseMessage) -> None:
    if not row.task_id:
        return
    task = TgTask.query.get(row.task_id)
    if task is not None and task.status != 'done':
        task.status = 'done'
        task.completed_at = msk_now()


def reconcile_chat_expense_row(row: ChatExpenseMessage) -> dict:
    """Сверить с базой: ЗП по табелю, обычные расходы. Снять стикер, если уже учтено."""
    hit = classify_chat_row_vs_db(row)
    if hit['in_db']:
        _close_chat_task(row)
        if row.status not in ('imported', 'matched'):
            row.status = 'matched'
        if hit.get('expense') is not None and not row.expense_id:
            row.expense_id = hit['expense'].id
        _safe_react(row.tg_chat_id, row.tg_message_id, None)
    elif hit['reason'] == 'payroll_wait':
        _close_chat_task(row)
        if row.status not in ('imported', 'matched'):
            row.status = 'payroll'
        _safe_react(row.tg_chat_id, row.tg_message_id, None)
    return hit


def reconcile_chat_expenses_period(start: date, end: date) -> int:
    extra = timedelta(days=20)
    lo = start - extra
    hi = end + extra
    day_col = func.coalesce(
        func.date(ChatExpenseMessage.tg_date),
        func.date(ChatExpenseMessage.created_at),
    )
    rows = ChatExpenseMessage.query.filter(day_col >= lo, day_col <= hi).all()
    n = 0
    for row in rows:
        before = (row.status, row.expense_id)
        reconcile_chat_expense_row(row)
        if (row.status, row.expense_id) != before:
            n += 1
    if n:
        db.session.commit()
    return n


def reconcile_payroll_for_month(month: int, year: int) -> int:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return reconcile_chat_expenses_period(start, end)


def analyze_unrecorded_chat_expenses(start: date, end: date) -> dict:
    """Сообщения чата расходов за период, которых нет в регистре Expense."""
    if end < start:
        start, end = end, start
    try:
        reconcile_chat_expenses_period(start, end)
    except Exception:
        db.session.rollback()
        try:
            current_app.logger.exception('reconcile chat expenses failed')
        except Exception:
            pass
    day_col = func.coalesce(
        func.date(ChatExpenseMessage.tg_date),
        func.date(ChatExpenseMessage.created_at),
    )
    rows = (
        ChatExpenseMessage.query
        .filter(day_col >= start, day_col <= end)
        .order_by(ChatExpenseMessage.id.asc())
        .all()
    )
    missing = []
    unparsed = []
    rejected = []
    in_db = []
    payroll_wait = []
    for row in rows:
        hit = classify_chat_row_vs_db(row)
        payload = {
            'row': row,
            'day': _row_day(row),
            'reason': hit['reason'],
            'expense': hit['expense'],
            'invoice': hit['invoice'],
            'tg_url': telegram_message_url(row.tg_chat_id, row.tg_message_id),
        }
        if hit['in_db']:
            in_db.append(payload)
        elif hit['reason'] == 'unparseable':
            unparsed.append(payload)
        elif hit['reason'] == 'rejected':
            rejected.append(payload)
        elif hit['reason'] == 'payroll_wait':
            payroll_wait.append(payload)
        else:
            missing.append(payload)
    missing_sum = sum(
        (Decimal(str(x['row'].parsed_amount or 0)) for x in missing),
        Decimal(0),
    )
    return {
        'start': start,
        'end': end,
        'total': len(rows),
        'missing': missing,
        'unparsed': unparsed,
        'rejected': rejected,
        'in_db': in_db,
        'payroll_wait': payroll_wait,
        'missing_sum': missing_sum,
        'in_db_sum': sum(
            (Decimal(str(x['row'].parsed_amount or 0)) for x in in_db),
            Decimal(0),
        ),
        'payroll_wait_sum': sum(
            (Decimal(str(x['row'].parsed_amount or 0)) for x in payroll_wait),
            Decimal(0),
        ),
    }

