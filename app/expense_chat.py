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

from app.models import (
    db, Expense, BudgetItem, TgTask, User,
    ChatExpenseMessage, ChatExpenseAlias,
)
from app.utils import msk_now, msk_today


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
            model="llama-3.3-70b-versatile",
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
        if not tg_chat_id or not tg_message_id or not text:
            return {"ok": False, "error": "empty"}

        # Идемпотентность: если это сообщение уже видели — ничего не делаем.
        existing = ChatExpenseMessage.query.filter_by(
            tg_chat_id=tg_chat_id, tg_message_id=tg_message_id,
        ).first()
        if existing is not None:
            return {"ok": True, "status": existing.status, "chat_expense_id": existing.id}

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

        # 1) Уже есть такой расход в БД — ставим реакцию и закрываем.
        dup = find_duplicate_expense(
            parsed["amount"], parsed["description"], msg_dt.date()
        )
        if dup is not None:
            row.status = "matched"
            row.expense_id = dup.id
            db.session.add(row)
            db.session.commit()
            _safe_react(tg_chat_id, tg_message_id, "✅")
            return {"ok": True, "status": "matched", "expense_id": dup.id}

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
    title = f"Расход из ТГ: {row.parsed_amount:.0f} ₽"
    details_parts = [
        f"<b>{row.parsed_amount:.0f} ₽</b> — {row.parsed_description}",
    ]
    if row.parsed_payment_type:
        details_parts.append(
            "наличные" if row.parsed_payment_type == "cash" else "безнал"
        )
    if row.sender_name:
        details_parts.append(f"От: {row.sender_name}")
    details_parts.append(f"Источник: ТГ-чат «Расходы Жемчужниково»")
    if source == "alias":
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


def _safe_react(chat_id, message_id, emoji="✅"):
    """Ставит реакцию, ничего не роняя если бот/TG недоступны."""
    try:
        from app import telegram as tg
        tg.set_reaction(chat_id, message_id, emoji)
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
        )
        db.session.add(expense)
        db.session.flush()

        row.expense_id = expense.id
        row.status = "imported"
        row.suggested_budget_item_id = final_item_id

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

    _safe_react(row.tg_chat_id, row.tg_message_id, "✅")
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
