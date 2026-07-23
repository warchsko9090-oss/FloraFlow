"""Сервис очереди оцифровки PDF-счетов в раздел ВиУМ.

Фаза 2 (текущая):
* `maybe_enqueue(invoice, expense=None)` — после оплаты счёта решает,
  попадает ли он в инбокс ВиУМ. Дедуп по `invoice_id`.

Фаза 3 (наполнение в `phase3_*` шагах):
* `parse_queue_item(item)`     — pdfplumber + Groq, заполняет
  `parsed_payload` и переводит статус в `ready`.
* `suggest_material(text)`     — alias → fuzzy → LLM.
* `commit_intake(item, lines)` — создаёт `ViumOperation(intake)` +
  `ViumOperationLine` + `ViumLot`, бамит alias.
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from datetime import datetime
from decimal import Decimal
from typing import Iterable

from flask import current_app

from app.models import (
    db,
    PaymentInvoice, Expense, BudgetItem,
    ViumInvoiceQueue, ViumMaterial, ViumMaterialAlias,
    ViumOperation, ViumOperationLine,
    AppSetting, TgTask,
)
from app import vium_service

log = logging.getLogger(__name__)

INTAKE_MODE_AUTO = 'auto'
INTAKE_MODE_FORCE = 'force'
INTAKE_MODE_SKIP = 'skip'

# Ключ настройки: создавать ли TgTask-карточку при появлении новых записей в инбоксе.
NOTIFY_SETTING_KEY = 'vium_inbox_notify'
NOTIFY_DEFAULT = '1'  # включено по умолчанию


# ---------------------------------------------------------------------------
# Фаза 2: помещение счёта в очередь после оплаты
# ---------------------------------------------------------------------------

def _is_paid(invoice: PaymentInvoice | None) -> bool:
    if not invoice:
        return False
    return (invoice.status or '').lower() in ('paid', 'partial')


def _should_enqueue(invoice: PaymentInvoice) -> bool:
    """Решает, должен ли этот счёт попасть в инбокс ВиУМ.

    Логика:
      * `vium_intake_mode == 'force'` — да всегда;
      * `vium_intake_mode == 'skip'`  — никогда;
      * иначе — только если у статьи бюджета `is_vium_source=True`.
    """
    mode = (invoice.vium_intake_mode or INTAKE_MODE_AUTO).lower()
    if mode == INTAKE_MODE_FORCE:
        return True
    if mode == INTAKE_MODE_SKIP:
        return False
    item = invoice.item
    return bool(item and getattr(item, 'is_vium_source', False))


def maybe_enqueue(invoice: PaymentInvoice | None,
                  expense: Expense | None = None) -> ViumInvoiceQueue | None:
    """После оплаты счёта поставить его в инбокс, если соответствует
    настройкам. Дедуп по `invoice_id`. НЕ коммитит сессию.

    Возвращает созданную (или уже существующую) запись очереди, либо None.
    """
    if not invoice:
        return None
    try:
        if not _is_paid(invoice):
            return None
        if not _should_enqueue(invoice):
            return None

        existing = ViumInvoiceQueue.query.filter_by(invoice_id=invoice.id).first()
        if existing:
            # Если есть и не в финальном состоянии — обновим expense (если был)
            if existing.expense_id is None and expense is not None:
                existing.expense_id = expense.id
            return existing

        item = ViumInvoiceQueue(
            invoice_id=invoice.id,
            expense_id=(expense.id if expense else None),
            status='new',
            created_at=datetime.now(),
        )
        db.session.add(item)
        db.session.flush()

        try:
            _create_inbox_tg_task(item)
        except Exception:
            log.exception('vium_inbox: failed to create TgTask card')

        return item
    except Exception:
        log.exception('vium_inbox.maybe_enqueue failed (invoice_id=%s)',
                      getattr(invoice, 'id', None))
        return None


def _create_inbox_tg_task(item: ViumInvoiceQueue) -> TgTask | None:
    """Опциональная карточка в дашборде для админа.

    Видна на ленте /dashboard, ссылка ведёт прямо в карточку инбокса.
    """
    setting = AppSetting.query.get(NOTIFY_SETTING_KEY)
    enabled = (setting.value if setting else NOTIFY_DEFAULT)
    if str(enabled).strip().lower() in ('0', 'false', 'no', 'off'):
        return None

    inv = item.invoice
    title = 'Новый счёт для ВиУМ — оцифровать'
    if inv and inv.original_name:
        title = f'Оцифровать счёт ВиУМ: {inv.original_name}'

    body_lines = ['Новый счёт ждёт оцифровки в раздел «Учет ВиУМ».']
    if inv:
        body_lines.append(f'Счёт: {inv.original_name or inv.filename}')
        if inv.amount is not None:
            body_lines.append(f'Сумма: {inv.amount} ₽')
        if inv.item:
            body_lines.append(f'Статья: {inv.item.name}')
    body_lines.append(f'Откройте: /vium/inbox/{item.id}')
    raw = '\n'.join(body_lines)

    dedup_key = f'vium_inbox:{item.id}'

    existing = TgTask.query.filter_by(dedup_key=dedup_key).first()
    if existing:
        return existing

    task = TgTask(
        raw_text=raw,
        title=title,
        details=raw,
        assignee_role='admin',
        action_type='vium_inbox',
        action_payload=json.dumps({'queue_id': item.id, 'invoice_id': item.invoice_id}),
        status='new',
        severity='info',
        dedup_key=dedup_key,
        source='vium',
        created_at=datetime.now(),
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now(),
    )
    db.session.add(task)
    return task


# ---------------------------------------------------------------------------
# Фаза 3: парсер PDF + suggest + commit_intake
# ---------------------------------------------------------------------------

ALIAS_KEY_LIMIT = 200
ALIAS_WORDS = 4


def _normalize_alias_key(text: str | None) -> str:
    """Нормализует строку счёта в ключ для поиска по алиасам.

    Берём первые несколько значимых слов, lower, без знаков препинания.
    """
    if not text:
        return ''
    s = unicodedata.normalize('NFKC', text)
    s = s.lower()
    s = re.sub(r'[^0-9a-zа-яё\s]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    if not s:
        return ''
    words = s.split(' ')
    key = ' '.join(words[:ALIAS_WORDS])
    return key[:ALIAS_KEY_LIMIT]


def suggest_material(description: str) -> int | None:
    """Подсказывает `ViumMaterial.id` по строке счёта.

    Цепочка: точный alias → fuzzy → LLM. Возвращает id материала или None.
    """
    if not description:
        return None
    key = _normalize_alias_key(description)
    if not key:
        return None

    # 1) Точный alias.
    alias = (
        ViumMaterialAlias.query
        .filter_by(alias_key=key)
        .order_by(ViumMaterialAlias.hit_count.desc())
        .first()
    )
    if alias:
        return alias.material_id

    # 2) Fuzzy по названиям материалов.
    try:
        from rapidfuzz import process, fuzz
        materials = ViumMaterial.query.filter(
            ViumMaterial.is_archived == False  # noqa: E712
        ).all()
        choices = {m.id: m.name.lower() for m in materials}
        if choices:
            target = description.lower()
            best = process.extractOne(
                target, choices, scorer=fuzz.WRatio
            )
            # process.extractOne для dict возвращает (value, score, key)
            if best and best[1] >= 80:
                return best[2]
    except Exception:
        log.debug('rapidfuzz suggest skipped', exc_info=True)

    return None


def remember_alias(description: str, material_id: int,
                   user_id: int | None = None) -> None:
    """Запомнить связку «строка счёта → материал»."""
    key = _normalize_alias_key(description)
    if not key or not material_id:
        return
    rec = (
        ViumMaterialAlias.query
        .filter_by(alias_key=key, material_id=material_id)
        .first()
    )
    if rec:
        rec.hit_count = (rec.hit_count or 0) + 1
        rec.last_used_at = datetime.now()
        return
    db.session.add(ViumMaterialAlias(
        alias_key=key,
        material_id=material_id,
        hit_count=1,
        last_used_at=datetime.now(),
        created_by_user_id=user_id,
    ))


# ----- PDF parsing -----------------------------------------------------------

def _invoice_pdf_path(invoice: PaymentInvoice) -> str | None:
    try:
        upload = current_app.config.get('UPLOAD_FOLDER') or ''
        if not upload:
            return None
        path = os.path.join(upload, 'invoices', invoice.filename)
        return path if os.path.isfile(path) else None
    except Exception:
        return None


def parse_queue_item(item: ViumInvoiceQueue) -> ViumInvoiceQueue:
    """Извлечь позиции из PDF, заполнить `parsed_payload`,
    перевести `status='ready'`. Не коммитит.
    """
    if not item.invoice:
        item.status = 'error'
        item.error = 'Счёт не найден'
        return item

    path = _invoice_pdf_path(item.invoice)
    if not path:
        item.status = 'error'
        item.error = 'Файл PDF не найден на сервере'
        return item

    item.status = 'parsing'
    db.session.flush()

    try:
        from app.vium_pdf_parser import extract_invoice_lines
    except Exception as e:
        item.status = 'error'
        item.error = f'Парсер недоступен: {e}'
        return item

    try:
        lines = extract_invoice_lines(path)
    except Exception as e:
        log.exception('vium_pdf_parser failed')
        item.status = 'error'
        item.error = f'Ошибка парсера: {e}'
        return item

    if not lines:
        item.status = 'error'
        item.error = 'В PDF не удалось распознать ни одной позиции.'
        return item

    item.parsed_payload = json.dumps(lines, ensure_ascii=False)
    item.parsed_at = datetime.now()
    item.status = 'ready'
    item.error = None
    return item


def commit_intake(item: ViumInvoiceQueue,
                  lines_payload: list[dict],
                  user_id: int | None = None) -> ViumOperation:
    """Создаёт ViumOperation(intake) + строки + партии. Не коммитит.

    `lines_payload` — список словарей `{material_id, qty, unit_price,
    description}` от UI.
    """
    if not lines_payload:
        raise ValueError('Нечего проводить — список позиций пуст.')

    inv = item.invoice
    op = ViumOperation(
        kind='intake',
        date=(datetime.now().date() if not inv or not inv.created_at
              else inv.created_at.date()),
        comment=(inv.original_name if inv else None),
        invoice_id=item.invoice_id,
        created_by_user_id=user_id,
    )
    db.session.add(op)
    db.session.flush()

    for ln in lines_payload:
        try:
            mid = int(ln['material_id'])
        except (KeyError, TypeError, ValueError):
            continue
        qty = Decimal(str(ln.get('qty') or 0))
        if qty <= 0:
            continue
        price = Decimal(str(ln.get('unit_price') or 0))
        if price < 0:
            price = Decimal('0')
        desc = (ln.get('description') or '').strip() or None

        line = ViumOperationLine(
            operation_id=op.id,
            material_id=mid,
            qty=qty,
            unit_price=price,
            note=desc,
        )
        db.session.add(line)

        if desc:
            remember_alias(desc, mid, user_id=user_id)

    db.session.flush()
    vium_service.apply_intake(op)

    item.status = 'done'
    item.operation_id = op.id
    item.processed_at = datetime.now()

    try:
        dedup_key = f'vium_inbox:{item.id}'
        task = TgTask.query.filter_by(dedup_key=dedup_key).first()
        if task and task.status not in ('done', 'completed'):
            task.status = 'done'
            task.completed_at = datetime.now()
            if user_id:
                task.completed_by_id = user_id
    except Exception:
        log.debug('cannot close inbox tg task', exc_info=True)

    return op
