"""PDF счетов: источник правды — колонка payment_invoice.file_blob."""
from __future__ import annotations

import io
import os
import re
from decimal import Decimal

from flask import current_app, send_file

from app.models import PaymentInvoice, db


def disk_path(inv: PaymentInvoice) -> str | None:
    upload = current_app.config.get('UPLOAD_FOLDER') or ''
    if not upload or not inv.filename:
        return None
    path = os.path.join(upload, 'invoices', inv.filename)
    return path if os.path.isfile(path) else None


def invoice_bytes(inv: PaymentInvoice) -> bytes | None:
    blob = getattr(inv, 'file_blob', None)
    if blob:
        return bytes(blob)
    path = disk_path(inv)
    if not path:
        return None
    with open(path, 'rb') as fh:
        return fh.read()


def has_file(inv: PaymentInvoice) -> bool:
    if getattr(inv, 'file_blob', None):
        return True
    return bool(disk_path(inv))


def has_receipt(inv: PaymentInvoice) -> bool:
    return bool(getattr(inv, 'receipt_name', None) or getattr(inv, 'receipt_blob', None))


def receipt_bytes(inv: PaymentInvoice) -> bytes | None:
    blob = getattr(inv, 'receipt_blob', None)
    if blob:
        return bytes(blob)
    return None


def flask_send_receipt(inv: PaymentInvoice, *, as_attachment: bool = True):
    data = receipt_bytes(inv)
    if not data:
        return None
    name = getattr(inv, 'receipt_name', None) or 'receipt.jpg'
    mime = 'image/jpeg'
    lower = name.lower()
    if lower.endswith('.png'):
        mime = 'image/png'
    elif lower.endswith('.webp'):
        mime = 'image/webp'
    elif lower.endswith('.pdf'):
        mime = 'application/pdf'
    return send_file(
        io.BytesIO(data),
        mimetype=mime,
        as_attachment=as_attachment,
        download_name=name,
    )


def attach_file(inv: PaymentInvoice, data: bytes, save_name: str | None = None) -> str:
    """Пишет blob и кэш на диск. Возвращает имя файла."""
    if save_name:
        inv.filename = save_name
    inv.file_blob = data
    name = inv.filename or save_name or 'invoice.pdf'
    upload = current_app.config.get('UPLOAD_FOLDER') or ''
    if upload:
        inv_dir = os.path.join(upload, 'invoices')
        os.makedirs(inv_dir, exist_ok=True)
        path = os.path.join(inv_dir, name)
        try:
            with open(path, 'wb') as fh:
                fh.write(data)
        except OSError:
            pass
    return name


def attach_receipt(inv: PaymentInvoice, data: bytes, name: str | None = None) -> str:
    """Подтверждение оплаты (ПП, скрин, фото) → receipt_blob."""
    safe = (name or 'receipt.jpg').replace('\\', '/').split('/')[-1][:255] or 'receipt.jpg'
    inv.receipt_blob = data
    inv.receipt_name = safe
    return safe


def materialize_path(inv: PaymentInvoice) -> str | None:
    """Путь к файлу для парсеров (pdfplumber). При необходимости выгружает blob."""
    existing = disk_path(inv)
    if existing:
        return existing
    data = invoice_bytes(inv)
    if not data:
        return None
    upload = current_app.config.get('UPLOAD_FOLDER') or ''
    if not upload:
        return None
    inv_dir = os.path.join(upload, 'invoices')
    os.makedirs(inv_dir, exist_ok=True)
    name = inv.filename or f'inv_{inv.id}.pdf'
    path = os.path.join(inv_dir, name)
    with open(path, 'wb') as fh:
        fh.write(data)
    if not inv.filename:
        inv.filename = name
    return path


def flask_send(inv: PaymentInvoice, *, as_attachment: bool = True):
    data = invoice_bytes(inv)
    if not data:
        return None
    name = inv.original_name or inv.filename or 'invoice.pdf'
    mime = 'application/pdf'
    lower = name.lower()
    if lower.endswith('.png'):
        mime = 'image/png'
    elif lower.endswith(('.jpg', '.jpeg', '.jfif')):
        mime = 'image/jpeg'
    elif lower.endswith('.webp'):
        mime = 'image/webp'
    return send_file(
        io.BytesIO(data),
        mimetype=mime,
        as_attachment=as_attachment,
        download_name=name,
    )


def delete_unpaid_invoice(inv: PaymentInvoice) -> str | None:
    """Удаляет неоплаченный счёт и привязанные факты. Не коммитит. None = ок."""
    if (inv.status or '') == 'paid':
        return 'Оплаченный счёт нельзя удалить'
    for kid in list(getattr(inv, 'fact_invoices', None) or []):
        err = delete_unpaid_invoice(kid)
        if err:
            return err
    from app.models import ChatExpenseMessage, Expense
    ChatExpenseMessage.query.filter_by(matched_invoice_id=inv.id).update(
        {'matched_invoice_id': None}, synchronize_session=False
    )
    Expense.query.filter_by(invoice_id=inv.id).update(
        {'invoice_id': None}, synchronize_session=False
    )
    try:
        from app.models import BankSlipItem
        BankSlipItem.query.filter_by(invoice_id=inv.id).update(
            {'invoice_id': None}, synchronize_session=False
        )
    except Exception:
        pass
    try:
        from app.models import ViumInvoiceQueue, ViumOperation, ViumLot
        ViumInvoiceQueue.query.filter_by(invoice_id=inv.id).delete(synchronize_session=False)
        ViumOperation.query.filter_by(invoice_id=inv.id).update(
            {'invoice_id': None}, synchronize_session=False
        )
        ViumLot.query.filter_by(source_invoice_id=inv.id).update(
            {'source_invoice_id': None}, synchronize_session=False
        )
    except Exception:
        pass
    path = disk_path(inv)
    db.session.delete(inv)
    if path:
        try:
            os.remove(path)
        except OSError:
            pass
    return None


def ensure_expense_for_paid_invoice(inv: PaymentInvoice):
    """Пишет расход по оплаченному счёту, если его ещё нет. Не коммитит.

    Без статьи бюджета расход всё равно создаётся на служебную статью
    «К разнесению» — чтобы оплата не пропадала и попадала админу на дашборд.
    """
    from decimal import Decimal

    from app.models import Expense, BudgetItem
    from app.utils import msk_today

    existing = Expense.query.filter_by(invoice_id=inv.id).first()
    if existing:
        return existing
    budget_id = inv.budget_item_id
    unassigned = False
    if not budget_id:
        try:
            from app.expense_chat import classify_budget_item
            text = ' '.join(filter(None, [inv.summary, inv.comment, inv.original_name]))
            budget_id, _src = classify_budget_item(text)
        except Exception:
            budget_id = None
    if not budget_id:
        budget_id = ensure_unassigned_budget_item().id
        unassigned = True
        inv.budget_item_id = inv.budget_item_id or budget_id
    desc = (inv.summary or inv.comment or inv.original_name or 'Счёт на оплату').strip()[:500]
    if unassigned and not desc.lower().startswith('без статьи'):
        desc = f'Без статьи · {desc}'[:500]
    amt = inv.amount or Decimal('0')
    if amt <= 0 and inv.planned_amount:
        amt = inv.planned_amount
    ptype = inv.payment_type if getattr(inv, 'payment_type', None) in ('cash', 'cashless') else 'cashless'
    exp = Expense(
        date=msk_today(),
        budget_item_id=budget_id,
        description=desc,
        amount=amt,
        payment_type=ptype,
        invoice_id=inv.id,
    )
    db.session.add(exp)
    db.session.flush()
    return exp


UNASSIGNED_BUDGET_CODE = 'UNASSIGNED'


def ensure_unassigned_budget_item():
    """Служебная статья для оплат без выбранной статьи бюджета."""
    from app.models import BudgetItem, db
    item = BudgetItem.query.filter_by(code=UNASSIGNED_BUDGET_CODE).first()
    if item:
        return item
    item = BudgetItem(
        code=UNASSIGNED_BUDGET_CODE,
        name='К разнесению (без статьи)',
        is_amortization=False,
        is_vium_source=False,
    )
    db.session.add(item)
    db.session.flush()
    return item


def invoice_base_amount(inv: PaymentInvoice) -> Decimal:
    """Сумма к отображению: у плана — planned_amount, иначе amount."""
    if (getattr(inv, 'kind', None) or '') == 'plan':
        return Decimal(str(inv.planned_amount or 0))
    return Decimal(str(inv.amount or 0))


def invoice_paid_amount(inv: PaymentInvoice) -> Decimal:
    """Уже оплачено по счёту/плану (факты + расходы)."""
    from sqlalchemy import func
    from app.models import Expense, PaymentInvoice as PI
    total = Decimal('0')
    if (getattr(inv, 'kind', None) or '') == 'plan':
        kids = list(getattr(inv, 'fact_invoices', None) or [])
        if not kids:
            kids = PI.query.filter_by(plan_id=inv.id).all()
        for kid in kids:
            total += Decimal(str(kid.amount or 0))
        if has_file(inv):
            total += Decimal(str(inv.amount or 0))
        return total
    for e in (getattr(inv, 'expenses', None) or []):
        total += Decimal(str(e.amount or 0))
    if total <= 0:
        exp_sum = db.session.query(func.coalesce(func.sum(Expense.amount), 0)).filter(
            Expense.invoice_id == inv.id
        ).scalar()
        total = Decimal(str(exp_sum or 0))
    return total


def invoice_remaining_amount(inv: PaymentInvoice) -> Decimal:
    base = invoice_base_amount(inv)
    paid = invoice_paid_amount(inv)
    left = base - paid
    return left if left > 0 else Decimal('0')


def _paid_chat_text(inv: PaymentInvoice) -> str:
    """Строка как в чате расходов: «10000р- гсм. Безнал»."""
    purpose = (inv.summary or inv.comment or inv.original_name or 'счёт').strip()
    purpose = re.sub(r'(?i)^оплата\s*[·•\-–—]\s*', '', purpose).strip()
    plan = getattr(inv, 'plan', None)
    if plan is not None:
        parent = (plan.summary or plan.comment or plan.original_name or '').strip()
        if parent:
            purpose = parent
    purpose = re.sub(r'\s+', ' ', purpose)
    if purpose.endswith('.'):
        purpose = purpose[:-1].rstrip()
    try:
        val = float(inv.amount or 0)
    except (TypeError, ValueError):
        val = 0
    if val <= 0 and getattr(inv, 'planned_amount', None):
        try:
            val = float(inv.planned_amount or 0)
        except (TypeError, ValueError):
            pass
    if abs(val - round(val)) < 0.005:
        amount = str(int(round(val)))
    else:
        amount = f"{val:.2f}".replace('.', ',')
    ptype = 'Нал' if getattr(inv, 'payment_type', None) == 'cash' else 'Безнал'
    return f'{amount}р- {purpose}. {ptype}'


def notify_invoice_paid_chat(inv: PaymentInvoice) -> None:
    """В чат «Расходы»: подтверждение (фото/PDF) + подпись «10000р- гсм. Безнал»."""
    from app.telegram import send_document, send_photo_bytes, send_message

    text = _paid_chat_text(inv)
    caption = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    data = receipt_bytes(inv) or invoice_bytes(inv)
    name = (
        getattr(inv, 'receipt_name', None)
        or getattr(inv, 'original_name', None)
        or getattr(inv, 'filename', None)
        or 'payment.jpg'
    )
    lower = (name or '').lower()
    ok, err = False, 'no media'
    if data:
        is_image = lower.endswith(('.jpg', '.jpeg', '.png', '.webp', '.bmp', '.gif'))
        if is_image:
            ok, err = send_photo_bytes(data, filename=name, caption=caption, chat_type='expenses')
            if not ok:
                ok, err = send_document(
                    filename=name, caption=caption, file_bytes=data, chat_type='expenses',
                )
        else:
            ok, err = send_document(
                filename=name, caption=caption, file_bytes=data, chat_type='expenses',
            )
    if not ok:
        ok, err = send_message(caption, chat_type='expenses')
    if not ok:
        try:
            current_app.logger.warning('notify invoice paid chat failed: %s', err)
        except Exception:
            pass


def backfill_blobs(logger=None) -> int:
    """Копирует PDF с диска в БД, если blob ещё пустой."""
    n = 0
    try:
        rows = PaymentInvoice.query.filter(
            (PaymentInvoice.file_blob.is_(None)) | (PaymentInvoice.file_blob == b'')
        ).all()
    except Exception as exc:
        if logger:
            logger.warning('invoice blob backfill skipped: %s', exc)
        db.session.rollback()
        return 0
    for inv in rows:
        path = disk_path(inv)
        if not path:
            continue
        try:
            with open(path, 'rb') as fh:
                inv.file_blob = fh.read()
            n += 1
        except OSError:
            continue
    if n:
        db.session.commit()
        if logger:
            logger.info('invoice blob backfill: %s files', n)
    return n
