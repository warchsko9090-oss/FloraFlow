"""Каталог файлов системы: ссылки на исходники, без копирования в архив."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime

from flask import current_app, url_for
from sqlalchemy import func, or_
from sqlalchemy.orm import undefer

from app.models import (
    BankSlip,
    Client,
    Employee,
    FileArchive,
    ForeignEmployeeDocument,
    Order,
    PatentPayment,
    Payment,
    PaymentInvoice,
    RegistrationRenewal,
    SaleInvoice,
    db,
)

PAGE_SIZE = 40

CATEGORIES = (
    'Поступление',
    'Отгрузка',
    'Инвентаризация',
    'Финансы',
    'Прочее',
    'Счёт поставщика',
    'Квитанция',
    'Выписка',
    'Оплата заказа',
    'Счёт клиенту',
    'Кадры',
    'Фото отгрузки',
    'Фото работ',
)

_VIEW_EXT = {'.pdf', '.png', '.jpg', '.jpeg', '.webp', '.gif'}
_DISK_ROOTS = ('shipment', 'photo')


@dataclass
class CatalogRow:
    name: str
    category: str
    comment: str
    when: datetime | None
    size: int
    source_label: str
    source_url: str | None
    file_url: str
    delete_url: str | None


def _dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return None


def _size_label(n: int) -> str:
    if not n:
        return '—'
    if n < 1024:
        return f'{n} Б'
    if n < 1024 * 1024:
        return f'{n / 1024:.1f} КБ'
    return f'{n / 1024 / 1024:.1f} МБ'


def _viewable(name: str) -> bool:
    ext = os.path.splitext(name or '')[1].lower()
    return ext in _VIEW_EXT


def _open_url(source: str, item_id: int, name: str) -> str:
    return url_for(
        'directory.archive_open',
        source=source,
        item_id=item_id,
        dl=0 if _viewable(name) else 1,
    )


def _disk_url(rel: str, name: str) -> str:
    return url_for(
        'directory.archive_open_disk',
        p=rel,
        dl=0 if _viewable(name) else 1,
    )


def _upload_root() -> str:
    return os.path.abspath(current_app.config.get('UPLOAD_FOLDER') or '')


def under_upload(path: str) -> bool:
    root = _upload_root()
    if not root or not path:
        return False
    target = os.path.abspath(path)
    return target == root or target.startswith(root + os.sep)


def safe_disk_rel(rel: str) -> str | None:
    rel = (rel or '').replace('\\', '/').lstrip('/')
    parts = [p for p in rel.split('/') if p]
    if not parts or any(p in ('.', '..') for p in parts):
        return None
    if parts[0] not in _DISK_ROOTS:
        return None
    return '/'.join(parts)


def _safe_collect(fn):
    try:
        return list(fn())
    except Exception:
        current_app.logger.exception('file catalog: %s', getattr(fn, '__name__', fn))
        db.session.rollback()
        return []


def _manual_rows():
    rows = []
    for f in FileArchive.query.order_by(FileArchive.id.desc()).all():
        name = f.original_name or f.filename or 'файл'
        rows.append(CatalogRow(
            name=name,
            category=f.category or 'Прочее',
            comment=(f.comment or '').strip(),
            when=_dt(f.uploaded_at),
            size=int(f.size_bytes or 0),
            source_label='Загружен в архив',
            source_url=None,
            file_url=_open_url('manual', f.id, name),
            delete_url=url_for('directory.archive_delete', file_id=f.id),
        ))
    return rows


def _invoice_rows():
    rows = []
    q = (
        db.session.query(
            PaymentInvoice.id,
            PaymentInvoice.original_name,
            PaymentInvoice.filename,
            PaymentInvoice.created_at,
            PaymentInvoice.comment,
            PaymentInvoice.summary,
            func.coalesce(func.length(PaymentInvoice.file_blob), 0),
        )
        .filter(or_(
            PaymentInvoice.filename.isnot(None),
            PaymentInvoice.file_blob.isnot(None),
        ))
    )
    upload = _upload_root()
    for inv_id, original, filename, created, comment, summary, blob_len in q:
        name = original or filename or f'invoice_{inv_id}.pdf'
        size = int(blob_len or 0)
        if size <= 0 and filename and upload:
            path = os.path.join(upload, 'invoices', filename)
            if os.path.isfile(path):
                size = os.path.getsize(path)
        if size <= 0 and not filename and not blob_len:
            continue
        note = (summary or comment or '').strip()
        rows.append(CatalogRow(
            name=name,
            category='Счёт поставщика',
            comment=note,
            when=_dt(created),
            size=size,
            source_label=f'Счёт #{inv_id}',
            source_url=url_for('finance.expenses', tab='invoices'),
            file_url=_open_url('invoice', inv_id, name),
            delete_url=None,
        ))
    return rows


def _receipt_rows():
    rows = []
    q = (
        db.session.query(
            PaymentInvoice.id,
            PaymentInvoice.receipt_name,
            PaymentInvoice.created_at,
            PaymentInvoice.summary,
            func.coalesce(func.length(PaymentInvoice.receipt_blob), 0),
        )
        .filter(PaymentInvoice.receipt_name.isnot(None))
    )
    for inv_id, receipt_name, created, summary, blob_len in q:
        name = receipt_name or f'receipt_{inv_id}'
        rows.append(CatalogRow(
            name=name,
            category='Квитанция',
            comment=(summary or '').strip(),
            when=_dt(created),
            size=int(blob_len or 0),
            source_label=f'Счёт #{inv_id}',
            source_url=url_for('finance.expenses', tab='invoices'),
            file_url=_open_url('receipt', inv_id, name),
            delete_url=None,
        ))
    return rows


def _slip_rows():
    rows = []
    q = (
        db.session.query(
            BankSlip.id,
            BankSlip.original_name,
            BankSlip.created_at,
            BankSlip.kind,
            func.coalesce(func.length(BankSlip.file_blob), 0),
        )
        .filter(BankSlip.file_blob.isnot(None))
    )
    for slip_id, original, created, kind, blob_len in q:
        name = original or f'slip_{slip_id}'
        rows.append(CatalogRow(
            name=name,
            category='Выписка',
            comment=(kind or '').strip(),
            when=_dt(created),
            size=int(blob_len or 0),
            source_label=f'Выписка #{slip_id}',
            source_url=None,
            file_url=_open_url('slip', slip_id, name),
            delete_url=None,
        ))
    return rows


def _payment_rows():
    rows = []
    q = (
        db.session.query(
            Payment.id,
            Payment.order_id,
            Payment.date,
            Payment.comment,
            Payment.file_path,
            Client.name,
        )
        .join(Order, Order.id == Payment.order_id)
        .outerjoin(Client, Client.id == Order.client_id)
        .filter(Payment.file_path.isnot(None), Payment.file_path != '')
    )
    for pay_id, order_id, pay_date, comment, path, client_name in q:
        if not path or not os.path.isfile(path) or not under_upload(path):
            continue
        name = os.path.basename(path)
        who = client_name or 'заказ'
        note = (comment or '').strip()
        if note:
            note = f'{who}. {note}'
        else:
            note = who
        rows.append(CatalogRow(
            name=name,
            category='Оплата заказа',
            comment=note,
            when=_dt(pay_date),
            size=os.path.getsize(path),
            source_label=f'Заказ #{order_id}',
            source_url=url_for('orders.order_detail', order_id=order_id),
            file_url=_open_url('payment', pay_id, name),
            delete_url=None,
        ))
    return rows


def _sale_rows():
    rows = []
    q = (
        db.session.query(
            SaleInvoice.id,
            SaleInvoice.file_name,
            SaleInvoice.created_at,
            SaleInvoice.buyer_name,
            SaleInvoice.doc_number,
            SaleInvoice.doc_year,
            SaleInvoice.order_id,
            SaleInvoice.status,
            func.coalesce(func.length(SaleInvoice.file_blob), 0),
        )
        .filter(
            SaleInvoice.status != 'discarded',
            or_(SaleInvoice.file_name.isnot(None), SaleInvoice.file_blob.isnot(None)),
        )
    )
    for inv_id, file_name, created, buyer, doc_no, doc_year, order_id, _status, blob_len in q:
        name = file_name or f'schet_{doc_no or inv_id}.pdf'
        if order_id:
            source_url = url_for('orders.order_detail', order_id=order_id)
            source_label = f'Заказ #{order_id}'
        else:
            source_url = url_for('orders.sale_invoices_list')
            source_label = 'Реестр счетов'
        num = f'№{doc_no}' if doc_no else f'#{inv_id}'
        if doc_year:
            num = f'{num}/{doc_year}'
        rows.append(CatalogRow(
            name=name,
            category='Счёт клиенту',
            comment=' '.join(p for p in (num, buyer or '') if p),
            when=_dt(created),
            size=int(blob_len or 0),
            source_label=source_label,
            source_url=source_url,
            file_url=_open_url('sale', inv_id, name),
            delete_url=None,
        ))
    return rows


def _hr_doc_rows():
    rows = []
    q = (
        db.session.query(
            ForeignEmployeeDocument.id,
            ForeignEmployeeDocument.original_name,
            ForeignEmployeeDocument.title,
            ForeignEmployeeDocument.category,
            ForeignEmployeeDocument.file_rel_path,
            ForeignEmployeeDocument.uploaded_at,
            ForeignEmployeeDocument.size_bytes,
            ForeignEmployeeDocument.employee_id,
            Employee.name,
        )
        .join(Employee, Employee.id == ForeignEmployeeDocument.employee_id)
    )
    upload = _upload_root()
    for doc_id, original, title, category, rel, uploaded, size, employee_id, emp_name in q:
        path = os.path.join(upload, rel or '')
        if not rel or not os.path.isfile(path) or not under_upload(path):
            continue
        name = original or os.path.basename(rel)
        rows.append(CatalogRow(
            name=name,
            category='Кадры',
            comment=' · '.join(p for p in (emp_name, title or category) if p),
            when=_dt(uploaded),
            size=int(size or 0) or os.path.getsize(path),
            source_label=emp_name or 'Сотрудник',
            source_url=url_for('hr.foreign_employee_card', employee_id=employee_id),
            file_url=_open_url('hrdoc', doc_id, name),
            delete_url=None,
        ))
    return rows


def _hr_check_rows():
    rows = []
    q = (
        db.session.query(
            PatentPayment.id,
            PatentPayment.check_original_name,
            PatentPayment.check_file_rel_path,
            PatentPayment.payment_date,
            PatentPayment.comment,
            PatentPayment.employee_id,
            Employee.name,
        )
        .join(Employee, Employee.id == PatentPayment.employee_id)
        .filter(PatentPayment.check_file_rel_path.isnot(None))
    )
    upload = _upload_root()
    for pay_id, original, rel, pay_date, comment, employee_id, emp_name in q:
        path = os.path.join(upload, rel or '')
        if not rel or not os.path.isfile(path) or not under_upload(path):
            continue
        name = original or os.path.basename(rel)
        rows.append(CatalogRow(
            name=name,
            category='Кадры',
            comment=' · '.join(p for p in (emp_name, 'чек патента', (comment or '').strip()) if p),
            when=_dt(pay_date),
            size=os.path.getsize(path),
            source_label=emp_name or 'Сотрудник',
            source_url=url_for('hr.foreign_employee_card', employee_id=employee_id),
            file_url=_open_url('hrcheck', pay_id, name),
            delete_url=None,
        ))
    return rows


def _hr_renew_rows():
    rows = []
    q = (
        db.session.query(
            RegistrationRenewal.id,
            RegistrationRenewal.doc_original_name,
            RegistrationRenewal.doc_file_rel_path,
            RegistrationRenewal.renewal_date,
            RegistrationRenewal.comment,
            RegistrationRenewal.employee_id,
            Employee.name,
        )
        .join(Employee, Employee.id == RegistrationRenewal.employee_id)
        .filter(RegistrationRenewal.doc_file_rel_path.isnot(None))
    )
    upload = _upload_root()
    for ren_id, original, rel, ren_date, comment, employee_id, emp_name in q:
        path = os.path.join(upload, rel or '')
        if not rel or not os.path.isfile(path) or not under_upload(path):
            continue
        name = original or os.path.basename(rel)
        rows.append(CatalogRow(
            name=name,
            category='Кадры',
            comment=' · '.join(p for p in (emp_name, 'продление регистрации', (comment or '').strip()) if p),
            when=_dt(ren_date),
            size=os.path.getsize(path),
            source_label=emp_name or 'Сотрудник',
            source_url=url_for('hr.foreign_employee_card', employee_id=employee_id),
            file_url=_open_url('hrrenew', ren_id, name),
            delete_url=None,
        ))
    return rows


def _disk_rows(root_name: str, category: str, source_label: str, source_url: str | None):
    rows = []
    upload = _upload_root()
    base = os.path.join(upload, root_name)
    if not upload or not os.path.isdir(base):
        return rows
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in ('.', '..') and not d.startswith('.')]
        for filename in filenames:
            full = os.path.join(dirpath, filename)
            if not os.path.isfile(full) or not under_upload(full):
                continue
            rel = os.path.relpath(full, upload).replace('\\', '/')
            if not safe_disk_rel(rel):
                continue
            when = datetime.fromtimestamp(os.path.getmtime(full))
            order_url = source_url
            label = source_label
            match = re.search(r'ord(\d+)_', filename)
            if match and root_name == 'shipment':
                order_id = int(match.group(1))
                label = f'Заказ #{order_id}'
                order_url = url_for('orders.order_detail', order_id=order_id)
            rows.append(CatalogRow(
                name=filename,
                category=category,
                comment=rel,
                when=when,
                size=os.path.getsize(full),
                source_label=label,
                source_url=order_url,
                file_url=_disk_url(rel, filename),
                delete_url=None,
            ))
    return rows


def collect_rows() -> list[CatalogRow]:
    rows: list[CatalogRow] = []
    rows.extend(_safe_collect(_manual_rows))
    rows.extend(_safe_collect(_invoice_rows))
    rows.extend(_safe_collect(_receipt_rows))
    rows.extend(_safe_collect(_slip_rows))
    rows.extend(_safe_collect(_payment_rows))
    rows.extend(_safe_collect(_sale_rows))
    rows.extend(_safe_collect(_hr_doc_rows))
    rows.extend(_safe_collect(_hr_check_rows))
    rows.extend(_safe_collect(_hr_renew_rows))
    personnel = url_for('hr.personnel')
    rows.extend(_safe_collect(lambda: _disk_rows('shipment', 'Фото отгрузки', 'Отгрузки', None)))
    rows.extend(_safe_collect(lambda: _disk_rows('photo', 'Фото работ', 'Табель', personnel)))
    return rows


def filter_rows(rows: list[CatalogRow], *, q: str, category: str, date_from: str, date_to: str, sort: str):
    needle = (q or '').strip().lower()
    start = _parse_date(date_from)
    end = _parse_date(date_to)
    out = []
    for row in rows:
        if category and row.category != category:
            continue
        if needle:
            hay = f'{row.name} {row.comment} {row.source_label} {row.category}'.lower()
            if needle not in hay:
                continue
        day = row.when.date() if row.when else None
        if start and (day is None or day < start):
            continue
        if end and (day is None or day > end):
            continue
        out.append(row)

    def sort_key(row: CatalogRow):
        if sort == 'name':
            return (row.name or '').lower()
        if sort == 'size':
            return -int(row.size or 0)
        if sort == 'category':
            return (row.category or '', -(row.when.timestamp() if row.when else 0))
        if sort == 'date_asc':
            return row.when or datetime.max
        return datetime.min

    if sort == 'date_desc':
        out.sort(key=lambda r: r.when or datetime.min, reverse=True)
    elif sort == 'date_asc':
        out.sort(key=lambda r: r.when or datetime.max)
    elif sort == 'size':
        out.sort(key=lambda r: r.size or 0, reverse=True)
    elif sort == 'category':
        out.sort(key=lambda r: ((r.category or ''), r.when or datetime.min), reverse=False)
        out.sort(key=lambda r: r.category or '')
    else:
        out.sort(key=lambda r: (r.name or '').lower())
    return out


def _parse_date(raw: str) -> date | None:
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, '%Y-%m-%d').date()
    except ValueError:
        return None


def page_window(page: int, pages: int) -> list[int]:
    if pages <= 9:
        return list(range(1, pages + 1))
    start = max(1, page - 3)
    end = min(pages, start + 6)
    start = max(1, end - 6)
    return list(range(start, end + 1))


def open_catalog_file(source: str, item_id: int, download: bool):
    """Отдаёт исходный файл. None — не найден."""
    from flask import send_file
    import io

    source = (source or '').strip()
    if source == 'manual':
        rec = FileArchive.query.get(item_id)
        if not rec:
            return None
        full = os.path.join(_upload_root(), rec.filename or '')
        if not under_upload(full) or not os.path.isfile(full):
            return None
        return send_file(
            full,
            as_attachment=download,
            download_name=rec.original_name or os.path.basename(full),
        )
    if source == 'invoice':
        inv = PaymentInvoice.query.get(item_id)
        if not inv:
            return None
        from app.invoice_files import flask_send
        return flask_send(inv, as_attachment=download)
    if source == 'receipt':
        inv = PaymentInvoice.query.options(undefer(PaymentInvoice.receipt_blob)).get(item_id)
        if not inv:
            return None
        from app.invoice_files import flask_send_receipt
        return flask_send_receipt(inv, as_attachment=download)
    if source == 'slip':
        slip = BankSlip.query.options(undefer(BankSlip.file_blob)).get(item_id)
        if not slip or not slip.file_blob:
            return None
        name = slip.original_name or 'slip'
        return send_file(
            io.BytesIO(bytes(slip.file_blob)),
            as_attachment=download,
            download_name=name,
        )
    if source == 'payment':
        pay = Payment.query.get(item_id)
        if not pay or not pay.file_path or not os.path.isfile(pay.file_path) or not under_upload(pay.file_path):
            return None
        return send_file(pay.file_path, as_attachment=download, download_name=os.path.basename(pay.file_path))
    if source == 'sale':
        inv = SaleInvoice.query.get(item_id)
        if not inv or inv.status == 'discarded':
            return None
        from app.tg_sale import render_sale_pdf, sale_public_number, allocate_sale_doc_number
        allocate_sale_doc_number(inv)
        blob = inv.file_blob or render_sale_pdf(inv)
        if blob and not inv.file_blob:
            inv.file_blob = blob
            inv.file_name = inv.file_name or f'schet_{sale_public_number(inv)}.pdf'
            db.session.commit()
        if not blob:
            return None
        name = inv.file_name or f'schet_{sale_public_number(inv)}.pdf'
        return send_file(io.BytesIO(bytes(blob)), mimetype='application/pdf', as_attachment=download, download_name=name)
    if source == 'hrdoc':
        doc = ForeignEmployeeDocument.query.get(item_id)
        if not doc:
            return None
        full = os.path.join(_upload_root(), doc.file_rel_path or '')
        if not under_upload(full) or not os.path.isfile(full):
            return None
        return send_file(full, as_attachment=download, download_name=doc.original_name or os.path.basename(full))
    if source == 'hrcheck':
        pay = PatentPayment.query.get(item_id)
        if not pay or not pay.check_file_rel_path:
            return None
        full = os.path.join(_upload_root(), pay.check_file_rel_path)
        if not under_upload(full) or not os.path.isfile(full):
            return None
        name = pay.check_original_name or os.path.basename(full)
        return send_file(full, as_attachment=download, download_name=name)
    if source == 'hrrenew':
        ren = RegistrationRenewal.query.get(item_id)
        if not ren or not ren.doc_file_rel_path:
            return None
        full = os.path.join(_upload_root(), ren.doc_file_rel_path)
        if not under_upload(full) or not os.path.isfile(full):
            return None
        name = ren.doc_original_name or os.path.basename(full)
        return send_file(full, as_attachment=download, download_name=name)
    return None


def open_disk_file(rel: str, download: bool):
    from flask import send_file
    safe = safe_disk_rel(rel)
    if not safe:
        return None
    full = os.path.join(_upload_root(), *safe.split('/'))
    if not under_upload(full) or not os.path.isfile(full):
        return None
    return send_file(full, as_attachment=download, download_name=os.path.basename(full))
