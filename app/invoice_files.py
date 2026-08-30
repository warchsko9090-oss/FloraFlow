"""PDF счетов: источник правды — колонка payment_invoice.file_blob."""
from __future__ import annotations

import io
import os

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
    elif lower.endswith(('.jpg', '.jpeg')):
        mime = 'image/jpeg'
    elif lower.endswith('.webp'):
        mime = 'image/webp'
    return send_file(
        io.BytesIO(data),
        mimetype=mime,
        as_attachment=as_attachment,
        download_name=name,
    )


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
