"""Разнос выписки / платёжки по счетам Mini App «Оплата»."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import date
from decimal import Decimal

from flask import current_app
from sqlalchemy.exc import IntegrityError

from app.models import db, BankSlip, BankSlipItem, PaymentInvoice, ChatExpenseMessage, User
from app.utils import msk_now

log = logging.getLogger(__name__)

_MAX_BYTES = 12 * 1024 * 1024
_MEDIA_EXT = {
    '.pdf', '.jpg', '.jpeg', '.png', '.webp', '.jfif', '.bmp', '.heic', '.tif', '.tiff',
}


def file_hash(data: bytes) -> str:
    return hashlib.sha256(data or b'').hexdigest()


def extract_telegram_media(msg: dict) -> dict | None:
    photos = msg.get('photo') or []
    if photos:
        best = max(photos, key=lambda p: (p.get('file_size') or 0, p.get('width') or 0))
        if best.get('file_id'):
            return {
                'file_id': best['file_id'],
                'filename': 'photo.jpg',
                'mime': 'image/jpeg',
            }
    doc = msg.get('document') or {}
    file_id = doc.get('file_id')
    if not file_id:
        return None
    name = doc.get('file_name') or 'document.bin'
    ext = os.path.splitext(name)[1].lower()
    mime = (doc.get('mime_type') or '').lower()
    ok = (
        ext in _MEDIA_EXT
        or mime.startswith('image/')
        or 'pdf' in mime
        or mime in ('', 'application/octet-stream', 'application/x-pdf')
    )
    if not ok:
        return None
    return {'file_id': file_id, 'filename': name, 'mime': mime}


def _compact_no(value: str) -> str:
    s = (value or '').lower().replace('ё', 'е')
    s = re.sub(r'[№n#]', '', s)
    return re.sub(r'[\s.]+', '', s)


def _hay(inv: PaymentInvoice) -> str:
    return ' '.join(filter(None, [
        inv.summary, inv.comment, inv.original_name, inv.filename,
    ]))


def _kind_ok(inv: PaymentInvoice) -> bool:
    return (inv.kind or 'invoice') != 'plan'


def _unpaid():
    return [
        inv for inv in PaymentInvoice.query.filter(
            PaymentInvoice.status.in_(['new', 'draft']),
        ).all()
        if _kind_ok(inv)
    ]


def _paid():
    return [
        inv for inv in PaymentInvoice.query.filter(
            PaymentInvoice.status == 'paid',
        ).all()
        if _kind_ok(inv)
    ]


def _find_by_invoice_no(invoice_no: str, pool: list[PaymentInvoice]) -> list[PaymentInvoice]:
    compact = _compact_no(invoice_no)
    if len(compact) < 3:
        return []
    compact_dashless = compact.replace('-', '')
    hits = []
    for inv in pool:
        hay = _compact_no(_hay(inv))
        hay_d = hay.replace('-', '')
        if compact in hay or (len(compact_dashless) >= 4 and compact_dashless in hay_d):
            hits.append(inv)
    return hits


def _closest_amount(pool: list[PaymentInvoice], amount: Decimal) -> PaymentInvoice | None:
    if not pool:
        return None
    if len(pool) == 1:
        return pool[0]

    def _delta(inv):
        try:
            return abs(Decimal(str(inv.amount or 0)) - amount)
        except Exception:
            return Decimal('999999')

    return sorted(pool, key=_delta)[0]


def _already_processed(pay: dict) -> BankSlipItem | None:
    q = BankSlipItem.query.filter(BankSlipItem.action.in_(['matched', 'created']))
    amount = pay.get('amount')
    no = (pay.get('invoice_no') or '').strip()
    if amount is not None:
        q = q.filter(BankSlipItem.amount == amount)
    if no:
        q = q.filter(BankSlipItem.invoice_no == no)
    elif pay.get('payee'):
        q = q.filter(BankSlipItem.payee == pay['payee'])
    else:
        return None
    return q.order_by(BankSlipItem.id.desc()).first()


def _match_payment(pay: dict) -> tuple[PaymentInvoice | None, str]:
    """Возвращает (invoice, reason) reason: match | paid | none."""
    amount = pay.get('amount')
    no = (pay.get('invoice_no') or '').strip()
    unpaid = _unpaid()
    paid = _paid()
    if no:
        unpaid_hits = _find_by_invoice_no(no, unpaid)
        if unpaid_hits:
            return _closest_amount(unpaid_hits, amount or Decimal('0')), 'match'
        paid_hits = _find_by_invoice_no(no, paid)
        if paid_hits:
            hit = _closest_amount(paid_hits, amount or Decimal('0'))
            if hit is not None and amount is not None:
                try:
                    if abs(Decimal(str(hit.amount or 0)) - amount) <= max(Decimal('100'), amount * Decimal('0.05')):
                        return hit, 'paid'
                except Exception:
                    return hit, 'paid'
            return hit, 'paid'
        return None, 'none'
    if amount is not None:
        delta = max(Decimal('100'), amount * Decimal('0.05'))
        close = []
        for inv in unpaid:
            try:
                if abs(Decimal(str(inv.amount or 0)) - amount) <= delta:
                    close.append(inv)
            except Exception:
                continue
        if close:
            hay = ' '.join(filter(None, [pay.get('payee'), pay.get('purpose'), no]))
            try:
                from rapidfuzz import fuzz
                from app.expense_chat import _normalize_alias_key
                key = _normalize_alias_key(hay, max_words=12)
                best, best_score = None, 0
                for inv in close:
                    other = _normalize_alias_key(_hay(inv), max_words=12)
                    score = fuzz.token_set_ratio(key, other) if key and other else 40
                    if score > best_score:
                        best_score, best = score, inv
                if best is not None and (best_score >= 50 or (len(close) == 1 and best_score >= 35)):
                    return best, 'match'
            except Exception:
                log.exception('bank slip fuzzy match failed')
                if len(close) == 1:
                    return close[0], 'match'
        paid_amt = []
        delta = max(Decimal('100'), (amount * Decimal('0.05')))
        for inv in paid:
            try:
                if abs(Decimal(str(inv.amount or 0)) - amount) <= delta:
                    paid_amt.append(inv)
            except Exception:
                continue
        if len(paid_amt) == 1:
            return paid_amt[0], 'paid'
    return None, 'none'


def _attach_receipt(inv: PaymentInvoice, data: bytes, filename: str) -> None:
    from app.invoice_files import attach_file, has_file
    inv.receipt_blob = data
    inv.receipt_name = (filename or 'receipt.jpg')[:255]
    if not has_file(inv):
        attach_file(inv, data, filename)


def _mark_paid_quiet(inv: PaymentInvoice) -> None:
    if inv.status == 'paid':
        return
    inv.status = 'paid'
    if not inv.payment_type:
        inv.payment_type = 'cashless'
    for kid in list(getattr(inv, 'fact_invoices', None) or []):
        kid.status = 'paid'
    try:
        from app.invoice_files import ensure_expense_for_paid_invoice
        ensure_expense_for_paid_invoice(inv)
    except Exception:
        log.exception('bank slip expense for invoice %s', inv.id)


def _summary_for(pay: dict) -> str:
    parts = [pay.get('payee') or '']
    if pay.get('invoice_no'):
        parts.append(f"сч. №{pay['invoice_no']}")
    purpose = (pay.get('purpose') or '').strip()
    if purpose and purpose not in parts[0]:
        parts.append(purpose[:180])
    text = re.sub(r'\s+', ' ', ' · '.join(p for p in parts if p)).strip()
    return (text or 'Оплата по выписке')[:500]


def _create_draft(pay: dict, data: bytes, filename: str) -> PaymentInvoice:
    from app.invoice_files import attach_file
    summary = _summary_for(pay)
    budget_id = None
    try:
        from app.expense_chat import _top_alias_for
        alias = _top_alias_for(summary)
        if alias is not None:
            budget_id = alias.budget_item_id
    except Exception:
        budget_id = None
    inv = PaymentInvoice(
        filename=filename[:255] or 'bank.jpg',
        original_name=(filename or 'bank.jpg')[:255],
        summary=summary,
        line_items='[]',
        source='bank_slip',
        budget_item_id=budget_id,
        amount=pay.get('amount') or Decimal('0'),
        status='draft',
        priority='normal',
        comment=summary[:500],
        payment_type='cashless',
        kind='invoice',
        due_date=pay.get('date') if isinstance(pay.get('date'), date) else None,
    )
    db.session.add(inv)
    db.session.flush()
    attach_file(inv, data, inv.filename)
    inv.receipt_blob = data
    inv.receipt_name = (filename or 'receipt.jpg')[:255]
    return inv


def _item_row(slip: BankSlip, pay: dict, action: str, note: str, inv: PaymentInvoice | None) -> BankSlipItem:
    row = BankSlipItem(
        slip_id=slip.id,
        payee=(pay.get('payee') or '')[:300],
        amount=pay.get('amount'),
        invoice_no=(pay.get('invoice_no') or '')[:80] or None,
        paid_on=pay.get('date') if isinstance(pay.get('date'), date) else None,
        purpose=(pay.get('purpose') or '')[:800] or None,
        vat=(pay.get('vat') or '')[:120] or None,
        action=action,
        note=(note or '')[:300] or None,
        invoice_id=inv.id if inv else None,
    )
    db.session.add(row)
    return row


def serialize_item(row: BankSlipItem) -> dict:
    inv = row.invoice
    return {
        'id': row.id,
        'action': row.action,
        'payee': row.payee or '',
        'amount': float(row.amount or 0),
        'invoice_no': row.invoice_no or '',
        'date': row.paid_on.isoformat() if row.paid_on else None,
        'purpose': row.purpose or '',
        'vat': row.vat or '',
        'note': row.note or '',
        'invoice_id': row.invoice_id,
        'invoice_summary': (inv.summary or inv.comment or '') if inv else '',
        'invoice_status': inv.status if inv else None,
    }


def serialize_slip(slip: BankSlip) -> dict:
    items = [serialize_item(x) for x in (slip.items or [])]
    counts = {'matched': 0, 'created': 0, 'skipped': 0}
    for it in items:
        counts[it['action']] = counts.get(it['action'], 0) + 1
    error = ''
    try:
        raw = json.loads(slip.parsed_json or '{}')
        error = raw.get('error') or ''
    except Exception:
        error = ''
    return {
        'ok': True,
        'slip_id': slip.id,
        'kind': slip.kind or '',
        'duplicate': False,
        'original_name': slip.original_name,
        'items': items,
        'counts': counts,
        'error': error,
    }


def apply_parsed(
    data: bytes,
    filename: str,
    parsed: dict,
    *,
    source: str = 'upload',
    user: User | None = None,
    tg_chat_id: str | None = None,
    tg_message_id: int | None = None,
) -> dict:
    digest = file_hash(data)
    existing = BankSlip.query.filter_by(file_hash=digest).first()
    if existing:
        payload = serialize_slip(existing)
        payload['duplicate'] = True
        return payload

    slip = BankSlip(
        file_hash=digest,
        original_name=(filename or 'bank.jpg')[:255],
        file_blob=data,
        source=source[:20],
        kind=(parsed.get('kind') or '')[:40] or None,
        parsed_json=json.dumps({
            'kind': parsed.get('kind'),
            'error': parsed.get('error'),
            'payments': [
                {
                    'payee': p.get('payee'),
                    'amount': str(p.get('amount')),
                    'invoice_no': p.get('invoice_no'),
                    'date': p['date'].isoformat() if isinstance(p.get('date'), date) else p.get('date'),
                    'purpose': p.get('purpose'),
                    'vat': p.get('vat'),
                }
                for p in (parsed.get('payments') or [])
            ],
        }, ensure_ascii=False),
        tg_chat_id=(tg_chat_id or '')[:64] or None,
        tg_message_id=tg_message_id,
        created_by_user_id=user.id if user else None,
    )
    db.session.add(slip)
    try:
        db.session.flush()
    except IntegrityError:
        db.session.rollback()
        again = BankSlip.query.filter_by(file_hash=digest).first()
        if again:
            payload = serialize_slip(again)
            payload['duplicate'] = True
            return payload
        raise

    save_name = f"bank_{digest[:10]}_{os.path.splitext(filename or 'bank.jpg')[1] or '.jpg'}"
    payments = parsed.get('payments') or []
    for pay in payments:
        dup = _already_processed(pay)
        if dup is not None:
            _item_row(
                slip, pay, 'skipped',
                f'уже разносили (#{dup.invoice_id or dup.id})',
                dup.invoice,
            )
            continue
        inv, reason = _match_payment(pay)
        if reason == 'match' and inv is not None:
            _attach_receipt(inv, data, save_name)
            _mark_paid_quiet(inv)
            _item_row(slip, pay, 'matched', f'оплачен счёт #{inv.id}', inv)
        elif reason == 'paid' and inv is not None:
            _attach_receipt(inv, data, save_name)
            _item_row(slip, pay, 'skipped', f'уже оплачен счёт #{inv.id}', inv)
        else:
            created = _create_draft(pay, data, save_name)
            _item_row(slip, pay, 'created', 'черновик — поправьте', created)
    db.session.commit()
    return serialize_slip(slip)


def ingest_bytes(
    data: bytes,
    filename: str,
    *,
    source: str = 'upload',
    user: User | None = None,
    tg_chat_id: str | None = None,
    tg_message_id: int | None = None,
) -> dict:
    from app.bank_slip_parse import parse_bank_file
    if not data:
        return {'ok': False, 'error': 'empty'}
    if len(data) > _MAX_BYTES:
        return {'ok': False, 'error': 'too_large'}
    parsed = parse_bank_file(data, filename)
    return apply_parsed(
        data, filename, parsed,
        source=source, user=user,
        tg_chat_id=tg_chat_id, tg_message_id=tg_message_id,
    )


def _chat_summary(result: dict) -> str:
    if result.get('duplicate'):
        return 'Эту выписку уже разносили.'
    items = result.get('items') or []
    if not items:
        return 'Не разобрал платежи на фото.'
    lines = []
    counts = result.get('counts') or {}
    head = []
    if counts.get('matched'):
        head.append(f"оплатил {counts['matched']}")
    if counts.get('created'):
        head.append(f"черновик {counts['created']}")
    if counts.get('skipped'):
        head.append(f"пропуск {counts['skipped']}")
    title = 'Выписка: ' + ', '.join(head) if head else 'Выписка'
    lines.append(title)
    for it in items[:8]:
        amt = it.get('amount') or 0
        try:
            amt_s = f"{int(round(float(amt))):,}".replace(',', ' ')
        except Exception:
            amt_s = str(amt)
        who = (it.get('payee') or it.get('invoice_no') or 'платёж')[:40]
        note = it.get('note') or it.get('action')
        lines.append(f"• {who} {amt_s} ₽ — {note}")
    if counts.get('created'):
        lines.append('Черновики в Оплате — поправьте статью и выведите в оплату.')
    return '\n'.join(lines)


def ingest_expenses_chat_media(msg: dict, media: dict) -> dict:
    """Обрабатывает фото/PDF из чата расходов. handled=True — не парсить как текст."""
    from app.telegram import download_bot_file, send_chat_message

    chat = msg.get('chat') or {}
    tg_chat_id = str(chat.get('id') or '')
    tg_message_id = int(msg.get('message_id') or 0)
    caption = (msg.get('caption') or msg.get('text') or '').strip()
    blob, err = download_bot_file(media.get('file_id'))
    if not blob:
        log.warning('bank slip download failed: %s', err)
        return {'ok': False, 'handled': False, 'error': err or 'download'}

    sender = msg.get('from') or {}
    sender_name = (sender.get('first_name') or '')[:150]
    filename = media.get('filename') or 'photo.jpg'
    result = ingest_bytes(
        blob, filename,
        source='expenses_chat',
        tg_chat_id=tg_chat_id,
        tg_message_id=tg_message_id,
    )
    if not result.get('ok'):
        return {'ok': False, 'handled': False, 'error': result.get('error')}

    payments = result.get('items') or []
    found = any(it.get('action') in ('matched', 'created') for it in payments)
    kind = result.get('kind') or ''
    looks_bank = kind in ('statement_list', 'payment_order', 'receipt') or found or result.get('duplicate')
    if not found and caption and not looks_bank:
        return {'ok': True, 'handled': False, 'status': 'not_bank'}

    row = ChatExpenseMessage(
        tg_chat_id=tg_chat_id,
        tg_message_id=tg_message_id,
        tg_date=msk_now(),
        raw_text=(caption or '[выписка]')[:4000],
        sender_name=sender_name or None,
        status='imported',
        parsed_description=_chat_summary(result)[:500],
        parsed_payment_type='cashless',
    )
    matched_ids = [it.get('invoice_id') for it in payments if it.get('action') == 'matched' and it.get('invoice_id')]
    if matched_ids:
        row.matched_invoice_id = matched_ids[0]
    amounts = [it.get('amount') for it in payments if it.get('amount')]
    if amounts:
        try:
            row.parsed_amount = Decimal(str(sum(Decimal(str(a)) for a in amounts)))
        except Exception:
            pass
    db.session.add(row)
    db.session.commit()

    try:
        send_chat_message(tg_chat_id, _chat_summary(result))
    except Exception:
        log.exception('bank slip chat reply failed')
    return {
        'ok': True,
        'handled': True,
        'status': 'imported',
        'slip_id': result.get('slip_id'),
        'counts': result.get('counts'),
        'chat_expense_id': row.id,
    }
