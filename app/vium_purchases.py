"""Заявки на закупку в учёте ВиУМ.

Источник правды для цен/сроков — ViumPurchase / ViumPurchaseLine.
AI Ops purchase_request может создавать и дополнять заявки (без затирания цен).
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from app.models import (
    OpsActionDraft, ViumPurchase, ViumPurchaseLine, db,
)
from app.utils import msk_now, msk_today

LINE_STATUSES = ('planned', 'ordered', 'received', 'cancelled')
PURCHASE_STATUSES = ('open', 'closed', 'cancelled')


def _text_key(text: str) -> str:
    return re.sub(r'\s+', ' ', (text or '').strip().lower())[:120]


def _parse_qty_num(raw) -> Decimal | None:
    if raw in (None, ''):
        return None
    if isinstance(raw, (int, float, Decimal)):
        try:
            return Decimal(str(raw))
        except (InvalidOperation, ValueError):
            return None
    s = str(raw).strip().replace(',', '.')
    m = re.search(r'(\d+(?:\.\d+)?)', s)
    if not m:
        return None
    try:
        return Decimal(m.group(1))
    except (InvalidOperation, ValueError):
        return None


def _parse_date(raw) -> date | None:
    if not raw:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    try:
        return datetime.strptime(str(raw)[:10], '%Y-%m-%d').date()
    except Exception:
        return None


def _draft_items(draft: OpsActionDraft) -> list[dict]:
    try:
        from app.ai_ops import draft_payload, _normalize_purchase_items
    except Exception:
        return []
    pl = draft_payload(draft)
    return _normalize_purchase_items(
        pl.get('items') or (pl.get('entities') or {}).get('items')
    )


def upsert_purchase_from_ops_draft(
    draft: OpsActionDraft,
    *,
    close: bool = False,
) -> ViumPurchase | None:
    """Создаёт/обновляет заявку ВиУМ по черновику AI Ops. Цены/сроки сохраняет."""
    if not draft or draft.action_type != 'purchase_request':
        return None

    try:
        from app.ai_ops import draft_payload
    except Exception:
        return None

    pl = draft_payload(draft)
    ent = pl.get('entities') or {}
    items = _draft_items(draft)
    if not items and not close:
        return None

    day = _parse_date(ent.get('day') or pl.get('digest_date')) or msk_today()
    title = (ent.get('title') or 'Заявка на закупку')[:200]
    channel = (draft.channel or pl.get('channel') or '')[:20] or None
    quote = (draft.quote or pl.get('quote') or '')[:4000] or None

    purchase = None
    if draft.id:
        purchase = ViumPurchase.query.filter_by(ops_draft_id=draft.id).first()
    if purchase is None and draft.task_id:
        purchase = (
            ViumPurchase.query
            .filter_by(task_id=draft.task_id, source='ai_ops')
            .order_by(ViumPurchase.id.desc())
            .first()
        )

    if purchase is None:
        purchase = ViumPurchase(
            title=title,
            request_date=day,
            status='open',
            source='ai_ops',
            channel=channel,
            quote=quote,
            ops_draft_id=draft.id,
            task_id=draft.task_id,
        )
        db.session.add(purchase)
        db.session.flush()
    else:
        purchase.title = title or purchase.title
        purchase.request_date = day or purchase.request_date
        purchase.channel = channel or purchase.channel
        if quote:
            purchase.quote = quote
        purchase.ops_draft_id = draft.id
        purchase.task_id = draft.task_id or purchase.task_id
        purchase.updated_at = msk_now()

    if close or draft.status in ('posted', 'rejected', 'skipped'):
        purchase.status = 'closed'

    by_ext = {ln.external_key: ln for ln in purchase.lines if ln.external_key}
    by_text = {_text_key(ln.title): ln for ln in purchase.lines}

    for i, it in enumerate(items):
        ext = str(it.get('id') or f'i{i + 1}')[:120]
        text = (it.get('text') or '').strip()[:300]
        if not text:
            continue
        qty_text = (it.get('qty') or None)
        if qty_text is not None:
            qty_text = str(qty_text)[:80]
        done = bool(it.get('done'))
        line = by_ext.get(ext) or by_text.get(_text_key(text))
        if line is None:
            line = ViumPurchaseLine(
                purchase_id=purchase.id,
                external_key=ext,
                title=text,
                qty_text=qty_text,
                qty=_parse_qty_num(qty_text),
                status='ordered' if done else 'planned',
                sort_order=i,
            )
            db.session.add(line)
            by_ext[ext] = line
            by_text[_text_key(text)] = line
        else:
            line.external_key = ext
            line.title = text
            if qty_text and not line.qty_text:
                line.qty_text = qty_text
            if line.qty is None and qty_text:
                line.qty = _parse_qty_num(qty_text)
            # AI-галочка поднимает статус до ordered, но не откатывает received
            if done and line.status == 'planned':
                line.status = 'ordered'
            elif not done and line.status == 'ordered':
                line.status = 'planned'
            line.sort_order = i
            line.updated_at = msk_now()

    return purchase


def sync_pending_ops_purchases() -> int:
    """Бэкфилл: все pending purchase_request → ViumPurchase."""
    drafts = (
        OpsActionDraft.query
        .filter_by(action_type='purchase_request', status='pending')
        .order_by(OpsActionDraft.id.asc())
        .all()
    )
    n = 0
    for dr in drafts:
        if upsert_purchase_from_ops_draft(dr):
            n += 1
    if n:
        db.session.commit()
    return n


def line_amount(line: ViumPurchaseLine) -> Decimal | None:
    return line.amount


def purchases_overview(
    *,
    status: str = 'open',
    start: date | None = None,
    end: date | None = None,
    overdue_only: bool = False,
) -> dict:
    today = msk_today()
    q = ViumPurchase.query
    if status and status != 'all':
        q = q.filter(ViumPurchase.status == status)
    if start:
        q = q.filter(ViumPurchase.request_date >= start)
    if end:
        q = q.filter(ViumPurchase.request_date <= end)
    purchases = q.order_by(
        ViumPurchase.request_date.desc(),
        ViumPurchase.id.desc(),
    ).all()

    rows = []
    total_amount = Decimal('0')
    amount_known = False
    open_lines = 0
    ordered_lines = 0
    overdue_lines = 0

    for p in purchases:
        for ln in p.lines:
            if overdue_only:
                if not (
                    ln.delivery_date
                    and ln.delivery_date < today
                    and ln.status in ('planned', 'ordered')
                ):
                    continue
            amt = ln.amount
            if amt is not None:
                total_amount += amt
                amount_known = True
            if ln.status == 'planned':
                open_lines += 1
            elif ln.status == 'ordered':
                ordered_lines += 1
            if (
                ln.delivery_date
                and ln.delivery_date < today
                and ln.status in ('planned', 'ordered')
            ):
                overdue_lines += 1
            rows.append({
                'purchase': p,
                'line': ln,
                'amount': amt,
                'is_overdue': bool(
                    ln.delivery_date
                    and ln.delivery_date < today
                    and ln.status in ('planned', 'ordered')
                ),
            })

    # Группы для шаблона: заявка → видимые строки
    groups = []
    by_pid: dict[int, list] = {}
    for r in rows:
        by_pid.setdefault(r['purchase'].id, []).append(r)
    for p in purchases:
        glines = by_pid.get(p.id) or []
        if overdue_only and not glines:
            continue
        groups.append({'purchase': p, 'rows': glines if overdue_only else [
            {
                'purchase': p,
                'line': ln,
                'amount': ln.amount,
                'is_overdue': bool(
                    ln.delivery_date
                    and ln.delivery_date < today
                    and ln.status in ('planned', 'ordered')
                ),
            }
            for ln in p.lines
        ]})

    return {
        'purchases': purchases,
        'groups': groups,
        'rows': rows,
        'total_amount': total_amount if amount_known else None,
        'open_lines': open_lines,
        'ordered_lines': ordered_lines,
        'overdue_lines': overdue_lines,
        'purchase_count': len(groups) if overdue_only else len(purchases),
        'today': today,
    }


def create_manual_purchase(
    *,
    title: str,
    request_date: date,
    lines: list[dict],
    user_id: int | None = None,
    note: str | None = None,
) -> ViumPurchase:
    purchase = ViumPurchase(
        title=(title or 'Заявка на закупку')[:200],
        request_date=request_date or msk_today(),
        status='open',
        source='manual',
        note=(note or None),
        created_by_user_id=user_id,
    )
    db.session.add(purchase)
    db.session.flush()
    for i, raw in enumerate(lines):
        text = (raw.get('title') or raw.get('text') or '').strip()
        if not text:
            continue
        qty_text = (raw.get('qty_text') or raw.get('qty') or None)
        if qty_text is not None:
            qty_text = str(qty_text)[:80]
        qty = raw.get('qty_num')
        if qty is None:
            qty = _parse_qty_num(qty_text)
        price = raw.get('unit_price')
        try:
            price = Decimal(str(price)) if price not in (None, '') else None
        except (InvalidOperation, ValueError):
            price = None
        line = ViumPurchaseLine(
            purchase_id=purchase.id,
            external_key=f'm{i + 1}',
            title=text[:300],
            qty_text=qty_text,
            qty=qty,
            unit=(raw.get('unit') or None),
            unit_price=price,
            supplier=(raw.get('supplier') or None),
            delivery_date=_parse_date(raw.get('delivery_date')),
            status=(raw.get('status') if raw.get('status') in LINE_STATUSES else 'planned'),
            sort_order=i,
            note=(raw.get('note') or None),
        )
        db.session.add(line)
    db.session.commit()
    return purchase


def update_line_fields(line_id: int, form) -> ViumPurchaseLine | None:
    line = ViumPurchaseLine.query.get(line_id)
    if not line:
        return None
    title = (form.get('title') or '').strip()
    if title:
        line.title = title[:300]
    qty_text = form.get('qty_text')
    if qty_text is not None:
        line.qty_text = (str(qty_text).strip()[:80] or None)
    qty_raw = form.get('qty')
    if qty_raw is not None:
        line.qty = _parse_qty_num(qty_raw) if str(qty_raw).strip() else None
    price_raw = form.get('unit_price')
    if price_raw is not None:
        try:
            line.unit_price = (
                Decimal(str(price_raw).replace(',', '.'))
                if str(price_raw).strip() else None
            )
        except (InvalidOperation, ValueError):
            pass
    if 'supplier' in form:
        line.supplier = (form.get('supplier') or '').strip()[:200] or None
    if 'delivery_date' in form:
        line.delivery_date = _parse_date(form.get('delivery_date'))
    st = (form.get('status') or '').strip()
    if st in LINE_STATUSES:
        line.status = st
    if 'note' in form:
        line.note = (form.get('note') or '').strip()[:500] or None
    line.updated_at = msk_now()
    if line.purchase:
        line.purchase.updated_at = msk_now()
    db.session.commit()
    return line


def set_purchase_status(purchase_id: int, status: str) -> ViumPurchase | None:
    if status not in PURCHASE_STATUSES:
        return None
    p = ViumPurchase.query.get(purchase_id)
    if not p:
        return None
    p.status = status
    p.updated_at = msk_now()
    db.session.commit()
    return p
