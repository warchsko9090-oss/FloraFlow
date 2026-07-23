"""Helpers for stock purchase lots (себестоимость поступлений)."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from app.models import StockBalance, StockPurchaseLot, db


def apply_purchase_lot(
    *,
    plant_id: int,
    size_id: int,
    field_id: int,
    year: int,
    quantity: int,
    purchase_price=0,
    supplier_id=None,
    document_id=None,
    document_row_id=None,
    stock: Optional[StockBalance] = None,
) -> Optional[StockPurchaseLot]:
    """Создаёт/наращивает лот закупки и аккуратно пишет StockBalance.purchase_price.

    Не перезаписывает уже заданную другую цену на балансе — для другой
    цены/поставщика всегда появляется отдельный лот (строка себестоимости).
    """
    qty = int(quantity or 0)
    if qty <= 0:
        return None
    try:
        pp = Decimal(str(purchase_price or 0))
    except Exception:
        pp = Decimal('0')

    supplier_id = int(supplier_id) if supplier_id else None

    lot_q = StockPurchaseLot.query.filter_by(
        plant_id=plant_id,
        size_id=size_id,
        field_id=field_id,
        year=year,
        purchase_price=pp,
    )
    if supplier_id:
        lot_q = lot_q.filter_by(supplier_id=supplier_id)
    else:
        lot_q = lot_q.filter(StockPurchaseLot.supplier_id.is_(None))
    lot = lot_q.first()
    if lot:
        lot.quantity = int(lot.quantity or 0) + qty
        if document_id and not lot.document_id:
            lot.document_id = document_id
    else:
        lot = StockPurchaseLot(
            plant_id=plant_id,
            size_id=size_id,
            field_id=field_id,
            year=year,
            supplier_id=supplier_id,
            purchase_price=pp,
            quantity=qty,
            document_id=document_id,
            document_row_id=document_row_id,
        )
        db.session.add(lot)

    if stock is None:
        stock = StockBalance.query.filter_by(
            plant_id=plant_id, size_id=size_id, field_id=field_id, year=year,
        ).first()
    if stock is not None and pp > 0:
        cur = Decimal(str(stock.purchase_price or 0))
        if cur <= 0:
            stock.purchase_price = pp

    return lot


def purchase_lots_for_batches(plant_ids=None, field_ids=None):
    q = StockPurchaseLot.query.filter(StockPurchaseLot.quantity > 0)
    if plant_ids:
        q = q.filter(StockPurchaseLot.plant_id.in_(list(plant_ids)))
    if field_ids:
        q = q.filter(StockPurchaseLot.field_id.in_(list(field_ids)))
    return q.all()
