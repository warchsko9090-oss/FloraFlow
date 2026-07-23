"""Сервисный слой ВиУМ: FIFO-логика прихода/расхода/остатков.

Все операции работают через `ViumOperation` + `ViumOperationLine`. Сервис:

* `apply_intake(operation)`  — для каждой intake-строки создаёт `ViumLot`
  (qty_received = qty_remaining = qty, unit_price = строки).
* `apply_consume(operation)` / `apply_writeoff(operation)` — режут партии
  по FIFO (`received_at, id`), уменьшают `qty_remaining`. В строку
  кладут JSON `lot_consumption = [{lot_id, qty, unit_price}, ...]`.
* `apply_adjust(operation)` — ручная корректировка остатка (по сути
  intake с note='inventory' или writeoff на излишек / недостачу).
* `current_balance(material_id)` — суммарный остаток + средневзвешенная
  цена по живым партиям.
* `materials_overview()` — те же остатки сразу для всех материалов
  (для отчёта).
* `validate_consume_qty(material_id, qty, exclude_op_id=None)` — хватит
  ли остатков на списание. Возвращает (ok, available_qty).

Сервис ничего сам не коммитит — только пишет в `db.session`. Решение
о `db.session.commit()` оставляем за роутом, чтобы при ошибке откатить
всё (включая запись `ViumOperation`).
"""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Iterable, Tuple

from sqlalchemy import func

from .models import db, ViumLot, ViumOperation, ViumOperationLine


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

ZERO = Decimal('0')
QTY_EPS = Decimal('0.0005')   # точность qty (3 знака)


def _to_dec(value) -> Decimal:
    """Безопасное приведение к Decimal."""
    if value is None or value == '':
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


# ---------------------------------------------------------------------------
# Остатки и валидация
# ---------------------------------------------------------------------------

def current_balance(material_id: int) -> dict:
    """Остаток + средневзвешенная цена + стоимость по материалу.

    Возвращает dict вида:
        {qty: Decimal, avg_price: Decimal, value: Decimal, lots: int}
    """
    rows = (
        db.session.query(
            func.coalesce(func.sum(ViumLot.qty_remaining), 0),
            func.coalesce(
                func.sum(ViumLot.qty_remaining * ViumLot.unit_price), 0
            ),
            func.count(ViumLot.id),
        )
        .filter(
            ViumLot.material_id == material_id,
            ViumLot.qty_remaining > 0,
        )
        .one()
    )
    qty = _to_dec(rows[0])
    value = _to_dec(rows[1])
    lots = int(rows[2] or 0)
    avg = (value / qty) if qty > ZERO else ZERO
    return {
        'qty': qty,
        'avg_price': avg,
        'value': value,
        'lots': lots,
    }


def materials_overview() -> dict:
    """Остатки/стоимости сразу по всем материалам (один SQL).

    Возвращает dict {material_id: {qty, value, avg_price, lots}}.
    """
    rows = (
        db.session.query(
            ViumLot.material_id,
            func.coalesce(func.sum(ViumLot.qty_remaining), 0),
            func.coalesce(
                func.sum(ViumLot.qty_remaining * ViumLot.unit_price), 0
            ),
            func.count(ViumLot.id),
        )
        .filter(ViumLot.qty_remaining > 0)
        .group_by(ViumLot.material_id)
        .all()
    )
    out: dict[int, dict] = {}
    for mat_id, qty, value, lots in rows:
        q = _to_dec(qty)
        v = _to_dec(value)
        out[int(mat_id)] = {
            'qty': q,
            'value': v,
            'avg_price': (v / q) if q > ZERO else ZERO,
            'lots': int(lots or 0),
        }
    return out


def list_active_lots(material_id: int) -> list[ViumLot]:
    """Живые партии материала в порядке FIFO."""
    return (
        ViumLot.query
        .filter(
            ViumLot.material_id == material_id,
            ViumLot.qty_remaining > 0,
        )
        .order_by(ViumLot.received_at.asc(), ViumLot.id.asc())
        .all()
    )


def validate_consume_qty(material_id: int, qty) -> Tuple[bool, Decimal]:
    """Хватит ли остатков на списание qty единиц материала."""
    qty_dec = _to_dec(qty)
    bal = current_balance(material_id)
    return (bal['qty'] + QTY_EPS >= qty_dec, bal['qty'])


# ---------------------------------------------------------------------------
# Применение операций
# ---------------------------------------------------------------------------

def apply_intake(op: ViumOperation) -> None:
    """Создать партии из intake-операции. Идемпотентность не гарантируется
    (повторный вызов создаст вторые партии); вызывайте один раз сразу
    после `db.session.add(line)` и до commit.
    """
    if op.kind != 'intake':
        raise ValueError(f'apply_intake: операция не intake (kind={op.kind!r})')
    for line in op.lines:
        qty = _to_dec(line.qty)
        if qty <= ZERO:
            continue
        price = _to_dec(line.unit_price)
        lot = ViumLot(
            material_id=line.material_id,
            received_at=op.date,
            qty_received=qty,
            qty_remaining=qty,
            unit_price=price,
            source_invoice_id=op.invoice_id,
            source_operation_id=op.id,
            note=(line.note or None),
        )
        db.session.add(lot)


def _consume_fifo(material_id: int, qty_to_take: Decimal) -> list[dict]:
    """Внутренняя функция: уменьшить остатки партий по FIFO и вернуть
    срезы партий, использованных для покрытия `qty_to_take`.

    Возвращает список dict `{lot_id, qty, unit_price}` для записи в
    `ViumOperationLine.lot_consumption`.

    Бросает `ValueError`, если остатков не хватает.
    """
    if qty_to_take <= ZERO:
        return []
    consumption: list[dict] = []
    remaining = qty_to_take
    lots = list_active_lots(material_id)
    for lot in lots:
        if remaining <= ZERO:
            break
        avail = _to_dec(lot.qty_remaining)
        if avail <= ZERO:
            continue
        take = avail if avail <= remaining else remaining
        lot.qty_remaining = avail - take
        consumption.append({
            'lot_id': int(lot.id),
            'qty': str(take),
            'unit_price': str(_to_dec(lot.unit_price)),
        })
        remaining -= take
    if remaining > QTY_EPS:
        raise ValueError(
            f'Недостаточно остатков для material_id={material_id}: '
            f'не хватает {remaining}'
        )
    return consumption


def apply_consume(op: ViumOperation) -> None:
    """Списание (расход / writeoff). Режет партии по FIFO."""
    if op.kind not in ('consume', 'writeoff'):
        raise ValueError(
            f'apply_consume: операция не consume/writeoff (kind={op.kind!r})'
        )
    for line in op.lines:
        qty = _to_dec(line.qty)
        if qty <= ZERO:
            continue
        consumption = _consume_fifo(line.material_id, qty)
        line.lot_consumption = json.dumps(consumption, ensure_ascii=False)
        # Себестоимость списания (для отчётов): сумма qty*price по срезам.
        if consumption:
            cost = sum(
                _to_dec(c['qty']) * _to_dec(c['unit_price'])
                for c in consumption
            )
            if qty > ZERO:
                line.unit_price = (cost / qty).quantize(Decimal('0.0001'))


def apply_adjust(op: ViumOperation) -> None:
    """Корректировка инвентаризации.

    Положительное qty (intake-style): создаём партию по указанной цене
    (если не указана — по средневзвешенной из остатков, либо 0).
    Отрицательное qty: режем FIFO так же, как consume.
    """
    if op.kind != 'adjust':
        raise ValueError(f'apply_adjust: операция не adjust (kind={op.kind!r})')
    for line in op.lines:
        qty = _to_dec(line.qty)
        if qty == ZERO:
            continue
        if qty > ZERO:
            price = _to_dec(line.unit_price)
            if price <= ZERO:
                bal = current_balance(line.material_id)
                price = bal['avg_price']
            lot = ViumLot(
                material_id=line.material_id,
                received_at=op.date,
                qty_received=qty,
                qty_remaining=qty,
                unit_price=price,
                source_operation_id=op.id,
                note='корректировка (+)',
            )
            db.session.add(lot)
            line.unit_price = price
        else:
            consumption = _consume_fifo(line.material_id, -qty)
            line.lot_consumption = json.dumps(consumption, ensure_ascii=False)
            if consumption:
                cost = sum(
                    _to_dec(c['qty']) * _to_dec(c['unit_price'])
                    for c in consumption
                )
                if qty < ZERO:
                    line.unit_price = (cost / -qty).quantize(Decimal('0.0001'))


def apply_operation(op: ViumOperation) -> None:
    """Универсальная точка входа: вызывает нужный apply_* по op.kind."""
    if op.kind == 'intake':
        apply_intake(op)
    elif op.kind in ('consume', 'writeoff'):
        apply_consume(op)
    elif op.kind == 'adjust':
        apply_adjust(op)
    else:
        raise ValueError(f'Неизвестный тип операции: {op.kind!r}')


# ---------------------------------------------------------------------------
# Хелперы для UI
# ---------------------------------------------------------------------------

OP_KIND_LABELS = {
    'intake':   'Поступление',
    'consume':  'Расход',
    'writeoff': 'Списание',
    'adjust':   'Корректировка',
}


def op_kind_label(kind: str) -> str:
    return OP_KIND_LABELS.get(kind, kind or '')


def parse_lot_consumption(payload: str | None) -> list[dict]:
    """Безопасный JSON-парс `lot_consumption` для шаблонов."""
    if not payload:
        return []
    try:
        data = json.loads(payload)
        return data if isinstance(data, list) else []
    except Exception:
        return []
