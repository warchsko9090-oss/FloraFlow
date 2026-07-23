"""Сервис тех.карт ВиУМ.

Тех.карта = норма расхода материалов на 1 куст для пары (plant, size).
В отличие от фактического склада (`ViumLot`), результат — «плановое»
списание (`ViumPlannedConsume`), которое создаётся хуком после
сохранения `DiggingLog`. Реальный остаток партий не трогаем.

Все функции пишут в `db.session`, но **не коммитят** — решение о коммите
оставляем за вызывающим кодом (как в `vium_service`).
"""
from __future__ import annotations

import logging
from decimal import Decimal
from datetime import date as _date
from typing import Iterable

from sqlalchemy import func

from app.models import (
    db,
    DiggingLog,
    Plant, Size,
    ViumLot, ViumMaterial,
    ViumOperation, ViumOperationLine,
    ViumTechCardLine, ViumPlannedConsume,
)
from app import vium_service

log = logging.getLogger(__name__)
ZERO = Decimal('0')


# ---------------------------------------------------------------------------
# Чтение тех.карты
# ---------------------------------------------------------------------------

def card_for(plant_id: int, size_id: int) -> list[ViumTechCardLine]:
    """Активные строки тех.карты для пары (plant, size)."""
    if not plant_id or not size_id:
        return []
    return (
        ViumTechCardLine.query
        .filter_by(plant_id=plant_id, size_id=size_id, is_active=True)
        .order_by(ViumTechCardLine.id.asc())
        .all()
    )


def all_pairs_with_cards() -> list[dict]:
    """Все пары (plant, size), у которых есть строки тех.карты.

    Возвращает список словарей с агрегатом — удобно для index-страницы.
    """
    rows = (
        db.session.query(
            ViumTechCardLine.plant_id,
            ViumTechCardLine.size_id,
            Plant.name.label('plant_name'),
            Size.name.label('size_name'),
            func.count(ViumTechCardLine.id).label('lines_count'),
        )
        .join(Plant, Plant.id == ViumTechCardLine.plant_id)
        .join(Size, Size.id == ViumTechCardLine.size_id)
        .filter(ViumTechCardLine.is_active == True)  # noqa: E712
        .group_by(
            ViumTechCardLine.plant_id, ViumTechCardLine.size_id,
            Plant.name, Size.name,
        )
        .order_by(Plant.name.asc(), Size.name.asc())
        .all()
    )
    return [
        {
            'plant_id': r.plant_id,
            'size_id': r.size_id,
            'plant_name': r.plant_name,
            'size_name': r.size_name,
            'lines_count': int(r.lines_count or 0),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Применение тех.карты к выкопкам
# ---------------------------------------------------------------------------

def apply_planned_consume_for_log(log_obj: DiggingLog) -> int:
    """Создать ViumPlannedConsume для каждой строки тех.карты.

    Возвращает количество созданных записей (0 если карты нет, или
    у `log_obj` нет plant_id/size_id/quantity).
    """
    if not log_obj or not log_obj.plant_id or not log_obj.size_id:
        return 0
    qty = int(log_obj.quantity or 0)
    if qty <= 0:
        return 0

    lines = card_for(log_obj.plant_id, log_obj.size_id)
    if not lines:
        return 0

    created = 0
    for line in lines:
        per_bush = Decimal(str(line.qty_per_bush or 0))
        if per_bush <= 0:
            continue
        planned = per_bush * Decimal(qty)
        db.session.add(ViumPlannedConsume(
            digging_log_id=log_obj.id,
            plant_id=log_obj.plant_id,
            size_id=log_obj.size_id,
            material_id=line.material_id,
            qty_planned=planned,
            log_date=log_obj.date,
        ))
        created += 1
    return created


def replace_planned_for_log(log_obj: DiggingLog) -> int:
    """Удалить старые плановые записи и пересоздать (после правки qty)."""
    if not log_obj or not log_obj.id:
        return 0
    ViumPlannedConsume.query.filter_by(digging_log_id=log_obj.id).delete(
        synchronize_session='fetch'
    )
    db.session.flush()
    return apply_planned_consume_for_log(log_obj)


def delete_planned_for_log(log_id: int) -> int:
    """Удалить плановые записи по id выкопки (при удалении/откате)."""
    if not log_id:
        return 0
    n = ViumPlannedConsume.query.filter_by(
        digging_log_id=log_id
    ).delete(synchronize_session=False)
    return int(n or 0)


# ---------------------------------------------------------------------------
# Сводки для отчёта планового расхода
# ---------------------------------------------------------------------------

def _intake_qty_for_period(material_id: int | None, start: _date, end: _date) -> dict[int, Decimal]:
    """Сумма прихода (`qty_received` партий) по материалам за период.

    Если `material_id` задан — фильтруем по нему. Возвращает {material_id: qty}.
    """
    q = db.session.query(
        ViumLot.material_id,
        func.coalesce(func.sum(ViumLot.qty_received), 0),
    ).filter(
        ViumLot.received_at >= start,
        ViumLot.received_at <= end,
    )
    if material_id is not None:
        q = q.filter(ViumLot.material_id == material_id)
    q = q.group_by(ViumLot.material_id)
    return {int(mid): vium_service._to_dec(qty) for mid, qty in q.all()}


def _real_consume_qty_for_period(material_id: int | None, start: _date, end: _date) -> dict[int, Decimal]:
    """Фактический расход за период (sum по строкам consume/writeoff операций)."""
    q = db.session.query(
        ViumOperationLine.material_id,
        func.coalesce(func.sum(ViumOperationLine.qty), 0),
    ).join(
        ViumOperation, ViumOperation.id == ViumOperationLine.operation_id
    ).filter(
        ViumOperation.kind.in_(('consume', 'writeoff')),
        ViumOperation.date >= start,
        ViumOperation.date <= end,
    )
    if material_id is not None:
        q = q.filter(ViumOperationLine.material_id == material_id)
    q = q.group_by(ViumOperationLine.material_id)
    return {int(mid): vium_service._to_dec(qty) for mid, qty in q.all()}


def _planned_consume_qty_for_period(material_id: int | None, start: _date, end: _date) -> dict[int, Decimal]:
    """Плановый расход за период (sum по ViumPlannedConsume)."""
    q = db.session.query(
        ViumPlannedConsume.material_id,
        func.coalesce(func.sum(ViumPlannedConsume.qty_planned), 0),
    ).filter(
        ViumPlannedConsume.log_date >= start,
        ViumPlannedConsume.log_date <= end,
    )
    if material_id is not None:
        q = q.filter(ViumPlannedConsume.material_id == material_id)
    q = q.group_by(ViumPlannedConsume.material_id)
    return {int(mid): vium_service._to_dec(qty) for mid, qty in q.all()}


def materials_plan_overview(start: _date, end: _date) -> list[dict]:
    """Сводный отчёт по материалам за период.

    Для каждого активного материала возвращает:
      * intake_period       — сумма прихода за период;
      * real_consume_period — фактический расход за период;
      * planned_consume_period — плановый расход за период;
      * intake_total / planned_consume_total — накопл. от старта до `end`,
        чтобы можно было показать актуальный «плановый остаток».
      * plan_balance        — intake_total - planned_consume_total (может < 0);
      * fact_balance        — реальный остаток партий (qty_remaining).
    """
    materials = (
        ViumMaterial.query
        .order_by(ViumMaterial.name.asc())
        .all()
    )
    if not materials:
        return []

    epoch_start = _date(2000, 1, 1)

    intake_p = _intake_qty_for_period(None, start, end)
    real_p = _real_consume_qty_for_period(None, start, end)
    plan_p = _planned_consume_qty_for_period(None, start, end)
    intake_total = _intake_qty_for_period(None, epoch_start, end)
    plan_total = _planned_consume_qty_for_period(None, epoch_start, end)

    fact_overview = vium_service.materials_overview()

    out = []
    for m in materials:
        intake_total_v = intake_total.get(m.id, ZERO)
        plan_total_v = plan_total.get(m.id, ZERO)
        out.append({
            'material': m,
            'intake_period': intake_p.get(m.id, ZERO),
            'real_consume_period': real_p.get(m.id, ZERO),
            'planned_consume_period': plan_p.get(m.id, ZERO),
            'intake_total': intake_total_v,
            'planned_consume_total': plan_total_v,
            'plan_balance': intake_total_v - plan_total_v,
            'fact_balance': fact_overview.get(m.id, {}).get('qty', ZERO),
            'avg_price': fact_overview.get(m.id, {}).get('avg_price', ZERO),
        })
    return out


def pairs_plan_overview(start: _date, end: _date,
                        fot_per_unit: Decimal | None = None) -> list[dict]:
    """Сводка плановых расходов по парам (plant, size) за период.

    Возвращает список словарей: пара, кол-во выкопанных, список материалов
    с qty_planned за период и денежной оценкой по средней цене.

    Если передан `fot_per_unit` (агрегат ФОТ ₽/шт за период), для каждой
    пары дополнительно считаются `fot_value = dug_qty × fot_per_unit` и
    `total_with_fot = total_value + fot_value`. ФОТ распределяется на все
    выкопанные кусты дня без разбивки по парам — поэтому здесь это просто
    общий ФОТ ₽/шт, помноженный на количество выкопок данной пары.
    """
    fot_unit = Decimal(str(fot_per_unit)) if fot_per_unit not in (None, '') else ZERO
    rows = (
        db.session.query(
            ViumPlannedConsume.plant_id,
            ViumPlannedConsume.size_id,
            ViumPlannedConsume.material_id,
            Plant.name.label('plant_name'),
            Size.name.label('size_name'),
            func.coalesce(func.sum(ViumPlannedConsume.qty_planned), 0).label('qty_total'),
        )
        .join(Plant, Plant.id == ViumPlannedConsume.plant_id)
        .join(Size, Size.id == ViumPlannedConsume.size_id)
        .filter(
            ViumPlannedConsume.log_date >= start,
            ViumPlannedConsume.log_date <= end,
        )
        .group_by(
            ViumPlannedConsume.plant_id,
            ViumPlannedConsume.size_id,
            ViumPlannedConsume.material_id,
            Plant.name, Size.name,
        )
        .order_by(Plant.name.asc(), Size.name.asc())
        .all()
    )
    if not rows:
        return []

    fact_overview = vium_service.materials_overview()
    materials_map = {m.id: m for m in ViumMaterial.query.all()}

    dug_rows = (
        db.session.query(
            DiggingLog.plant_id,
            DiggingLog.size_id,
            func.coalesce(func.sum(DiggingLog.quantity), 0),
        )
        .filter(
            DiggingLog.date >= start,
            DiggingLog.date <= end,
            DiggingLog.status != 'rejected',
        )
        .group_by(DiggingLog.plant_id, DiggingLog.size_id)
        .all()
    )
    dug_map = {(int(p), int(s)): int(q or 0) for p, s, q in dug_rows}

    pairs: dict[tuple[int, int], dict] = {}
    for r in rows:
        key = (int(r.plant_id), int(r.size_id))
        if key not in pairs:
            pairs[key] = {
                'plant_id': r.plant_id,
                'size_id': r.size_id,
                'plant_name': r.plant_name,
                'size_name': r.size_name,
                'dug_qty': dug_map.get(key, 0),
                'lines': [],
                'total_value': ZERO,
            }
        m = materials_map.get(int(r.material_id))
        avg = fact_overview.get(int(r.material_id), {}).get('avg_price', ZERO)
        qty_total = vium_service._to_dec(r.qty_total)
        line_value = qty_total * avg if avg else ZERO
        pairs[key]['lines'].append({
            'material': m,
            'qty_planned': qty_total,
            'avg_price': avg,
            'value': line_value,
        })
        pairs[key]['total_value'] = pairs[key]['total_value'] + line_value

    for p in pairs.values():
        dug = Decimal(int(p['dug_qty'] or 0))
        fot_value = dug * fot_unit if fot_unit else ZERO
        p['fot_unit'] = fot_unit
        p['fot_value'] = fot_value
        p['total_with_fot'] = p['total_value'] + fot_value

    return list(pairs.values())
