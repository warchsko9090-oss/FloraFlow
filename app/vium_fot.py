"""Сервис расчёта ФОТ-доли на 1 куст за день.

Используется в отчёте «Себестоимость» как дополнительная компонента
к закупке/опексу/амортизации, и в отчёте /vium/plan-report как индикатор
эффективности дня.

Расчёт on-demand (ничего не кешируется и не пишется в БД), потому что
часы могут вводиться задним числом — например, бригадир введёт 10-го мая
часы за 9-е, и отчёт за 9-е должен сам пересчитаться.

Логика дня D:
1. Берём всех Employee с role in ('brigadier','worker') (это рабочие на копке).
2. Для каждого ищем TimeLog за D и считаем рубли по тем же формулам, что
   `_calc_payroll_row` в `app/hr.py`, но только за один день, без месячных
   корректировок и выплат.
3. Суммируем по всем сотрудникам => Daily_FOT.
4. qty_dug = sum(DiggingLog.quantity where date=D, status!='rejected').
5. ФОТ ₽/шт = Daily_FOT / qty_dug. None если qty_dug=0.

Окладные сотрудники (`is_salary=True`) корректно распределяются по году:
их месячный оклад делится на количество отработанных дней этого месяца
(т. е. дней, где есть `TimeLog` с часами или is_day_off). Это даёт
честный «доля оклада за день D», без пиков на любой случайный день.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from decimal import Decimal
from datetime import date as _date, timedelta
from typing import Iterable

from sqlalchemy import func

from app.models import (
    db,
    Employee, SalaryRate, TimeLog,
    DiggingLog,
)

log = logging.getLogger(__name__)
ZERO = Decimal('0')

# Роли, для которых считаем «производственный» ФОТ. Бригадиры и рабочие.
WORKER_ROLES = ('brigadier', 'worker')

# Кеш ставок в рамках одного запроса. Ключ: (year, role) -> {rate_type: Decimal}
_rates_cache_key = '_vium_fot_rates_cache'


# ---------------------------------------------------------------------------
# Внутренние помощники
# ---------------------------------------------------------------------------

def _to_dec(v) -> Decimal:
    if v is None or v == '':
        return ZERO
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def _rates_for_year(year: int) -> dict[str, dict[str, Decimal]]:
    """{role: {rate_type: rate_value}} для данного года.

    Если для года нет записей в SalaryRate — пытаемся взять
    предыдущий доступный год (чтобы не получить сплошные нули).
    """
    rates = SalaryRate.query.filter_by(year=year).all()
    if not rates:
        # fallback на любой ранее доступный год
        latest_year = (
            db.session.query(func.max(SalaryRate.year))
            .filter(SalaryRate.year <= year)
            .scalar()
        )
        if latest_year:
            rates = SalaryRate.query.filter_by(year=latest_year).all()
    out: dict[str, dict[str, Decimal]] = {}
    for r in rates:
        out.setdefault(r.role, {})[r.rate_type] = _to_dec(r.rate_value)
    return out


def _worker_employees() -> list[Employee]:
    return (
        Employee.query
        .filter(
            Employee.is_active == True,  # noqa: E712
            Employee.role.in_(WORKER_ROLES),
        )
        .all()
    )


def _employee_worked_days_in_month(emp_id: int, year: int, month: int) -> int:
    """Сколько дней в месяце у сотрудника есть TimeLog с часами/выходным.

    Используется для распределения оклада окладного сотрудника по дням.
    """
    if month == 12:
        next_first = _date(year + 1, 1, 1)
    else:
        next_first = _date(year, month + 1, 1)
    first = _date(year, month, 1)
    rows = TimeLog.query.filter(
        TimeLog.employee_id == emp_id,
        TimeLog.date >= first,
        TimeLog.date < next_first,
    ).all()
    days = 0
    for tl in rows:
        worked = float(tl.hours_norm or 0) + float(tl.hours_norm_over or 0) \
                 + float(tl.hours_spec or 0) + float(tl.hours_spec_over or 0)
        if worked > 0 or getattr(tl, 'is_day_off', False):
            days += 1
    return days


# ---------------------------------------------------------------------------
# Дневные расчёты
# ---------------------------------------------------------------------------

def _daily_fot_for_employee(emp: Employee, d: _date,
                             rates_by_role: dict[str, dict[str, Decimal]]) -> Decimal:
    """ФОТ конкретного сотрудника за день D (₽).

    Правила:
      * is_salary=True: оклад_месячный / число_отработанных_дней_этого_месяца,
        если у сотрудника есть запись TimeLog за D (с часами или выходным).
        Иначе 0.
      * mechanic (формально не входит в WORKER_ROLES, но оставлено для общности):
        rates['norm'] за каждый рабочий день (по флагу часов).
      * Прочие — суммируем (часы * ставка) по всем 4 типам часов.
        Выходные: если is_day_off — добавляем 10ч × ставка_norm (но не более
        3 оплачиваемых выходных в месяц; за конкретный день D мы просто
        считаем флаг — лимит «3 выходных в месяц» применяется только при
        месячных отчётах).
    """
    tl = TimeLog.query.filter_by(employee_id=emp.id, date=d).first()
    if not tl:
        return ZERO

    rates = rates_by_role.get(emp.role, {})

    if emp.is_salary:
        worked_today = (
            float(tl.hours_norm or 0) + float(tl.hours_norm_over or 0)
            + float(tl.hours_spec or 0) + float(tl.hours_spec_over or 0)
        )
        if worked_today <= 0 and not getattr(tl, 'is_day_off', False):
            return ZERO
        worked_days = _employee_worked_days_in_month(emp.id, d.year, d.month)
        if worked_days <= 0:
            return ZERO
        salary = _to_dec(emp.fixed_salary)
        if salary <= 0:
            return ZERO
        return (salary / Decimal(worked_days))

    if emp.role == 'mechanic':
        worked_today = (
            float(tl.hours_norm or 0) + float(tl.hours_norm_over or 0)
            + float(tl.hours_spec or 0) + float(tl.hours_spec_over or 0)
        )
        if worked_today > 0 or getattr(tl, 'is_day_off', False):
            return rates.get('norm', ZERO)
        return ZERO

    earned = ZERO
    earned += _to_dec(tl.hours_norm) * rates.get('norm', ZERO)
    earned += _to_dec(tl.hours_norm_over) * rates.get('norm_over', ZERO)
    earned += _to_dec(tl.hours_spec) * rates.get('spec', ZERO)
    earned += _to_dec(tl.hours_spec_over) * rates.get('spec_over', ZERO)
    if getattr(tl, 'is_day_off', False):
        # 10 часов × norm — оплата выходного дня (без месячного лимита).
        earned += Decimal('10') * rates.get('norm', ZERO)
    return earned


def daily_fot_amount(d: _date) -> Decimal:
    """Суммарный ФОТ всех рабочих за день D."""
    if not d:
        return ZERO
    rates = _rates_for_year(d.year)
    employees = _worker_employees()
    total = ZERO
    for emp in employees:
        total += _daily_fot_for_employee(emp, d, rates)
    return total


def daily_dug_qty(d: _date) -> int:
    """Сумма выкопок за день D (status != rejected)."""
    if not d:
        return 0
    val = (
        db.session.query(func.coalesce(func.sum(DiggingLog.quantity), 0))
        .filter(
            DiggingLog.date == d,
            DiggingLog.status != 'rejected',
        )
        .scalar()
    )
    return int(val or 0)


def daily_fot_per_unit(d: _date) -> Decimal | None:
    """ФОТ ₽/шт за день D. None если выкопок не было."""
    qty = daily_dug_qty(d)
    if qty <= 0:
        return None
    return daily_fot_amount(d) / Decimal(qty)


# ---------------------------------------------------------------------------
# Периодные расчёты
# ---------------------------------------------------------------------------

def _iter_days(start: _date, end: _date) -> Iterable[_date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def period_fot_amount(start: _date, end: _date) -> Decimal:
    """Сумма ФОТ за период (агрегат по дням)."""
    if not start or not end or end < start:
        return ZERO
    # Оптимизация: ставки меняем при смене года.
    total = ZERO
    employees = _worker_employees()
    rates_by_year: dict[int, dict[str, dict[str, Decimal]]] = {}
    for d in _iter_days(start, end):
        rates = rates_by_year.get(d.year)
        if rates is None:
            rates = _rates_for_year(d.year)
            rates_by_year[d.year] = rates
        for emp in employees:
            total += _daily_fot_for_employee(emp, d, rates)
    return total


def period_dug_qty(start: _date, end: _date) -> int:
    if not start or not end or end < start:
        return 0
    val = (
        db.session.query(func.coalesce(func.sum(DiggingLog.quantity), 0))
        .filter(
            DiggingLog.date >= start,
            DiggingLog.date <= end,
            DiggingLog.status != 'rejected',
        )
        .scalar()
    )
    return int(val or 0)


def period_fot_per_unit(start: _date, end: _date) -> Decimal:
    """Агрегированный ФОТ ₽/шт за период (сумма ФОТ / сумма выкопанных)."""
    qty = period_dug_qty(start, end)
    if qty <= 0:
        return ZERO
    return period_fot_amount(start, end) / Decimal(qty)


def missing_hours_dates(start: _date, end: _date) -> list[_date]:
    """Даты, где была выкопка, но нет ни одной записи TimeLog у рабочих.

    Используем для предупреждения «часы за DD.MM не введены, ФОТ за этот
    день пока 0».
    """
    if not start or not end or end < start:
        return []
    # Дни с выкопкой (быстрее, чем итерировать по календарю).
    dug_days = (
        db.session.query(DiggingLog.date)
        .filter(
            DiggingLog.date >= start,
            DiggingLog.date <= end,
            DiggingLog.status != 'rejected',
        )
        .group_by(DiggingLog.date)
        .all()
    )
    if not dug_days:
        return []
    dug_set = {row[0] for row in dug_days}

    # Дни, где у любого worker/brigadier есть TimeLog (часы или выходной).
    worker_emp_ids = [e.id for e in _worker_employees()]
    if not worker_emp_ids:
        return sorted(dug_set)
    tl_days = (
        db.session.query(TimeLog.date)
        .filter(
            TimeLog.employee_id.in_(worker_emp_ids),
            TimeLog.date >= start,
            TimeLog.date <= end,
        )
        .group_by(TimeLog.date)
        .all()
    )
    tl_set = {row[0] for row in tl_days}

    return sorted(d for d in dug_set if d not in tl_set)
