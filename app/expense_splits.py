# -*- coding: utf-8 -*-
"""Разбивка расхода по нескольким статьям бюджета (ExpenseBudgetLine)."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from flask import request

from app.models import BudgetItem, Expense, ExpenseBudgetLine, db

TWOPLACES = Decimal('0.01')


def D(value) -> Decimal:
    if value is None or value == '':
        return Decimal('0')
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(' ', '').replace(',', '.'))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal('0')


def money2(value) -> Decimal:
    return D(value).quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def parse_split_lines_from_form(form=None) -> list[dict]:
    """Читает split_item_id[] + split_amount[] из формы.

    Если массивов нет — fallback на одно поле budget_item / budget_item_id.
    """
    form = form if form is not None else request.form
    ids = form.getlist('split_item_id')
    amts = form.getlist('split_amount')
    lines: list[dict] = []
    if ids:
        for i, raw_id in enumerate(ids):
            if raw_id in (None, ''):
                continue
            try:
                bid = int(raw_id)
            except (TypeError, ValueError):
                continue
            amt = money2(amts[i] if i < len(amts) else 0)
            if amt <= 0:
                continue
            lines.append({'budget_item_id': bid, 'amount': amt})
        return lines

    single = form.get('budget_item') or form.get('budget_item_id')
    if single not in (None, ''):
        try:
            bid = int(single)
        except (TypeError, ValueError):
            return []
        amt = money2(form.get('amount'))
        if amt > 0:
            lines.append({'budget_item_id': bid, 'amount': amt})
    return lines


def validate_split_lines(lines: list[dict], total: Decimal) -> tuple[bool, str]:
    total = money2(total)
    if total <= 0:
        return False, 'Сумма расхода должна быть больше нуля'
    if not lines:
        return False, 'Укажите хотя бы одну статью бюджета'
    for row in lines:
        if not BudgetItem.query.get(row['budget_item_id']):
            return False, 'Статья бюджета не найдена'
        if money2(row['amount']) <= 0:
            return False, 'Сумма по статье должна быть больше нуля'
    s = money2(sum((money2(r['amount']) for r in lines), Decimal('0')))
    if s != total:
        return False, f'Сумма по статьям ({s}) не равна сумме расхода ({total})'
    return True, ''


def apply_budget_lines(expense: Expense, lines: list[dict]) -> None:
    """Перезаписывает строки разбивки. expense.amount уже должен быть задан."""
    total = money2(expense.amount)
    ok, msg = validate_split_lines(lines, total)
    if not ok:
        raise ValueError(msg)

    expense.budget_lines.clear()
    db.session.flush()
    for i, row in enumerate(lines):
        expense.budget_lines.append(ExpenseBudgetLine(
            budget_item_id=int(row['budget_item_id']),
            amount=money2(row['amount']),
            sort_order=i,
        ))
    expense.budget_item_id = int(lines[0]['budget_item_id'])


def lines_as_dicts(expense: Expense | None) -> list[dict]:
    if not expense:
        return []
    rows = list(getattr(expense, 'budget_lines', None) or [])
    if rows:
        return [
            {
                'budget_item_id': ln.budget_item_id,
                'amount': float(money2(ln.amount)),
                'code': getattr(ln.item, 'code', None) if ln.item else None,
                'name': getattr(ln.item, 'name', None) if ln.item else None,
            }
            for ln in sorted(rows, key=lambda x: (x.sort_order or 0, x.id or 0))
        ]
    if expense.budget_item_id and expense.amount is not None:
        return [{
            'budget_item_id': expense.budget_item_id,
            'amount': float(money2(expense.amount)),
            'code': getattr(expense.item, 'code', None) if expense.item else None,
            'name': getattr(expense.item, 'name', None) if expense.item else None,
        }]
    return []


def ensure_legacy_line(expense: Expense) -> None:
    """Если у расхода ещё нет строк — создать одну из budget_item_id."""
    if not expense or not expense.id:
        return
    if expense.budget_lines:
        return
    if not expense.budget_item_id:
        return
    expense.budget_lines.append(ExpenseBudgetLine(
        budget_item_id=expense.budget_item_id,
        amount=money2(expense.amount),
        sort_order=0,
    ))


def expand_expenses_for_list(expenses: list) -> list[dict]:
    """Один расход с N статьями → N строк журнала (для UI).

    У каждой строки: expense, line_item, line_amount, is_split, part/parts,
    split_tag, show_actions (кнопки только на первой части).
    """
    rows: list[dict] = []
    for e in expenses or []:
        lines = sorted(
            list(getattr(e, 'budget_lines', None) or []),
            key=lambda x: (x.sort_order or 0, x.id or 0),
        )
        if len(lines) > 1:
            n = len(lines)
            for i, ln in enumerate(lines):
                rows.append({
                    'expense': e,
                    'line': ln,
                    'line_item': ln.item,
                    'line_amount': ln.amount,
                    'is_split': True,
                    'part': i + 1,
                    'parts': n,
                    'split_tag': f'Σ#{e.id}',
                    'show_actions': i == 0,
                })
        else:
            ln = lines[0] if lines else None
            rows.append({
                'expense': e,
                'line': ln,
                'line_item': (ln.item if ln and ln.item else e.item),
                'line_amount': (ln.amount if ln is not None else e.amount),
                'is_split': False,
                'part': 1,
                'parts': 1,
                'split_tag': '',
                'show_actions': True,
            })
    return rows


def multi_split_expense_ids_subquery():
    """expense_id с более чем одной строкой разбивки."""
    from sqlalchemy import func
    return (
        db.session.query(ExpenseBudgetLine.expense_id)
        .group_by(ExpenseBudgetLine.expense_id)
        .having(func.count(ExpenseBudgetLine.id) > 1)
    )


def budget_fact_rows_for_year(year: int):
    """Факт бюджета по статьям/месяцам с учётом разбивки.

    Берём сумму ExpenseBudgetLine; если строк нет (наследие) — Expense.amount.
    Возвращает list[(budget_item_id, month, amount)].
    """
    from sqlalchemy import func

    actual_month = func.coalesce(Expense.target_month, func.extract('month', Expense.date))
    actual_year = func.coalesce(Expense.target_year, func.extract('year', Expense.date))

    with_lines = (
        db.session.query(
            ExpenseBudgetLine.budget_item_id.label('budget_item_id'),
            actual_month.label('month'),
            func.sum(ExpenseBudgetLine.amount).label('amount'),
        )
        .join(Expense, Expense.id == ExpenseBudgetLine.expense_id)
        .filter(actual_year == year)
        .group_by(ExpenseBudgetLine.budget_item_id, actual_month)
    )

    # Расходы без строк (на случай до бэкапа) — по старой схеме
    orphan = (
        db.session.query(
            Expense.budget_item_id.label('budget_item_id'),
            actual_month.label('month'),
            func.sum(Expense.amount).label('amount'),
        )
        .outerjoin(ExpenseBudgetLine, ExpenseBudgetLine.expense_id == Expense.id)
        .filter(actual_year == year, ExpenseBudgetLine.id.is_(None))
        .group_by(Expense.budget_item_id, actual_month)
    )

    rows: dict[tuple[int, int], Decimal] = {}
    for q in (with_lines, orphan):
        for bid, month, amt in q.all():
            if bid is None or month is None:
                continue
            key = (int(bid), int(month))
            rows[key] = rows.get(key, Decimal('0')) + money2(amt)
    return [(bid, month, amt) for (bid, month), amt in rows.items()]


def cost_expense_rows_by_year(
    *,
    period_start=None,
    period_end=None,
    project_ids=None,
    general_only: bool = False,
):
    """Факт расходов по (год, статья[, project_id]) с учётом разбивки.

    general_only=True — только общие (без order/project), как себестоимость полей.
    project_ids — фильтр проектов (себестоимость контейнеров).
    Возвращает list[(year, budget_item_id, amount)] или
    list[(year, budget_item_id, project_id, amount)] если project_ids задан.
    """
    from sqlalchemy import func

    year_col = func.extract('year', Expense.date).label('year')
    filters = [Expense.order_id.is_(None)]
    if general_only:
        filters.append(Expense.project_id.is_(None))
    if project_ids is not None:
        filters.append(Expense.project_id.in_([int(x) for x in project_ids]))
    if period_start and period_end:
        filters.extend([Expense.date >= period_start, Expense.date <= period_end])

    with_project = project_ids is not None
    line_cols = [year_col, ExpenseBudgetLine.budget_item_id]
    orphan_cols = [year_col, Expense.budget_item_id]
    group_line = [year_col, ExpenseBudgetLine.budget_item_id]
    group_orphan = [year_col, Expense.budget_item_id]
    if with_project:
        line_cols.append(Expense.project_id)
        orphan_cols.append(Expense.project_id)
        group_line.append(Expense.project_id)
        group_orphan.append(Expense.project_id)

    with_lines = (
        db.session.query(
            *line_cols,
            func.sum(ExpenseBudgetLine.amount).label('amount'),
        )
        .join(Expense, Expense.id == ExpenseBudgetLine.expense_id)
        .filter(*filters)
        .group_by(*group_line)
    )
    orphan = (
        db.session.query(
            *orphan_cols,
            func.sum(Expense.amount).label('amount'),
        )
        .outerjoin(ExpenseBudgetLine, ExpenseBudgetLine.expense_id == Expense.id)
        .filter(*filters, ExpenseBudgetLine.id.is_(None))
        .group_by(*group_orphan)
    )

    out: list[tuple] = []
    for q in (with_lines, orphan):
        for row in q.all():
            if with_project:
                year, bid, pid, amt = row
                if bid is None or year is None:
                    continue
                out.append((int(year), int(bid), int(pid) if pid is not None else None, money2(amt)))
            else:
                year, bid, amt = row
                if bid is None or year is None:
                    continue
                out.append((int(year), int(bid), money2(amt)))
    return out
