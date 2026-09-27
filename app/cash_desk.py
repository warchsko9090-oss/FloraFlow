"""Касса в Mini App оплат: наличные по людям, отдельно от табеля.

Плюс — касса должна человеку (чековый приход).
Минус — человек должен кассе (поступление ДС по плану или быстрому расходу).
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import case, func, or_

from app.models import (
    BudgetItem,
    CashHolder,
    CashMove,
    ChatExpenseMessage,
    Employee,
    Expense,
    PaymentInvoice,
    ViumInvoiceQueue,
    db,
)
from app.tg_pay import _can_edit, require_user
from app.utils import msk_now, msk_today

bp = Blueprint('cash_desk', __name__, url_prefix='/tg/pay')


def _deny(user):
    if not _can_edit(user):
        return jsonify({'error': 'Касса только для администратора'}), 403
    return None


def _num(value) -> float:
    return float(Decimal(str(value or 0)).quantize(Decimal('0.01')))


def _amount(value) -> Decimal:
    raw = str(value or '').replace(' ', '').replace('\xa0', '').replace(',', '.')
    try:
        amount = Decimal(raw).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        raise ValueError('Укажите сумму больше 0')
    if amount <= 0:
        raise ValueError('Укажите сумму больше 0')
    return amount


def _date(value):
    if not value:
        return msk_today()
    try:
        day = datetime.strptime(str(value)[:10], '%Y-%m-%d').date()
    except ValueError:
        raise ValueError('Проверьте дату')
    if day.year < 2020 or day.year > 2100:
        raise ValueError('Проверьте дату')
    return day


def _employee_id(value):
    if value in (None, '', 0, '0'):
        return None
    try:
        emp_id = int(value)
    except (TypeError, ValueError):
        raise ValueError('Сотрудник табеля не найден')
    if not Employee.query.get(emp_id):
        raise ValueError('Сотрудник табеля не найден')
    return emp_id


def _balances() -> dict[int, Decimal]:
    signed = func.coalesce(func.sum(
        case((CashMove.kind == 'receipt', CashMove.amount), else_=-CashMove.amount)
    ), 0)
    rows = db.session.query(CashMove.holder_id, signed).group_by(CashMove.holder_id).all()
    return {holder_id: Decimal(str(total or 0)) for holder_id, total in rows}


def _allocated(source_kind: str) -> dict[int, Decimal]:
    rows = (
        db.session.query(CashMove.source_id, func.coalesce(func.sum(CashMove.amount), 0))
        .filter(
            CashMove.kind == 'payout',
            CashMove.source_kind == source_kind,
            CashMove.source_id.isnot(None),
        )
        .group_by(CashMove.source_id)
        .all()
    )
    return {source_id: Decimal(str(total or 0)) for source_id, total in rows}


def _title(text, fallback: str) -> str:
    clean = ' '.join((text or '').split())
    return (clean or fallback)[:180]


def _source_rows():
    plan_used = _allocated('plan')
    quick_used = _allocated('quick')
    plans = (
        PaymentInvoice.query
        .filter(
            PaymentInvoice.kind == 'plan',
            PaymentInvoice.payment_type == 'cash',
            PaymentInvoice.status != 'draft',
        )
        .order_by(PaymentInvoice.id.desc())
        .limit(80)
        .all()
    )
    items = []
    for inv in plans:
        basis = Decimal(str(inv.planned_amount or inv.amount or 0)).quantize(Decimal('0.01'))
        left = (basis - plan_used.get(inv.id, Decimal('0'))).quantize(Decimal('0.01'))
        if basis <= 0 or left <= Decimal('0.009'):
            continue
        when = inv.week_start or inv.due_date or (inv.created_at.date() if inv.created_at else None)
        items.append({
            'kind': 'plan',
            'id': inv.id,
            'title': _title(inv.summary, 'План без названия'),
            'label': 'План',
            'amount': basis,
            'left': left,
            'date': when.isoformat() if when else '',
        })

    quicks = (
        ChatExpenseMessage.query
        .filter(
            ChatExpenseMessage.parsed_payment_type == 'cash',
            ChatExpenseMessage.status.notin_(('rejected', 'unparseable')),
            ChatExpenseMessage.parsed_amount > 0,
        )
        .order_by(ChatExpenseMessage.id.desc())
        .limit(80)
        .all()
    )
    linked = [row.matched_invoice_id for row in quicks if row.matched_invoice_id]
    plan_ids = set()
    if linked:
        plan_ids = {
            row.id for row in PaymentInvoice.query.filter(
                PaymentInvoice.id.in_(linked),
                PaymentInvoice.kind == 'plan',
                PaymentInvoice.payment_type == 'cash',
            ).all()
        }
    for row in quicks:
        if row.matched_invoice_id in plan_ids:
            continue
        basis = Decimal(str(row.parsed_amount or 0)).quantize(Decimal('0.01'))
        left = (basis - quick_used.get(row.id, Decimal('0'))).quantize(Decimal('0.01'))
        if left <= Decimal('0.009'):
            continue
        when = row.tg_date or row.created_at
        items.append({
            'kind': 'quick',
            'id': row.id,
            'title': _title(row.parsed_description or row.raw_text, 'Быстрый расход'),
            'label': 'Быстрый',
            'amount': basis,
            'left': left,
            'date': when.date().isoformat() if when else '',
        })
    items.sort(key=lambda item: item['date'], reverse=True)
    return items[:40]


def _one_source(kind: str, source_id: int):
    for item in _source_rows():
        if item['kind'] == kind and item['id'] == source_id:
            return item
    return None


def _holder_json(holder: CashHolder, balance: Decimal) -> dict:
    employee = holder.employee
    return {
        'id': holder.id,
        'name': holder.name,
        'active': bool(holder.is_active),
        'employee_id': holder.employee_id,
        'employee_name': employee.name if employee else '',
        'balance': _num(balance),
    }


def _holder_or_404(holder_id: int) -> CashHolder | None:
    return CashHolder.query.get(holder_id)


@bp.route('/api/cash/holders')
@require_user
def holders(user):
    denied = _deny(user)
    if denied:
        return denied
    show_hidden = request.args.get('hidden') == '1'
    balances = _balances()
    query = CashHolder.query
    if show_hidden:
        query = query.filter(CashHolder.is_active.is_(False))
    else:
        query = query.filter(CashHolder.is_active.is_(True))
    rows = query.order_by(CashHolder.name).all()
    hidden_count = CashHolder.query.filter(CashHolder.is_active.is_(False)).count()
    return jsonify({
        'holders': [_holder_json(row, balances.get(row.id, Decimal('0'))) for row in rows],
        'hidden_count': hidden_count,
        'hidden': show_hidden,
    })


@bp.route('/api/cash/employees')
@require_user
def employees(user):
    denied = _deny(user)
    if denied:
        return denied
    rows = (
        Employee.query
        .filter(or_(Employee.is_active.is_(True), Employee.is_active.is_(None)))
        .order_by(Employee.name)
        .all()
    )
    return jsonify({'employees': [{'id': row.id, 'name': row.name} for row in rows]})


@bp.route('/api/cash/holders', methods=['POST'])
@require_user
def create_holder(user):
    denied = _deny(user)
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    name = ' '.join((body.get('name') or '').split())[:150]
    if not name:
        return jsonify({'error': 'Укажите имя'}), 400
    try:
        employee_id = _employee_id(body.get('employee_id'))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    holder = CashHolder(
        name=name,
        employee_id=employee_id,
        is_active=True,
        created_at=msk_now(),
    )
    db.session.add(holder)
    db.session.commit()
    return jsonify({'holder': _holder_json(holder, Decimal('0'))})


@bp.route('/api/cash/holders/<int:holder_id>', methods=['GET'])
@require_user
def holder_detail(user, holder_id: int):
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    moves = (
        CashMove.query
        .filter_by(holder_id=holder.id)
        .order_by(CashMove.move_date.desc(), CashMove.id.desc())
        .limit(40)
        .all()
    )
    balance = _balances().get(holder.id, Decimal('0'))
    payload = []
    for move in moves:
        signed = move.amount if move.kind == 'receipt' else -move.amount
        if move.kind == 'receipt':
            title = move.note or 'Чековый приход'
        else:
            title = move.note or 'Поступление ДС'
        payload.append({
            'id': move.id,
            'kind': move.kind,
            'amount': _num(move.amount),
            'signed': _num(signed),
            'date': move.move_date.isoformat() if move.move_date else '',
            'title': title,
        })
    return jsonify({'holder': _holder_json(holder, balance), 'moves': payload})


@bp.route('/api/cash/holders/<int:holder_id>', methods=['PATCH'])
@require_user
def update_holder(user, holder_id: int):
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    body = request.get_json(silent=True) or {}
    if 'name' in body:
        name = ' '.join((body.get('name') or '').split())[:150]
        if not name:
            return jsonify({'error': 'Укажите имя'}), 400
        holder.name = name
    if 'employee_id' in body:
        try:
            holder.employee_id = _employee_id(body.get('employee_id'))
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400
    if 'is_active' in body:
        holder.is_active = bool(body.get('is_active'))
    db.session.commit()
    balance = _balances().get(holder.id, Decimal('0'))
    return jsonify({'holder': _holder_json(holder, balance)})


@bp.route('/api/cash/holders/<int:holder_id>', methods=['DELETE'])
@require_user
def delete_holder(user, holder_id: int):
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    if CashMove.query.filter_by(holder_id=holder.id).first():
        return jsonify({
            'error': 'Есть движения. Скрыть сотрудника? История сохранится.',
            'code': 'has_moves',
        }), 409
    db.session.delete(holder)
    db.session.commit()
    return jsonify({'ok': True})


def _plan_left(plan: PaymentInvoice) -> Decimal:
    from app.tg_pay import _fact_amount
    planned = Decimal(str(plan.planned_amount or 0)).quantize(Decimal('0.01'))
    fact = Decimal(str(_fact_amount(plan) or 0)).quantize(Decimal('0.01'))
    left = (planned - fact).quantize(Decimal('0.01'))
    return left if left > 0 else Decimal('0')


def _real_budget(item: BudgetItem | None) -> BudgetItem | None:
    if item is None or (item.code or '') == 'UNASSIGNED':
        return None
    return item


def _finish_cash_receipt(holder: CashHolder, user, amount: Decimal, day, inv: PaymentInvoice, note: str):
    from app.invoice_files import ensure_expense_for_paid_invoice, notify_invoice_paid_chat

    expense = ensure_expense_for_paid_invoice(inv)
    expense.date = day
    expense.payment_type = 'cash'
    expense.description = f'{(inv.summary or note)} · {holder.name}'[:500]
    try:
        from app.vium_inbox import maybe_enqueue
        maybe_enqueue(inv, expense=expense)
    except Exception:
        current_app.logger.exception('vium after cash receipt')

    move = CashMove(
        holder_id=holder.id,
        kind='receipt',
        amount=amount,
        move_date=day,
        source_kind='invoice',
        source_id=inv.id,
        note=note[:300],
        created_by_user_id=user.id,
        created_at=msk_now(),
    )
    db.session.add(move)
    db.session.commit()

    try:
        notify_invoice_paid_chat(inv)
    except Exception:
        current_app.logger.exception('cash receipt chat notify')
    if inv.plan_id and inv.week_start:
        try:
            from app.tg_pay import refresh_week_plan_pin
            refresh_week_plan_pin(inv.week_start)
        except Exception:
            current_app.logger.exception('week pin after cash receipt')
    return move, expense


@bp.route('/api/cash/week-plans')
@require_user
def week_plans(user):
    denied = _deny(user)
    if denied:
        return denied
    from app.tg_pay import _default_plan_week, _purpose
    week = _default_plan_week()
    rows = (
        PaymentInvoice.query
        .filter(
            PaymentInvoice.kind == 'plan',
            PaymentInvoice.week_start == week,
            PaymentInvoice.status != 'draft',
        )
        .order_by(PaymentInvoice.id.desc())
        .all()
    )
    plans = []
    for inv in rows:
        left = _plan_left(inv)
        planned = Decimal(str(inv.planned_amount or 0))
        if planned <= 0 or left <= Decimal('0.009'):
            continue
        item = _real_budget(inv.item)
        plans.append({
            'id': inv.id,
            'title': _purpose(inv),
            'left': _num(left),
            'amount': _num(planned),
            'payment_type': 'cash' if inv.payment_type == 'cash' else 'cashless',
            'budget_item_id': item.id if item else None,
            'budget_name': item.name if item else '',
        })
    return jsonify({'week_start': week.isoformat(), 'plans': plans})


@bp.route('/api/cash/holders/<int:holder_id>/receipt', methods=['POST'])
@require_user
def add_receipt(user, holder_id: int):
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    body = request.get_json(silent=True) or {}
    mode = body.get('mode') or 'article'
    try:
        amount = _amount(body.get('amount'))
        day = _date(body.get('date'))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    stamp = f"cash_{holder.id}_{int(msk_now().timestamp())}"
    if mode == 'plan':
        try:
            plan_id = int(body.get('plan_id'))
        except (TypeError, ValueError):
            return jsonify({'error': 'Выберите строку плана'}), 400
        from app.tg_pay import _default_plan_week, _purpose
        plan = PaymentInvoice.query.get(plan_id)
        if plan is None or (plan.kind or '') != 'plan' or (plan.status or '') == 'draft':
            return jsonify({'error': 'Строка плана не найдена'}), 400
        if plan.week_start != _default_plan_week():
            return jsonify({'error': 'Это не план текущей недели'}), 400
        left = _plan_left(plan)
        if amount > left + Decimal('0.009'):
            return jsonify({'error': 'Сумма больше остатка по плану'}), 400
        item = _real_budget(plan.item)
        if item is None:
            return jsonify({'error': 'У этой строки плана нет статьи бюджета'}), 400
        purpose = _purpose(plan)[:500]
        inv = PaymentInvoice(
            filename=stamp,
            original_name=purpose[:255],
            summary=purpose,
            comment=holder.name[:500],
            source='cash',
            budget_item_id=item.id,
            amount=amount,
            status='paid',
            priority=plan.priority or 'normal',
            payment_type='cash',
            kind='invoice',
            plan_id=plan.id,
            week_start=plan.week_start,
            created_by_user_id=user.id,
        )
        db.session.add(inv)
        db.session.flush()
        already = Decimal(str(plan.planned_amount or 0)) - left
        if Decimal(str(plan.planned_amount or 0)) > 0 and already + amount + Decimal('0.009') >= Decimal(str(plan.planned_amount or 0)):
            plan.status = 'paid'
        move, expense = _finish_cash_receipt(holder, user, amount, day, inv, purpose)
    else:
        try:
            budget_id = int(body.get('budget_item_id'))
        except (TypeError, ValueError):
            return jsonify({'error': 'Выберите статью'}), 400
        item = _real_budget(BudgetItem.query.get(budget_id))
        if item is None:
            return jsonify({'error': 'Выберите статью'}), 400
        article = (item.name or 'Расход').strip()[:180]
        inv = PaymentInvoice(
            filename=stamp,
            original_name=article[:255],
            summary=article[:500],
            comment=holder.name[:500],
            source='cash',
            budget_item_id=item.id,
            amount=amount,
            status='paid',
            priority='normal',
            payment_type='cash',
            kind='invoice',
            due_date=day,
            created_by_user_id=user.id,
        )
        db.session.add(inv)
        db.session.flush()
        move, expense = _finish_cash_receipt(holder, user, amount, day, inv, article)

    return jsonify({
        'ok': True,
        'id': move.id,
        'invoice_id': inv.id,
        'expense_id': expense.id,
    })


@bp.route('/api/cash/sources')
@require_user
def sources(user):
    denied = _deny(user)
    if denied:
        return denied
    rows = _source_rows()
    return jsonify({'sources': [{
        **item,
        'amount': _num(item['amount']),
        'left': _num(item['left']),
    } for item in rows]})


@bp.route('/api/cash/holders/<int:holder_id>/payout', methods=['POST'])
@require_user
def add_payout(user, holder_id: int):
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    body = request.get_json(silent=True) or {}
    kind = body.get('source_kind')
    if kind not in ('plan', 'quick'):
        return jsonify({'error': 'Выберите расход из приложения оплат'}), 400
    try:
        source_id = int(body.get('source_id'))
        amount = _amount(body.get('amount'))
        day = _date(body.get('date'))
    except (TypeError, ValueError) as exc:
        text = str(exc)
        if not text.startswith(('Укажите', 'Проверьте')):
            text = 'Проверьте сумму и расход'
        return jsonify({'error': text}), 400
    source = _one_source(kind, source_id)
    if not source:
        return jsonify({'error': 'Этот расход уже полностью выдан или не наличный'}), 400
    if amount > source['left'] + Decimal('0.009'):
        return jsonify({'error': 'Сумма больше остатка по этому расходу'}), 400
    move = CashMove(
        holder_id=holder.id,
        kind='payout',
        amount=amount,
        move_date=day,
        source_kind=kind,
        source_id=source_id,
        note=source['title'][:300],
        created_by_user_id=user.id,
        created_at=msk_now(),
    )
    db.session.add(move)
    db.session.commit()
    return jsonify({'ok': True, 'id': move.id})


@bp.route('/api/cash/moves/<int:move_id>', methods=['DELETE'])
@require_user
def delete_move(user, move_id: int):
    denied = _deny(user)
    if denied:
        return denied
    move = CashMove.query.get(move_id)
    if not move:
        return jsonify({'error': 'Запись не найдена'}), 404
    week = None
    if move.kind == 'receipt' and move.source_kind == 'invoice' and move.source_id:
        inv = PaymentInvoice.query.get(move.source_id)
        if inv is not None and (inv.source or '') == 'cash':
            plan = PaymentInvoice.query.get(inv.plan_id) if inv.plan_id else None
            week = (plan.week_start if plan else None) or inv.week_start
            if plan is not None:
                from app.invoice_files import has_file
                other = db.session.query(func.coalesce(func.sum(PaymentInvoice.amount), 0)).filter(
                    PaymentInvoice.plan_id == plan.id,
                    PaymentInvoice.id != inv.id,
                ).scalar()
                own = Decimal(str(plan.amount or 0)) if has_file(plan) else Decimal('0')
                covered = (own + Decimal(str(other or 0))).quantize(Decimal('0.01'))
                planned = Decimal(str(plan.planned_amount or 0))
                plan.status = 'paid' if planned > 0 and covered + Decimal('0.009') >= planned else 'new'
            ViumInvoiceQueue.query.filter_by(invoice_id=inv.id).delete(synchronize_session=False)
            Expense.query.filter_by(invoice_id=inv.id).delete(synchronize_session=False)
            db.session.delete(inv)
    db.session.delete(move)
    db.session.commit()
    if week:
        try:
            from app.tg_pay import refresh_week_plan_pin
            refresh_week_plan_pin(week)
        except Exception:
            current_app.logger.exception('week pin after cash receipt delete')
    return jsonify({'ok': True})
