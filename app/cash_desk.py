"""Касса в Mini App оплат: наличные по людям, отдельно от табеля и ERP.

Плюс — расход сотрудника. Минус — пополнение его кассы (выдача ДС).
Оба вида движений живут только в кассе: без чата расходов и без Expense.
Стартовое сальдо задаётся в админке приложений основной программы.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import Blueprint, jsonify, request
from sqlalchemy import case, func, or_

from app.models import (
    CashHolder,
    CashMove,
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
    totals = {holder_id: Decimal(str(total or 0)) for holder_id, total in rows}
    for holder in CashHolder.query.all():
        opening = Decimal(str(holder.opening_balance or 0))
        totals[holder.id] = opening + totals.get(holder.id, Decimal('0'))
    return totals


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
            title = move.note or 'Расход'
        else:
            title = move.note or 'Поступление ДС'
        # posted=True только у старых записей, когда пополнение ещё писало Expense.
        payload.append({
            'id': move.id,
            'kind': move.kind,
            'amount': _num(move.amount),
            'signed': _num(signed),
            'date': move.move_date.isoformat() if move.move_date else '',
            'title': title,
            'posted': move.source_kind == 'invoice',
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


@bp.route('/api/cash/holders/<int:holder_id>/receipt', methods=['POST'])
@require_user
def add_receipt(user, holder_id: int):
    """Расход сотрудника: только запись в кассе."""
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    body = request.get_json(silent=True) or {}
    purpose = str(body.get('purpose') or '').strip()
    if not purpose:
        return jsonify({'error': 'Напишите, на что расход'}), 400
    try:
        amount = _amount(body.get('amount'))
        day = _date(body.get('date'))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    move = CashMove(
        holder_id=holder.id,
        kind='receipt',
        amount=amount,
        move_date=day,
        source_kind='spend',
        note=purpose[:300],
        created_by_user_id=user.id,
        created_at=msk_now(),
    )
    db.session.add(move)
    db.session.commit()
    return jsonify({'ok': True, 'id': move.id})


@bp.route('/api/cash/holders/<int:holder_id>/payout', methods=['POST'])
@require_user
def add_payout(user, holder_id: int):
    """Пополнение кассы сотрудника: только внутри кассы, без чата и без Expense."""
    denied = _deny(user)
    if denied:
        return denied
    holder = _holder_or_404(holder_id)
    if not holder:
        return jsonify({'error': 'Сотрудник не найден'}), 404
    body = request.get_json(silent=True) or {}
    purpose = str(body.get('purpose') or body.get('note') or '').strip()
    try:
        amount = _amount(body.get('amount'))
        day = _date(body.get('date'))
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    move = CashMove(
        holder_id=holder.id,
        kind='payout',
        amount=amount,
        move_date=day,
        source_kind='topup',
        note=(purpose or 'Поступление ДС')[:300],
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

    # Старые пополнения могли создать счёт/расход — подчищаем хвост при удалении.
    week = None
    if move.source_kind == 'invoice' and move.source_id:
        from flask import current_app
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
        from flask import current_app
        try:
            from app.tg_pay import refresh_week_plan_pin
            refresh_week_plan_pin(week)
        except Exception:
            current_app.logger.exception('week pin after legacy cash receipt delete')
    return jsonify({'ok': True})
