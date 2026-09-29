"""Админка трёх Telegram-приложений в основной программе. Только для админа."""

from decimal import Decimal, InvalidOperation

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required

from app.auth import _assign_telegram_id, _parse_telegram_id
from app.models import CashHolder, SaleCompany, User, db
from app.utils import log_action

bp = Blueprint('tg_apps', __name__, url_prefix='/apps')

PAY_ROLES = ('admin', 'executive')
SALE_ROLES = ('admin', 'executive', 'shop_manager', 'accountant')
ROLE_LABEL = {
    'admin': 'Админ',
    'executive': 'Руководитель',
    'shop_manager': 'Менеджер',
    'accountant': 'Бухгалтер',
}
TABS = ('pay', 'sale', 'cash')


def _admin_only():
    if not current_user.is_authenticated or current_user.role != 'admin':
        flash('Раздел только для администратора', 'danger')
        return redirect(url_for('orders.orders_list'))
    return None


def _tab():
    tab = (request.values.get('tab') or 'cash').strip()
    return tab if tab in TABS else 'cash'


def _money_input(value) -> str:
    amount = Decimal(str(value or 0)).quantize(Decimal('0.01'))
    text = f'{amount:.2f}'.replace('.', ',')
    if text.endswith(',00'):
        return text[:-3]
    return text


def _signed_amount(raw) -> Decimal:
    text = str(raw or '').replace(' ', '').replace('\xa0', '').replace(',', '.')
    if text in ('', '-', '+', '.', '-.', '+.'):
        raise ValueError('Укажите сальдо')
    try:
        amount = Decimal(text).quantize(Decimal('0.01'))
    except (InvalidOperation, ValueError):
        raise ValueError('Проверьте сальдо')
    if amount.copy_abs() > Decimal('9999999999.99'):
        raise ValueError('Слишком большая сумма')
    return amount


def _users(roles):
    rows = (
        User.query
        .filter(User.role.in_(roles))
        .order_by(User.username)
        .all()
    )
    return [{
        'id': row.id,
        'username': row.username,
        'role': ROLE_LABEL.get(row.role, row.role or ''),
        'telegram_id': '' if row.telegram_id is None else str(row.telegram_id),
    } for row in rows]


def _save_telegram():
    user = User.query.get(request.form.get('user_id'))
    if not user:
        flash('Пользователь не найден', 'danger')
        return
    tg_id, err = _parse_telegram_id(request.form.get('telegram_id'))
    if err:
        flash(err, 'danger')
        return
    bind_err = _assign_telegram_id(user, tg_id)
    if bind_err:
        flash(bind_err, 'danger')
        return
    db.session.commit()
    log_action(f'Обновил Telegram ID пользователя {user.username}')
    flash(f'Telegram ID для {user.username} сохранён', 'success')


def _save_company():
    company = SaleCompany.query.get(request.form.get('company_id'))
    if not company:
        flash('Фирма не найдена', 'danger')
        return
    limits = {
        'short_name': 120, 'legal_name': 300, 'inn': 20, 'kpp': 20, 'ogrn': 20,
        'legal_address': 500, 'fact_address': 500, 'bank_name': 200,
        'bik': 20, 'rs': 40, 'ks': 40, 'phone': 40, 'director': 200,
    }
    for field, limit in limits.items():
        setattr(company, field, (request.form.get(field) or '').strip()[:limit])
    if not company.short_name:
        flash('Укажите короткое имя фирмы', 'danger')
        return
    vat = request.form.get('vat_mode') or 'none'
    company.vat_mode = 'included_22' if vat == 'included_22' else 'none'
    company.is_active = request.form.get('is_active') == '1'
    db.session.commit()
    log_action(f'Обновил фирму счетов {company.short_name}')
    flash(f'Фирма «{company.short_name}» сохранена', 'success')


def _save_opening():
    holder = CashHolder.query.get(request.form.get('holder_id'))
    if not holder:
        flash('Сотрудник кассы не найден', 'danger')
        return
    try:
        holder.opening_balance = _signed_amount(request.form.get('opening_balance'))
    except ValueError as exc:
        flash(str(exc), 'danger')
        return
    db.session.commit()
    log_action(f'Стартовое сальдо кассы {holder.name}: {holder.opening_balance}')
    flash(f'Стартовое сальдо для {holder.name} сохранено', 'success')


@bp.route('', methods=['GET', 'POST'])
@login_required
def index():
    denied = _admin_only()
    if denied:
        return denied
    tab = _tab()
    if request.method == 'POST':
        action = request.form.get('op')
        if action == 'telegram':
            _save_telegram()
        elif action == 'company':
            _save_company()
        elif action == 'opening':
            _save_opening()
        else:
            flash('Неизвестное действие', 'danger')
        return redirect(url_for('tg_apps.index', tab=tab))

    holders = []
    if tab == 'cash':
        from app.cash_desk import _balances
        balances = _balances()
        for holder in CashHolder.query.order_by(CashHolder.name, CashHolder.id).all():
            balance = balances.get(holder.id, Decimal('0'))
            holders.append({
                'id': holder.id,
                'name': holder.name,
                'active': bool(holder.is_active),
                'employee_name': holder.employee.name if holder.employee else '',
                'opening': _money_input(holder.opening_balance),
                'balance': _money_input(balance),
            })
    companies = []
    if tab == 'sale':
        for company in SaleCompany.query.order_by(SaleCompany.sort_order, SaleCompany.id).all():
            companies.append(company)
    return render_template(
        'apps/telegram.html',
        tab=tab,
        pay_users=_users(PAY_ROLES) if tab == 'pay' else [],
        sale_users=_users(SALE_ROLES) if tab == 'sale' else [],
        companies=companies,
        holders=holders,
    )
