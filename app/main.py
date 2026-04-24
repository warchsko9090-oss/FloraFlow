import os
import re
import json
from decimal import Decimal
from calendar import monthrange
from flask import Blueprint, redirect, url_for, send_file, send_from_directory, current_app, render_template, jsonify, request, make_response
from flask_login import current_user, login_required
from datetime import timedelta, date, datetime
from sqlalchemy import func, and_, or_
from app.models import (
    db, Order, OrderItem, Payment, PaymentInvoice, Document, PatentPeriod,
    DiggingTask, DiggingLog, ActionLog, TgTask, Client, Plant, Size, Field,
    Expense, TimeLog, Employee, User, MapSettings, DocumentRow
)
from app.utils import msk_today, msk_now, natural_key

bp = Blueprint('main', __name__)


# =========================================================================
# ====== ХЕЛПЕРЫ ДАШБОРДА И АНАЛИТИКИ =====================================
# =========================================================================
# Обе функции — _collect_kpis и _collect_analytics — собирают агрегированные
# данные для визуализации. Все запросы обёрнуты в try/except, чтобы битая
# запись в одной таблице не ломала дашборд целиком.

_ACTIVE_ORDER_STATUSES = ('reserved', 'in_progress', 'ready', 'shipped')


def _month_bounds(any_day):
    """Возвращает (first_day, last_day) календарного месяца для any_day."""
    first = any_day.replace(day=1)
    last = any_day.replace(day=monthrange(any_day.year, any_day.month)[1])
    return first, last


def _prev_month_bounds(any_day):
    """(first, last) предыдущего календарного месяца."""
    first_this = any_day.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    return _month_bounds(last_prev)


def _safe_float(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _pct_delta(cur, prev):
    if not prev:
        return None
    try:
        return round((cur - prev) * 100.0 / prev, 1)
    except Exception:
        return None


def _cash_in_for(first_day, last_day):
    """Поступления денег за период — сумма всех платежей клиентов по Payment.date.

    Именно это мы считаем «выручкой за месяц» в KPI — по запросу бизнеса
    (факт поступлений, а не зарезервированные заказы).
    """
    q = db.session.query(func.coalesce(func.sum(Payment.amount), 0)).filter(
        Payment.date >= first_day,
        Payment.date <= last_day,
    )
    return _safe_float(q.scalar() or 0)


# Старое имя оставляем как алиас, чтобы не ломать внешние места, если они где-то
# остались после рефакторинга.
def _payments_for(first_day, last_day):
    return _cash_in_for(first_day, last_day)


def _expenses_for(first_day, last_day):
    q = db.session.query(func.coalesce(func.sum(Expense.amount), 0)).filter(
        Expense.date >= first_day,
        Expense.date <= last_day,
    )
    return _safe_float(q.scalar() or 0)


def _client_debt(client):
    """Дебиторка клиента = неоплаченный остаток по АКТИВНЫМ заказам.

    Берём только не удалённые заказы в статусах reserved/in_progress/ready/shipped
    (исключаем canceled и ghost). По каждому считаем:
        debt = total_sum − paid_sum (если положительный — добавляем).

    Такой подход даёт честную текущую задолженность, без шума старых ghost-записей
    и без привязки к fixed_balance.
    """
    orders = Order.query.filter(
        Order.client_id == client.id,
        Order.is_deleted.is_(False),
        Order.status.in_(_ACTIVE_ORDER_STATUSES),
    ).all()
    total_debt = 0.0
    for o in orders:
        billed = _safe_float(o.total_sum)
        paid = _safe_float(o.paid_sum)
        diff = billed - paid
        if diff > 0.5:  # копеечные округления отсекаем
            total_debt += diff
    return total_debt


def _all_debtors():
    """Список всех клиентов с положительной задолженностью (по логике сверки)."""
    debtors = []
    for client in Client.query.all():
        try:
            debt = _client_debt(client)
        except Exception:
            current_app.logger.exception(f'_client_debt failed for client id={client.id}')
            continue
        if debt > 0.5:
            debtors.append({'id': client.id, 'name': client.name, 'debt': debt})
    debtors.sort(key=lambda r: r['debt'], reverse=True)
    return debtors


def _overall_debt():
    return sum(r['debt'] for r in _all_debtors())


def _debtors_count(limit_clients=None):
    debtors = _all_debtors()
    if limit_clients:
        return debtors[:limit_clients]
    return debtors


def _ready_orders_stats():
    """Заказы в статусе ready — кол-во и сумма."""
    orders = Order.query.filter(Order.status == 'ready', Order.is_deleted.is_(False)).all()
    total_sum = sum(_safe_float(o.total_sum) for o in orders)
    return {'count': len(orders), 'sum': total_sum}


def _drafts_count():
    return Document.query.filter_by(doc_type='client_draft').count()


def _overdue_digging_count(today):
    return DiggingTask.query.filter(
        DiggingTask.status != 'done',
        DiggingTask.planned_date < today,
    ).count()


# Полевые типы задач, относящиеся к работе бригады. Менеджеру (user)
# такие задачи в ленту не падают — даже если AI/ручной ввод по ошибке
# поставил assignee_role='user'. Это защитный фильтр против мисклассификации.
_TG_TASK_FIELD_TYPES = ('digging', 'watering', 'weeding')

# Допустимые значения scope для админа на дашборде. all/mine — «кто» (видимость),
# sales/finance/field — «что» (тематика). Хранятся в cookie feed_scope.
_ADMIN_DASHBOARD_SCOPES = ('all', 'mine', 'sales', 'finance', 'field')


def _role_match(role):
    """Возвращает SQL-условие «роль входит в TgTask.assignee_role».

    TgTask.assignee_role у нас хранится либо одной строкой ('admin',
    'user', 'executive'…), либо CSV — несколько ролей через запятую
    ('admin,executive,user' для аномалий). Точное сравнение '== role'
    не ловит CSV-форму, поэтому учитываем четыре варианта положения
    роли в строке.
    """
    return or_(
        TgTask.assignee_role == role,
        TgTask.assignee_role.like(f'{role},%'),
        TgTask.assignee_role.like(f'%,{role}'),
        TgTask.assignee_role.like(f'%,{role},%'),
    )


def _tg_task_scope_cond(user, admin_scope='all'):
    """Условие видимости TgTask для пользователя.

    Возвращает SQLAlchemy-выражение для .filter(...) или None, если
    фильтр не нужен (admin, scope=all).

    Правила:
      • admin + scope='all'                   — без фильтра (видит всё);
      • admin + scope='mine'                  — свои (исполнитель/создатель)
                                                ИЛИ адресованные роли admin
                                                (chat_expense, «админские» задачи);
      • admin + scope in {sales,finance,field} — без фильтра по ролям
                                                на SQL-уровне, тематическая
                                                фильтрация делается позже
                                                через _card_topic;
      • executive   — свои + роль executive (строка или CSV) + «ничьи» (NULL);
      • user        — свои + роль user, без полевых типов
                      (копка/полив/прополка — работа бригады,
                       менеджеру не нужна даже при неправильной роли);
      • user2       — свои + роли user2/brigadier + «ничьи» (NULL).
    """
    role = (user.role or '')

    if role == 'admin':
        if admin_scope == 'mine':
            return or_(
                TgTask.assignee_id == user.id,
                TgTask.created_by_id == user.id,
                _role_match('admin'),
            )
        return None  # 'all' и топики фильтруются позже по _card_topic

    if role == 'user':
        return and_(
            or_(
                TgTask.assignee_id == user.id,
                _role_match('user'),
            ),
            TgTask.action_type.notin_(_TG_TASK_FIELD_TYPES),
        )

    if role == 'user2':
        return or_(
            TgTask.assignee_id == user.id,
            _role_match('user2'),
            _role_match('brigadier'),
            TgTask.assignee_role.is_(None),
        )

    if role == 'executive':
        return or_(
            TgTask.assignee_id == user.id,
            _role_match('executive'),
            TgTask.assignee_role.is_(None),
        )

    # Любая иная роль — только явно свои задачи.
    return (TgTask.assignee_id == user.id)


def _card_topic(card):
    """Классифицирует карточку ленты по тематике. Нужно только админу,
    чтобы переключатель Все/Продажи/Финансы/Поле мог отфильтровать
    общий поток дашборда без изменения SQL-запросов.

    Возвращает одну из:
      • 'sales'    — заказы, черновики клиентов, «устаревший резерв/готовность»,
                     «должник с новым заказом»;
      • 'finance'  — chat_expense, счета на оплату, бюджетные аномалии,
                     кассовые разрывы;
      • 'field'    — DiggingTask, TG-задачи типа digging/watering/weeding;
      • 'hr'       — патенты, инфо по сотрудникам;
      • 'admin'    — персональный дайджест, админские задачи;
      • 'other'    — всё остальное (не ломаем поток, просто не попадает
                     в конкретную тему).
    """
    t = card.get('type') if isinstance(card, dict) else None
    if not t:
        return 'other'

    if t == 'task':              # DiggingTask
        return 'field'
    if t == 'draft':             # черновик заказа от клиента
        return 'sales'
    if t == 'chat_expense':
        return 'finance'
    if t == 'invoice':
        return 'finance'
    if t == 'patent':
        return 'hr'
    if t == 'digest':
        return 'admin'
    if t == 'anomaly':
        kind = (card.get('kind') or '')
        if kind.startswith('stale_') or kind.startswith('debtor'):
            return 'sales'
        if kind in ('budget_exceeded', 'cashgap', 'wc_low',
                    'overspend_vs_budget', 'budget_overrun'):
            return 'finance'
        if kind.startswith('stock_') or kind.startswith('digging_'):
            return 'field'
        return 'other'
    if t in ('tg_task',):
        at = (card.get('action_type') or '')
        if at in _TG_TASK_FIELD_TYPES:
            return 'field'
        if at in ('create_order', 'shipment', 'order'):
            return 'sales'
        if at in ('expense', 'income', 'payment'):
            return 'finance'
        return 'other'
    return 'other'


def _tg_tasks_stats(user, today):
    """Агрегаты по TG-задачам: активные / просроченные / закрытые за 7 дней.

    Видимость уважаем через общий helper _tg_task_scope_cond.
    """
    def _scoped(q):
        cond = _tg_task_scope_cond(user)
        return q.filter(cond) if cond is not None else q

    active = _scoped(TgTask.query.filter(TgTask.status == 'new')).count()
    overdue = _scoped(TgTask.query.filter(
        TgTask.status == 'new',
        TgTask.deadline.isnot(None),
        TgTask.deadline < today,
    )).count()
    week_ago = today - timedelta(days=7)
    done_7d = _scoped(TgTask.query.filter(
        TgTask.status == 'done',
        TgTask.created_at >= datetime.combine(week_ago, datetime.min.time()),
    )).count() if hasattr(TgTask, 'created_at') else 0

    return {'active': active, 'overdue': overdue, 'done_7d': done_7d}


def _user1_daily_stats(user, today):
    """Часы табеля и выкопка пользователя за сегодня."""
    hours = 0.0
    try:
        emp = Employee.query.filter(
            db.func.lower(Employee.name) == (user.username or '').lower()
        ).first()
        if emp:
            tl = TimeLog.query.filter_by(employee_id=emp.id, date=today).first()
            if tl:
                hours = _safe_float(
                    (tl.hours_norm or 0) + (tl.hours_norm_over or 0)
                    + (tl.hours_spec or 0) + (tl.hours_spec_over or 0)
                )
    except Exception:
        current_app.logger.exception('kpi: user1 hours failed')

    dig_count = db.session.query(func.coalesce(func.sum(DiggingLog.quantity), 0)).filter(
        DiggingLog.user_id == user.id,
        DiggingLog.date == today,
        DiggingLog.status != 'rejected',
    ).scalar() or 0

    tasks_today = DiggingTask.query.filter(
        DiggingTask.planned_date == today,
        DiggingTask.status != 'done',
    ).count()

    return {
        'hours': hours,
        'dig_qty': int(dig_count),
        'tasks_today': tasks_today,
    }


def _collect_kpis(user, today):
    """Собирает KPI-плитки под текущего пользователя.

    Возвращает список словарей: {id, title, value, sub, icon, color, url, [pct]}.
    Порядок = порядок вывода на дашборде.
    """
    tiles = []
    role = user.role or ''
    is_boss = role in ('admin', 'executive')
    is_manager = role == 'user'
    is_prod = role == 'user2'

    first_day, last_day = _month_bounds(today)
    prev_first, prev_last = _prev_month_bounds(today)

    # ----- Поступления денег за месяц (admin/executive/user) -----
    # Логика: сумма всех Payment.amount за календарный месяц.
    if is_boss or is_manager:
        try:
            cur = _cash_in_for(first_day, last_day)
            prev = _cash_in_for(prev_first, prev_last)
            tiles.append({
                'id': 'revenue_month',
                'title': 'Поступления за месяц',
                'value': cur,
                'value_fmt': f'{cur:,.0f} ₽'.replace(',', ' '),
                'sub': f'{first_day.strftime("%b")} · фактически пришло на счёт',
                'icon': 'fa-wallet',
                'color': 'success',
                'pct': _pct_delta(cur, prev),
                'url': url_for('finance.expenses'),
            })
        except Exception:
            current_app.logger.exception('kpi: cash_in failed')

    # ----- Дебиторка (admin/executive/user) -----
    if is_boss or is_manager:
        try:
            all_debtors = _all_debtors()
            debt = sum(r['debt'] for r in all_debtors)
            top = all_debtors[:5]
            tiles.append({
                'id': 'debt',
                'title': 'Дебиторка',
                'value': debt,
                'value_fmt': f'{debt:,.0f} ₽'.replace(',', ' '),
                'sub': (f'Клиентов с долгом: {len(all_debtors)} · по активным заказам'
                        if all_debtors else 'Нет должников'),
                'icon': 'fa-hand-holding-usd',
                'color': 'danger',
                'url': (url_for('crm.crm_client_analytics')
                        if 'crm.crm_client_analytics' in current_app.view_functions
                        else url_for('main.analytics')),
                'top': top,
            })
        except Exception:
            current_app.logger.exception('kpi: debt failed')

    # ----- Готово к отгрузке -----
    if is_boss or is_manager or is_prod:
        try:
            ready = _ready_orders_stats()
            tiles.append({
                'id': 'ready_orders',
                'title': 'Готово к отгрузке',
                'value': ready['count'],
                'value_fmt': f"{ready['count']} зак.",
                'sub': f"на {ready['sum']:,.0f} ₽".replace(',', ' '),
                'icon': 'fa-truck-loading',
                'color': 'primary',
                'url': url_for('orders.orders_list') + '?status=ready',
            })
        except Exception:
            current_app.logger.exception('kpi: ready_orders failed')

    # ----- Новые заявки с сайта -----
    if is_boss or is_manager:
        try:
            cnt = _drafts_count()
            if cnt > 0:
                tiles.append({
                    'id': 'drafts',
                    'title': 'Новые заявки',
                    'value': cnt,
                    'value_fmt': str(cnt),
                    'sub': 'ждут подтверждения',
                    'icon': 'fa-inbox',
                    'color': 'warning',
                    'url': url_for('orders.orders_list'),
                })
        except Exception:
            current_app.logger.exception('kpi: drafts failed')

    # ----- Денежный поток (admin/executive) -----
    if is_boss:
        try:
            inc = _payments_for(first_day, last_day)
            out = _expenses_for(first_day, last_day)
            net = inc - out
            tiles.append({
                'id': 'cashflow',
                'title': 'Кэш-флоу за месяц',
                'value': net,
                'value_fmt': f'{net:,.0f} ₽'.replace(',', ' '),
                'sub': f'приход {inc:,.0f} / расход {out:,.0f}'.replace(',', ' '),
                'icon': 'fa-money-bill-trend-up',
                'color': 'success' if net >= 0 else 'danger',
                'url': url_for('finance.expenses'),
            })
        except Exception:
            current_app.logger.exception('kpi: cashflow failed')

    # ----- Просрочки по выкопке -----
    if is_boss or is_manager or is_prod:
        try:
            overdue = _overdue_digging_count(today)
            if overdue > 0:
                tiles.append({
                    'id': 'overdue_digging',
                    'title': 'Просрочки выкопки',
                    'value': overdue,
                    'value_fmt': str(overdue),
                    'sub': 'задач в диспетчерской',
                    'icon': 'fa-triangle-exclamation',
                    'color': 'danger',
                    'url': url_for('digging.dispatch_view') if 'digging.dispatch_view' in current_app.view_functions else url_for('digging.mobile_index'),
                })
        except Exception:
            current_app.logger.exception('kpi: overdue_digging failed')

    # ----- Поручения (TG-задачи) -----
    try:
        tg = _tg_tasks_stats(user, today)
        if tg['active'] > 0 or tg['overdue'] > 0 or tg['done_7d'] > 0:
            sub_parts = []
            if tg['overdue']:
                sub_parts.append(f"⚠ {tg['overdue']} просроч.")
            if tg['done_7d']:
                sub_parts.append(f"✓ {tg['done_7d']} за 7 дн.")
            tiles.append({
                'id': 'tg_tasks',
                'title': 'Поручения',
                'value': tg['active'],
                'value_fmt': f"{tg['active']} активн.",
                'sub': ' · '.join(sub_parts) if sub_parts else 'всё спокойно',
                'icon': 'fa-list-check',
                'color': 'danger' if tg['overdue'] > 0 else 'info',
                'url': url_for('main.index') + '#group-today',
            })
    except Exception:
        current_app.logger.exception('kpi: tg_tasks failed')

    # ----- Ежедневка оператора (user1 или user2 по факту работ) -----
    # Показываем, если пользователь роль user/user2 И у него есть хоть часы или выкопка.
    if role in ('user', 'user2'):
        try:
            s = _user1_daily_stats(user, today)
            if s['tasks_today'] > 0 or s['hours'] > 0 or s['dig_qty'] > 0:
                status_ok = s['hours'] >= 1 and s['dig_qty'] >= 1
                tiles.append({
                    'id': 'daily_ops',
                    'title': 'Сегодня',
                    'value': s['tasks_today'],
                    'value_fmt': f"{s['tasks_today']} задач",
                    'sub': f"табель: {s['hours']:.1f} ч · выкопка: {s['dig_qty']} шт.",
                    'icon': 'fa-circle-check' if status_ok else 'fa-clipboard-list',
                    'color': 'success' if status_ok else 'secondary',
                    'url': url_for('digging.mobile_index'),
                })
        except Exception:
            current_app.logger.exception('kpi: daily_ops failed')

    return tiles


def _analytics_data(user, today):
    """Данные для страницы /analytics — графики и таблицы."""
    role = user.role or ''
    is_boss = role in ('admin', 'executive')

    data = {
        'revenue_months': [],   # [{label, value}] за последние 12 календарных месяцев
        'cashflow_months': [],  # [{label, inc, out}]
        'order_funnel': [],     # [{status, count, sum}]
        'top_clients': [],      # [{name, revenue, share}]
        'top_plants': [],       # [{name, qty, revenue}]
        'tg_summary': {},       # {active, overdue, done_7d, by_assignee:[{name,count}]}
        'debtors': [],          # top 10
        'show_revenue_chart': is_boss,   # менеджеру (user) не показываем график выручки по месяцам
    }

    # --- 12 месяцев (считаем назад от текущего) ---
    months = []
    anchor = today.replace(day=1)
    for i in range(11, -1, -1):
        # i месяцев назад
        y = anchor.year
        m = anchor.month - i
        while m <= 0:
            m += 12
            y -= 1
        first = date(y, m, 1)
        last = date(y, m, monthrange(y, m)[1])
        months.append((first, last))

    try:
        for first, last in months:
            rev = _cash_in_for(first, last)
            data['revenue_months'].append({
                'label': first.strftime('%m.%y'),
                'value': rev,
            })
    except Exception:
        current_app.logger.exception('analytics: revenue_months failed')

    if is_boss:
        try:
            for first, last in months:
                inc = _payments_for(first, last)
                out = _expenses_for(first, last)
                data['cashflow_months'].append({
                    'label': first.strftime('%m.%y'),
                    'inc': inc,
                    'out': out,
                })
        except Exception:
            current_app.logger.exception('analytics: cashflow_months failed')

    # --- Воронка заказов по статусу (без призрачных заказов) ---
    try:
        rows = db.session.query(
            Order.status, func.count(Order.id).label('cnt')
        ).filter(
            Order.is_deleted.is_(False),
            Order.status != 'ghost',
        ).group_by(Order.status).all()
        status_labels = {
            'reserved': 'Зарезервирован',
            'in_progress': 'В работе',
            'ready': 'Готов',
            'shipped': 'Отгружен',
            'canceled': 'Отменён',
        }
        data['order_funnel'] = [
            {'status': status_labels.get(s, s or '—'), 'count': int(cnt)}
            for s, cnt in rows if s
        ]
    except Exception:
        current_app.logger.exception('analytics: order_funnel failed')

    # --- Топ клиентов за текущий календарный год (по выручке) ---
    try:
        year_first = date(today.year, 1, 1)
        year_last = date(today.year, 12, 31)
        rows = db.session.query(
            Client.id, Client.name,
            func.coalesce(func.sum(OrderItem.price * OrderItem.quantity), 0).label('rev')
        ).join(Order, Order.client_id == Client.id
        ).join(OrderItem, OrderItem.order_id == Order.id
        ).filter(
            Order.is_deleted.is_(False),
            Order.status != 'canceled',
            func.date(Order.date) >= year_first,
            func.date(Order.date) <= year_last,
        ).group_by(Client.id, Client.name).order_by(func.sum(OrderItem.price * OrderItem.quantity).desc()).limit(10).all()
        total = sum(_safe_float(r.rev) for r in rows) or 1.0
        data['top_clients'] = [
            {'name': r.name, 'revenue': _safe_float(r.rev), 'share': round(_safe_float(r.rev) * 100 / total, 1)}
            for r in rows
        ]
    except Exception:
        current_app.logger.exception('analytics: top_clients failed')

    # --- Топ растений за календарный год (по объёму продаж в шт. и выручке) ---
    try:
        year_first = date(today.year, 1, 1)
        year_last = date(today.year, 12, 31)
        rows = db.session.query(
            Plant.id, Plant.name,
            func.coalesce(func.sum(OrderItem.quantity), 0).label('qty'),
            func.coalesce(func.sum(OrderItem.price * OrderItem.quantity), 0).label('rev')
        ).join(OrderItem, OrderItem.plant_id == Plant.id
        ).join(Order, OrderItem.order_id == Order.id
        ).filter(
            Order.is_deleted.is_(False),
            Order.status != 'canceled',
            func.date(Order.date) >= year_first,
            func.date(Order.date) <= year_last,
        ).group_by(Plant.id, Plant.name
        ).order_by(func.sum(OrderItem.quantity).desc()).limit(10).all()
        data['top_plants'] = [
            {'name': r.name, 'qty': int(r.qty or 0), 'revenue': _safe_float(r.rev)}
            for r in rows
        ]
    except Exception:
        current_app.logger.exception('analytics: top_plants failed')

    # --- Поручения: по статусам и по исполнителям ---
    try:
        active = TgTask.query.filter(TgTask.status == 'new').count()
        overdue = TgTask.query.filter(
            TgTask.status == 'new',
            TgTask.deadline.isnot(None),
            TgTask.deadline < today,
        ).count()
        done_cnt = TgTask.query.filter(TgTask.status == 'done').count()

        by_assignee = []
        try:
            rows = db.session.query(
                User.username, func.count(TgTask.id)
            ).join(TgTask, TgTask.assignee_id == User.id
            ).filter(TgTask.status == 'new'
            ).group_by(User.username
            ).order_by(func.count(TgTask.id).desc()).limit(8).all()
            by_assignee = [{'name': n or '—', 'count': int(c)} for n, c in rows]
        except Exception:
            current_app.logger.exception('analytics: tg by_assignee failed')

        data['tg_summary'] = {
            'active': active,
            'overdue': overdue,
            'done_total': done_cnt,
            'by_assignee': by_assignee,
        }
    except Exception:
        current_app.logger.exception('analytics: tg_summary failed')

    # --- Должники (топ-10) ---
    try:
        data['debtors'] = _debtors_count(limit_clients=10)
    except Exception:
        current_app.logger.exception('analytics: debtors failed')

    return data


@bp.route('/')
def index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.login'))

    today = msk_today()

    # Для руководителей — при первом заходе в день прогоняем сканер аномалий,
    # а по понедельникам после 10:00 МСК — генерим еженедельный дайджест.
    # Обе операции идемпотентны: второй вызов за день ничего не делает.
    if (current_user.role or '') in ('admin', 'executive'):
        try:
            from app.anomaly_engine import ensure_daily_scan
            ensure_daily_scan()
        except Exception:
            current_app.logger.exception('anomaly scan failed')
        try:
            from app.weekly_digest import ensure_weekly_digest
            ensure_weekly_digest()
        except Exception:
            current_app.logger.exception('weekly digest failed')
    
    # Структура папок для дашборда
    # overdue — автоматически свёрнут, перенесён в конец;
    # today — всегда раскрыт по умолчанию.
    groups = {
        'today': {'id': 'today', 'label': '✅ План на сегодня', 'color': 'success', 'icon': 'fa-calendar-day', 'cards': [], 'is_open': True, 'order': 1},
        'tomorrow': {'id': 'tomorrow', 'label': '➡️ В планах на завтра', 'color': 'primary', 'icon': 'fa-arrow-right', 'cards': [], 'is_open': False, 'order': 2},
        'future': {'id': 'future', 'label': '⏳ Предстоящие задачи', 'color': 'secondary', 'icon': 'fa-calendar-alt', 'cards': [], 'is_open': False, 'order': 3},
        'overdue': {'id': 'overdue', 'label': '🔥 Срочно / Просрочено', 'color': 'danger', 'icon': 'fa-exclamation-circle', 'cards': [], 'is_open': False, 'order': 4},
    }

    def add_to_group(card, target_date, is_forced_urgent=False):
        if is_forced_urgent:
            groups['overdue']['cards'].append(card)
            return
        
        diff = (target_date - today).days
        if diff < 0:
            groups['overdue']['cards'].append(card)
        elif diff == 0:
            groups['today']['cards'].append(card)
        elif diff == 1:
            groups['tomorrow']['cards'].append(card)
        else:
            groups['future']['cards'].append(card)

    # Каждую секцию оборачиваем в try/except — одна битая запись не должна ломать весь дашборд.
    import json
    from sqlalchemy import or_

    # Переключатель для админа между тематическими вкладками дашборда.
    # Значения: 'all' (по умолчанию), 'mine', 'sales', 'finance', 'field'.
    # 'all'/'mine' — «кто» (видимость), 'sales'/'finance'/'field' — «что» (тема).
    # Для остальных ролей переключатель не показываем и всегда используем 'all'
    # (их видимость определяется жёсткими ролевыми фильтрами).
    admin_scope = (
        request.args.get('scope')
        or request.cookies.get('feed_scope')
        or 'all'
    )
    if admin_scope not in _ADMIN_DASHBOARD_SCOPES:
        admin_scope = 'all'
    if current_user.role != 'admin':
        admin_scope = 'all'

    # Для удобства — сокращения «админ смотрит конкретную тему».
    is_admin = (current_user.role == 'admin')
    is_topic_scope = is_admin and admin_scope in ('sales', 'finance', 'field')

    # 1. Счета на оплату
    if current_user.role in ['admin', 'executive']:
        try:
            invoices = PaymentInvoice.query.filter(PaymentInvoice.status != 'paid').all()
            for inv in invoices:
                try:
                    remaining = (inv.amount or 0) - sum((e.amount or 0) for e in (inv.expenses or []))
                    if remaining > 0:
                        d = inv.due_date or today
                        is_urgent = (inv.priority == 'high') or ((d - today).days <= 3)
                        card = {
                            'id': f'inv_{inv.id}',
                            'type': 'invoice',
                            'title': 'Счет на оплату',
                            'text': f'{inv.item.name if inv.item else "Без статьи"}: {inv.original_name or ""}',
                            'amount': remaining,
                            'url': url_for('finance.expenses', invoice_id=inv.id),
                            'date_str': d.strftime('%d.%m.%Y'),
                            'raw_date': d,
                        }
                        add_to_group(card, d, is_forced_urgent=is_urgent)
                except Exception:
                    current_app.logger.exception('feed: invoice card failed')
        except Exception:
            current_app.logger.exception('feed: invoices block failed')

    # 2. Черновики с сайта
    if current_user.role in ['admin', 'user']:
        try:
            drafts = Document.query.filter_by(doc_type='client_draft').all()
            for d in drafts:
                try:
                    card = {
                        'id': f'draft_{d.id}',
                        'type': 'draft',
                        'title': 'Новая заявка с сайта',
                        'text': 'Требует подтверждения менеджером.',
                        'url': url_for('orders.client_draft_detail', doc_id=d.id),
                        'date_str': (d.date or today).strftime('%d.%m.%Y'),
                        'raw_date': today,
                    }
                    add_to_group(card, today, is_forced_urgent=True)
                except Exception:
                    current_app.logger.exception('feed: draft card failed')
        except Exception:
            current_app.logger.exception('feed: drafts block failed')

    # 3. Выкопка (Задачи) — работа бригады. В общей ленте видит её только
    # бригадир (user2). Админ видит копку в scope='all' и scope='field';
    # в 'mine'/'sales'/'finance' эти карточки затирают важные для его темы,
    # поэтому не рендерим. Менеджер (user) копку не видит никогда — это
    # не его участок работ, хоть и на одном заказе.
    _show_digging = (
        current_user.role == 'user2'
        or (is_admin and admin_scope in ('all', 'field'))
    )
    if _show_digging:
        try:
            tasks = DiggingTask.query.filter(DiggingTask.status == 'pending').all()
            for t in tasks:
                try:
                    item = t.item
                    if not (item and item.order and not item.order.is_deleted):
                        continue
                    d = t.planned_date
                    card = {
                        'id': f'task_{t.id}',
                        'type': 'task',
                        'order_id': item.order_id,
                        'client_name': item.order.client.name if item.order.client else '—',
                        'plant_name': item.plant.name if item.plant else '—',
                        'size_name': item.size.name if item.size else '—',
                        'field_name': item.field.name if item.field else '—',
                        'qty': t.planned_qty,
                        'comment': t.comment,
                        'task_id': t.id,
                        'date_str': d.strftime('%d.%m.%Y') if d else '',
                        'raw_date': d or today,
                    }
                    add_to_group(card, d or today)
                except Exception:
                    current_app.logger.exception('feed: digging task card failed')
        except Exception:
            current_app.logger.exception('feed: digging block failed')

    # 4. Патенты HR
    if current_user.role in ['admin', 'user2', 'executive']:
        try:
            patents = PatentPeriod.query.filter_by(is_current=True, status='active').all()
            for p in patents:
                try:
                    if not p.end_date:
                        continue
                    days_left = (p.end_date - today).days
                    if days_left > 14:
                        continue
                    card = {
                        'id': f'pat_{p.id}',
                        'type': 'patent',
                        'title': 'Истекает патент',
                        'text': f'{p.employee.name if p.employee else "Сотрудник"} (осталось {days_left} дн.)',
                        'url': url_for('hr.foreign_employee_card', employee_id=p.employee.id) if p.employee else url_for('hr.personnel'),
                        'date_str': p.end_date.strftime('%d.%m.%Y'),
                        'raw_date': p.end_date,
                    }
                    add_to_group(card, p.end_date, is_forced_urgent=(days_left <= 7))
                except Exception:
                    current_app.logger.exception('feed: patent card failed')
        except Exception:
            current_app.logger.exception('feed: patents block failed')

    # 5. Задачи из Telegram (AI)
    try:
        # Системные карточки (аномалии + дайджест) в этот блок не попадают —
        # их показываем отдельно в блоке 6, там у них свой рендер.
        exclude_system = (TgTask.action_type.notin_(['anomaly', 'digest']))

        scope_cond = _tg_task_scope_cond(current_user, admin_scope=admin_scope)
        tg_query = TgTask.query.filter(TgTask.status == 'new', exclude_system)
        if scope_cond is not None:
            tg_query = tg_query.filter(scope_cond)

        for t in tg_query.all():
            try:
                d = t.deadline or today
                payload_order_id = None
                if t.action_payload:
                    try:
                        payload_order_id = json.loads(t.action_payload).get('order_id')
                    except Exception:
                        pass

                # «Тянущиеся» задачи (passive):
                #   • если дедлайн прошёл — карточка висит в группе «Просрочено»
                #     с человекочитаемым «просрочено N дн.»;
                #   • иначе — всегда в «Сегодня», независимо от того, когда
                #     дедлайн в будущем. Это обеспечивает правило «висит каждый
                #     день, пока исполнитель не нажмёт Готово».
                days_overdue = (today - d).days if d < today else 0
                if days_overdue > 0:
                    date_str = f'просрочено {days_overdue} дн.'
                elif d == today:
                    date_str = f'до {d.strftime("%d.%m")} (сегодня)'
                elif (d - today).days == 1:
                    date_str = f'до {d.strftime("%d.%m")} (завтра)'
                else:
                    date_str = f'до {d.strftime("%d.%m.%Y")}'

                reassigned_from_name = None
                if t.reassigned_from_id and t.reassigned_from:
                    reassigned_from_name = t.reassigned_from.username

                # Кто создал задачу вручную — покажем на карточке маленьким бейджем.
                created_by_name = None
                if t.created_by_id and t.created_by:
                    created_by_name = t.created_by.username

                # Для карточек «расход из ТГ» подтянем поля из payload,
                # чтобы шаблон мог отрисовать селект статьи + кнопки.
                chat_expense = None
                if t.action_type == 'chat_expense' and t.action_payload:
                    try:
                        pl = json.loads(t.action_payload) or {}
                        chat_expense = {
                            'id': pl.get('chat_expense_id'),
                            'amount': pl.get('amount'),
                            'description': pl.get('description'),
                            'payment_type': pl.get('payment_type'),
                            'suggested_budget_item_id': pl.get('suggested_budget_item_id'),
                            'classifier_source': pl.get('classifier_source'),
                            'tg_chat_id': pl.get('tg_chat_id'),
                            'tg_message_id': pl.get('tg_message_id'),
                            'sender': pl.get('sender'),
                        }
                    except Exception:
                        chat_expense = None

                card = {
                    'id': f'tgtask_{t.id}',
                    'type': 'chat_expense' if t.action_type == 'chat_expense' else 'tg_task',
                    'title': t.title,
                    'details': t.details,
                    'task_id': t.id,
                    'action_type': t.action_type,
                    'payload_order_id': payload_order_id,
                    'chat_expense': chat_expense,
                    'assignee_id': t.assignee_id,
                    'assignee_name': t.assignee.username if t.assignee else None,
                    'assignee_role': t.assignee_role,
                    'reassigned_from_name': reassigned_from_name,
                    'created_by_name': created_by_name,
                    'source': t.source,
                    'date_str': date_str,
                    'raw_date': d,
                    'deadline_iso': d.isoformat(),
                    'days_overdue': days_overdue,
                    'color': 'info',
                }
                # Ставим в «Просрочено» только если дедлайн прошёл; иначе —
                # всегда в «Сегодня» (передаём today, а не d).
                add_to_group(card, d if days_overdue > 0 else today)
            except Exception:
                current_app.logger.exception('feed: tg_task card failed')
    except Exception:
        current_app.logger.exception('feed: tg_tasks block failed (table may be missing)')

    # 6. Аномалии и еженедельный дайджест.
    # Раньше блок был только для admin/executive, но теперь, когда
    # _role_match умеет ловить CSV-формат assignee_role ('admin,executive,user'
    # у аномалий про заказы), мы можем показывать этот блок всем ролям,
    # а реальный набор карточек фильтрует scope_cond. Это даёт менеджеру
    # его stale_reserved, а бригадиру (если появятся полевые аномалии) —
    # профильные. Финансовые аномалии с ролями 'admin,executive' к менеджеру
    # не попадут: _role_match('user') их не ловит.
    if (current_user.role or '') in ('admin', 'executive', 'user', 'user2'):
        try:
            # Дайджест — персональный (assignee_id == current_user.id),
            # иначе каждый админ видел бы дайджесты коллег. Аномалии —
            # общие, но фильтруем тем же scope_cond, что и обычные TgTask.
            sys_scope_cond = _tg_task_scope_cond(current_user, admin_scope=admin_scope)
            sys_q = TgTask.query.filter(
                TgTask.status == 'new',
                or_(
                    and_(
                        TgTask.action_type == 'digest',
                        TgTask.assignee_id == current_user.id,
                    ),
                    TgTask.action_type == 'anomaly',
                ),
            )
            if sys_scope_cond is not None:
                sys_q = sys_q.filter(sys_scope_cond)
            sys_tasks = sys_q.order_by(
                # digest всегда вверху, а аномалии — по severity
                TgTask.action_type.desc(),
                TgTask.severity.desc(),
                TgTask.first_seen_at.desc(),
            ).all()
            for t in sys_tasks:
                try:
                    # «Длится N дней» — на основе first_seen_at
                    days_active = 0
                    if t.first_seen_at:
                        delta = (msk_now() - t.first_seen_at)
                        days_active = max(0, delta.days)
                    weeks_active = days_active // 7
                    if t.action_type == 'digest':
                        card = {
                            'id': f'digest_{t.id}',
                            'type': 'digest',
                            'title': t.title,
                            'content_html': t.details,
                            'task_id': t.id,
                            'severity': t.severity or 'info',
                            'date_str': t.first_seen_at.strftime('%d.%m.%Y %H:%M') if t.first_seen_at else '',
                            'raw_date': today,
                        }
                    else:
                        # Достаём url из payload, если правило его туда положило
                        # (удобно для прямого перехода в редактор заказа и т.п.).
                        _anomaly_url = None
                        if t.action_payload:
                            try:
                                _pl = json.loads(t.action_payload)
                                if isinstance(_pl, dict):
                                    _anomaly_url = _pl.get('url') or None
                            except Exception:
                                pass
                        card = {
                            'id': f'anomaly_{t.id}',
                            'type': 'anomaly',
                            'kind': t.dedup_key.split(':')[0] if t.dedup_key else '—',
                            'title': t.title,
                            'details': t.details,
                            'task_id': t.id,
                            'severity': t.severity or 'info',
                            'days_active': days_active,
                            'weeks_active': weeks_active,
                            'date_str': (
                                f'длится {days_active} дн.'
                                if days_active > 0 else 'новая'
                            ),
                            'raw_date': today,
                            'url': _anomaly_url,
                        }
                    add_to_group(card, today, is_forced_urgent=(t.severity == 'danger'))
                except Exception:
                    current_app.logger.exception('feed: anomaly/digest card failed')
        except Exception:
            current_app.logger.exception('feed: system cards block failed')

    # Пост-фильтр по теме для админа. SQL-уровень мы оставили простым
    # (чтобы не усложнять 6 разных запросов), а итоговую ленту режем здесь
    # через _card_topic: админ выбрал «Финансы» — оставляем finance+admin,
    # выбрал «Продажи» — sales. «Все» и «Только мои» без пост-фильтра
    # (в mine всё нужное уже ограничено scope_cond/assignee_id).
    if is_topic_scope:
        for g in groups.values():
            g['cards'] = [c for c in g['cards'] if _card_topic(c) == admin_scope]

    # Сортировка внутри папок по дате и удаление пустых папок
    for g in groups.values():
        g['cards'].sort(key=lambda x: x.get('raw_date', today))

    # Счётчик в лейбле просроченных
    ov = groups.get('overdue')
    if ov and ov['cards']:
        ov['label'] = f"🔥 Срочно / Просрочено ({len(ov['cards'])})"

    feed_groups = [g for g in sorted(groups.values(), key=lambda x: x['order']) if g['cards']]

    # KPI-плитки для шапки дашборда (ролевые).
    try:
        kpi_tiles = _collect_kpis(current_user, today)
    except Exception:
        current_app.logger.exception('feed: _collect_kpis failed')
        kpi_tiles = []

    resp = make_response(render_template(
        'feed.html',
        feed_groups=feed_groups,
        kpi_tiles=kpi_tiles,
        today=today,
        admin_scope=admin_scope,
    ))
    # Если админ явно передал ?scope=... в URL — запоминаем его выбор в cookie
    # на 30 дней, чтобы переключатель «сохранялся» между заходами.
    req_scope = request.args.get('scope')
    if current_user.role == 'admin' and req_scope in _ADMIN_DASHBOARD_SCOPES:
        resp.set_cookie('feed_scope', req_scope,
                        max_age=60 * 60 * 24 * 30, samesite='Lax')
    return resp


def _tasks_analytics(today, user=None):
    """Аналитика по поручениям (TgTask) для контроля сотрудников.

    Параметр `user` задаёт scope:
      • None или admin/executive — видим все задачи;
      • user / user2 — только свои (assignee_id=user) + своя роль + null.

    Возвращает:
      - summary: общие счётчики
      - by_employee: список исполнителей с метриками (скрыт в user-scope)
      - daily: серия за последние 30 дней (созданные / выполненные в день)
      - age_buckets: распределение активных по возрасту
      - by_source: распределение по источнику (tg / manual / anomaly / digest / fallback)
      - by_action_type: распределение по типу задачи
      - hot: «горячие» задачи — просрочены > 3 дней, top 10 по возрасту
    """
    horizon_days = 30
    start_dt = datetime.combine(today - timedelta(days=horizon_days - 1), datetime.min.time())

    # Пользовательский scope: для executive/user/user2 — видит только своё.
    # Для admin — вся аналитика (без ограничений).
    def _apply_scope(q):
        """Накладываем фильтр доступа через общий helper видимости."""
        if user is None:
            return q
        cond = _tg_task_scope_cond(user)
        return q.filter(cond) if cond is not None else q

    # ----- Общая сводка -----
    active_total = _apply_scope(TgTask.query.filter(TgTask.status == 'new')).count()
    overdue_total = _apply_scope(TgTask.query.filter(
        TgTask.status == 'new',
        TgTask.deadline.isnot(None),
        TgTask.deadline < today,
    )).count()
    done_30d = _apply_scope(TgTask.query.filter(
        TgTask.status == 'done',
        TgTask.completed_at.isnot(None),
        TgTask.completed_at >= start_dt,
    )).count()

    # Среднее и медианное время выполнения (часы, last 90 дней для большей выборки)
    window_start = datetime.combine(today - timedelta(days=90), datetime.min.time())
    completed_times = _apply_scope(
        db.session.query(TgTask.created_at, TgTask.completed_at).filter(
            TgTask.status == 'done',
            TgTask.completed_at.isnot(None),
            TgTask.created_at.isnot(None),
            TgTask.completed_at >= window_start,
        )
    ).all()
    hours_list = []
    for cr, cm in completed_times:
        try:
            dh = (cm - cr).total_seconds() / 3600.0
            if 0 <= dh <= 24 * 60:  # отсечём битые даты (больше 60 дней — маловероятно)
                hours_list.append(dh)
        except Exception:
            pass
    avg_hours = round(sum(hours_list) / len(hours_list), 1) if hours_list else 0
    med_hours = round(sorted(hours_list)[len(hours_list) // 2], 1) if hours_list else 0

    summary = {
        'active': active_total,
        'overdue': overdue_total,
        'done_30d': done_30d,
        'avg_hours': avg_hours,
        'med_hours': med_hours,
        'sample_size': len(hours_list),
        'scope': 'self' if scoped else 'all',
    }

    # ----- По исполнителям (скрыт в user-scope: показывать коллег некорректно) -----
    users = [] if scoped else User.query.all()
    by_employee = []
    for u in users:
        active = TgTask.query.filter(TgTask.assignee_id == u.id, TgTask.status == 'new').count()
        overdue = TgTask.query.filter(
            TgTask.assignee_id == u.id,
            TgTask.status == 'new',
            TgTask.deadline.isnot(None),
            TgTask.deadline < today,
        ).count()
        done = TgTask.query.filter(
            TgTask.assignee_id == u.id,
            TgTask.status == 'done',
            TgTask.completed_at.isnot(None),
            TgTask.completed_at >= start_dt,
        ).count()
        # Среднее время выполнения по сотруднику
        pairs = db.session.query(TgTask.created_at, TgTask.completed_at).filter(
            TgTask.assignee_id == u.id,
            TgTask.status == 'done',
            TgTask.completed_at.isnot(None),
            TgTask.created_at.isnot(None),
            TgTask.completed_at >= window_start,
        ).all()
        emp_hours = []
        for cr, cm in pairs:
            try:
                dh = (cm - cr).total_seconds() / 3600.0
                if 0 <= dh <= 24 * 60:
                    emp_hours.append(dh)
            except Exception:
                pass
        emp_avg = round(sum(emp_hours) / len(emp_hours), 1) if emp_hours else None

        # Скрываем пустые строки, но оставляем тех, у кого есть активные или история
        if active == 0 and overdue == 0 and done == 0:
            continue
        # Score — грубая оценка эффективности (больше = лучше):
        # база = done; штраф за просрочки; бонус за скорость.
        score = done - overdue * 2
        if emp_avg is not None:
            if emp_avg < 24:
                score += 3
            elif emp_avg < 72:
                score += 1
        by_employee.append({
            'username': u.username,
            'role': u.role,
            'active': active,
            'overdue': overdue,
            'done_30d': done,
            'avg_hours': emp_avg,
            'score': score,
        })
    by_employee.sort(key=lambda r: (-r['score'], -r['done_30d']))

    # ----- Ежедневная серия (30 дней): сколько создано и сколько закрыто -----
    created_rows = _apply_scope(
        db.session.query(
            func.date(TgTask.created_at), func.count(TgTask.id)
        ).filter(TgTask.created_at >= start_dt)
    ).group_by(func.date(TgTask.created_at)).all()
    done_rows = _apply_scope(
        db.session.query(
            func.date(TgTask.completed_at), func.count(TgTask.id)
        ).filter(
            TgTask.completed_at.isnot(None),
            TgTask.completed_at >= start_dt,
        )
    ).group_by(func.date(TgTask.completed_at)).all()

    created_map = {}
    for d, c in created_rows:
        if isinstance(d, str):
            try:
                d = datetime.strptime(d, '%Y-%m-%d').date()
            except Exception:
                continue
        created_map[d] = int(c)

    done_map = {}
    for d, c in done_rows:
        if isinstance(d, str):
            try:
                d = datetime.strptime(d, '%Y-%m-%d').date()
            except Exception:
                continue
        done_map[d] = int(c)

    daily = []
    for i in range(horizon_days - 1, -1, -1):
        d = today - timedelta(days=i)
        daily.append({
            'label': d.strftime('%d.%m'),
            'created': created_map.get(d, 0),
            'done': done_map.get(d, 0),
        })

    # ----- Распределение активных задач по возрасту -----
    # Бакеты: <1 дня, 1-3, 3-7, 7-14, >14
    buckets = {
        '< 1 дня': 0, '1-3 дня': 0, '3-7 дней': 0, '1-2 недели': 0, '> 2 недель': 0,
    }
    active_tasks = _apply_scope(TgTask.query.filter(TgTask.status == 'new')).all()
    now = msk_now()
    for t in active_tasks:
        if not t.created_at:
            continue
        age = (now - t.created_at).days
        if age < 1:       buckets['< 1 дня'] += 1
        elif age < 3:     buckets['1-3 дня'] += 1
        elif age < 7:     buckets['3-7 дней'] += 1
        elif age < 14:    buckets['1-2 недели'] += 1
        else:             buckets['> 2 недель'] += 1
    age_buckets = [{'label': k, 'count': v} for k, v in buckets.items()]

    # ----- Распределение по источнику задач -----
    # Старые записи без source трактуем как 'tg' (потоковый AI-парсинг был
    # единственным источником до v2). Так аналитика остаётся честной без
    # ручного ребекфилла.
    source_rows = _apply_scope(
        db.session.query(TgTask.source, func.count(TgTask.id))
    ).group_by(TgTask.source).all()
    source_titles = {
        'tg': 'Из чата (AI)',
        'manual': 'Ручные',
        'anomaly': 'Аномалии',
        'digest': 'Дайджест',
        'fallback': 'Сбой AI',
    }
    by_source = []
    for src, cnt in source_rows:
        key = src or 'tg'
        by_source.append({'key': key, 'label': source_titles.get(key, key), 'count': int(cnt)})
    by_source.sort(key=lambda x: -x['count'])

    # ----- Распределение по action_type — на чём чаще «застревает» команда -----
    action_rows = _apply_scope(
        db.session.query(TgTask.action_type, func.count(TgTask.id)).filter(
            TgTask.action_type.notin_(['anomaly', 'digest'])
        )
    ).group_by(TgTask.action_type).all()
    action_titles = {
        'create_order': 'Создать заказ',
        'shipment': 'Отгрузка',
        'income': 'Поступление',
        'payment': 'Оплата/счёт',
        'digging': 'Копка/поле',
        'info': 'Прочее',
    }
    by_action_type = []
    for at, cnt in action_rows:
        key = at or 'info'
        by_action_type.append({'key': key, 'label': action_titles.get(key, key), 'count': int(cnt)})
    by_action_type.sort(key=lambda x: -x['count'])

    # ----- «Горячие» задачи — давно просрочены и до сих пор открыты -----
    hot_threshold_days = 3
    hot_cutoff = today - timedelta(days=hot_threshold_days)
    hot_query = _apply_scope(TgTask.query.filter(
        TgTask.status == 'new',
        TgTask.deadline.isnot(None),
        TgTask.deadline < hot_cutoff,
    )).order_by(TgTask.deadline.asc()).limit(15).all()
    hot = []
    for t in hot_query:
        days_overdue = (today - t.deadline).days if t.deadline else 0
        hot.append({
            'id': t.id,
            'title': t.title or '(без названия)',
            'assignee': t.assignee.username if t.assignee else None,
            'assignee_role': t.assignee_role,
            'deadline': t.deadline.isoformat() if t.deadline else None,
            'days_overdue': days_overdue,
            'source': t.source or 'tg',
        })

    return {
        'summary': summary,
        'by_employee': by_employee,
        'daily': daily,
        'age_buckets': age_buckets,
        'by_source': by_source,
        'by_action_type': by_action_type,
        'hot': hot,
    }


@bp.route('/analytics')
@login_required
def analytics():
    """Полноценная страница аналитики с графиками и топами."""
    today = msk_today()
    try:
        data = _analytics_data(current_user, today)
    except Exception:
        current_app.logger.exception('analytics page: _analytics_data failed')
        data = {}
    # Для быстрого превью плиток сверху.
    try:
        kpi_tiles = _collect_kpis(current_user, today)
    except Exception:
        current_app.logger.exception('analytics page: kpi_tiles failed')
        kpi_tiles = []
    return render_template(
        'analytics.html',
        analytics=data,
        kpi_tiles=kpi_tiles,
        today=today,
    )


@bp.route('/dashboard/anomalies')
@login_required
def anomaly_history():
    """Таймлайн аномалий и список дайджестов — для admin/executive."""
    from flask import flash
    if (current_user.role or '') not in ('admin', 'executive'):
        flash('Раздел доступен только руководителям.', 'warning')
        return redirect(url_for('main.index'))

    # Активные аномалии
    active = TgTask.query.filter(
        TgTask.action_type == 'anomaly',
        TgTask.status == 'new',
    ).order_by(TgTask.first_seen_at.desc()).all()

    # Закрытые аномалии — последние 60 дней
    horizon = msk_now() - timedelta(days=60)
    resolved = TgTask.query.filter(
        TgTask.action_type == 'anomaly',
        TgTask.status == 'done',
        TgTask.completed_at.isnot(None),
        TgTask.completed_at >= horizon,
    ).order_by(TgTask.completed_at.desc()).all()

    # Список дайджестов (последние 12)
    from app.models import WeeklyDigest
    digests = (WeeklyDigest.query
               .filter_by(user_id=current_user.id)
               .order_by(WeeklyDigest.week_start.desc())
               .limit(12).all())

    def _enrich(card):
        card.days_active = (
            (msk_now() - card.first_seen_at).days if card.first_seen_at else 0
        )
        card.weeks_active = card.days_active // 7
        card.kind = (card.dedup_key.split(':')[0] if card.dedup_key else '—')
        if card.status == 'done' and card.completed_at and card.first_seen_at:
            card.lived_days = (card.completed_at - card.first_seen_at).days
        else:
            card.lived_days = card.days_active
        return card

    active = [_enrich(c) for c in active]
    resolved = [_enrich(c) for c in resolved]

    return render_template(
        'anomaly_history.html',
        active=active,
        resolved=resolved,
        digests=digests,
        today=msk_today(),
    )


@bp.route('/analytics/tasks')
@login_required
def analytics_tasks():
    """Аналитика по поручениям.

    • admin/executive — видят все задачи и срез по сотрудникам;
    • user/user2     — видят только свои задачи (свой scope), без таблицы коллег.
    """
    today = msk_today()
    try:
        data = _tasks_analytics(today, user=current_user)
    except Exception:
        current_app.logger.exception('analytics_tasks: _tasks_analytics failed')
        data = {
            'summary': {}, 'by_employee': [], 'daily': [], 'age_buckets': [],
            'by_source': [], 'by_action_type': [], 'hot': [],
        }
    return render_template('analytics_tasks.html', data=data, today=today)

@bp.route('/api/feed/dismiss/<card_id>', methods=['POST'])
def dismiss_card(card_id):
    return ""

@bp.route('/api/feed/complete_task', methods=['POST'])
@login_required
def complete_task():
    task_id = request.form.get('task_id')
    fact_qty = int(request.form.get('fact_qty', 0))
    
    task = DiggingTask.query.get(task_id)
    if task and task.status == 'pending' and fact_qty > 0:
        log = DiggingLog(
            date=msk_today(), order_item_id=task.order_item_id, plant_id=task.item.plant_id,
            size_id=task.item.size_id, field_id=task.item.field_id, year=task.item.year,
            user_id=current_user.id, quantity=fact_qty, status='pending' 
        )
        db.session.add(log)
        task.status = 'done'
        task.item.order.refresh_status_by_dug()
        db.session.commit()
        
        return f"""
        <div class="card border-0 shadow-sm mb-3 bg-success bg-opacity-10 text-center py-3 fade-me-in">
            <h5 class="text-success m-0"><i class="fas fa-check-circle"></i> Принято {fact_qty} шт!</h5>
        </div>
        """
    return "Ошибка ввода", 400

@bp.route('/api/feed/complete_tg_task', methods=['POST'])
@login_required
def complete_tg_task():
    # Идемпотентная операция: любой повторный/«уже выполненный»/даже пустой
    # запрос всё равно убирает карточку с фронта, чтобы пользователь не упёрся
    # в 400 и висящую плитку. 400 ломает HTMX swap — карточка остаётся на
    # экране, и кнопка «Сделано» выглядит неработающей.
    task_id_raw = request.form.get('task_id')
    done_html = """
    <div class="card border-0 shadow-sm mb-3 bg-primary bg-opacity-10 text-center py-3 fade-me-in">
        <h6 class="text-primary m-0"><i class="fas fa-check-double"></i> Задача выполнена!</h6>
    </div>
    """
    try:
        task_id = int(task_id_raw) if task_id_raw not in (None, '') else None
    except (TypeError, ValueError):
        task_id = None

    if task_id is None:
        current_app.logger.warning(
            'complete_tg_task: missing/invalid task_id (raw=%r, user=%s)',
            task_id_raw, getattr(current_user, 'id', None),
        )
        return done_html

    task = TgTask.query.get(task_id)
    if task is None:
        # Карточку уже удалили/завершили — просто говорим «ок», чтобы фронт скрыл плитку
        return done_html

    if task.status == 'new':
        task.status = 'done'
        task.completed_at = msk_now()
        task.completed_by_id = current_user.id
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            current_app.logger.exception('complete_tg_task commit failed')
    return done_html


# --- Кнопки для аномалии "stale_reserved" (заказ висит в резерве 14+ дней) ---
#
# Обе операции идемпотентны: повторный клик по уже закрытой карточке просто
# возвращает HTML-пустышку, чтобы HTMX удалил карточку с фронта.
# Отдельные эндпоинты нужны, чтобы:
#   - /ack: обнулить таймер через Order.reserve_ack_at = now (детектор на
#     следующем run_daily_scan не возьмёт этот заказ),
#   - /cancel: мягко удалить заказ (is_deleted=True + status='canceled'),
#     освободив резервы (все выборки фильтруют по is_deleted и status!=canceled).

def _extract_order_id_from_task(task):
    """Вытаскивает order_id из action_payload TgTask (JSON), возвращает int|None."""
    if not task or not task.action_payload:
        return None
    try:
        payload = json.loads(task.action_payload)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    val = payload.get('order_id')
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _close_stale_reserved_task(task):
    """Закрыть TgTask с kind=stale_reserved (помечаем done + updated_at)."""
    if task is None:
        return
    if task.status == 'new':
        task.status = 'done'
        task.completed_at = msk_now()
        task.completed_by_id = current_user.id
        task.updated_at = msk_now()


@bp.route('/api/feed/stale_reserved/<int:task_id>/ack', methods=['POST'])
@login_required
def stale_reserved_ack(task_id):
    """Менеджер подтвердил актуальность резерва — обнуляем таймер детектора
    и закрываем карточку."""
    done_html = (
        '<div class="card border-0 shadow-sm mb-3 bg-success bg-opacity-10 text-center py-3 fade-me-in">'
        '<h6 class="text-success m-0"><i class="fas fa-check-circle"></i> Подтверждено. Таймер сброшен.</h6>'
        '</div>'
    )
    task = TgTask.query.get(task_id)
    if task is None:
        return done_html
    order_id = _extract_order_id_from_task(task)
    order = Order.query.get(order_id) if order_id else None
    try:
        if order is not None:
            order.reserve_ack_at = msk_now()
        _close_stale_reserved_task(task)
        db.session.commit()
        if order is not None:
            try:
                _log_action_safe(f'Подтвердил актуальность заказа #{order.id} (резерв)')
            except Exception:
                pass
    except Exception:
        db.session.rollback()
        current_app.logger.exception('stale_reserved_ack commit failed')
    return done_html


@bp.route('/api/feed/stale_reserved/<int:task_id>/cancel', methods=['POST'])
@login_required
def stale_reserved_cancel(task_id):
    """Клиент не актуален — отменяем заказ (soft-delete), освобождая резервы."""
    done_html = (
        '<div class="card border-0 shadow-sm mb-3 bg-secondary bg-opacity-10 text-center py-3 fade-me-in">'
        '<h6 class="text-secondary m-0"><i class="fas fa-box-archive"></i> Заказ удалён, резервы освобождены.</h6>'
        '</div>'
    )
    task = TgTask.query.get(task_id)
    if task is None:
        return done_html
    order_id = _extract_order_id_from_task(task)
    order = Order.query.get(order_id) if order_id else None
    try:
        if order is not None and not order.is_deleted:
            order.status = 'canceled'
            order.canceled_at = msk_now()
            order.is_deleted = True
        _close_stale_reserved_task(task)
        db.session.commit()
        if order is not None:
            try:
                _log_action_safe(f'Удалил заказ #{order.id} как неактуальный (резерв освобождён)')
            except Exception:
                pass
    except Exception:
        db.session.rollback()
        current_app.logger.exception('stale_reserved_cancel commit failed')
    return done_html


def _log_action_safe(text):
    """Тонкая обёртка над utils.log_action — чтобы в случае ошибки не
    валить основной запрос."""
    try:
        from app.utils import log_action
        log_action(text)
    except Exception:
        pass


# --- Подтверждение / отклонение / смена статьи в карточках «расход из ТГ» ---
# Эти роуты вызываются HTMX-формами в feed.html для карточек chat_expense.
# Возвращают HTML-свап с результатом (похоже на stale_reserved ack/cancel).
def _chat_expense_role_ok():
    return (current_user.role or '') in ('admin', 'executive')


def _chat_expense_done_html(title, icon='fa-check-circle', cls='success'):
    return (
        f'<div class="card border-0 shadow-sm mb-3 bg-{cls} bg-opacity-10 '
        f'text-center py-3 fade-me-in">'
        f'<h6 class="text-{cls} m-0"><i class="fas {icon}"></i> {title}</h6>'
        f'</div>'
    )


@bp.route('/api/expenses/chat/<int:msg_id>/import', methods=['POST'])
@login_required
def chat_expense_import(msg_id):
    if not _chat_expense_role_ok():
        return _chat_expense_done_html('Доступ запрещён', 'fa-ban', 'danger'), 403
    budget_item_id = request.form.get('budget_item_id', type=int)
    try:
        from app.expense_chat import confirm_chat_expense
        ok, msg = confirm_chat_expense(
            msg_id, current_user, budget_item_id=budget_item_id,
        )
    except Exception:
        current_app.logger.exception('chat_expense_import failed')
        return _chat_expense_done_html('Ошибка: см. логи', 'fa-triangle-exclamation', 'danger'), 500
    if not ok:
        if msg == 'budget_item_required':
            return _chat_expense_done_html(
                'Выберите статью бюджета перед подтверждением.',
                'fa-triangle-exclamation', 'warning',
            ), 400
        return _chat_expense_done_html(
            f'Не удалось: {msg}', 'fa-triangle-exclamation', 'warning',
        ), 400
    try:
        _log_action_safe(f'Импортировал расход из ТГ #{msg_id}')
    except Exception:
        pass
    return _chat_expense_done_html('Проведено в расходы. Спасибо!')


@bp.route('/api/expenses/chat/<int:msg_id>/reject', methods=['POST'])
@login_required
def chat_expense_reject(msg_id):
    if not _chat_expense_role_ok():
        return _chat_expense_done_html('Доступ запрещён', 'fa-ban', 'danger'), 403
    try:
        from app.expense_chat import reject_chat_expense
        ok, msg = reject_chat_expense(msg_id, current_user)
    except Exception:
        current_app.logger.exception('chat_expense_reject failed')
        return _chat_expense_done_html('Ошибка: см. логи', 'fa-triangle-exclamation', 'danger'), 500
    if not ok:
        return _chat_expense_done_html(
            f'Не удалось: {msg}', 'fa-triangle-exclamation', 'warning',
        ), 400
    try:
        _log_action_safe(f'Отклонил расход из ТГ #{msg_id}')
    except Exception:
        pass
    return _chat_expense_done_html(
        'Скрыто из задач.', 'fa-eye-slash', 'secondary',
    )


@bp.route('/api/expenses/chat/<int:msg_id>/reclassify', methods=['POST'])
@login_required
def chat_expense_reclassify(msg_id):
    if not _chat_expense_role_ok():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    budget_item_id = request.form.get('budget_item_id', type=int)
    if not budget_item_id:
        return jsonify({'ok': False, 'error': 'budget_item_required'}), 400
    try:
        from app.expense_chat import reclassify_chat_expense
        ok, msg = reclassify_chat_expense(msg_id, current_user, budget_item_id)
    except Exception:
        current_app.logger.exception('chat_expense_reclassify failed')
        return jsonify({'ok': False, 'error': 'internal'}), 500
    if not ok:
        return jsonify({'ok': False, 'error': msg}), 400
    return jsonify({'ok': True})


# --- Админский форс-скан аномалий (в обход дневного щита ensure_daily_scan) ---
# Зачем: на проде `ensure_daily_scan` пропускает повторные запуски в течение
# суток. После деплоя нового детектора или при отладке нужно дернуть руками.
# Возвращает сводку в JSON + список упавших детекторов с текстом ошибки —
# удобно диагностировать, если какой-то детектор свалился молча.
@bp.route('/api/anomaly/rescan', methods=['POST'])
@login_required
def anomaly_force_rescan():
    if (current_user.role or '') not in ('admin', 'executive'):
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    try:
        from app.anomaly_engine import run_daily_scan
        result = run_daily_scan()
    except Exception as e:
        current_app.logger.exception('anomaly_force_rescan failed')
        return jsonify({'ok': False, 'error': str(e)}), 500
    return jsonify(result)


# ========================================================
# WEBHOOK ТЕЛЕГРАМ БОТА ДЛЯ ПОСТАНОВКИ ЗАДАЧ ОТ РУКОВОДИТЕЛЯ
# ========================================================

# Кольцевой буфер последних входящих апдейтов для админского /api/telegram/_debug.
# Живёт в памяти процесса — у каждого gunicorn-воркера свой. Для диагностики
# этого достаточно; на 1 реплике (как у тебя в Amvera) видно всё подряд.
from collections import deque as _tg_deque
_TG_DEBUG_LOG = _tg_deque(maxlen=100)


def _tg_env_csv(name):
    """Читает env-переменную со списком значений через запятую/пробел/; и чистит пустые."""
    raw = os.environ.get(name, '') or ''
    parts = [p.strip() for p in re.split(r'[,;\s]+', raw) if p.strip()]
    return parts


def _tg_allowed_chat_ids():
    """Whitelist chat_id. Разрешены: явный TG_ALLOWED_CHAT_IDS + все TG_CHAT_ID*."""
    ids = set()
    for v in _tg_env_csv('TG_ALLOWED_CHAT_IDS'):
        ids.add(str(v))
    for key in (
        'TG_CHAT_ID',
        'TG_CHAT_ID_HR',
        'TG_CHAT_ID_ORDERS',
        'TG_CHAT_ID_PATENTS',
        'TG_CHAT_ID_EXPENSES',
    ):
        v = (os.environ.get(key) or '').strip()
        if v:
            ids.add(str(v))
    return ids


def _tg_allowed_senders():
    """Whitelist отправителей по username (без @), в нижнем регистре.
    Если переменная пустая — вернём None (значит «разрешено всем», удобно для дебага).
    """
    parts = _tg_env_csv('TG_ALLOWED_SENDERS')
    if not parts:
        return None
    cleaned = set()
    for p in parts:
        cleaned.add(p.lstrip('@').lower())
    return cleaned


def _tg_bot_username():
    """Имя бота без @ (по умолчанию FloraFlovvBot). Можно переопределить TG_BOT_USERNAME."""
    name = (os.environ.get('TG_BOT_USERNAME') or 'FloraFlovvBot').strip()
    return name.lstrip('@')


def _tg_mentions_bot(text, bot_username):
    """@упоминание бота в ЛЮБОМ месте текста, регистронезависимо."""
    if not text or not bot_username:
        return False
    pattern = r'(?<!\w)@' + re.escape(bot_username) + r'(?!\w)'
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


def _tg_audit(entry):
    """Складываем запись в ring-буфер + пишем в лог приложения."""
    try:
        _TG_DEBUG_LOG.appendleft(entry)
        current_app.logger.info('tg webhook %s', entry)
    except Exception:
        pass


# Кольцевой буфер последних увиденных "живых" Telegram-людей (id + first_name + username).
# Его смысл — дать админу быстро подсмотреть user_id сотрудников без username
# (Бехруз, Алексей) и занести их в TG_USER_ID_MAP без лишних движений.
_TG_SEEN_PEOPLE = _tg_deque(maxlen=50)


def _tg_user_id_map():
    """Читает env TG_USER_ID_MAP вида '123456:aleksei,987654:behruz'.

    Используется для двух вещей:
      • подстановка @<canonical> вместо text_mention (когда у человека нет username);
      • любая будущая расстановка assignee_id по Telegram ID.

    Возвращает dict: {telegram_user_id(str) -> canonical_username(str)}.
    """
    mapping = {}
    for part in _tg_env_csv('TG_USER_ID_MAP'):
        if ':' not in part:
            continue
        tg_id, canonical = part.split(':', 1)
        tg_id = tg_id.strip()
        canonical = canonical.strip().lstrip('@')
        if tg_id and canonical:
            mapping[tg_id] = canonical
    return mapping


def _tg_remember_person(person, source):
    """Запоминаем, кого мы «видели» в чате — поможет админу узнать их user_id.

    `source` — откуда пришла запись: 'sender' | 'text_mention' | 'mention_entity'.
    """
    if not isinstance(person, dict):
        return
    uid = person.get('id')
    if not uid:
        return
    try:
        _TG_SEEN_PEOPLE.appendleft({
            'ts': datetime.utcnow().isoformat(),
            'user_id': uid,
            'first_name': person.get('first_name') or '',
            'last_name': person.get('last_name') or '',
            'username': (person.get('username') or '').lstrip('@'),
            'source': source,
        })
    except Exception:
        pass


def _tg_extract_mentions(msg):
    """Достаём из сообщения все упоминания людей.

    Возвращает список dict: {'user_id', 'first_name', 'username', 'type'}.
    Телеграм хранит упоминания в message.entities (если текст) или caption_entities
    (если картинка/видео). Нам интересны два типа:
      • 'mention'       — @username в тексте (у отправителя есть @);
      • 'text_mention'  — кликабельное имя без @ (у человека нет username) — тут
                          Telegram приносит user.id и first_name.
    """
    out = []
    text = msg.get('text') or msg.get('caption') or ''
    entities = msg.get('entities') or msg.get('caption_entities') or []
    for ent in entities:
        etype = ent.get('type')
        if etype == 'text_mention':
            u = ent.get('user') or {}
            out.append({
                'type': 'text_mention',
                'user_id': u.get('id'),
                'first_name': u.get('first_name') or '',
                'username': (u.get('username') or '').lstrip('@'),
                'mention_text': text[ent.get('offset', 0): ent.get('offset', 0) + ent.get('length', 0)],
            })
        elif etype == 'mention':
            raw = text[ent.get('offset', 0): ent.get('offset', 0) + ent.get('length', 0)]
            out.append({
                'type': 'mention',
                'user_id': None,
                'first_name': '',
                'username': raw.lstrip('@'),
                'mention_text': raw,
            })
    return out


def _tg_resolve_mentions(text, mentions, id_map):
    """Подставляем в тексте @<canonical_username> вместо text_mention.

    Пример: текст «Бехруз, копаем» с entity text_mention (user_id=777)
    и TG_USER_ID_MAP="777:behruz" превращается в «@behruz, копаем».
    AI-агенту это гораздо проще распарсить и сматчить через find_user.
    """
    if not text or not mentions or not id_map:
        return text
    # Идём по text_mention, заменяем вхождение mention_text на '@canonical'.
    # Используем обычный str.replace — допустимо, т.к. дубликаты имён в коротком
    # сообщении маловероятны, а результат всё равно проверит AI через find_user.
    new_text = text
    for m in mentions:
        if m.get('type') != 'text_mention':
            continue
        uid = str(m.get('user_id') or '')
        canonical = id_map.get(uid)
        if not canonical:
            continue
        mention_text = m.get('mention_text') or m.get('first_name') or ''
        if not mention_text:
            continue
        new_text = new_text.replace(mention_text, f"@{canonical}")
    return new_text


@bp.route('/api/telegram/webhook', methods=['POST'])
def telegram_webhook():
    """
    Telegram присылает сюда обновления. Ловим сообщения из наших супергрупп,
    только от разрешённых отправителей (руководители К и В), где упомянут
    @FloraFlovvBot — и передаём их AI-агенту, который нарезает задачи в дашборд.

    Безопасность:
      • Whitelist chat_id (TG_ALLOWED_CHAT_IDS + TG_CHAT_ID*) — иначе игнор.
      • Whitelist отправителей по username (TG_ALLOWED_SENDERS) — иначе игнор.
      • Триггер: @упоминание бота (TG_BOT_USERNAME) в любом месте текста,
        регистронезависимо. В личке бот отвечает всегда (для тестов).
    """
    data = request.get_json(silent=True) or {}

    # 1) Извлекаем «сообщение» из любого типа апдейта (обычные, правки, каналы).
    msg = (
        data.get('message')
        or data.get('edited_message')
        or data.get('channel_post')
        or data.get('edited_channel_post')
    )
    if not msg:
        _tg_audit({'ts': datetime.utcnow().isoformat(), 'stage': 'no_message',
                   'update_keys': list(data.keys())})
        return jsonify({'status': 'ok'})

    text = msg.get('text') or msg.get('caption') or ''
    chat = msg.get('chat') or {}
    sender_obj = msg.get('from') or {}
    chat_id = str(chat.get('id') or '')
    chat_title = chat.get('title') or chat.get('username') or chat.get('type') or ''
    sender_username = (sender_obj.get('username') or '').lstrip('@')
    sender_first = sender_obj.get('first_name') or ''
    sender_label = f"{sender_first}" + (f" (@{sender_username})" if sender_username else '')
    is_private = chat.get('type') == 'private'

    # Сразу запоминаем отправителя (в т.ч. его user_id). Это помогает
    # администратору увидеть tg-id сотрудников без username в /_debug.
    _tg_remember_person(sender_obj, source='sender')

    # Разбираем упоминания людей (в т.ч. text_mention — когда нет @username).
    mentions = _tg_extract_mentions(msg)
    for m in mentions:
        _tg_remember_person(
            {
                'id': m.get('user_id'),
                'first_name': m.get('first_name'),
                'username': m.get('username'),
            },
            source=m.get('type') or 'mention',
        )

    # Подменяем text_mention на @<canonical_username> — так AI-агент надёжнее
    # увидит получателя задачи (актуально для Бехруза/Алексея без @).
    id_map = _tg_user_id_map()
    text_for_ai = _tg_resolve_mentions(text, mentions, id_map)

    base_audit = {
        'ts': datetime.utcnow().isoformat(),
        'chat_id': chat_id,
        'chat_title': chat_title,
        'chat_type': chat.get('type'),
        'sender': sender_label or 'unknown',
        'sender_username': sender_username,
        'sender_id': sender_obj.get('id'),
        'text_preview': (text or '')[:200],
        'mentions': mentions,
        'text_for_ai_preview': (text_for_ai or '')[:200] if text_for_ai != text else None,
    }

    # 2) Whitelist чатов. В личке — пропускаем (удобно для ручного теста бота).
    allowed_chats = _tg_allowed_chat_ids()
    if not is_private and allowed_chats and chat_id not in allowed_chats:
        _tg_audit({**base_audit, 'stage': 'chat_not_allowed', 'allowed_chats': sorted(allowed_chats)})
        return jsonify({'status': 'ok', 'reason': 'chat_not_allowed'})

    # 2.5) «Расходы Жемчужниково»: сообщения из этого чата не требуют упоминания
    # бота и идут НЕ в AI-агента, а в специализированный монитор расходов.
    # Это до шагов 3-5, потому что там фильтр на @FloraFlovvBot и на белый
    # список отправителей (в чате расходов пишут разные сотрудники).
    expenses_chat_id = (os.environ.get('TG_CHAT_ID_EXPENSES') or '').strip()
    if expenses_chat_id and chat_id == expenses_chat_id:
        try:
            from app.expense_chat import ingest_message
            result = ingest_message(msg)
            _tg_audit({**base_audit, 'stage': 'expense_chat', 'result': result})
        except Exception as exc:
            current_app.logger.exception('expense_chat.ingest_message failed')
            _tg_audit({**base_audit, 'stage': 'expense_chat_error', 'error': str(exc)})
        return jsonify({'status': 'ok'})

    # 3) Whitelist отправителей (по username).
    allowed_senders = _tg_allowed_senders()
    if not is_private and allowed_senders is not None:
        if not sender_username or sender_username.lower() not in allowed_senders:
            _tg_audit({**base_audit, 'stage': 'sender_not_allowed',
                       'allowed_senders': sorted(allowed_senders)})
            return jsonify({'status': 'ok', 'reason': 'sender_not_allowed'})

    # 4) Триггер: упоминание бота в тексте (либо личка).
    bot_username = _tg_bot_username()
    mentioned = _tg_mentions_bot(text, bot_username)
    if not (is_private or mentioned):
        _tg_audit({**base_audit, 'stage': 'bot_not_mentioned', 'bot_username': bot_username})
        return jsonify({'status': 'ok', 'reason': 'bot_not_mentioned'})

    # 5) Всё ок — отдаём AI-агенту. Текст с подменой text_mention -> @<canonical>.
    sender_for_ai = sender_label or 'Руководитель'
    try:
        from ai_tools_agent import process_telegram_message_with_ai
        result_msg = process_telegram_message_with_ai(text_for_ai, sender_for_ai)
        _tg_audit({**base_audit, 'stage': 'ai_ok', 'result': str(result_msg)})
    except Exception as exc:
        current_app.logger.exception('telegram webhook AI failed')
        try:
            db.session.add(TgTask(
                raw_text=text,
                title="Сообщение из чата (Сбой AI)",
                details=text,
                assignee_role='user',
                sender_name=sender_for_ai,
                deadline=msk_today() + timedelta(days=1),
                source='fallback',
                action_type='info',
            ))
            db.session.commit()
        except Exception:
            db.session.rollback()
        _tg_audit({**base_audit, 'stage': 'ai_error', 'error': str(exc)})

    return jsonify({'status': 'ok'})


@bp.route('/api/telegram/_debug', methods=['GET'])
@login_required
def telegram_debug():
    """Админский эндпоинт: последние 100 входящих апдейтов Telegram (этого воркера).

    Удобно, чтобы глазами увидеть, почему сообщение не превратилось в задачу:
      • stage=no_message            — Telegram прислал апдейт без текста
      • stage=chat_not_allowed      — чат не в whitelist
      • stage=sender_not_allowed    — отправитель не в whitelist
      • stage=bot_not_mentioned     — нет @FloraFlovvBot в тексте
      • stage=ai_ok                 — AI отработал, смотри result
      • stage=ai_error              — внутренняя ошибка AI, см. error

    В поле seen_people — все "живые" Telegram-люди, которых мы видели
    (отправители + упоминания). Отсюда удобно подсмотреть user_id Бехруза
    и Алексея, чтобы занести их в TG_USER_ID_MAP.
    """
    if current_user.role != 'admin':
        return jsonify({'error': 'forbidden'}), 403

    # Уникальные «увиденные люди» — свежая запись перекрывает старую по user_id.
    seen_by_id = {}
    for p in _TG_SEEN_PEOPLE:
        uid = p.get('user_id')
        if uid is None:
            continue
        seen_by_id[str(uid)] = p

    id_map = _tg_user_id_map()
    seen_list = []
    for uid, p in seen_by_id.items():
        seen_list.append({**p, 'mapped_to': id_map.get(uid)})
    seen_list.sort(key=lambda x: x.get('ts') or '', reverse=True)

    return jsonify({
        'bot_username': _tg_bot_username(),
        'allowed_chats': sorted(_tg_allowed_chat_ids()),
        'allowed_senders': sorted(_tg_allowed_senders() or []),
        'user_id_map': id_map,
        'seen_people': seen_list,
        'recent': list(_TG_DEBUG_LOG),
    })


@bp.route('/api/telegram/webhook_info', methods=['GET'])
@login_required
def telegram_webhook_info():
    """Спросить у Telegram: 'куда ты сейчас шлёшь апдейты и были ли ошибки?'.

    Под капотом — getWebhookInfo (Bot API). Самый быстрый способ понять,
    почему recent=[] в /_debug: обычно либо url пустой, либо last_error_message
    говорит конкретную причину (ssl/404/timeout/blocked by Telegram).
    """
    if current_user.role != 'admin':
        return jsonify({'error': 'forbidden'}), 403
    token = (os.environ.get('TG_BOT_TOKEN') or '').strip()
    if not token:
        return jsonify({'error': 'TG_BOT_TOKEN not set'}), 500
    try:
        import requests as _rq
        r = _rq.get(f"https://api.telegram.org/bot{token}/getWebhookInfo", timeout=10)
        return jsonify({'http_status': r.status_code, 'data': r.json()})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 500


@bp.route('/api/telegram/set_webhook', methods=['POST', 'GET'])
@login_required
def telegram_set_webhook():
    """Зарегистрировать/сбросить webhook у Telegram.

    URL берётся (в порядке приоритета):
      • ?url=... в query-string (или form-field),
      • env TG_WEBHOOK_URL,
      • иначе — текущий host приложения + /api/telegram/webhook.

    Telegram принимает webhook ТОЛЬКО на https.

    Особые режимы:
      • ?drop=1     — удалить webhook (delete_webhook);
      • ?secret=... — Telegram будет присылать токен в заголовке
                      X-Telegram-Bot-Api-Secret-Token (удобно для верификации).
    """
    if current_user.role != 'admin':
        return jsonify({'error': 'forbidden'}), 403
    token = (os.environ.get('TG_BOT_TOKEN') or '').strip()
    if not token:
        return jsonify({'error': 'TG_BOT_TOKEN not set'}), 500

    # Сначала — поддержка удаления.
    drop = (request.values.get('drop') or '').strip().lower() in ('1', 'true', 'yes')
    import requests as _rq
    if drop:
        try:
            r = _rq.post(f"https://api.telegram.org/bot{token}/deleteWebhook",
                         json={'drop_pending_updates': True}, timeout=10)
            return jsonify({'action': 'deleteWebhook', 'http_status': r.status_code, 'data': r.json()})
        except Exception as exc:
            return jsonify({'error': str(exc)}), 500

    # Вычисляем URL вебхука.
    url = (
        (request.values.get('url') or '').strip()
        or (os.environ.get('TG_WEBHOOK_URL') or '').strip()
        or (request.host_url.rstrip('/') + url_for('main.telegram_webhook'))
    )
    if not url.startswith('https://'):
        return jsonify({
            'error': 'webhook url must start with https://',
            'got_url': url,
            'hint': 'передай ?url=https://<your-domain>/api/telegram/webhook или выставь TG_WEBHOOK_URL',
        }), 400

    payload = {
        'url': url,
        'allowed_updates': ['message', 'edited_message', 'channel_post', 'edited_channel_post'],
        'drop_pending_updates': False,
    }
    secret = (request.values.get('secret') or '').strip()
    if secret:
        payload['secret_token'] = secret
    try:
        r = _rq.post(f"https://api.telegram.org/bot{token}/setWebhook", json=payload, timeout=10)
        return jsonify({
            'action': 'setWebhook',
            'url': url,
            'http_status': r.status_code,
            'data': r.json(),
        })
    except Exception as exc:
        return jsonify({'error': str(exc), 'url': url}), 500


# =====================================================================
# ЗАДАЧИ-ПОРУЧЕНИЯ: РУЧНОЕ СОЗДАНИЕ И ПЕРЕНАЗНАЧЕНИЕ (TgTask v2)
# =====================================================================
#
# Политика доступа:
#   • создавать вручную могут admin / executive / user (менеджеры) — им рисуем
#     кнопку «+ Новая задача»;
#   • переназначать (и «отправлять дальше по цепочке») — любой залогиненный,
#     чтобы исполнитель мог передать задачу коллеге;
#   • закрывать (кнопка «Сделано») — как сейчас, через /api/feed/complete_tg_task.

# Что можно ставить в action_type при ручном создании. 'manual' добавлен
# на всякий случай — если менеджер не хочет привязываться к конкретной
# операции (чаще всего будет 'info').
_TG_TASK_MANUAL_ACTION_TYPES = {
    'info', 'create_order', 'digging', 'shipment', 'income', 'payment',
}
_TG_TASK_ROLES = {'admin', 'executive', 'user', 'user2'}


def _tg_task_can_create(user):
    """Кто имеет право нажать «+ Новая задача»."""
    return getattr(user, 'role', None) in {'admin', 'executive', 'user'}


def _tg_task_user_options_query():
    """Список пользователей для селекта «Исполнитель».

    Не исключаем никого — у бизнеса всего несколько аккаунтов, лишние
    фильтры только прячут живых людей. Сортировка: сначала роль (admin сверху),
    потом username.
    """
    return User.query.order_by(User.role.asc(), User.username.asc())


def _tg_task_parse_date(raw):
    """Пытаемся распарсить дату из формы. Возвращаем (date|None, error_msg|None)."""
    raw = (raw or '').strip()
    if not raw:
        return None, None
    for fmt in ('%Y-%m-%d', '%d.%m.%Y'):
        try:
            return datetime.strptime(raw, fmt).date(), None
        except ValueError:
            continue
    return None, f'Неверный формат даты: {raw!r}. Используй YYYY-MM-DD или DD.MM.YYYY.'


@bp.route('/api/tg_task/create', methods=['POST'])
@login_required
def api_tg_task_create():
    """Ручное создание задачи-поручения через UI.

    Поля формы (multipart/form-urlencoded):
      • title        — обязательно, до 200 символов;
      • details      — свободный текст;
      • assignee_id  — id конкретного пользователя (опционально);
      • assignee_role — роль-отдел, если конкретного исполнителя нет (опционально);
      • deadline     — YYYY-MM-DD или DD.MM.YYYY; если пусто — завтра;
      • action_type  — из _TG_TASK_MANUAL_ACTION_TYPES (default 'info').
    """
    if not _tg_task_can_create(current_user):
        return jsonify({'error': 'forbidden'}), 403

    form = request.form
    title = (form.get('title') or '').strip()[:200]
    if not title:
        return jsonify({'error': 'title required'}), 400

    details = (form.get('details') or '').strip()
    action_type = (form.get('action_type') or 'info').strip()
    if action_type not in _TG_TASK_MANUAL_ACTION_TYPES:
        action_type = 'info'

    # Исполнитель: либо конкретный пользователь, либо роль-отдел. Одно из двух
    # обязательно — иначе карточка «зависнет» (assignee_role IS NULL видна всем).
    assignee_id = None
    raw_assignee = (form.get('assignee_id') or '').strip()
    if raw_assignee:
        try:
            assignee_id = int(raw_assignee)
            if not User.query.get(assignee_id):
                assignee_id = None
        except ValueError:
            assignee_id = None

    assignee_role = (form.get('assignee_role') or '').strip() or None
    if assignee_role and assignee_role not in _TG_TASK_ROLES:
        assignee_role = None

    # Если явный исполнитель задан — роль заберём из его профиля (чтобы KPI
    # по отделу считался корректно, когда задача уходит в конкретного человека).
    if assignee_id and not assignee_role:
        u = User.query.get(assignee_id)
        if u and u.role:
            assignee_role = u.role

    dead, err = _tg_task_parse_date(form.get('deadline'))
    if err:
        return jsonify({'error': err}), 400
    if not dead:
        dead = msk_today() + timedelta(days=1)

    task = TgTask(
        raw_text=f'[manual] {title}',
        title=title,
        details=details,
        assignee_role=assignee_role,
        assignee_id=assignee_id,
        deadline=dead,
        action_type=action_type,
        action_payload='{}',
        status='new',
        sender_name=current_user.username,
        source='manual',
        created_by_id=current_user.id,
        updated_at=msk_now(),
    )
    db.session.add(task)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception('tg_task create failed')
        return jsonify({'error': 'db error'}), 500

    current_app.logger.info(
        'tg_task create_ok id=%s by=%s assignee_id=%s assignee_role=%s deadline=%s',
        task.id, current_user.username, assignee_id, assignee_role, dead.isoformat(),
    )

    # HX-запрос ожидает HTMX-friendly ответ: просим клиента перезагрузить
    # страницу — лента перестроится и задача встанет в нужную группу.
    if request.headers.get('HX-Request'):
        resp = jsonify({'status': 'ok', 'id': task.id})
        resp.headers['HX-Trigger'] = 'tg-task-changed'
        resp.headers['HX-Refresh'] = 'true'
        return resp
    return jsonify({'status': 'ok', 'id': task.id})


@bp.route('/api/tg_task/_recent', methods=['GET'])
@login_required
def api_tg_task_recent():
    """Диагностика: последние 30 задач TgTask с реальными полями.

    Нужен, чтобы быстро увидеть, как именно AI/ручной ввод заполнил задачу:
    кто назначен (assignee_id + assignee_role), какой action_type,
    какой source и кем создано. Именно от этих полей зависит, кому
    задача покажется в ленте. Доступ — только admin/executive.
    """
    if current_user.role not in ('admin', 'executive'):
        return jsonify({'error': 'forbidden'}), 403

    rows = TgTask.query.order_by(TgTask.id.desc()).limit(30).all()
    # Подтянем юзеров одним запросом (исполнитель, создатель, «передал кто»).
    ids = set()
    for t in rows:
        for v in (t.assignee_id, getattr(t, 'created_by_id', None),
                  getattr(t, 'reassigned_from_id', None)):
            if v:
                ids.add(v)
    users = {u.id: u for u in User.query.filter(User.id.in_(ids)).all()} if ids else {}

    def _u(uid):
        if not uid:
            return None
        u = users.get(uid)
        if not u:
            return {'id': uid, 'name': f'#{uid}'}
        return {'id': u.id, 'username': u.username, 'role': u.role}

    out = []
    for t in rows:
        out.append({
            'id': t.id,
            'title': t.title,
            'action_type': t.action_type,
            'status': t.status,
            'source': getattr(t, 'source', None),
            'assignee_id': t.assignee_id,
            'assignee_role': t.assignee_role,
            'assignee': _u(t.assignee_id),
            'created_by': _u(getattr(t, 'created_by_id', None)),
            'reassigned_from': _u(getattr(t, 'reassigned_from_id', None)),
            'deadline': t.deadline.isoformat() if t.deadline else None,
            'created_at': t.created_at.isoformat() if t.created_at else None,
            'raw_text': (t.raw_text or '')[:200],
        })
    return jsonify({
        'count': len(out),
        'tasks': out,
    })


@bp.route('/api/tg_task/new_form', methods=['GET'])
@login_required
def api_tg_task_new_form():
    """HTML-фрагмент формы «+ Новая задача» для HTMX-модалки."""
    if not _tg_task_can_create(current_user):
        return jsonify({'error': 'forbidden'}), 403
    users = _tg_task_user_options_query().all()
    default_deadline = (msk_today() + timedelta(days=1)).isoformat()
    return render_template(
        'partials/tg_task_new_form.html',
        users=users,
        action_types=sorted(_TG_TASK_MANUAL_ACTION_TYPES),
        roles=sorted(_TG_TASK_ROLES),
        default_deadline=default_deadline,
    )


@bp.route('/api/tg_task/<int:task_id>/reassign_form', methods=['GET'])
@login_required
def api_tg_task_reassign_form(task_id):
    """HTML-фрагмент формы «Передать задачу» для HTMX-модалки.

    Доступ — любой залогиненный (исполнитель тоже может передать дальше).
    """
    task = TgTask.query.get_or_404(task_id)
    if task.status != 'new':
        return '<div class="alert alert-warning m-3">Задача уже закрыта.</div>'
    users = _tg_task_user_options_query().all()
    return render_template(
        'partials/tg_task_reassign_form.html',
        task=task,
        users=users,
        roles=sorted(_TG_TASK_ROLES),
    )


@bp.route('/api/tg_task/<int:task_id>/reassign', methods=['POST'])
@login_required
def api_tg_task_reassign(task_id):
    """Переназначение задачи другому сотруднику / в другой отдел.

    Поля:
      • assignee_id  — новый конкретный исполнитель (опционально);
      • assignee_role — новая роль-отдел (опционально).
    Должно быть хотя бы одно, иначе задача «зависнет» без получателя.
    """
    task = TgTask.query.get_or_404(task_id)
    if task.status != 'new':
        return jsonify({'error': 'task already closed'}), 400

    form = request.form
    new_assignee_id = None
    raw_assignee = (form.get('assignee_id') or '').strip()
    if raw_assignee:
        try:
            candidate = int(raw_assignee)
            if User.query.get(candidate):
                new_assignee_id = candidate
        except ValueError:
            pass
    new_assignee_role = (form.get('assignee_role') or '').strip() or None
    if new_assignee_role and new_assignee_role not in _TG_TASK_ROLES:
        new_assignee_role = None

    # Если ставим конкретного исполнителя и роль явно не задали — возьмём его роль.
    if new_assignee_id and not new_assignee_role:
        u = User.query.get(new_assignee_id)
        if u and u.role:
            new_assignee_role = u.role

    if not new_assignee_id and not new_assignee_role:
        return jsonify({'error': 'assignee_id or assignee_role is required'}), 400

    # Сценарий «передаю ту же самую задачу себе же» — бессмысленный.
    if (new_assignee_id and new_assignee_id == task.assignee_id
            and new_assignee_role == task.assignee_role):
        return jsonify({'error': 'no changes'}), 400

    prev_assignee_id = task.assignee_id
    task.reassigned_from_id = prev_assignee_id or current_user.id
    task.reassigned_at = msk_now()
    task.assignee_id = new_assignee_id
    task.assignee_role = new_assignee_role
    task.updated_at = msk_now()
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception('tg_task reassign failed')
        return jsonify({'error': 'db error'}), 500

    current_app.logger.info(
        'tg_task reassign_ok id=%s by=%s from_assignee=%s to_assignee=%s to_role=%s',
        task.id, current_user.username, prev_assignee_id, new_assignee_id, new_assignee_role,
    )

    if request.headers.get('HX-Request'):
        resp = jsonify({'status': 'ok', 'id': task.id})
        resp.headers['HX-Trigger'] = 'tg-task-changed'
        resp.headers['HX-Refresh'] = 'true'
        return resp
    return jsonify({'status': 'ok', 'id': task.id})


@bp.route('/uploads/<path:filename>')
def serve_uploaded_file(filename):
    try:
        response = send_from_directory(
            current_app.config['UPLOAD_FOLDER'],
            filename,
            max_age=60 * 60 * 24 * 30,
        )
        response.headers['Cache-Control'] = 'public, max-age=2592000, immutable'
        return response
    except (OSError, FileNotFoundError):
        return "", 404

@bp.route('/admin/backup_db')
@login_required
def backup_db():
    if current_user.role != 'admin': 
        return redirect(url_for('main.index'))
    
    db_path = current_app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
    return send_file(db_path, as_attachment=True, download_name="backup.db")

@bp.route('/sw.js')
def service_worker():
    response = send_file('static/sw.js', mimetype='application/javascript')
    response.headers['Cache-Control'] = 'no-cache'
    return response

@bp.route('/manifest.json')
def manifest():
    return send_file('static/manifest.json', mimetype='application/json')

@bp.route('/static/icon-192.png')
def app_icon():
    try:
        return send_file('static/icon-192.png')
    except (OSError, FileNotFoundError):
        return "", 404

@bp.route('/guide')
@login_required
def guide():
    return render_template('guide.html')

@bp.route('/offline')
def offline():
    return render_template('offline.html')

@bp.route('/api/cache-manifest')
@login_required
def cache_manifest():
    # Версию кэша намеренно загрубляем: меняется раз в ~15 минут,
    # а не после каждой записи в ActionLog. Иначе любой POST любого
    # пользователя заставлял всех клиентов синхронно перекачивать
    # десятки тяжёлых страниц и блокировал SQLite write-lock.
    bucket = int(datetime.now().timestamp()) // 900  # 15 минут
    data_version = f"u{current_user.id}_b{bucket}"

    # Прогреваем только лёгкую "оболочку" + ближайшие заказы.
    # Тяжёлые отчёты (expenses/budget/reports/*, cost, crm/*, logs)
    # пользователь открывает редко — незачем грузить их фоном у всех.
    urls = [
        url_for('orders.orders_list'),
        url_for('orders.order_create'),
        url_for('stock.stock_report'),
        url_for('directory.directory'),
        url_for('main.guide')
    ]

    last_orders = (Order.query
                   .filter(Order.is_deleted == False)
                   .order_by(Order.date.desc())
                   .limit(5)
                   .all())
    for o in last_orders:
        urls.append(url_for('orders.order_detail', order_id=o.id))

    response = jsonify({'version': data_version, 'urls': urls})
    response.headers['Cache-Control'] = 'private, max-age=60'
    return response

# ==========================================================
# API ДЛЯ ВИЗУАЛЬНОГО СКЛАДА (КАРТЫ)
# ==========================================================
from app.models import Field, StockBalance, DocumentRow

# Глобальный проброс полей во все HTML шаблоны
@bp.app_context_processor
def inject_global_vars():
    data = {}
    try:
        fields = Field.query.all()
        data['all_nursery_fields'] = sorted(fields, key=natural_key)
    except Exception:
        data['all_nursery_fields'] = []

    # Список статей бюджета — нужен в feed.html для селекта в карточке
    # «расход из ТГ». Считаем лениво (и с try/except), ошибка не должна
    # ломать рендер всего приложения.
    try:
        from app.models import BudgetItem as _BI
        data['all_budget_items'] = _BI.query.order_by(_BI.name).all()
    except Exception:
        data['all_budget_items'] = []

    # Счётчики для плашки-бейджа на вкладке «Дашборд». Логика должна точно
    # совпадать с раскладкой групп на самом дашборде (см. функцию index()),
    # иначе у пользователя «4 в просрочке, а 1 на сегодня» при пустой группе
    # «План на сегодня». Поэтому мы воспроизводим те же три источника:
    #   1) обычные TgTask (без action_type 'anomaly'/'digest'):
    #        deadline < today           → overdue
    #        остальные (NULL / сегодня / будущее) → today  ← см. main.py:843
    #   2) аномалии (только admin/executive):
    #        severity == 'danger'       → overdue  (is_forced_urgent)
    #        остальные                  → today
    #   3) дайджест (только admin/executive, свой): → today
    counts = {'today': 0, 'overdue': 0}
    try:
        if current_user.is_authenticated:
            today = msk_today()
            role = (current_user.role or '')

            # --- 1) Обычные TgTask (уважаем scope видимости как в фиде) ---
            scope = _tg_task_scope_cond(current_user)
            base_tgt = TgTask.query.filter(
                TgTask.status == 'new',
                TgTask.action_type.notin_(['anomaly', 'digest']),
            )
            if scope is not None:
                base_tgt = base_tgt.filter(scope)

            overdue_tgt = base_tgt.filter(
                TgTask.deadline.isnot(None),
                TgTask.deadline < today,
            ).count()
            total_tgt = base_tgt.count()
            today_tgt = max(0, total_tgt - overdue_tgt)
            counts['overdue'] += overdue_tgt
            counts['today'] += today_tgt

            # --- 2) Аномалии: раньше считали только admin/executive; теперь
            # CSV-роли разруливает _role_match, и в ленте блок 6 рендерится
            # для всех ролей. Чтобы бейдж сходился с содержимым ленты,
            # применяем тот же scope_cond к подсчёту. Менеджер увидит в бейдже
            # только те аномалии, которые реально попадают в его ленту
            # (stale_reserved и т.п.), финансовые к нему в подсчёт не попадут.
            anomalies_q = TgTask.query.filter(
                TgTask.status == 'new',
                TgTask.action_type == 'anomaly',
            )
            if scope is not None:
                anomalies_q = anomalies_q.filter(scope)
            anom_danger = anomalies_q.filter(TgTask.severity == 'danger').count()
            anom_total = anomalies_q.count()
            counts['overdue'] += anom_danger
            counts['today'] += max(0, anom_total - anom_danger)

            # --- 3) Персональный дайджест — только для admin/executive ---
            if role in ('admin', 'executive'):
                digest_count = TgTask.query.filter(
                    TgTask.status == 'new',
                    TgTask.action_type == 'digest',
                    TgTask.assignee_id == current_user.id,
                ).count()
                counts['today'] += digest_count
    except Exception:
        current_app.logger.exception('dashboard badge counts failed')
    data['dashboard_counts'] = counts
    return data

@bp.route('/api/visual_stock/field/<int:field_id>')
@login_required
def api_field_stock(field_id):
    """Возвращает всё, что реально растет на конкретном поле + резерв/свободно"""
    from app.stock_helpers import get_reserved_map
    rmap = get_reserved_map()
    stocks = StockBalance.query.filter(StockBalance.field_id == field_id, StockBalance.quantity > 0).all()
    items = []
    for s in stocks:
        reserved = int(rmap.get((s.plant_id, s.size_id, s.field_id, s.year), 0) or 0)
        qty = int(s.quantity or 0)
        free = max(0, qty - reserved)
        items.append({
            'plant_id': s.plant_id,
            'plant_name': s.plant.name if s.plant else 'Неизвестно',
            'size_id': s.size_id,
            'size_name': s.size.name if s.size else '-',
            'year': s.year,
            'qty': qty,
            'reserved': reserved,
            'free': free,
        })
    return jsonify({'status': 'ok', 'items': items})


@bp.route('/api/visual_stock/fields_summary')
@login_required
def api_fields_summary():
    """Сводка по всем полям для подписей на карте:
    {field_id: {total, top: [...], last_recount_date, recount_overdue}}

    Поле считается «просроченным по пересчёту», если последний документ
    `field_recount` по нему старше 4 месяцев (≈120 дней) или отсутствует вовсе.
    """
    stocks = (StockBalance.query
              .filter(StockBalance.quantity > 0)
              .all())
    by_field = {}
    for s in stocks:
        if not s.field_id:
            continue
        d = by_field.setdefault(s.field_id, {'total': 0, 'rows': []})
        d['total'] += int(s.quantity or 0)
        d['rows'].append({
            'plant': s.plant.name if s.plant else '—',
            'size': s.size.name if s.size else '',
            'qty': int(s.quantity or 0),
        })

    # Последний пересчёт по каждому полю: max(Document.date) среди строк field_recount
    last_recount = {}
    try:
        rows = (db.session.query(
                    DocumentRow.field_to_id,
                    func.max(Document.date)
                )
                .join(Document, DocumentRow.document_id == Document.id)
                .filter(Document.doc_type == 'field_recount',
                        DocumentRow.field_to_id.isnot(None))
                .group_by(DocumentRow.field_to_id)
                .all())
        for fid, dt in rows:
            last_recount[fid] = dt
    except Exception:
        last_recount = {}

    today = msk_today()
    overdue_cutoff = today - timedelta(days=120)  # ≈ 4 месяца

    # Учитываем все поля из БД, не только те, где есть остатки
    all_fields = Field.query.all()

    result = {}
    for f in all_fields:
        d = by_field.get(f.id, {'total': 0, 'rows': []})
        d['rows'].sort(key=lambda r: r['qty'], reverse=True)
        last_dt = last_recount.get(f.id)
        last_date = None
        overdue = True
        if last_dt is not None:
            try:
                last_date = last_dt.date() if hasattr(last_dt, 'date') else last_dt
                overdue = last_date < overdue_cutoff
            except Exception:
                last_date = None
                overdue = True
        result[f.id] = {
            'total': d['total'],
            'top': d['rows'][:3],
            'last_recount_date': last_date.isoformat() if last_date else None,
            'recount_overdue': bool(overdue),
        }

    return jsonify({'status': 'ok', 'fields': result})

@bp.route('/api/visual_stock/move', methods=['POST'])
@login_required
def api_visual_move():
    """Создает документ перемещения с визуальной карты"""
    from app.utils import get_or_create_stock
    data = request.json
    try:
        qty = int(data.get('qty', 0))
        if qty <= 0:
            return jsonify({'status': 'error', 'message': 'Количество должно быть больше нуля'})

        # 1. Создаем документ
        doc = Document(doc_type='move', user_id=current_user.id, comment="Визуальное перемещение (карта)")
        db.session.add(doc)
        db.session.flush()
        
        # 2. Добавляем строку документа
        row = DocumentRow(
            document_id=doc.id,
            plant_id=data['plant_id'],
            size_id=data['size_id'],
            year=data['year'],
            field_from_id=data['field_from'],
            field_to_id=data['field_to'],
            quantity=qty
        )
        db.session.add(row)
        
        # 3. Меняем остатки физически
        src_stock = get_or_create_stock(data['plant_id'], data['size_id'], data['field_from'], data['year'])
        dest_stock = get_or_create_stock(data['plant_id'], data['size_id'], data['field_to'], data['year'])
        
        if src_stock.quantity < qty:
            return jsonify({'status': 'error', 'message': f'На исходном поле осталось всего {src_stock.quantity} шт.'})
            
        src_stock.quantity -= qty
        dest_stock.quantity += qty
        
        db.session.commit()
        from app.utils import log_action
        log_action(f"Перемещение (Карта): {qty} шт.")
        
        return jsonify({'status': 'ok'})
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)})


@bp.route('/api/visual_stock/plants_catalog')
@login_required
def api_visual_plants_catalog():
    """Каталог (растение+размер) для боковой панели карты — источник drag-карточек"""
    from app.models import Plant, Size
    plants = Plant.query.order_by(Plant.name).all()
    sizes = Size.query.order_by(Size.name).all()
    return jsonify({
        'status': 'ok',
        'plants': [{'id': p.id, 'name': p.name} for p in plants],
        'sizes': [{'id': s.id, 'name': s.name} for s in sizes],
    })


@bp.route('/api/visual_stock/income', methods=['POST'])
@login_required
def api_visual_income():
    """Визуальное поступление: drop карточки растения на поле"""
    from app.utils import get_or_create_stock, log_action
    data = request.json or {}
    try:
        qty = int(data.get('qty', 0))
        year = int(data.get('year') or msk_now().year)
        plant_id = int(data['plant_id'])
        size_id = int(data['size_id'])
        field_to = int(data['field_to'])
        if qty <= 0:
            return jsonify({'status': 'error', 'message': 'Количество должно быть больше нуля'})

        doc = Document(doc_type='income', user_id=current_user.id,
                       comment=data.get('comment') or 'Визуальное поступление (карта)')
        db.session.add(doc)
        db.session.flush()

        db.session.add(DocumentRow(
            document_id=doc.id,
            plant_id=plant_id,
            size_id=size_id,
            field_to_id=field_to,
            year=year,
            quantity=qty,
        ))
        get_or_create_stock(plant_id, size_id, field_to, year).quantity += qty

        db.session.commit()
        log_action(f"Поступление (Карта): {qty} шт.")
        return jsonify({'status': 'ok', 'doc_id': doc.id})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)})


@bp.route('/api/visual_stock/writeoff', methods=['POST'])
@login_required
def api_visual_writeoff():
    """Визуальное списание: клик по позиции на поле -> подтверждение кол-ва"""
    from app.utils import get_or_create_stock, log_action
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Только для администратора'})
    data = request.json or {}
    try:
        qty = int(data.get('qty', 0))
        year = int(data['year'])
        plant_id = int(data['plant_id'])
        size_id = int(data['size_id'])
        field_from = int(data['field_from'])
        if qty <= 0:
            return jsonify({'status': 'error', 'message': 'Количество должно быть больше нуля'})

        src_stock = get_or_create_stock(plant_id, size_id, field_from, year)
        if (src_stock.quantity or 0) < qty:
            return jsonify({'status': 'error', 'message': f'На поле осталось всего {src_stock.quantity or 0} шт.'})

        from app.stock_helpers import get_reserved_map
        rmap = get_reserved_map(plant_id=plant_id, size_id=size_id)
        reserved = int(rmap.get((plant_id, size_id, field_from, year), 0) or 0)
        free = max(0, int(src_stock.quantity or 0) - reserved)
        if qty > free:
            return jsonify({'status': 'error', 'message': f'Списание {qty} шт превышает свободный остаток ({free} шт). Резерв по позиции: {reserved} шт.'})

        doc = Document(doc_type='writeoff', user_id=current_user.id,
                       comment=data.get('comment') or 'Визуальное списание (карта)')
        db.session.add(doc)
        db.session.flush()
        db.session.add(DocumentRow(
            document_id=doc.id,
            plant_id=plant_id,
            size_id=size_id,
            field_from_id=field_from,
            year=year,
            quantity=qty,
        ))
        src_stock.quantity -= qty

        db.session.commit()
        log_action(f"Списание (Карта): {qty} шт.")
        return jsonify({'status': 'ok', 'doc_id': doc.id})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)})


@bp.route('/api/visual_stock/field_recount', methods=['POST'])
@login_required
def api_visual_field_recount():
    """Визуальный пересчёт (изменение остатков) — аналог field_recount из stock.documents"""
    from app.utils import get_or_create_stock, log_action
    if current_user.role == 'user2':
        return jsonify({'status': 'error', 'message': 'Только для менеджера/админа'})
    data = request.json or {}
    try:
        field_id = int(data['field_id'])
        rows = data.get('rows') or []

        # Агрегируем по ключу
        fact_map = {}
        for r in rows:
            try:
                pid = int(r['plant_id'])
                sid = int(r['size_id'])
                yr = int(r['year'])
                fact = int(r['fact_qty'])
            except Exception:
                continue
            if fact < 0:
                return jsonify({'status': 'error', 'message': 'Фактическое количество не может быть отрицательным'})
            fact_map[(pid, sid, yr)] = fact

        if not fact_map:
            return jsonify({'status': 'error', 'message': 'Нет данных для пересчета'})

        from app.models import Field
        field_obj = Field.query.get(field_id)
        doc_comment = f'Пересчет по полю: {field_obj.name if field_obj else field_id} (карта)'

        from app.stock_helpers import get_reserved_map
        rmap = get_reserved_map()

        # Сначала валидируем: пересчёт не должен уводить свободный остаток в минус (fact < reserved)
        for (pid, sid, yr), fact_qty in fact_map.items():
            reserved = int(rmap.get((pid, sid, field_id, yr), 0) or 0)
            if int(fact_qty) < reserved:
                from app.models import Plant as _P, Size as _S
                pl = _P.query.get(pid); sz = _S.query.get(sid)
                name = (pl.name if pl else '') + ((' · ' + sz.name) if sz else '')
                return jsonify({'status': 'error', 'message': f'Пересчёт {fact_qty} шт по позиции «{name}» меньше резерва {reserved} шт.'}), 409

        doc = Document(doc_type='field_recount', user_id=current_user.id, comment=doc_comment)
        db.session.add(doc)
        db.session.flush()

        changed_rows = 0
        for (pid, sid, yr), fact_qty in fact_map.items():
            stock = get_or_create_stock(pid, sid, field_id, yr)
            current_qty = int(stock.quantity or 0)
            delta = int(fact_qty) - current_qty
            if delta == 0:
                continue
            stock.quantity += delta
            db.session.add(DocumentRow(
                document_id=doc.id,
                plant_id=pid,
                size_id=sid,
                field_to_id=field_id,
                year=yr,
                quantity=delta,
            ))
            changed_rows += 1

        if changed_rows == 0:
            db.session.delete(doc)
            db.session.commit()
            return jsonify({'status': 'ok', 'changed': 0, 'message': 'Изменений нет'})

        db.session.commit()
        log_action(f"Карточка пересчета (Карта) #{doc.id} по полю {field_id}")
        return jsonify({'status': 'ok', 'doc_id': doc.id, 'changed': changed_rows})
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)})


# =========================================================================
# ====== РЕДАКТОР ВИЗУАЛЬНОЙ КАРТЫ ПОЛЕЙ ==================================
# =========================================================================
# Админ может настроить вид карты: загрузить фоновое фото, переставить и
# изменить размер/форму полей. Координаты храним в процентах от канваса,
# чтобы карта корректно масштабировалась под любое окно.

_MAP_BG_DIR = 'map_backgrounds'
_MAP_BG_ALLOWED = {'.jpg', '.jpeg', '.png', '.webp'}


def _safe_shape(shape_raw):
    """Нормализуем полигон: массив пар чисел [[x,y],...], клампим к 0..100.
    Возвращаем JSON-строку или None (=> фронт нарисует прямоугольник)."""
    if shape_raw in (None, '', [], {}):
        return None
    try:
        points = shape_raw if isinstance(shape_raw, list) else json.loads(shape_raw)
    except Exception:
        return None
    cleaned = []
    for pt in points or []:
        try:
            x = float(pt[0]); y = float(pt[1])
        except (TypeError, ValueError, IndexError):
            continue
        x = max(0.0, min(100.0, x))
        y = max(0.0, min(100.0, y))
        cleaned.append([round(x, 2), round(y, 2)])
    if len(cleaned) < 3:
        return None
    return json.dumps(cleaned)


def _field_to_map_dict(f):
    """Сериализуем поле для карты. Клиент сам подставит авто-сетку, если координат нет."""
    shape = None
    if f.map_shape:
        try:
            shape = json.loads(f.map_shape)
        except Exception:
            shape = None
    return {
        'id': f.id,
        'name': f.name,
        'x': f.map_x,
        'y': f.map_y,
        'w': f.map_w,
        'h': f.map_h,
        'shape': shape,
        'color': f.map_color,
        'z': f.map_z,
        'layout': f.map_layout or None,
    }


_MAP_BG_FIT_ALLOWED = {'contain', 'cover', 'stretch', 'custom'}
_MAP_CANVAS_ASPECTS = {'auto', '16/9', '4/3', '1/1', '21/9', '3/1', '2/1'}
# Координаты полей в процентах канваса. Разрешаем выходить «чуть-чуть» за край —
# это даёт дополнительную рабочую зону без глобального рескейла канваса.
_MAP_FIELD_COORD_MIN = -20.0
_MAP_FIELD_COORD_MAX = 200.0


def _map_settings_payload():
    s = MapSettings.get()
    bg_url = None
    if s.background_path:
        # Важно: сам файл раздаётся через /uploads/<path:filename>
        bg_url = url_for('main.serve_uploaded_file', filename=s.background_path)
    return {
        'background_url': bg_url,
        'bg_width': s.bg_width,
        'bg_height': s.bg_height,
        'bg_opacity': float(s.bg_opacity) if s.bg_opacity is not None else 1.0,
        'bg_fit': (s.bg_fit or 'cover'),
        'bg_offset_x': float(s.bg_offset_x) if s.bg_offset_x is not None else 0.0,
        'bg_offset_y': float(s.bg_offset_y) if s.bg_offset_y is not None else 0.0,
        'bg_scale': float(s.bg_scale) if s.bg_scale is not None else 1.0,
        'bg_rotation': float(s.bg_rotation) if s.bg_rotation is not None else 0.0,
        'canvas_width': int(s.canvas_width) if s.canvas_width else 1600,
        'canvas_aspect': (s.canvas_aspect or 'auto'),
    }


@bp.route('/api/visual_stock/map_config')
@login_required
def api_map_config():
    """Полный снимок карты: фон + layout всех полей."""
    fields = Field.query.all()
    return jsonify({
        'status': 'ok',
        'settings': _map_settings_payload(),
        'fields': [_field_to_map_dict(f) for f in sorted(fields, key=natural_key)],
    })


@bp.route('/api/visual_stock/map_layout', methods=['POST'])
@login_required
def api_map_layout_save():
    """Сохраняем новые координаты/форму/цвет полей. Только админ."""
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Только для администратора'}), 403
    data = request.get_json(silent=True) or {}
    items = data.get('fields') or []
    if not isinstance(items, list):
        return jsonify({'status': 'error', 'message': 'Ожидается массив fields'}), 400

    updated = 0
    try:
        for it in items:
            try:
                fid = int(it.get('id'))
            except (TypeError, ValueError):
                continue
            f = Field.query.get(fid)
            if f is None:
                continue
            # Координаты тоже клампим к канвасу, чтобы клиент не уехал за границы.
            def _num(v, lo, hi, default=None):
                try:
                    x = float(v)
                except (TypeError, ValueError):
                    return default
                return max(lo, min(hi, x))
            f.map_x = _num(it.get('x'), _MAP_FIELD_COORD_MIN, _MAP_FIELD_COORD_MAX, f.map_x)
            f.map_y = _num(it.get('y'), _MAP_FIELD_COORD_MIN, _MAP_FIELD_COORD_MAX, f.map_y)
            f.map_w = _num(it.get('w'), 0.5, _MAP_FIELD_COORD_MAX, f.map_w)
            f.map_h = _num(it.get('h'), 0.5, _MAP_FIELD_COORD_MAX, f.map_h)
            f.map_shape = _safe_shape(it.get('shape'))
            color = it.get('color')
            if isinstance(color, str) and len(color) <= 20:
                f.map_color = color or None
            try:
                f.map_z = int(it.get('z')) if it.get('z') is not None else f.map_z
            except (TypeError, ValueError):
                pass
            # Раскладка карточки — админ выбирает её в редакторе.
            layout = it.get('layout')
            if layout in (None, '', 'auto', 'stack', 'row', 'compact', 'number-only'):
                f.map_layout = layout or None
            updated += 1

        db.session.commit()
        return jsonify({'status': 'ok', 'updated': updated})
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception('map_layout save failed')
        return jsonify({'status': 'error', 'message': str(e)}), 500


@bp.route('/api/visual_stock/map_background', methods=['POST'])
@login_required
def api_map_background_upload():
    """Загрузка фоновой картинки карты. Только админ."""
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Только для администратора'}), 403
    f = request.files.get('file')
    if not f or not f.filename:
        return jsonify({'status': 'error', 'message': 'Файл не передан'}), 400
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in _MAP_BG_ALLOWED:
        return jsonify({'status': 'error', 'message': f'Недопустимый формат. Разрешены: {", ".join(sorted(_MAP_BG_ALLOWED))}'}), 400

    upload_root = current_app.config['UPLOAD_FOLDER']
    bg_dir = os.path.join(upload_root, _MAP_BG_DIR)
    try:
        os.makedirs(bg_dir, exist_ok=True)
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Не удалось создать каталог: {e}'}), 500

    # Используем timestamp в имени, чтобы сбить браузерный кэш у клиентов.
    from werkzeug.utils import secure_filename
    base = secure_filename(os.path.splitext(f.filename)[0]) or 'bg'
    fname = f"{base}_{int(datetime.utcnow().timestamp())}{ext}"
    rel_path = f"{_MAP_BG_DIR}/{fname}"
    abs_path = os.path.join(bg_dir, fname)
    try:
        f.save(abs_path)
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Ошибка сохранения: {e}'}), 500

    # Пытаемся определить натуральные размеры — нужны для корректного aspect-ratio канваса.
    width = height = None
    try:
        from PIL import Image
        with Image.open(abs_path) as im:
            width, height = im.size
    except Exception:
        # Pillow может быть не установлен — фронт в этом случае использует дефолтный aspect 16:9.
        pass

    s = MapSettings.get()
    old_path = s.background_path
    s.background_path = rel_path
    if width and height:
        s.bg_width = int(width)
        s.bg_height = int(height)
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

    # Старый файл больше не нужен — удаляем, чтобы не копить мусор.
    if old_path and old_path != rel_path:
        try:
            old_abs = os.path.join(upload_root, old_path)
            if os.path.isfile(old_abs):
                os.remove(old_abs)
        except Exception:
            pass

    return jsonify({'status': 'ok', 'settings': _map_settings_payload()})


@bp.route('/api/visual_stock/map_background/clear', methods=['POST'])
@login_required
def api_map_background_clear():
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Только для администратора'}), 403
    s = MapSettings.get()
    old_path = s.background_path
    s.background_path = None
    s.bg_width = None
    s.bg_height = None
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500
    if old_path:
        try:
            old_abs = os.path.join(current_app.config['UPLOAD_FOLDER'], old_path)
            if os.path.isfile(old_abs):
                os.remove(old_abs)
        except Exception:
            pass
    return jsonify({'status': 'ok', 'settings': _map_settings_payload()})


@bp.route('/api/visual_stock/map_background/transform', methods=['POST'])
@login_required
def api_map_background_transform():
    """Сохраняем параметры отображения фона: fit/offset/scale/rotation/opacity.

    Фото при этом не меняется — меняется только то, как оно показывается внутри канваса.
    Принимает любое подмножество полей; отсутствующие остаются как есть.
    """
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Только для администратора'}), 403
    data = request.get_json(silent=True) or {}

    def _num(v, lo, hi, default):
        try:
            x = float(v)
        except (TypeError, ValueError):
            return default
        if x != x:  # NaN
            return default
        return max(lo, min(hi, x))

    s = MapSettings.get()
    if 'fit' in data:
        fit = str(data.get('fit') or '').strip().lower()
        if fit not in _MAP_BG_FIT_ALLOWED:
            return jsonify({'status': 'error', 'message': f'fit должен быть одним из: {", ".join(sorted(_MAP_BG_FIT_ALLOWED))}'}), 400
        s.bg_fit = fit
    if 'offset_x' in data:
        s.bg_offset_x = _num(data.get('offset_x'), -200.0, 200.0, s.bg_offset_x or 0.0)
    if 'offset_y' in data:
        s.bg_offset_y = _num(data.get('offset_y'), -200.0, 200.0, s.bg_offset_y or 0.0)
    if 'scale' in data:
        s.bg_scale = _num(data.get('scale'), 0.1, 5.0, s.bg_scale or 1.0)
    if 'rotation' in data:
        s.bg_rotation = _num(data.get('rotation'), -180.0, 180.0, s.bg_rotation or 0.0)
    if 'opacity' in data:
        s.bg_opacity = _num(data.get('opacity'), 0.0, 1.0, s.bg_opacity or 1.0)

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception('map_background transform save failed')
        return jsonify({'status': 'error', 'message': str(e)}), 500
    return jsonify({'status': 'ok', 'settings': _map_settings_payload()})


@bp.route('/api/visual_stock/map_canvas', methods=['POST'])
@login_required
def api_map_canvas_save():
    """Параметры рабочей зоны карты: ширина канваса + соотношение сторон.
    Поля хранятся в процентах от канваса, поэтому при изменении его размеров
    их относительное положение не меняется — меняется только видимый масштаб.
    """
    if current_user.role != 'admin':
        return jsonify({'status': 'error', 'message': 'Только для администратора'}), 403
    data = request.get_json(silent=True) or {}
    s = MapSettings.get()
    if 'width' in data:
        try:
            w = int(float(data.get('width')))
        except (TypeError, ValueError):
            return jsonify({'status': 'error', 'message': 'width должно быть числом'}), 400
        # 600..6000 px — разумный диапазон для рабочей зоны.
        s.canvas_width = max(600, min(6000, w))
    if 'aspect' in data:
        asp = str(data.get('aspect') or '').strip().lower()
        if asp not in _MAP_CANVAS_ASPECTS:
            return jsonify({'status': 'error', 'message': f'aspect должен быть одним из: {", ".join(sorted(_MAP_CANVAS_ASPECTS))}'}), 400
        s.canvas_aspect = asp
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception('map_canvas save failed')
        return jsonify({'status': 'error', 'message': str(e)}), 500
    return jsonify({'status': 'ok', 'settings': _map_settings_payload()})