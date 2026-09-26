"""Еженедельный дайджест для admin/executive.

По понедельникам с 10:00 МСК — итоги **прошлой недели** (пн–вс).

План берётся оттуда же, откуда его смотрят в отчётах, а не из «средней недели»:
  • расходы — доля месячного бюджета (BudgetPlan) на дни этой недели;
  • поступления — факт недели; план cashflow показывается суммой месяца,
    а не разрезанной на неделю (в плане часто один крупный месяц, не темп);
  • выкопка — план календаря (DiggingTask) против факта (DiggingLog);
  • отгрузки и новые заказы — только факт: отдельного плана на них нет.

Порог `anomaly_ytd_threshold_pct` подсвечивает отклонение расходов от доли
бюджета и недобор выкопки относительно календаря.
"""

from __future__ import annotations

import calendar
import json
import os
import traceback
from datetime import datetime, timedelta, date

from flask import current_app
from sqlalchemy import func

from app.models import (
    db, User, TgTask, WeeklyDigest, AppSetting,
    Order, OrderItem, Payment, Expense,
    DiggingTask, DiggingLog, Document, DocumentRow,
    BudgetPlan, CashflowPlan,
)
from app.utils import msk_now, msk_today, MONTH_NAMES
from app.anomaly_engine import ANOMALY_ACTION_TYPE
from app.groq_util import groq_model_text


DIGEST_ACTION_TYPE = 'digest'
DIGEST_TARGET_ROLES = ('admin', 'executive')
DIGEST_TRIGGER_HOUR = 10
ANOMALY_THRESHOLD_KEY = 'anomaly_ytd_threshold_pct'
ANOMALY_THRESHOLD_DEFAULT = 30.0

_MIN_MONEY_BASELINE = 5_000.0
_MIN_DIG_PLAN = 10


def get_anomaly_threshold_pct():
    try:
        s = AppSetting.query.get(ANOMALY_THRESHOLD_KEY)
        if s and s.value is not None and str(s.value).strip() != '':
            return max(1.0, float(s.value))
    except Exception:
        pass
    return ANOMALY_THRESHOLD_DEFAULT


def set_anomaly_threshold_pct(value):
    try:
        v = float(value)
    except Exception:
        raise ValueError('Порог должен быть числом')
    if v < 1 or v > 200:
        raise ValueError('Порог должен быть в диапазоне 1..200 %')
    s = AppSetting.query.get(ANOMALY_THRESHOLD_KEY)
    if s is None:
        s = AppSetting(key=ANOMALY_THRESHOLD_KEY, value=str(round(v, 1)))
        db.session.add(s)
    else:
        s.value = str(round(v, 1))
    db.session.commit()
    return v


def _safe_float(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _money(v):
    try:
        return f'{float(v):,.0f} ₽'.replace(',', ' ')
    except Exception:
        return str(v)


def _week_monday(d):
    if isinstance(d, datetime):
        d = d.date()
    return d - timedelta(days=d.weekday())


def _last_week_bounds(today):
    this_monday = today - timedelta(days=today.weekday())
    last_sunday = this_monday - timedelta(days=1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def _delta_pct(cur, base):
    try:
        b = float(base)
    except Exception:
        return None
    if abs(b) < 1e-9:
        return None
    try:
        return round((float(cur) - b) * 100.0 / b, 1)
    except Exception:
        return None


def _pct_if_reliable(cur, base):
    if float(base or 0) < _MIN_MONEY_BASELINE:
        return None
    return _delta_pct(cur, base)


def _years_touched(monday, sunday):
    years = set()
    d = monday
    while d <= sunday:
        years.add(d.year)
        d += timedelta(days=1)
    return years


def _budget_by_month(years):
    if not years:
        return {}
    rows = db.session.query(
        BudgetPlan.year, BudgetPlan.month, func.sum(BudgetPlan.amount),
    ).filter(BudgetPlan.year.in_(years)).group_by(
        BudgetPlan.year, BudgetPlan.month,
    ).all()
    return {(int(y), int(m)): _safe_float(a) for y, m, a in rows}


def _cashflow_by_month(years):
    if not years:
        return {}
    rows = db.session.query(
        CashflowPlan.year, CashflowPlan.month, CashflowPlan.amount,
    ).filter(CashflowPlan.year.in_(years)).all()
    return {(int(y), int(m)): _safe_float(a) for y, m, a in rows}


def _week_budget_share(monday, sunday, plans):
    """Сколько месячного бюджета приходится на дни этой недели."""
    total = 0.0
    has_plan = False
    d = monday
    while d <= sunday:
        amount = plans.get((d.year, d.month), 0.0)
        if amount > 0:
            has_plan = True
            dim = calendar.monthrange(d.year, d.month)[1]
            total += amount / dim
        d += timedelta(days=1)
    return round(total, 2), has_plan


def _cashflow_month_progress(monday, sunday, plans):
    """План cashflow — сумма месяца из отчёта, плюс факт с 1-го по конец недели.

    Недельную «норму» из этой суммы не делаем: в плане часто один-два
    крупных месяца, и деление на 7 дней рисует несуществующий темп.
    """
    seen = []
    d = monday
    while d <= sunday:
        key = (d.year, d.month)
        if key not in seen:
            seen.append(key)
        d += timedelta(days=1)
    lines = []
    for year, month in seen:
        plan = plans.get((year, month), 0.0)
        if plan <= 0:
            continue
        start = date(year, month, 1)
        month_end = date(year, month, calendar.monthrange(year, month)[1])
        end = sunday if sunday < month_end else month_end
        fact = _safe_float(
            db.session.query(func.coalesce(func.sum(Payment.amount), 0))
            .filter(
                Payment.date >= start,
                Payment.date <= end,
                Payment.cash_inflow_filter(),
            ).scalar() or 0
        )
        lines.append({
            'year': year,
            'month': month,
            'label': f'{MONTH_NAMES.get(month, month)} {year}',
            'plan': round(plan, 2),
            'fact_to_date': round(fact, 2),
        })
    return lines


def _shipment_totals(start, end):
    """Отгрузки недели: одна цена на строку документа, без размножения джойном."""
    rows = db.session.query(
        Document.order_id,
        DocumentRow.plant_id,
        DocumentRow.size_id,
        DocumentRow.field_from_id,
        DocumentRow.year,
        DocumentRow.quantity,
    ).join(Document, DocumentRow.document_id == Document.id).filter(
        Document.doc_type == 'shipment',
        Document.order_id.isnot(None),
        func.date(Document.date) >= start,
        func.date(Document.date) <= end,
    ).all()
    if not rows:
        return 0.0, 0
    order_ids = {r.order_id for r in rows}
    items = OrderItem.query.filter(OrderItem.order_id.in_(order_ids)).all()
    exact = {}
    loose = {}
    for it in items:
        exact[(it.order_id, it.plant_id, it.size_id, it.field_id, it.year)] = it
        loose.setdefault((it.order_id, it.plant_id, it.size_id, it.field_id), it)
    money = 0.0
    qty = 0
    for r in rows:
        it = exact.get((r.order_id, r.plant_id, r.size_id, r.field_from_id, r.year))
        if it is None:
            it = loose.get((r.order_id, r.plant_id, r.size_id, r.field_from_id))
        q = int(_safe_float(r.quantity))
        price = _safe_float(it.price) if it is not None else 0.0
        qty += q
        money += price * q
    return round(money, 2), qty


def _collect_week_metrics(monday, sunday):
    cash_in = _safe_float(
        db.session.query(func.coalesce(func.sum(Payment.amount), 0))
        .filter(
            Payment.date >= monday,
            Payment.date <= sunday,
            Payment.cash_inflow_filter(),
        ).scalar() or 0
    )
    expenses = _safe_float(
        db.session.query(func.coalesce(func.sum(Expense.amount), 0))
        .filter(Expense.date >= monday, Expense.date <= sunday).scalar() or 0
    )

    ship_money, ship_qty = _shipment_totals(monday, sunday)

    new_orders_q = Order.query.filter(
        Order.is_deleted.is_(False),
        Order.status != 'canceled',
        func.date(Order.date) >= monday,
        func.date(Order.date) <= sunday,
    ).all()
    new_orders_count = len(new_orders_q)
    new_orders_total = sum(_safe_float(o.total_sum) for o in new_orders_q)

    planned = int(db.session.query(func.coalesce(func.sum(DiggingTask.planned_qty), 0))
                  .join(OrderItem, DiggingTask.order_item_id == OrderItem.id)
                  .join(Order, OrderItem.order_id == Order.id)
                  .filter(
                      DiggingTask.planned_date >= monday,
                      DiggingTask.planned_date <= sunday,
                      Order.is_deleted.is_(False),
                      Order.status != 'canceled',
                  ).scalar() or 0)
    dig_fact = int(db.session.query(func.coalesce(func.sum(DiggingLog.quantity), 0))
                   .filter(DiggingLog.date >= monday,
                           DiggingLog.date <= sunday,
                           DiggingLog.status != 'rejected').scalar() or 0)

    years = _years_touched(monday, sunday)
    expense_plan, expense_plan_set = _week_budget_share(monday, sunday, _budget_by_month(years))
    cash_months = _cashflow_month_progress(monday, sunday, _cashflow_by_month(years))

    return {
        'monday': monday.isoformat(),
        'sunday': sunday.isoformat(),
        'cash_in': round(cash_in, 2),
        'expenses': round(expenses, 2),
        'expense_plan': expense_plan,
        'expense_plan_set': expense_plan_set,
        'cash_months': cash_months,
        'ship_money': round(ship_money, 2),
        'ship_qty': ship_qty,
        'new_orders_count': new_orders_count,
        'new_orders_total': round(new_orders_total, 2),
        'digging_planned': planned,
        'digging_fact': dig_fact,
        'digging_diff': dig_fact - planned,
    }


def _compute_plan_gaps(metrics, threshold_pct):
    """Только там, где план в программе реально задан: бюджет и календарь выкопки."""
    gaps = []
    exp = float(metrics.get('expenses') or 0)
    exp_plan = float(metrics.get('expense_plan') or 0)
    if metrics.get('expense_plan_set'):
        pct = _pct_if_reliable(exp, exp_plan)
        if pct is not None and abs(pct) >= threshold_pct:
            gaps.append({
                'key': 'expenses',
                'label': 'Расходы',
                'current': exp,
                'baseline': exp_plan,
                'delta_pct': pct,
                'direction': 'down' if pct < 0 else 'up',
                'is_negative': pct > 0,
                'note': 'к доле бюджета на эти дни',
            })

    planned = int(metrics.get('digging_planned') or 0)
    fact = int(metrics.get('digging_fact') or 0)
    if planned >= _MIN_DIG_PLAN:
        pct = _delta_pct(fact, planned)
        if pct is not None and pct <= -threshold_pct:
            gaps.append({
                'key': 'digging',
                'label': 'Выкопка',
                'current': fact,
                'baseline': planned,
                'delta_pct': pct,
                'direction': 'down',
                'is_negative': True,
                'note': 'к плану календаря',
            })
    return gaps


def _collect_anomaly_flow(monday, sunday):
    week_start_dt = datetime.combine(monday, datetime.min.time())
    week_end_dt = datetime.combine(sunday, datetime.max.time())

    new_list = TgTask.query.filter(
        TgTask.action_type == ANOMALY_ACTION_TYPE,
        TgTask.first_seen_at.isnot(None),
        TgTask.first_seen_at >= week_start_dt,
        TgTask.first_seen_at <= week_end_dt,
    ).all()

    ongoing_list = TgTask.query.filter(
        TgTask.action_type == ANOMALY_ACTION_TYPE,
        TgTask.status == 'new',
        TgTask.first_seen_at < week_start_dt,
    ).all()

    resolved_list = TgTask.query.filter(
        TgTask.action_type == ANOMALY_ACTION_TYPE,
        TgTask.status == 'done',
        TgTask.completed_at.isnot(None),
        TgTask.completed_at >= week_start_dt,
        TgTask.completed_at <= week_end_dt,
    ).all()

    def _pack(card):
        return {
            'id': card.id,
            'kind': card.dedup_key.split(':')[0] if card.dedup_key else (card.action_type or '—'),
            'title': card.title,
            'severity': card.severity or 'info',
            'days_active': (
                (msk_now() - card.first_seen_at).days
                if card.first_seen_at else 0
            ),
        }

    return {
        'new': [_pack(c) for c in new_list],
        'ongoing': [_pack(c) for c in ongoing_list],
        'resolved': [_pack(c) for c in resolved_list],
    }



def _unique_cards(cards):
    seen = set()
    out = []
    for card in cards or []:
        title = (card.get('title') or '').strip()
        if title in seen:
            continue
        seen.add(title)
        out.append(card)
    return out


def _fmt_qty(x):
    try:
        return f'{int(round(float(x)))}'
    except Exception:
        return str(x)


def _render_template_digest(metrics, deviations, anomalies, last_digest_at, threshold_pct):
    def _delta(pct, bad):
        if pct is None:
            return '<span class="text-muted">—</span>'
        color = 'text-danger fw-bold' if bad else ('text-success' if pct != 0 else 'text-muted')
        arrow = '▲' if pct > 0 else ('▼' if pct < 0 else '•')
        return f'<span class="{color}">{arrow} {pct:+.0f}%</span>'

    def _tr(label, fact, plan, delta_html, warn=False):
        cls = ' class="table-warning"' if warn else ''
        return (
            f'<tr{cls}>'
            f'<td>{label}</td>'
            f'<td class="text-end fw-semibold">{fact}</td>'
            f'<td class="text-end text-muted">{plan}</td>'
            f'<td class="text-end">{delta_html}</td>'
            f'</tr>'
        )

    try:
        md = date.fromisoformat(metrics['monday'])
        sd = date.fromisoformat(metrics['sunday'])
        period_str = f'{md.strftime("%d.%m")} – {sd.strftime("%d.%m.%Y")}'
    except Exception:
        period_str = f'{metrics.get("monday")} – {metrics.get("sunday")}'

    last_line = ''
    if last_digest_at:
        last_line = (
            f'<div class="small text-muted mb-2">Предыдущий дайджест: '
            f'{last_digest_at.strftime("%d.%m.%Y %H:%M")}</div>'
        )

    cash = float(metrics.get('cash_in') or 0)
    exp = float(metrics.get('expenses') or 0)
    exp_plan = float(metrics.get('expense_plan') or 0)
    exp_set = bool(metrics.get('expense_plan_set'))
    exp_pct = _pct_if_reliable(exp, exp_plan) if exp_set else None
    exp_bad = bool(exp_pct is not None and exp_pct > 0 and abs(exp_pct) >= threshold_pct)

    planned = int(metrics.get('digging_planned') or 0)
    dug = int(metrics.get('digging_fact') or 0)
    dig_pct = _delta_pct(dug, planned) if planned >= _MIN_DIG_PLAN else None
    dig_bad = bool(dig_pct is not None and dig_pct < 0 and abs(dig_pct) >= threshold_pct)
    if planned > 0:
        dig_plan_cell = f'{planned} шт'
        diff = dug - planned
        color = 'text-danger fw-bold' if dig_bad else ('text-success' if diff > 0 else 'text-muted')
        dig_delta = f'<span class="{color}">{diff:+d} шт</span>'
    else:
        dig_plan_cell = 'не ставился'
        dig_delta = '<span class="text-muted">—</span>'

    ship = (
        f'{_money(metrics.get("ship_money") or 0)}'
        f' · {_fmt_qty(metrics.get("ship_qty") or 0)} шт'
    )
    orders = (
        f'{_fmt_qty(metrics.get("new_orders_count") or 0)}'
        f' · {_money(metrics.get("new_orders_total") or 0)}'
    )

    parts = ['<div class="digest-body">']
    parts.append(f'<div class="fw-bold mb-1">Неделя {period_str}</div>')
    parts.append(
        '<div class="small text-muted mb-2">'
        'Расходы сравниваются с долей месячного бюджета на эти дни. '
        'Выкопка — с планом календаря. '
        'Поступления: факт недели; план cashflow, если он задан, показан суммой месяца, '
        'а не «нормой на неделю». '
        f'Подсветка при отклонении от {threshold_pct:.0f}%.'
        '</div>'
    )
    parts.append(last_line)
    parts.append(
        '<table class="table table-sm table-borderless mb-2 digest-metrics-table" '
        'style="font-size:12.5px;">'
    )
    parts.append(
        '<thead><tr class="text-muted small">'
        '<th>Показатель</th>'
        '<th class="text-end">Факт</th>'
        '<th class="text-end">План</th>'
        '<th class="text-end">Δ</th>'
        '</tr></thead><tbody>'
    )
    parts.append(_tr('Поступления', _money(cash), '—', '<span class="text-muted">—</span>'))
    if exp_set:
        parts.append(_tr(
            'Расходы', _money(exp), _money(exp_plan), _delta(exp_pct, exp_bad), exp_bad,
        ))
    else:
        parts.append(_tr(
            'Расходы', _money(exp), 'бюджет не задан', '<span class="text-muted">—</span>',
        ))
    parts.append(_tr(
        'Выкопка, шт', f'{dug} шт', dig_plan_cell, dig_delta, dig_bad,
    ))
    parts.append(_tr('Отгрузки', ship, '—', '<span class="text-muted">—</span>'))
    parts.append(_tr('Новые заказы', orders, '—', '<span class="text-muted">—</span>'))
    parts.append('</tbody></table>')

    for row in metrics.get('cash_months') or []:
        parts.append(
            '<div class="small text-muted mb-1">'
            f'План cashflow на {row["label"]}: <b>{_money(row["plan"])}</b>, '
            f'с начала месяца пришло <b>{_money(row["fact_to_date"])}</b>.'
            '</div>'
        )

    bad = [a for a in deviations if a.get('is_negative')]
    good = [a for a in deviations if not a.get('is_negative')]
    if bad:
        parts.append('<ul class="small mb-2 text-danger">')
        for a in bad:
            parts.append(
                f'<li><b>{a["label"]}</b>: {a["delta_pct"]:+.0f}% {a.get("note") or ""}</li>'
            )
        parts.append('</ul>')
    if good:
        parts.append(
            '<div class="small text-success mb-2">'
            + ', '.join(
                f'{a["label"]} {a["delta_pct"]:+.0f}% {a.get("note") or ""}'.strip()
                for a in good
            )
            + '</div>'
        )
    if not bad and not good:
        parts.append(
            '<div class="small text-muted mb-2">'
            'К бюджету и календарю выкопки заметных отклонений нет.'
            '</div>'
        )

    new_cards = _unique_cards(anomalies.get('new'))
    ongoing_cards = _unique_cards(anomalies.get('ongoing'))
    resolved_cards = _unique_cards(anomalies.get('resolved'))
    if new_cards or ongoing_cards or resolved_cards:
        parts.append(
            f'<div class="small mb-1"><b>Карточки:</b> '
            f'<span class="text-danger">+{len(new_cards)}</span> · '
            f'<span class="text-warning">{len(ongoing_cards)} открыты</span> · '
            f'<span class="text-success">{len(resolved_cards)} закрыто</span></div>'
        )
        if ongoing_cards:
            parts.append('<ul class="small mb-2">')
            for a in ongoing_cards[:5]:
                parts.append(
                    f'<li>{a["title"]} '
                    f'<span class="text-muted">({a["days_active"]} дн)</span></li>'
                )
            parts.append('</ul>')

    parts.append(
        '<div class="small"><a href="/dashboard/anomalies" '
        'class="btn btn-sm btn-outline-primary">История дайджестов</a></div>'
    )
    parts.append('</div>')
    return '\n'.join(parts)


def _render_llm_intro(metrics, deviations):
    api_key = os.environ.get('GROQ_API_KEY')
    if not api_key:
        return ''
    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        brief = {
            'поступления': metrics.get('cash_in'),
            'расходы': metrics.get('expenses'),
            'доля_бюджета': metrics.get('expense_plan'),
            'выкопка_факт': metrics.get('digging_fact'),
            'выкопка_план': metrics.get('digging_planned'),
            'отгрузки': metrics.get('ship_money'),
            'новые_заказы': metrics.get('new_orders_total'),
            'cashflow_месяц': metrics.get('cash_months'),
        }
        prompt = (
            'Ты — аналитик питомника. Одно-два предложения на русском по итогам недели. '
            'План расходов — доля месячного бюджета, план выкопки — календарь. '
            'Не выдумывай годовую норму и не дели план cashflow на неделю. Без вступлений.\n\n'
            f'Цифры: {json.dumps(brief, ensure_ascii=False, default=str)}\n'
            f'Отклонения: {json.dumps(deviations, ensure_ascii=False, default=str)}'
        )
        resp = client.chat.completions.create(
            model=groq_model_text(),
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.2,
        )
        text = (resp.choices[0].message.content or '').strip()
        if not text:
            return ''
        return f'<div class="alert alert-light border mb-2 small">{text}</div>'
    except Exception:
        traceback.print_exc()
        return ''


def build_digest_for_user(user, today):
    this_monday = today - timedelta(days=today.weekday())

    existing = WeeklyDigest.query.filter_by(
        week_start=this_monday, user_id=user.id,
    ).first()
    if existing:
        return existing

    monday, sunday = _last_week_bounds(today)
    threshold_pct = get_anomaly_threshold_pct()
    metrics = _collect_week_metrics(monday, sunday)
    deviations = _compute_plan_gaps(metrics, threshold_pct)
    anomalies = _collect_anomaly_flow(monday, sunday)

    last_digest = (
        WeeklyDigest.query.filter_by(user_id=user.id)
        .order_by(WeeklyDigest.created_at.desc())
        .first()
    )
    last_digest_at = last_digest.created_at if last_digest else None

    intro_html = _render_llm_intro(metrics, deviations)
    table_html = _render_template_digest(
        metrics, deviations, anomalies, last_digest_at, threshold_pct
    )
    content_html = (intro_html or '') + table_html

    digest = WeeklyDigest(
        week_start=this_monday,
        user_id=user.id,
        content_html=content_html,
        summary_json=json.dumps({
            'metrics': metrics,
            'deviations': deviations,
            'anomalies': anomalies,
            'threshold_pct': threshold_pct,
        }, ensure_ascii=False, default=str),
    )
    db.session.add(digest)

    n_bad = sum(1 for a in deviations if a.get('is_negative'))
    severity = 'warning' if n_bad else 'info'
    title = f'Дайджест {monday.strftime("%d.%m")}–{sunday.strftime("%d.%m")}'
    if n_bad:
        title += f' · {n_bad} к плану'

    dedup_key = f'digest:{this_monday.isoformat()}:user={user.id}'
    if TgTask.query.filter_by(dedup_key=dedup_key).first() is None:
        db.session.add(TgTask(
            raw_text='[weekly_digest]',
            title=title,
            details=content_html,
            action_type=DIGEST_ACTION_TYPE,
            action_payload=json.dumps({'week_start': this_monday.isoformat()}),
            status='new',
            assignee_id=user.id,
            first_seen_at=msk_now(),
            last_seen_at=msk_now(),
            dedup_key=dedup_key,
            severity=severity,
            sender_name='system.digest',
            source='digest',
        ))

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception('build_digest_for_user commit failed')
        return WeeklyDigest.query.filter_by(
            week_start=this_monday, user_id=user.id,
        ).first()
    return digest


def ensure_weekly_digest():
    now = msk_now()
    today = now.date() if hasattr(now, 'date') else msk_today()
    if today.weekday() != 0 or now.hour < DIGEST_TRIGGER_HOUR:
        return 0
    created = 0
    for u in User.query.filter(User.role.in_(DIGEST_TARGET_ROLES)).all():
        if build_digest_for_user(u, today):
            created += 1
    return created
