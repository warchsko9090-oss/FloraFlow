"""Еженедельный дайджест для admin/executive.

По понедельникам с 10:00 МСК — итоги **прошлой недели** (пн–вс).

Сравнение **сезонное** (данные с 2023 года, питомник март–декабрь):
  • в сезоне — «ожидание» = медиана **той же недели сезона** в прошлых годах
    (неделя 1 сезона = первая пн–вс, начиная с 1 марта);
  • зимой (янв–фев) — отгрузки/заказы/выкопка не сравниваются;
    поступления и расходы — vs та же ISO-неделя прошлых зим.

Карточка на дашборде — каждую неделю. Порог `anomaly_ytd_threshold_pct`
только для подсветки существенных отклонений.
"""

from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timedelta, date

from flask import current_app
from sqlalchemy import func, and_

from app.models import (
    db, User, TgTask, WeeklyDigest, AppSetting,
    Order, OrderItem, Payment, Expense,
    DiggingTask, DiggingLog, Document, DocumentRow,
)
from app.utils import msk_now, msk_today
from app.anomaly_engine import ANOMALY_ACTION_TYPE


DIGEST_ACTION_TYPE = 'digest'
DIGEST_TARGET_ROLES = ('admin', 'executive')
DIGEST_TRIGGER_HOUR = 10
ANOMALY_THRESHOLD_KEY = 'anomaly_ytd_threshold_pct'
ANOMALY_THRESHOLD_DEFAULT = 30.0

HISTORY_FIRST_YEAR = 2023
SEASON_START_MONTH = 3
SEASON_END_MONTH = 12

_MIN_MONEY_BASELINE = 5_000.0
_MIN_COUNT_BASELINE = 0.5

_BAD_WHEN_UP = {'expenses'}

_WINTER_SKIP_METRICS = frozenset({
    'ship_money', 'ship_qty',
    'new_orders_count', 'new_orders_total',
    'digging_fact',
})

_ANOMALY_METRICS = [
    ('cash_in', 'Поступления', 'money'),
    ('expenses', 'Расходы', 'money'),
    ('ship_money', 'Отгрузки, ₽', 'money'),
    ('ship_qty', 'Отгрузки, шт', 'count'),
    ('new_orders_count', 'Новые заказы, шт', 'count'),
    ('new_orders_total', 'Новые заказы, ₽', 'money'),
    ('digging_fact', 'Выкопка, шт', 'count'),
]


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


def _median(values):
    if not values:
        return 0.0
    s = sorted(float(v) for v in values)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def _pct_if_reliable(cur, base, kind):
    if kind == 'money' and float(base or 0) < _MIN_MONEY_BASELINE:
        return None
    if kind == 'count' and float(base or 0) < _MIN_COUNT_BASELINE:
        return None
    return _delta_pct(cur, base)


def _week_in_season(monday):
    return SEASON_START_MONTH <= monday.month <= SEASON_END_MONTH


def _season_anchor_monday(year):
    """Первый понедельник на или после 1 марта."""
    d = date(year, SEASON_START_MONTH, 1)
    while d.weekday() != 0:
        d += timedelta(days=1)
    return d


def _season_week_index(monday):
    """Номер недели сезона (0-based) от якоря 1 марта."""
    if not _week_in_season(monday):
        return None
    anchor = _season_anchor_monday(monday.year)
    if monday < anchor:
        return None
    return (monday - anchor).days // 7


def _season_week_monday(year, week_index):
    return _season_anchor_monday(year) + timedelta(weeks=week_index)


def _iso_week_monday_in_year(calendar_year, iso_week):
    """Понедельник ISO-недели `iso_week` в календарном году (для зимы)."""
    try:
        return date.fromisocalendar(calendar_year, iso_week, 1)
    except ValueError:
        for w in (iso_week - 1, 52, 53):
            if w < 1:
                continue
            try:
                return date.fromisocalendar(calendar_year, w, 1)
            except ValueError:
                continue
    return None


def _shipment_rows(start, end):
    return db.session.query(
        func.date(Document.date).label('d'),
        (OrderItem.price * DocumentRow.quantity).label('money'),
        DocumentRow.quantity.label('qty'),
    ).select_from(Document).join(
        Order, Document.order_id == Order.id
    ).join(
        DocumentRow, DocumentRow.document_id == Document.id
    ).join(
        OrderItem, and_(
            OrderItem.order_id == Document.order_id,
            OrderItem.plant_id == DocumentRow.plant_id,
            OrderItem.size_id == DocumentRow.size_id,
            OrderItem.field_id == DocumentRow.field_from_id,
        )
    ).filter(
        Document.doc_type == 'shipment',
        func.date(Document.date) >= start,
        func.date(Document.date) <= end,
    ).all()


def _collect_week_metrics(monday, sunday):
    cash_in = _safe_float(
        db.session.query(func.coalesce(func.sum(Payment.amount), 0))
        .filter(Payment.date >= monday, Payment.date <= sunday).scalar() or 0
    )
    expenses = _safe_float(
        db.session.query(func.coalesce(func.sum(Expense.amount), 0))
        .filter(Expense.date >= monday, Expense.date <= sunday).scalar() or 0
    )

    ship_money = 0.0
    ship_qty = 0
    for row in _shipment_rows(monday, sunday):
        ship_money += _safe_float(row.money)
        ship_qty += int(_safe_float(row.qty))

    new_orders_q = Order.query.filter(
        Order.is_deleted.is_(False),
        Order.status != 'canceled',
        func.date(Order.date) >= monday,
        func.date(Order.date) <= sunday,
    ).all()
    new_orders_count = len(new_orders_q)
    new_orders_total = sum(_safe_float(o.total_sum) for o in new_orders_q)

    planned = int(db.session.query(func.coalesce(func.sum(DiggingTask.planned_qty), 0))
                  .filter(DiggingTask.planned_date >= monday,
                          DiggingTask.planned_date <= sunday).scalar() or 0)
    dig_fact = int(db.session.query(func.coalesce(func.sum(DiggingLog.quantity), 0))
                   .filter(DiggingLog.date >= monday,
                           DiggingLog.date <= sunday,
                           DiggingLog.status != 'rejected').scalar() or 0)

    return {
        'monday': monday.isoformat(),
        'sunday': sunday.isoformat(),
        'cash_in': round(cash_in, 2),
        'expenses': round(expenses, 2),
        'ship_money': round(ship_money, 2),
        'ship_qty': ship_qty,
        'new_orders_count': new_orders_count,
        'new_orders_total': round(new_orders_total, 2),
        'digging_planned': planned,
        'digging_fact': dig_fact,
        'digging_diff': dig_fact - planned,
    }


def _collect_seasonal_baseline(report_monday):
    """Ожидание на неделю = медиана той же недели сезона / ISO-недели в 2023…прошлый год."""
    report_year = report_monday.year
    in_season = _week_in_season(report_monday)
    years_pool = [y for y in range(HISTORY_FIRST_YEAR, report_year)]

    metric_keys = [k for k, _, _ in _ANOMALY_METRICS] + ['digging_planned']
    historical = {k: [] for k in metric_keys}
    skip_compare = set()
    used_years = []

    meta = {
        'baseline_type': 'seasonal_median',
        'history_first_year': HISTORY_FIRST_YEAR,
        'in_season': in_season,
        'winter_mode': not in_season,
        'comparison_years': [],
        'season_week_human': None,
        'iso_week': None,
        'comparison_label': '',
    }

    if not years_pool:
        baseline = {k: 0 for k in metric_keys}
        baseline['_meta'] = meta
        baseline['_skip_compare'] = list(_WINTER_SKIP_METRICS) if not in_season else []
        return baseline

    if in_season:
        swi = _season_week_index(report_monday)
        meta['season_week_human'] = (swi + 1) if swi is not None else None
        if swi is not None:
            for y in years_pool:
                mon = _season_week_monday(y, swi)
                if mon.month > SEASON_END_MONTH:
                    continue
                sun = mon + timedelta(days=6)
                m = _collect_week_metrics(mon, sun)
                for k in metric_keys:
                    historical[k].append(m.get(k, 0))
                used_years.append(y)
        years_label = ', '.join(str(y) for y in used_years) or '—'
        meta['comparison_label'] = (
            f'медиана недели {meta["season_week_human"]} сезона '
            f'({years_label})'
        )
    else:
        skip_compare = set(_WINTER_SKIP_METRICS)
        iso_week = report_monday.isocalendar()[1]
        meta['iso_week'] = iso_week
        for y in years_pool:
            mon = _iso_week_monday_in_year(y, iso_week)
            if not mon:
                continue
            sun = mon + timedelta(days=6)
            m = _collect_week_metrics(mon, sun)
            for k in ('cash_in', 'expenses'):
                historical[k].append(m.get(k, 0))
            used_years.append(y)
        years_label = ', '.join(str(y) for y in used_years) or '—'
        meta['comparison_label'] = (
            f'медиана ISO-нед. {iso_week} зимы ({years_label}); '
            f'отгрузки и заказы — вне сезона'
        )

    meta['comparison_years'] = used_years

    baseline = {}
    for k in metric_keys:
        if k in skip_compare:
            baseline[k] = 0
        elif historical[k]:
            val = _median(historical[k])
            baseline[k] = round(val, 2) if k not in ('ship_qty', 'new_orders_count', 'digging_fact', 'digging_planned') else round(val, 1)
        else:
            baseline[k] = 0

    baseline['_meta'] = meta
    baseline['_skip_compare'] = list(skip_compare)
    return baseline


def _compute_anomalies_vs_baseline(metrics, baseline, threshold_pct):
    skip = set(baseline.get('_skip_compare') or [])
    anomalies = []
    for key, label, kind in _ANOMALY_METRICS:
        if key in skip:
            continue
        cur = float(metrics.get(key, 0) or 0)
        base = float(baseline.get(key, 0) or 0)
        pct = _pct_if_reliable(cur, base, kind)
        if pct is None or abs(pct) < threshold_pct:
            continue
        bad_when_up = key in _BAD_WHEN_UP
        is_negative = (pct > 0) if bad_when_up else (pct < 0)
        anomalies.append({
            'key': key,
            'label': label,
            'current': cur,
            'baseline': base,
            'delta_pct': pct,
            'direction': 'down' if pct < 0 else 'up',
            'severity': 'warning' if is_negative else 'info',
            'is_negative': is_negative,
        })
    return anomalies


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


def _render_template_digest(metrics, baseline, deviations, anomalies,
                            last_digest_at, threshold_pct):
    skip = set(baseline.get('_skip_compare') or [])

    def _fmt_qty(x):
        try:
            return f'{int(round(float(x)))}'
        except Exception:
            return str(x)

    def _row(label, key, kind):
        cur = float(metrics.get(key, 0) or 0)
        if key in skip:
            fmt = _money if kind == 'money' else _fmt_qty
            return (
                f'<tr class="text-muted">'
                f'<td>{label}</td>'
                f'<td class="text-end fw-semibold">{fmt(cur)}</td>'
                f'<td class="text-end" colspan="2"><span class="small">вне сезона</span></td>'
                f'</tr>'
            )
        base = float(baseline.get(key, 0) or 0)
        pct = _pct_if_reliable(cur, base, kind)
        delta_html = ''
        is_warn = False
        if pct is not None:
            bad_when_up = key in _BAD_WHEN_UP
            is_bad = (pct > 0) if bad_when_up else (pct < 0)
            is_warn = is_bad and abs(pct) >= threshold_pct
            color = 'text-danger fw-bold' if is_warn else ('text-muted' if is_bad else 'text-success')
            arrow = '▲' if pct > 0 else ('▼' if pct < 0 else '•')
            delta_html = f'<span class="{color} ms-1">{arrow} {pct:+.0f}%</span>'
        elif base > 0:
            delta_html = '<span class="text-muted ms-1">—</span>'
        row_cls = ' class="table-warning"' if is_warn else ''
        fmt = _money if kind == 'money' else _fmt_qty
        return (
            f'<tr{row_cls}>'
            f'<td>{label}</td>'
            f'<td class="text-end fw-semibold">{fmt(cur)}</td>'
            f'<td class="text-end text-muted">{fmt(base)}</td>'
            f'<td class="text-end">{delta_html}</td>'
            f'</tr>'
        )

    try:
        md = date.fromisoformat(metrics['monday'])
        sd = date.fromisoformat(metrics['sunday'])
        period_str = f'{md.strftime("%d.%m")} – {sd.strftime("%d.%m.%Y")}'
    except Exception:
        period_str = f'{metrics.get("monday")} – {metrics.get("sunday")}'

    meta = baseline.get('_meta') or {}
    cmp_label = meta.get('comparison_label') or 'нет данных за прошлые годы'

    last_line = ''
    if last_digest_at:
        last_line = (
            f'<div class="small text-muted mb-2">Предыдущий дайджест: '
            f'{last_digest_at.strftime("%d.%m.%Y %H:%M")}</div>'
        )

    planned = int(metrics.get('digging_planned') or 0)
    dug = int(metrics.get('digging_fact') or 0)
    dig_diff = dug - planned
    if planned > 0:
        dig_pct = round(dug * 100.0 / planned)
        dig_plan_line = (
            f'<div class="small text-muted mb-2">'
            f'Выкопка за неделю: план <b>{planned}</b> шт · факт <b>{dug}</b> шт '
            f'(<span class="{"text-success" if dig_diff >= 0 else "text-danger"}">'
            f'{dig_diff:+d} шт, {dig_pct}% плана</span>)'
            f'</div>'
        )
    elif dug > 0:
        dig_plan_line = (
            f'<div class="small text-muted mb-2">'
            f'Выкопка за неделю: <b>{dug}</b> шт (план не ставился)'
            f'</div>'
        )
    else:
        dig_plan_line = ''

    winter_note = ''
    if meta.get('winter_mode'):
        winter_note = (
            '<div class="small alert alert-light border py-1 px-2 mb-2">'
            '<i class="fas fa-snowflake me-1 text-muted"></i> '
            'Зимний период: отгрузки и заказы не сравниваем с прошлыми годами.'
            '</div>'
        )

    parts = ['<div class="digest-body">']
    parts.append(f'<div class="fw-bold mb-1">Неделя {period_str}</div>')
    parts.append(
        f'<div class="small text-muted mb-2">'
        f'<b>Ожидание</b> — {cmp_label} (с {HISTORY_FIRST_YEAR} г.). '
        f'Сезон: март–декабрь. Подсветка при Δ ≥ {threshold_pct:.0f}%.'
        f'</div>'
    )
    parts.append(winter_note)
    parts.append(last_line)
    parts.append(dig_plan_line)

    parts.append(
        '<table class="table table-sm table-borderless mb-2 digest-metrics-table" '
        'style="font-size:12.5px;">'
    )
    parts.append(
        '<thead><tr class="text-muted small">'
        '<th>Показатель</th>'
        '<th class="text-end">Неделя</th>'
        '<th class="text-end">Ожидание</th>'
        '<th class="text-end">Δ</th>'
        '</tr></thead><tbody>'
    )
    for key, label, kind in _ANOMALY_METRICS:
        parts.append(_row(label, key, kind))
    parts.append('</tbody></table>')

    bad = [a for a in deviations if a.get('is_negative')]
    good = [a for a in deviations if not a.get('is_negative')]
    if bad:
        parts.append(
            f'<div class="small mb-1 text-danger fw-bold">'
            f'Ниже ожидания ({len(bad)}):</div><ul class="small mb-2">'
        )
        for a in bad:
            parts.append(
                f'<li><b>{a["label"]}</b>: {a["delta_pct"]:+.0f}% к прошлым годам</li>'
            )
        parts.append('</ul>')
    if good:
        parts.append(
            '<div class="small text-success mb-2">Выше ожидания: '
            + ', '.join(f'{a["label"]} ({a["delta_pct"]:+.0f}%)' for a in good)
            + '</div>'
        )
    if not bad and not good and not meta.get('winter_mode'):
        parts.append(
            '<div class="small text-muted mb-2">В пределах обычного для этой недели сезона.</div>'
        )

    new_n = len(anomalies.get('new') or [])
    ongoing_n = len(anomalies.get('ongoing') or [])
    resolved_n = len(anomalies.get('resolved') or [])
    if new_n or ongoing_n or resolved_n:
        parts.append(
            f'<div class="small mb-1"><b>Карточки-аномалии:</b> '
            f'<span class="text-danger">+{new_n}</span> · '
            f'<span class="text-warning">{ongoing_n} открыты</span> · '
            f'<span class="text-success">{resolved_n} закрыто</span></div>'
        )
        if ongoing_n:
            parts.append('<ul class="small mb-2">')
            for a in (anomalies.get('ongoing') or [])[:5]:
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


def _render_llm_intro(metrics, baseline, deviations, anomalies, threshold_pct):
    api_key = os.environ.get('GROQ_API_KEY')
    if not api_key:
        return ''
    try:
        from groq import Groq
        client = Groq(api_key=api_key)
        baseline_clean = {k: v for k, v in baseline.items() if not str(k).startswith('_')}
        meta = baseline.get('_meta') or {}
        prompt = (
            'Ты — аналитик питомника (сезон март–декабрь). Одно-два предложения на русском: '
            'как прошла неделя vs ожидание по прошлым годам (с 2023). '
            f'Контекст: {meta.get("comparison_label", "")}. '
            'Без вступлений.\n\n'
            f'Неделя: {json.dumps(metrics, ensure_ascii=False, default=str)}\n'
            f'Ожидание: {json.dumps(baseline_clean, ensure_ascii=False, default=str)}\n'
            f'Отклонения: {json.dumps(deviations, ensure_ascii=False, default=str)}'
        )
        resp = client.chat.completions.create(
            model='llama-3.3-70b-versatile',
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
    baseline = _collect_seasonal_baseline(monday)
    deviations = _compute_anomalies_vs_baseline(metrics, baseline, threshold_pct)
    anomalies = _collect_anomaly_flow(monday, sunday)

    last_digest = (
        WeeklyDigest.query.filter_by(user_id=user.id)
        .order_by(WeeklyDigest.created_at.desc())
        .first()
    )
    last_digest_at = last_digest.created_at if last_digest else None

    intro_html = _render_llm_intro(metrics, baseline, deviations, anomalies, threshold_pct)
    table_html = _render_template_digest(
        metrics, baseline, deviations, anomalies, last_digest_at, threshold_pct
    )
    content_html = (intro_html or '') + table_html

    digest = WeeklyDigest(
        week_start=this_monday,
        user_id=user.id,
        content_html=content_html,
        summary_json=json.dumps({
            'metrics': metrics,
            'baseline': baseline,
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
        title += f' · {n_bad} ниже ожидания'

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
