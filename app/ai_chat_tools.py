"""Чат-бот с tool-calling: отвечает на вопросы о состоянии базы, вызывая
конкретные read-only функции. Каждая функция возвращает не только данные,
но и ссылку на источник (раздел ERP), чтобы ответ был проверяемым.

Оркестратор (`process_chat_query`) использует Groq в режиме function calling:
модель сама выбирает нужный инструмент, получает JSON-ответ, а затем пишет
финальный короткий HTML-ответ пользователю с кнопкой «Перейти в [отчёт]».
"""

from __future__ import annotations

import os
import json
import traceback
from calendar import monthrange
from datetime import date, datetime, timedelta

from sqlalchemy import func, and_, or_

from app.models import (
    db,
    Client, Plant, Size, Field,
    StockBalance, Order, OrderItem, Payment,
    Document, DocumentRow,
    Expense, BudgetItem,
    Employee, TimeLog,
    PatentPeriod,
    DiggingLog,
    TgTask, User,
)
from app.utils import msk_today


# --------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ
# --------------------------------------------------------------------------

MAX_CANDIDATES = 8  # сколько кандидатов возвращать при неоднозначности


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


def _parse_period(month=None, year=None, date_from=None, date_to=None):
    """Унифицированный парсер периода, возвращает (first_date, last_date)
    или (None, None) если период не задан.

    Поддерживает:
      - month + year ("апрель 2026")
      - year ("за 2025")
      - date_from/date_to в формате YYYY-MM-DD
    """
    df = dt = None
    if date_from:
        try:
            df = datetime.strptime(str(date_from), '%Y-%m-%d').date()
        except Exception:
            df = None
    if date_to:
        try:
            dt = datetime.strptime(str(date_to), '%Y-%m-%d').date()
        except Exception:
            dt = None

    if df or dt:
        return df, dt

    if month and year:
        try:
            m = int(month); y = int(year)
            return date(y, m, 1), date(y, m, monthrange(y, m)[1])
        except Exception:
            return None, None
    if year:
        try:
            y = int(year)
            return date(y, 1, 1), date(y, 12, 31)
        except Exception:
            return None, None
    return None, None


def _ok(summary, data=None, source_url=None, source_title=None):
    return {
        'ok': True,
        'summary': summary,
        'data': data if data is not None else {},
        'source_url': source_url or '',
        'source_title': source_title or '',
    }


def _err(msg, suggestions=None):
    return {'ok': False, 'summary': msg, 'suggestions': suggestions or []}


def _ambig(what, candidates, suggestion_key='name'):
    return {
        'ok': False,
        'ambiguous': True,
        'summary': f'Найдено несколько подходящих {what} — уточните',
        'suggestions': [{'id': c.id, 'name': getattr(c, suggestion_key, str(c))}
                        for c in candidates[:MAX_CANDIDATES]],
    }


# ---- поиск сущностей по части имени ----

def _find_one(model, name, name_col='name'):
    """Возвращает (obj | None, candidates_list).
    Если ровно одно совпадение — obj, []; иначе None и список кандидатов.
    """
    if not name:
        return None, []
    col = getattr(model, name_col)
    matches = model.query.filter(func.lower(col).like(f'%{name.lower()}%')).limit(MAX_CANDIDATES).all()
    if not matches:
        return None, []
    if len(matches) == 1:
        return matches[0], []
    # Если среди совпадений есть точное — вернём его
    for m in matches:
        if (getattr(m, name_col) or '').lower() == name.lower():
            return m, []
    return None, matches


# --------------------------------------------------------------------------
# TOOLS — read-only функции-инструменты
# --------------------------------------------------------------------------

def tool_list_plants(query=None, limit=20):
    q = Plant.query
    if query:
        q = q.filter(func.lower(Plant.name).like(f'%{query.lower()}%'))
    rows = q.order_by(Plant.name).limit(int(limit or 20)).all()
    return _ok(
        f'Найдено растений: {len(rows)}',
        data={'plants': [{'id': p.id, 'name': p.name} for p in rows]},
        source_url='/directory',
        source_title='Справочник растений',
    )


def tool_list_clients(query=None, limit=20):
    q = Client.query
    if query:
        q = q.filter(func.lower(Client.name).like(f'%{query.lower()}%'))
    rows = q.order_by(Client.name).limit(int(limit or 20)).all()
    return _ok(
        f'Найдено клиентов: {len(rows)}',
        data={'clients': [{'id': c.id, 'name': c.name} for c in rows]},
        source_url='/directory',
        source_title='Справочник клиентов',
    )


def tool_list_fields(query=None):
    q = Field.query
    if query:
        q = q.filter(func.lower(Field.name).like(f'%{query.lower()}%'))
    rows = q.order_by(Field.name).limit(50).all()
    return _ok(
        f'Найдено полей: {len(rows)}',
        data={'fields': [{'id': f.id, 'name': f.name, 'planting_year': f.planting_year} for f in rows]},
        source_url='/directory',
        source_title='Справочник полей',
    )


def tool_get_cost(plant_name=None, field_name=None, size_name=None):
    """Себестоимость позиций на складе (StockBalance.current_total_cost).

    Достаточно хотя бы одного фильтра (plant / field / size). Если задан только
    field_name — вернём все растения этого поля с их себестоимостью.
    """
    if not (plant_name or field_name or size_name):
        return _err('Укажите хотя бы один фильтр: растение, поле или размер')

    q = StockBalance.query.filter(StockBalance.quantity > 0)
    label_parts = []

    if plant_name:
        plant, cands = _find_one(Plant, plant_name)
        if not plant:
            if cands: return _ambig('растений', cands)
            return _err(f'Растение "{plant_name}" не найдено')
        q = q.filter(StockBalance.plant_id == plant.id)
        label_parts.append(f'«{plant.name}»')
    if size_name:
        size, scands = _find_one(Size, size_name)
        if not size:
            if scands: return _ambig('размеров', scands)
            return _err(f'Размер "{size_name}" не найден')
        q = q.filter(StockBalance.size_id == size.id)
        label_parts.append(f'размер {size.name}')
    if field_name:
        fld, fcands = _find_one(Field, field_name)
        if not fld:
            if fcands: return _ambig('полей', fcands)
            return _err(f'Поле "{field_name}" не найдено')
        q = q.filter(StockBalance.field_id == fld.id)
        label_parts.append(f'поле {fld.name}')

    rows = q.all()
    if not rows:
        return _err(f'Позиций с указанными фильтрами нет на остатках')

    out = []
    for r in rows:
        out.append({
            'plant': r.plant.name if r.plant else '—',
            'size': r.size.name if r.size else '—',
            'field': r.field.name if r.field else '—',
            'year': r.year,
            'qty': int(r.quantity or 0),
            'unit_cost': round(_safe_float(r.current_total_cost), 2),
            'purchase_price': round(_safe_float(r.purchase_price), 2),
            'price': round(_safe_float(r.price), 2),
        })
    out.sort(key=lambda x: (x['plant'], x['size']))
    total_qty = sum(x['qty'] for x in out) or 1
    total_cost_mass = sum(x['qty'] * x['unit_cost'] for x in out)
    avg_cost = total_cost_mass / total_qty

    label = ' · '.join(label_parts) or 'со склада'
    avg_cost_fmt = f'{avg_cost:,.0f}'.replace(',', ' ')
    return _ok(
        f"Себестоимость ({label}): {len(out)} позиц., {total_qty} шт., "
        f"средняя {avg_cost_fmt} ₽/шт",
        data={
            'rows': out[:30],
            'rows_total': len(out),
            'avg_unit_cost': round(avg_cost, 2),
            'total_qty': total_qty,
        },
        source_url='/cost',
        source_title='Отчёт: Себестоимость',
    )


def tool_get_stock(plant_name=None, field_name=None, size_name=None):
    """Остатки на складе с учётом резерва (свободные и общие)."""
    q = StockBalance.query
    plant = None
    if plant_name:
        plant, cands = _find_one(Plant, plant_name)
        if not plant:
            if cands: return _ambig('растений', cands)
            return _err(f'Растение "{plant_name}" не найдено')
        q = q.filter(StockBalance.plant_id == plant.id)
    if size_name:
        size, scands = _find_one(Size, size_name)
        if not size:
            if scands: return _ambig('размеров', scands)
            return _err(f'Размер "{size_name}" не найден')
        q = q.filter(StockBalance.size_id == size.id)
    if field_name:
        fld, fcands = _find_one(Field, field_name)
        if not fld:
            if fcands: return _ambig('полей', fcands)
            return _err(f'Поле "{field_name}" не найдено')
        q = q.filter(StockBalance.field_id == fld.id)

    q = q.filter(StockBalance.quantity > 0)
    rows = q.limit(200).all()
    if not rows:
        return _err('Подходящих позиций на остатках нет')

    from app.stock_helpers import get_reserved_map
    rmap = get_reserved_map(plant_id=plant.id if plant else None)

    out = []
    for r in rows:
        reserved = rmap.get((r.plant_id, r.size_id, r.field_id, r.year), 0)
        out.append({
            'plant': r.plant.name if r.plant else '—',
            'size': r.size.name if r.size else '—',
            'field': r.field.name if r.field else '—',
            'year': r.year,
            'total': int(r.quantity or 0),
            'reserved': int(reserved),
            'free': int((r.quantity or 0) - reserved),
        })
    total = sum(x['total'] for x in out)
    reserved = sum(x['reserved'] for x in out)
    free = sum(x['free'] for x in out)
    return _ok(
        f'Позиций: {len(out)}. Всего: {total} шт., резерв: {reserved} шт., свободно: {free} шт.',
        data={'rows': out, 'total': total, 'reserved': reserved, 'free': free},
        source_url='/stock',
        source_title='Отчёт: Остатки',
    )


def tool_get_expenses(category_name=None, month=None, year=None, description_contains=None):
    """Расходы по статье/описанию за период из таблицы Expense."""
    first, last = _parse_period(month=month, year=year)
    q = db.session.query(Expense).join(BudgetItem, Expense.budget_item_id == BudgetItem.id)
    if first: q = q.filter(Expense.date >= first)
    if last:  q = q.filter(Expense.date <= last)
    if category_name:
        q = q.filter(func.lower(BudgetItem.name).like(f'%{category_name.lower()}%'))
    if description_contains:
        q = q.filter(func.lower(Expense.description).like(f'%{description_contains.lower()}%'))

    rows = q.order_by(Expense.date.desc()).limit(100).all()
    if not rows:
        period_label = ''
        if first and last:
            period_label = f' за {first.strftime("%m.%Y")}–{last.strftime("%m.%Y")}'
        cat_label = f' по статье «{category_name}»' if category_name else ''
        return _err(f'Расходов{cat_label}{period_label} не найдено')

    total = sum(_safe_float(r.amount) for r in rows)
    sample = [
        {
            'date': r.date.strftime('%d.%m.%Y') if r.date else '',
            'category': r.item.name if r.item else '—',
            'amount': _safe_float(r.amount),
            'description': r.description or '',
            'payment_type': r.payment_type,
        }
        for r in rows[:10]
    ]
    period_label = ''
    if first and last:
        period_label = f'{first.strftime("%m.%Y")}'
        if first.year != last.year or first.month != last.month:
            period_label = f'{first.strftime("%m.%Y")}–{last.strftime("%m.%Y")}'
    cat_label = f' · статья «{category_name}»' if category_name else ''
    return _ok(
        f'Расходов: {len(rows)} на сумму {_money(total)}{cat_label}'
        + (f' ({period_label})' if period_label else ''),
        data={'total': round(total, 2), 'count': len(rows), 'rows': sample},
        source_url='/expenses',
        source_title='Отчёт: Расходы',
    )


def tool_get_cash_in(month=None, year=None, client_name=None):
    """Поступления денег за период (Payment по Payment.date)."""
    first, last = _parse_period(month=month, year=year)
    q = db.session.query(func.coalesce(func.sum(Payment.amount), 0)).select_from(Payment)
    if client_name:
        client, cands = _find_one(Client, client_name)
        if not client:
            if cands: return _ambig('клиентов', cands)
            return _err(f'Клиент "{client_name}" не найден')
        q = q.join(Order, Payment.order_id == Order.id).filter(Order.client_id == client.id)
    if first: q = q.filter(Payment.date >= first)
    if last:  q = q.filter(Payment.date <= last)
    total = _safe_float(q.scalar() or 0)

    period_label = ''
    if first and last:
        period_label = first.strftime('%m.%Y') if first == last.replace(day=1) else f'{first}–{last}'
    return _ok(
        f'Поступило: {_money(total)}'
        + (f' за {period_label}' if period_label else '')
        + (f' от клиента «{client_name}»' if client_name else ''),
        data={'total': round(total, 2), 'period_from': first.isoformat() if first else None,
              'period_to': last.isoformat() if last else None},
        source_url='/expenses',
        source_title='Финансы',
    )


def tool_get_shipments(client_name=None, plant_name=None, month=None, year=None,
                       date_from=None, date_to=None):
    """Отгрузки за период — сразу и деньги, и штуки.

    Пользователь может спросить «сколько растений отгрузили» (интересует кол-во)
    или «на какую сумму отгрузили» (интересует сумма). Возвращаем оба числа,
    LLM сама подберёт нужное поле.
    """
    first, last = _parse_period(month=month, year=year,
                                 date_from=date_from, date_to=date_to)
    q = db.session.query(
        func.coalesce(func.sum(OrderItem.price * DocumentRow.quantity), 0).label('money'),
        func.coalesce(func.sum(DocumentRow.quantity), 0).label('qty'),
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
    ).filter(Document.doc_type == 'shipment')

    if client_name:
        client, cands = _find_one(Client, client_name)
        if not client:
            if cands: return _ambig('клиентов', cands)
            return _err(f'Клиент "{client_name}" не найден')
        q = q.filter(Order.client_id == client.id)
    if plant_name:
        plant, pcands = _find_one(Plant, plant_name)
        if not plant:
            if pcands: return _ambig('растений', pcands)
            return _err(f'Растение "{plant_name}" не найдено')
        q = q.filter(DocumentRow.plant_id == plant.id)
    if first: q = q.filter(func.date(Document.date) >= first)
    if last:  q = q.filter(func.date(Document.date) <= last)

    row = q.one()
    total_money = _safe_float(row.money or 0)
    total_qty = int(row.qty or 0)

    period_str = ''
    if first and last:
        if first.replace(day=1) == first and last == date(last.year, last.month, monthrange(last.year, last.month)[1]):
            period_str = f' за {first.strftime("%m.%Y")}'
        else:
            period_str = f' с {first.strftime("%d.%m.%Y")} по {last.strftime("%d.%m.%Y")}'

    return _ok(
        f'Отгружено{period_str}: {total_qty} шт. на сумму {_money(total_money)}'
        + (f' клиенту «{client_name}»' if client_name else '')
        + (f' (растение: {plant_name})' if plant_name else ''),
        data={
            'total_qty': total_qty,
            'total_money': round(total_money, 2),
            'period_from': first.isoformat() if first else None,
            'period_to': last.isoformat() if last else None,
        },
        source_url='/orders',
        source_title='Заказы',
    )


def tool_get_client_debt(client_name):
    """Долг клиента по активным заказам (reserved/in_progress/ready/shipped).

    Для каждого активного заказа: billed = price*qty, paid = сумма платежей.
    Если billed > paid — считаем разницу долгом.
    """
    if not client_name:
        return _err('Укажите имя клиента')
    client, cands = _find_one(Client, client_name)
    if not client:
        if cands: return _ambig('клиентов', cands)
        return _err(f'Клиент "{client_name}" не найден')

    active_statuses = ('reserved', 'in_progress', 'ready', 'shipped')
    orders = Order.query.filter(
        Order.client_id == client.id,
        Order.is_deleted.is_(False),
        Order.status.in_(active_statuses),
    ).all()
    total_debt = 0.0
    details = []
    for o in orders:
        billed = _safe_float(o.total_sum)
        paid = _safe_float(o.paid_sum)
        diff = billed - paid
        if diff > 0.5:
            total_debt += diff
            details.append({
                'order_id': o.id,
                'date': o.date.strftime('%d.%m.%Y') if o.date else '',
                'status': o.status,
                'billed': round(billed, 2),
                'paid': round(paid, 2),
                'debt': round(diff, 2),
            })
    details.sort(key=lambda r: r['debt'], reverse=True)
    return _ok(
        f'Долг «{client.name}»: {_money(total_debt)} по {len(details)} активным заказам',
        data={
            'client_id': client.id,
            'client_name': client.name,
            'debt': round(total_debt, 2),
            'orders': details[:10],
        },
        source_url='/crm/client_analytics',
        source_title='Аналитика: Клиенты',
    )


def tool_get_employee_hours(employee_name, month=None, year=None):
    """Часы сотрудника за период (TimeLog)."""
    if not employee_name:
        return _err('Укажите ФИО сотрудника')
    emp, cands = _find_one(Employee, employee_name)
    if not emp:
        if cands: return _ambig('сотрудников', cands)
        return _err(f'Сотрудник "{employee_name}" не найден')

    first, last = _parse_period(month=month, year=year)
    q = TimeLog.query.filter(TimeLog.employee_id == emp.id)
    if first: q = q.filter(TimeLog.date >= first)
    if last:  q = q.filter(TimeLog.date <= last)
    rows = q.all()

    total = 0.0
    norm = over = spec = spec_over = 0.0
    for r in rows:
        norm += _safe_float(r.hours_norm)
        over += _safe_float(r.hours_norm_over)
        spec += _safe_float(r.hours_spec)
        spec_over += _safe_float(r.hours_spec_over)
    total = norm + over + spec + spec_over
    period_label = ''
    if first and last:
        period_label = f' за {first.strftime("%m.%Y")}'
    return _ok(
        f'Часы {emp.name}{period_label}: {total:.1f} ч (норма {norm:.1f} + сверхур. {over:.1f}'
        f' + спец. {spec:.1f} + спец.сверхур. {spec_over:.1f})',
        data={
            'employee_id': emp.id,
            'employee_name': emp.name,
            'total': round(total, 2),
            'hours_norm': round(norm, 2),
            'hours_norm_over': round(over, 2),
            'hours_spec': round(spec, 2),
            'hours_spec_over': round(spec_over, 2),
            'entries_count': len(rows),
        },
        source_url='/personnel',
        source_title='Кадры: Табель',
    )


def tool_get_orders(client_name=None, status=None, year=None, month=None,
                    date_from=None, date_to=None, limit=20):
    """Список заказов по фильтрам."""
    q = Order.query.filter(Order.is_deleted.is_(False))
    if status:
        q = q.filter(Order.status == status)
    if client_name:
        client, cands = _find_one(Client, client_name)
        if not client:
            if cands: return _ambig('клиентов', cands)
            return _err(f'Клиент "{client_name}" не найден')
        q = q.filter(Order.client_id == client.id)
    first, last = _parse_period(month=month, year=year, date_from=date_from, date_to=date_to)
    if first: q = q.filter(func.date(Order.date) >= first)
    if last:  q = q.filter(func.date(Order.date) <= last)

    rows = q.order_by(Order.date.desc()).limit(int(limit or 20)).all()
    if not rows:
        return _err('Заказов по вашим фильтрам нет')

    out = []
    total_sum = 0.0
    for o in rows:
        s = _safe_float(o.total_sum)
        total_sum += s
        out.append({
            'id': o.id,
            'date': o.date.strftime('%d.%m.%Y') if o.date else '',
            'client': o.client.name if o.client else '—',
            'status': o.status,
            'total': round(s, 2),
        })
    return _ok(
        f'Заказов: {len(out)}; сумма показанных: {_money(total_sum)}',
        data={'orders': out, 'count': len(out), 'total_shown': round(total_sum, 2)},
        source_url='/orders',
        source_title='Заказы',
    )


def tool_get_digging(plant_name=None, field_name=None, date_from=None, date_to=None):
    """Выкопка за период (DiggingLog)."""
    first, last = _parse_period(date_from=date_from, date_to=date_to)
    q = db.session.query(DiggingLog).filter(DiggingLog.status != 'rejected')
    if first: q = q.filter(DiggingLog.date >= first)
    if last:  q = q.filter(DiggingLog.date <= last)
    if plant_name:
        plant, cands = _find_one(Plant, plant_name)
        if not plant:
            if cands: return _ambig('растений', cands)
            return _err(f'Растение "{plant_name}" не найдено')
        q = q.filter(DiggingLog.plant_id == plant.id)
    if field_name:
        fld, fcands = _find_one(Field, field_name)
        if not fld:
            if fcands: return _ambig('полей', fcands)
            return _err(f'Поле "{field_name}" не найдено')
        q = q.filter(DiggingLog.field_id == fld.id)

    rows = q.all()
    total_qty = sum(int(r.quantity or 0) for r in rows)
    sample = []
    for r in rows[:10]:
        sample.append({
            'date': r.date.strftime('%d.%m.%Y') if r.date else '',
            'plant': r.plant.name if r.plant else '—',
            'size': r.size.name if r.size else '—',
            'field': r.field.name if r.field else '—',
            'quantity': int(r.quantity or 0),
        })
    return _ok(
        f'Выкопано: {total_qty} шт. (записей {len(rows)})',
        data={'total_qty': total_qty, 'count': len(rows), 'sample': sample},
        source_url='/digging/analytics',
        source_title='Аналитика: Выкопка',
    )


def tool_get_plant_info(plant_name):
    """Сводная карточка растения: размеры, поля, остатки, средняя цена."""
    if not plant_name:
        return _err('Укажите название растения')
    plant, cands = _find_one(Plant, plant_name)
    if not plant:
        if cands: return _ambig('растений', cands)
        return _err(f'Растение "{plant_name}" не найдено')

    stocks = StockBalance.query.filter(StockBalance.plant_id == plant.id).all()
    total = 0
    by_size = {}
    by_field = {}
    price_sum = 0.0; price_w = 0
    for s in stocks:
        qty = int(s.quantity or 0)
        if qty <= 0: continue
        total += qty
        sname = s.size.name if s.size else '—'
        fname = s.field.name if s.field else '—'
        by_size[sname] = by_size.get(sname, 0) + qty
        by_field[fname] = by_field.get(fname, 0) + qty
        p = _safe_float(s.price)
        if p > 0:
            price_sum += p * qty
            price_w += qty
    avg_price = (price_sum / price_w) if price_w else 0.0
    return _ok(
        f'«{plant.name}»: {total} шт. на складе; размеров {len(by_size)}; '
        f'полей {len(by_field)}; средняя цена {avg_price:,.0f} ₽/шт'.replace(',', ' '),
        data={
            'plant_id': plant.id,
            'plant_name': plant.name,
            'total_qty': total,
            'by_size': [{'size': k, 'qty': v} for k, v in sorted(by_size.items())],
            'by_field': [{'field': k, 'qty': v} for k, v in sorted(by_field.items())],
            'avg_price': round(avg_price, 2),
        },
        source_url='/stock',
        source_title='Склад',
    )


def tool_get_field_info(field_name):
    """Что растёт на поле: позиции + остатки + резерв."""
    if not field_name:
        return _err('Укажите название поля')
    fld, cands = _find_one(Field, field_name)
    if not fld:
        if cands: return _ambig('полей', cands)
        return _err(f'Поле "{field_name}" не найдено')

    stocks = StockBalance.query.filter(StockBalance.field_id == fld.id, StockBalance.quantity > 0).all()
    from app.stock_helpers import get_reserved_map
    rmap = get_reserved_map()
    rows = []
    total = reserved_total = 0
    for s in stocks:
        qty = int(s.quantity or 0)
        res = int(rmap.get((s.plant_id, s.size_id, s.field_id, s.year), 0) or 0)
        total += qty; reserved_total += res
        rows.append({
            'plant': s.plant.name if s.plant else '—',
            'size': s.size.name if s.size else '—',
            'year': s.year,
            'total': qty, 'reserved': res, 'free': max(0, qty - res),
        })
    return _ok(
        f'Поле «{fld.name}»: позиций {len(rows)}; всего {total} шт., резерв {reserved_total}.',
        data={'field_id': fld.id, 'field_name': fld.name, 'rows': rows,
              'total': total, 'reserved': reserved_total, 'free': total - reserved_total},
        source_url='/stock/documents',
        source_title='Карта питомника',
    )


def tool_get_patent_status(employee_name=None):
    """Статус патентов (сколько дней осталось)."""
    today = msk_today()
    q = PatentPeriod.query.filter(PatentPeriod.is_current.is_(True))
    if employee_name:
        emp, cands = _find_one(Employee, employee_name)
        if not emp:
            if cands: return _ambig('сотрудников', cands)
            return _err(f'Сотрудник "{employee_name}" не найден')
        q = q.filter(PatentPeriod.employee_id == emp.id)
    rows = q.all()
    out = []
    for p in rows:
        days_left = (p.end_date - today).days if p.end_date else None
        out.append({
            'employee': p.employee.name if p.employee else '—',
            'start': p.start_date.strftime('%d.%m.%Y') if p.start_date else '',
            'end': p.end_date.strftime('%d.%m.%Y') if p.end_date else '',
            'days_left': days_left,
            'status': p.status,
        })
    out.sort(key=lambda r: (r['days_left'] if r['days_left'] is not None else 99999))
    return _ok(
        f'Активных патентов: {len(out)}',
        data={'patents': out},
        source_url='/personnel/foreign',
        source_title='Кадры: Иностранные сотрудники',
    )


def tool_get_tasks_summary(assignee_name=None):
    """Активные/просроченные/закрытые поручения (TgTask)."""
    today = msk_today()
    q_active = TgTask.query.filter(TgTask.status == 'new')
    q_overdue = TgTask.query.filter(
        TgTask.status == 'new',
        TgTask.deadline.isnot(None),
        TgTask.deadline < today,
    )
    done_start = datetime.combine(today - timedelta(days=30), datetime.min.time())
    q_done = TgTask.query.filter(
        TgTask.status == 'done',
        TgTask.completed_at.isnot(None),
        TgTask.completed_at >= done_start,
    )
    assignee_filter = None
    if assignee_name:
        user = User.query.filter(func.lower(User.username).like(f'%{assignee_name.lower()}%')).first()
        if not user:
            return _err(f'Сотрудник "{assignee_name}" не найден среди логинов')
        assignee_filter = (TgTask.assignee_id == user.id)
        q_active = q_active.filter(assignee_filter)
        q_overdue = q_overdue.filter(assignee_filter)
        q_done = q_done.filter(assignee_filter)
    active = q_active.count()
    overdue = q_overdue.count()
    done_30d = q_done.count()
    return _ok(
        f'Активных: {active}; просрочено: {overdue}; закрыто за 30 дней: {done_30d}',
        data={'active': active, 'overdue': overdue, 'done_30d': done_30d,
              'assignee': assignee_name},
        source_url='/analytics/tasks',
        source_title='Аналитика: Поручения',
    )


def tool_get_revenue(month=None, year=None):
    """Алиас для cash-in — «выручка за период» понимаем как поступления."""
    return tool_get_cash_in(month=month, year=year)


# --------------------------------------------------------------------------
# СХЕМА И МАРШРУТИЗАЦИЯ
# --------------------------------------------------------------------------

available_functions = {
    'list_plants': tool_list_plants,
    'list_clients': tool_list_clients,
    'list_fields': tool_list_fields,
    'get_cost': tool_get_cost,
    'get_stock': tool_get_stock,
    'get_expenses': tool_get_expenses,
    'get_cash_in': tool_get_cash_in,
    'get_revenue': tool_get_revenue,
    'get_shipments': tool_get_shipments,
    'get_client_debt': tool_get_client_debt,
    'get_employee_hours': tool_get_employee_hours,
    'get_orders': tool_get_orders,
    'get_digging': tool_get_digging,
    'get_plant_info': tool_get_plant_info,
    'get_field_info': tool_get_field_info,
    'get_patent_status': tool_get_patent_status,
    'get_tasks_summary': tool_get_tasks_summary,
}


# Права доступа к инструментам по ролям.
# admin и executive видят всё (включая финансы, зарплаты, патенты).
# user (менеджер) — кроме патентов, часов (чужих) и расходов/бюджета.
# user2 и прочие — только операционка (остатки, выкопка, справочники).
TOOL_ROLE_WHITELIST = {
    'admin':     set(available_functions.keys()),
    'executive': set(available_functions.keys()),
    'user': {
        'list_plants', 'list_clients', 'list_fields',
        'get_cost', 'get_stock',
        'get_cash_in', 'get_revenue', 'get_shipments',
        'get_client_debt', 'get_orders',
        'get_digging', 'get_plant_info', 'get_field_info',
        'get_tasks_summary',
    },
    'user2': {
        'list_plants', 'list_fields',
        'get_stock', 'get_digging',
        'get_plant_info', 'get_field_info',
        'get_tasks_summary',
    },
}


def _filter_tools_for_role(role):
    """Возвращает (subset_schema, subset_functions) по роли.

    Неизвестная роль получает минимум — только справочники и остатки.
    """
    allowed = TOOL_ROLE_WHITELIST.get(role or '', {
        'list_plants', 'list_fields', 'get_stock', 'get_plant_info', 'get_field_info',
    })
    schema = [t for t in tools_schema if t['function']['name'] in allowed]
    funcs = {name: fn for name, fn in available_functions.items() if name in allowed}
    return schema, funcs


tools_schema = [
    {"type": "function", "function": {
        "name": "list_plants",
        "description": "Поиск растений по подстроке имени. Используй когда пользователь назвал неточно.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"},
            "limit": {"type": "integer"},
        }},
    }},
    {"type": "function", "function": {
        "name": "list_clients",
        "description": "Поиск клиентов по подстроке имени.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"},
            "limit": {"type": "integer"},
        }},
    }},
    {"type": "function", "function": {
        "name": "list_fields",
        "description": "Поиск полей питомника по номеру/имени.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_cost",
        "description": "Себестоимость растений на складе (ед. и средневзвешенная). Нужен хотя бы один фильтр: plant_name ИЛИ field_name ИЛИ size_name. Можно комбинировать.",
        "parameters": {"type": "object", "properties": {
            "plant_name": {"type": "string"},
            "size_name": {"type": "string"},
            "field_name": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_stock",
        "description": "Остатки на складе. Можно фильтровать по растению, размеру, полю. Возвращает всего/резерв/свободно.",
        "parameters": {"type": "object", "properties": {
            "plant_name": {"type": "string"},
            "size_name": {"type": "string"},
            "field_name": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_expenses",
        "description": "Расходы по статье за период (месяц/год). Пример: 'Электричество за март 2026' → category_name='Электричество', month=3, year=2026.",
        "parameters": {"type": "object", "properties": {
            "category_name": {"type": "string"},
            "month": {"type": "integer", "description": "1-12"},
            "year": {"type": "integer"},
            "description_contains": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_cash_in",
        "description": "Поступления денег от клиентов за период (платежи). Это то, что в KPI называется 'выручкой за месяц'.",
        "parameters": {"type": "object", "properties": {
            "month": {"type": "integer"},
            "year": {"type": "integer"},
            "client_name": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_revenue",
        "description": "Синоним get_cash_in — используй когда пользователь просит 'выручку'.",
        "parameters": {"type": "object", "properties": {
            "month": {"type": "integer"},
            "year": {"type": "integer"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_shipments",
        "description": "Отгрузки за период. Возвращает СРАЗУ и штуки (total_qty), и деньги (total_money). Используй для 'сколько растений отгрузили' или 'на какую сумму отгрузили'.",
        "parameters": {"type": "object", "properties": {
            "client_name": {"type": "string"},
            "plant_name": {"type": "string"},
            "month": {"type": "integer"},
            "year": {"type": "integer"},
            "date_from": {"type": "string", "description": "YYYY-MM-DD"},
            "date_to": {"type": "string", "description": "YYYY-MM-DD"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_client_debt",
        "description": "Долг клиента по партнёрскому отчёту (fixed_balance + отгрузки − платежи, без ghost).",
        "parameters": {"type": "object", "properties": {
            "client_name": {"type": "string"},
        }, "required": ["client_name"]},
    }},
    {"type": "function", "function": {
        "name": "get_employee_hours",
        "description": "Часы сотрудника за период из табеля.",
        "parameters": {"type": "object", "properties": {
            "employee_name": {"type": "string"},
            "month": {"type": "integer"},
            "year": {"type": "integer"},
        }, "required": ["employee_name"]},
    }},
    {"type": "function", "function": {
        "name": "get_orders",
        "description": "Список заказов. Фильтры: клиент, статус (одно из: reserved, in_progress, ready, shipped, canceled), период (month+year или date_from+date_to).",
        "parameters": {"type": "object", "properties": {
            "client_name": {"type": "string"},
            "status": {"type": "string", "description": "reserved | in_progress | ready | shipped | canceled"},
            "year": {"type": "integer"},
            "month": {"type": "integer"},
            "date_from": {"type": "string"},
            "date_to": {"type": "string"},
            "limit": {"type": "integer"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_digging",
        "description": "Сколько растений выкопано за период. Фильтры: растение, поле.",
        "parameters": {"type": "object", "properties": {
            "plant_name": {"type": "string"},
            "field_name": {"type": "string"},
            "date_from": {"type": "string"},
            "date_to": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_plant_info",
        "description": "Сводная карточка растения: общий остаток, размеры, поля, средняя цена.",
        "parameters": {"type": "object", "properties": {
            "plant_name": {"type": "string"},
        }, "required": ["plant_name"]},
    }},
    {"type": "function", "function": {
        "name": "get_field_info",
        "description": "Что растёт на конкретном поле + остатки.",
        "parameters": {"type": "object", "properties": {
            "field_name": {"type": "string"},
        }, "required": ["field_name"]},
    }},
    {"type": "function", "function": {
        "name": "get_patent_status",
        "description": "Статус патентов: сколько осталось дней до истечения.",
        "parameters": {"type": "object", "properties": {
            "employee_name": {"type": "string"},
        }},
    }},
    {"type": "function", "function": {
        "name": "get_tasks_summary",
        "description": "Кол-во активных, просроченных и выполненных за 30 дней поручений. Можно по конкретному логину (assignee).",
        "parameters": {"type": "object", "properties": {
            "assignee_name": {"type": "string"},
        }},
    }},
]


# --------------------------------------------------------------------------
# ОРКЕСТРАТОР
# --------------------------------------------------------------------------

_MODEL = 'llama-3.3-70b-versatile'


def _build_system_prompt():
    today = msk_today()
    return f"""
Ты — информационный помощник ERP «Зелёный Фонд». Сегодня {today.strftime('%d.%m.%Y')}.

ЖЁСТКИЕ ПРАВИЛА:
1. НИКОГДА не придумывай цифры. Для любого фактического ответа вызывай инструмент.
2. Выбор инструмента по смыслу глагола в вопросе:
   - «Себестоимость», «сколько стоит в закупке», «средняя цена за штуку» → get_cost.
   - «Остатки», «сколько на складе», «сколько свободно» → get_stock.
   - «Расходы», «сколько потратили», «траты на ...» → get_expenses.
   - «Выручка», «сколько пришло денег», «поступления», «оплатили» → get_cash_in.
   - «Отгрузили», «сколько отгружено» → get_shipments (он сразу даёт и штуки, и сумму).
   - «Долг клиента», «сколько должен клиент» → get_client_debt.
   - «Часы», «табель» → get_employee_hours.
   - «Выкопали», «сколько вытащили» → get_digging.
   - «Заказы клиента», «заказы в работе» → get_orders.
   - «Что растёт на поле», «какие растения на поле X» → get_field_info.
   - «Где растёт», «на каких полях есть растение X» → get_plant_info.
   - «Патенты», «когда заканчивается патент» → get_patent_status.
   - «Поручения», «задачи», «кто сколько сделал» → get_tasks_summary.
3. КАЧЕСТВО ОТВЕТА:
   - Отвечай именно на тот вопрос, который задан. Если спросили «сколько штук» — давай число в штуках, если «на какую сумму» — в рублях.
   - Инструмент get_shipments возвращает и штуки (total_qty), и сумму (total_money). Бери нужное.
4. УТОЧНЕНИЯ:
   - Если инструмент вернул ambiguous=true — выбери нужного кандидата из suggestions и позови инструмент ещё раз с полным именем кандидата.
   - Если инструмент вернул ok=false без кандидатов — честно скажи «не нашёл» и предложи варианты.
5. ПЕРИОДЫ:
   - «март 2026» → month=3, year=2026.
   - «в этом году» → year={today.year}.
   - «за 2025 год» → year=2025.
   - «с 1 по 15 апреля» → date_from='{today.year}-04-01', date_to='{today.year}-04-15'.
6. ФОРМАТ ОТВЕТА:
   - Короткий HTML на русском. Ключевое число в <b>…</b>.
   - При нескольких строках — маленькая таблица: <table class='table table-sm table-bordered' style='font-size:12px;'>…</table>.
   - В КОНЦЕ всегда кнопка со ссылкой из source_url инструмента:
     <br><br><a href="URL" class="btn btn-sm btn-outline-primary">Перейти в source_title</a>
   - Никаких h1/h2, markdown-блоков, json. Без прелюдий «Согласно данным...» — сразу факт.

ПРИМЕРЫ (внутренние, не показывай пользователю):

Вопрос: «Какая себестоимость сосны горной на 11 поле?»
Действия: get_cost(plant_name='Сосна горная', field_name='11').
Ответ: «Себестоимость Сосны горной на поле 11: средняя <b>12 300 ₽/шт</b> (3 позиции, 45 шт).»

Вопрос: «Сколько растений отгрузили за апрель 2026?»
Действия: get_shipments(month=4, year=2026).
Ответ: «В апреле 2026 отгружено <b>1 240 шт</b> на сумму 3 420 000 ₽.»

Вопрос: «Сколько потратили на электричество в 2026?»
Действия: get_expenses(category_name='Электричество', year=2026).
Ответ: «По статье «Электричество» в 2026 — <b>108 552 ₽</b> (11 проводок).»

Вопрос: «Что растёт на 11 поле?»
Действия: get_field_info(field_name='11').
Ответ: таблица с растениями и остатками.
""".strip()


def _call_tool_safely(name, args, allowed_funcs=None):
    """Вызывает tool и всегда возвращает JSON-сериализуемый результат.

    Если передан allowed_funcs — проверяем, что функция в этом списке (защита
    от того, чтобы LLM не попыталась вызвать запрещённую для роли функцию).
    """
    pool = allowed_funcs if allowed_funcs is not None else available_functions
    fn = pool.get(name)
    if not fn:
        return {'ok': False, 'summary': f'Инструмент "{name}" недоступен для вашей роли.'}
    try:
        result = fn(**(args or {}))
        json.dumps(result, ensure_ascii=False, default=str)
        return result
    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        return {'ok': False, 'summary': f'Инструмент {name} упал: {e}'}


def _append_link(final_html, last_tool_result):
    """Дошиваем кнопку-ссылку, если модель забыла её вставить."""
    if not last_tool_result:
        return final_html
    url = last_tool_result.get('source_url')
    title = last_tool_result.get('source_title', 'отчёт')
    if url and url not in (final_html or ''):
        final_html = (final_html or '') + (
            f'<br><br><a href="{url}" class="btn btn-sm btn-outline-primary">'
            f'Перейти в {title}</a>'
        )
    return final_html


def _fallback_plain_answer(client, user_query, last_tool_result):
    """Если Groq зафейлил tool-calling — просим ответ БЕЗ инструментов, скармливая
    последний полезный результат ручкой в prompt. Это спасает разговор от
    голой ошибки «Failed to call a function»."""
    context = ''
    if last_tool_result:
        context = '\n\nДанные из инструмента:\n' + json.dumps(
            last_tool_result, ensure_ascii=False, default=str
        )[:2000]
    prompt = (
        'Пользователь спросил: «' + user_query + '». '
        'Сформулируй краткий HTML-ответ на русском (одно-два предложения, '
        'ключевое число в <b></b>). '
        'Если данных нет — честно скажи «не нашёл».' + context
    )
    try:
        resp = client.chat.completions.create(
            model=_MODEL,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.2,
        )
        text = (resp.choices[0].message.content or '').strip()
        return _append_link(text, last_tool_result)
    except Exception as e:
        # Всё совсем плохо — выдадим что есть.
        if last_tool_result:
            summary = last_tool_result.get('summary') or ''
            return _append_link(f'{summary}', last_tool_result)
        return (
            '<div class="text-warning">Не удалось получить ответ от ИИ. '
            f'Попробуйте переформулировать вопрос. (ошибка: {e})</div>'
        )


def process_chat_query(user_query, role='admin'):
    """Главная точка входа чата. Возвращает готовый HTML-ответ."""
    api_key = os.environ.get('GROQ_API_KEY')
    if not api_key:
        return (
            '<div class="text-danger">Не задан GROQ_API_KEY — чат не может обращаться к ИИ. '
            'Добавьте переменную окружения, чтобы заработали умные ответы.</div>'
        )

    try:
        from groq import Groq
    except Exception as e:
        return f'<div class="text-danger">Библиотека groq не установлена: {e}</div>'

    client = Groq(api_key=api_key)

    # Фильтр инструментов под роль — LLM получит только разрешённые.
    role_schema, role_funcs = _filter_tools_for_role(role)

    messages = [
        {"role": "system", "content": _build_system_prompt()},
        {"role": "user", "content": user_query},
    ]

    MAX_HOPS = 5
    last_tool_result = None

    for hop in range(MAX_HOPS):
        try:
            resp = client.chat.completions.create(
                model=_MODEL,
                messages=messages,
                tools=role_schema,
                tool_choice='auto',
                temperature=0.1,
            )
        except Exception as e:
            # Типичная ошибка Groq: 'tool_use_failed' — модель не смогла вызвать
            # функцию. Переключаемся на plain-ответ, используя то, что уже успели
            # собрать (если что-то успели).
            err_str = str(e)
            traceback.print_exc()
            db.session.rollback()
            if 'tool_use_failed' in err_str or 'Failed to call a function' in err_str:
                return _fallback_plain_answer(client, user_query, last_tool_result)
            return f'<div class="text-danger">Ошибка ИИ: {e}</div>'

        msg = resp.choices[0].message
        tool_calls = getattr(msg, 'tool_calls', None) or []

        if not tool_calls:
            final = (msg.content or '').strip()
            return _append_link(final or 'Не смог сформировать ответ. Переформулируйте вопрос.',
                                last_tool_result)

        # Сохраняем ассистент-сообщение с tool_calls для продолжения диалога.
        messages.append({
            'role': 'assistant',
            'content': msg.content or '',
            'tool_calls': [
                {
                    'id': tc.id,
                    'type': 'function',
                    'function': {
                        'name': tc.function.name,
                        'arguments': tc.function.arguments or '{}',
                    }
                }
                for tc in tool_calls
            ],
        })

        for tc in tool_calls:
            try:
                args = json.loads(tc.function.arguments or '{}')
            except Exception:
                args = {}
            result = _call_tool_safely(tc.function.name, args, allowed_funcs=role_funcs)
            last_tool_result = result
            messages.append({
                'tool_call_id': tc.id,
                'role': 'tool',
                'name': tc.function.name,
                'content': json.dumps(result, ensure_ascii=False, default=str),
            })

    # Дошли до конца лимита hop-ов — последняя попытка: попросим ответ без tools.
    return _fallback_plain_answer(client, user_query, last_tool_result)
