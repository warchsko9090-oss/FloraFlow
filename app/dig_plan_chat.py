"""План выкопки обычным сообщением в чат продаж.

Не мини-приложение: под сообщением кнопки дней. Нажатие правит это же
сообщение и показывает, что копают и что отгружают в выбранный день.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from html import escape

from sqlalchemy.orm import joinedload

from app.models import (
    db, DiggingTask, DiggingCalendarMark, Order, OrderItem, ShipmentPlan,
)
from app.utils import msk_today

_WD_SHORT = ('пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс')
_WD_LONG = (
    'Понедельник', 'Вторник', 'Среда', 'Четверг',
    'Пятница', 'Суббота', 'Воскресенье',
)
_MONTHS = (
    '', 'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
    'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря',
)


# Сегодня и ещё 14 дней — две недели вперёд, включая сегодняшний.
HORIZON_DAYS = 15


def horizon(anchor: date | None = None) -> tuple[date, date]:
    start = anchor or msk_today()
    return start, start + timedelta(days=HORIZON_DAYS - 1)


def _fmt_day(d: date) -> str:
    return f'{d.day} {_MONTHS[d.month]}'


def _can_send(user) -> bool:
    role = (getattr(user, 'role', None) or '')
    return role in ('admin', 'executive', 'user', 'user2', 'brigadier', 'shop_manager')


def _load(start: date, end: date):
    tasks = (
        DiggingTask.query
        .options(
            joinedload(DiggingTask.item).joinedload(OrderItem.plant),
            joinedload(DiggingTask.item).joinedload(OrderItem.size),
            joinedload(DiggingTask.item).joinedload(OrderItem.order).joinedload(Order.client),
        )
        .filter(DiggingTask.planned_date >= start, DiggingTask.planned_date <= end)
        .all()
    )
    ships = (
        ShipmentPlan.query
        .options(joinedload(ShipmentPlan.order).joinedload(Order.client))
        .filter(ShipmentPlan.planned_date >= start, ShipmentPlan.planned_date <= end)
        .all()
    )
    marks = (
        DiggingCalendarMark.query
        .filter(DiggingCalendarMark.planned_date >= start, DiggingCalendarMark.planned_date <= end)
        .all()
    )
    by_day = {}
    day = start
    while day <= end:
        by_day[day] = {'tasks': [], 'ships': [], 'crew_off': False, 'brig_off': False}
        day += timedelta(days=1)
    for task in tasks:
        bucket = by_day.get(task.planned_date)
        if bucket is not None:
            bucket['tasks'].append(task)
    for ship in ships:
        bucket = by_day.get(ship.planned_date)
        if bucket is not None:
            bucket['ships'].append(ship)
    for mark in marks:
        bucket = by_day.get(mark.planned_date)
        if bucket is None:
            continue
        if mark.kind == DiggingCalendarMark.KIND_CREW_OFF:
            bucket['crew_off'] = True
        elif mark.kind == DiggingCalendarMark.KIND_BRIGADIER_OFF:
            bucket['brig_off'] = True
    return by_day


def _order_label(order) -> str:
    if not order:
        return 'заказ'
    name = ''
    if order.client and order.client.name:
        name = order.client.name.strip()
    return f'№{order.id}' + (f' · {name}' if name else '')


def _task_line(task: DiggingTask) -> str:
    item = task.item
    plant = item.plant.name if item and item.plant and item.plant.name else 'растение'
    size = item.size.name if item and item.size and item.size.name else ''
    qty = int(task.planned_qty or 0)
    done = ' ✓' if (task.status or '') == 'done' else ''
    tail = f', {size}' if size else ''
    return f'{plant}{tail} — {qty} шт{done}'


def _day_bits(bucket: dict) -> tuple[int, int]:
    orders = set()
    qty = 0
    for task in bucket['tasks']:
        qty += int(task.planned_qty or 0)
        item = task.item
        if item and item.order_id:
            orders.add(item.order_id)
    return len(orders), qty


def keyboard(start: date, end: date, by_day: dict, *, selected: date | None) -> dict:
    rows = []
    row = []
    day = start
    while day <= end:
        bucket = by_day[day]
        n_orders, _qty = _day_bits(bucket)
        label = f'{day.day} {_WD_SHORT[day.weekday()]}'
        if day == selected:
            label = '• ' + label
        if n_orders:
            label += f' · {n_orders}'
        elif bucket['ships']:
            label += ' · отг'
        row.append({'text': label[:32], 'callback_data': f'dp:d:{day.isoformat()}:{start.isoformat()}'})
        if len(row) == 5:
            rows.append(row)
            row = []
        day += timedelta(days=1)
    if row:
        rows.append(row)
    if selected is not None:
        rows.append([{
            'text': '← Весь план',
            'callback_data': f'dp:w:{start.isoformat()}',
        }])
    return {'inline_keyboard': rows}


def overview_text(start: date, end: date, by_day: dict) -> str:
    orders = 0
    qty = 0
    ships = 0
    day = start
    while day <= end:
        n, q = _day_bits(by_day[day])
        orders += n
        qty += q
        ships += len(by_day[day]['ships'])
        day += timedelta(days=1)
    lines = [
        '<b>План выкопки</b>',
        f'{_fmt_day(start)} — {_fmt_day(end)}',
        f'Копка: {orders} зак. · {qty} шт',
    ]
    if ships:
        lines.append(f'Отгрузка: {ships}')
    if not orders and not ships:
        lines.append('На эти две недели копки и отгрузок в плане нет.')
    lines.append('Синий — копка, розовый — отгрузка. Нажмите день.')
    return '\n'.join(lines)


def day_text(day: date, bucket: dict) -> str:
    n_orders, qty = _day_bits(bucket)
    lines = [f'<b>{_WD_LONG[day.weekday()]}, {_fmt_day(day)}</b>']
    if bucket['crew_off']:
        lines.append('Выходной рабочей бригады.')
    if bucket['brig_off']:
        lines.append('Выходной бригадира.')
    if n_orders:
        lines.append(f'Копка: {n_orders} зак. · {qty} шт')
    elif not bucket['ships']:
        lines.append('В плане на этот день пусто.')
    lines.append('')

    grouped = {}
    for task in bucket['tasks']:
        item = task.item
        order = item.order if item else None
        grouped.setdefault(order.id if order else 0, {'order': order, 'lines': []})
        grouped[order.id if order else 0]['lines'].append(_task_line(task))
    for pack in grouped.values():
        lines.append(f'<b>{escape(_order_label(pack["order"]))}</b>')
        for line in pack['lines'][:8]:
            lines.append('• ' + escape(line))
        extra = len(pack['lines']) - 8
        if extra > 0:
            lines.append(f'• ещё {extra}')
        lines.append('')

    if bucket['ships']:
        lines.append('<b>Отгрузка</b>')
        for ship in bucket['ships']:
            note = f' — {ship.comment.strip()}' if (ship.comment or '').strip() else ''
            lines.append('• ' + escape(_order_label(ship.order) + note))
    return '\n'.join(lines).strip()


def _font(size: int, bold: bool = False):
    from PIL import ImageFont
    candidates = (
        [r'C:\Windows\Fonts\segoeuib.ttf', r'C:\Windows\Fonts\arialbd.ttf',
         '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf']
        if bold else
        [r'C:\Windows\Fonts\segoeui.ttf', r'C:\Windows\Fonts\arial.ttf',
         '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf']
    )
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def render_png(start: date, end: date, by_day: dict, selected: date | None) -> bytes:
    """Календарь на сегодня и две недели — картинка для сообщения в чат."""
    from io import BytesIO
    from PIL import Image, ImageDraw

    grid_start = start - timedelta(days=start.weekday())
    grid_end = end + timedelta(days=(6 - end.weekday()))
    weeks = ((grid_end - grid_start).days // 7) + 1
    cell_w, cell_h = 148, 128
    pad, head, dow_h, legend = 28, 78, 28, 46
    width = pad * 2 + cell_w * 7
    height = pad + head + dow_h + cell_h * weeks + legend + pad
    img = Image.new('RGB', (width, height), '#F4F0E6')
    draw = ImageDraw.Draw(img)
    title_f = _font(28, True)
    sub_f = _font(16)
    dow_f = _font(14, True)
    num_f = _font(26, True)
    small_f = _font(14, True)
    tiny_f = _font(13)

    draw.text((pad, pad), 'План выкопки', font=title_f, fill='#111814')
    draw.text((pad, pad + 36), f'{_fmt_day(start)} — {_fmt_day(end)}', font=sub_f, fill='#3D4741')

    y0 = pad + head
    for i, name in enumerate(_WD_SHORT):
        x = pad + i * cell_w
        draw.text((x + 10, y0), name, font=dow_f, fill='#7A7E78')

    today = msk_today()
    day = grid_start
    for w in range(weeks):
        for col in range(7):
            x = pad + col * cell_w
            y = y0 + dow_h + w * cell_h
            in_range = start <= day <= end
            bucket = by_day.get(day) if in_range else None
            fill = '#FFFFFF' if in_range else '#EFEBE1'
            outline = '#1f7a3a' if day == selected else '#d7e3d9'
            width_px = 3 if day == selected else 1
            draw.rounded_rectangle(
                (x + 4, y + 4, x + cell_w - 6, y + cell_h - 8),
                radius=10, fill=fill, outline=outline, width=width_px,
            )
            if not in_range:
                day += timedelta(days=1)
                continue
            num_color = '#111814' if day >= today else '#9aa396'
            draw.text((x + 14, y + 12), str(day.day), font=num_f, fill=num_color)
            if day == today:
                draw.rounded_rectangle((x + 52, y + 18, x + 118, y + 38), radius=6, fill='#eaf4eb')
                draw.text((x + 58, y + 20), 'сегодня', font=tiny_f, fill='#1f7a3a')
            n_orders, qty = _day_bits(bucket)
            yy = y + 52
            if n_orders:
                label = f'{n_orders} зак'
                draw.rounded_rectangle((x + 12, yy, x + 12 + 18 + 8 * len(label), yy + 24), radius=6, fill='#1d4ed8')
                draw.text((x + 20, yy + 3), label, font=small_f, fill='#ffffff')
                draw.text((x + 14, yy + 28), f'{qty} шт', font=tiny_f, fill='#1d4ed8')
                yy += 48
            if bucket['ships']:
                draw.ellipse((x + 14, yy + 4, x + 26, yy + 16), fill='#e11d48')
                draw.text((x + 32, yy), f'отгрузка {len(bucket["ships"])}', font=tiny_f, fill='#e11d48')
            elif bucket['crew_off'] or bucket['brig_off']:
                draw.text((x + 14, yy), 'выходной', font=tiny_f, fill='#9aa396')
            day += timedelta(days=1)

    ly = height - pad - 28
    draw.rounded_rectangle((pad, ly, pad + 18, ly + 18), radius=4, fill='#1d4ed8')
    draw.text((pad + 26, ly - 1), 'копка', font=small_f, fill='#111814')
    draw.ellipse((pad + 110, ly + 2, pad + 126, ly + 18), fill='#e11d48')
    draw.text((pad + 134, ly - 1), 'отгрузка', font=small_f, fill='#111814')

    buf = BytesIO()
    img.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


def render(mode: str, raw_date: str | None, anchor: date | None = None):
    selected = None
    if mode == 'day' and raw_date:
        try:
            selected = date.fromisoformat(raw_date[:10])
        except ValueError:
            selected = None
    start, end = horizon(anchor)
    by_day = _load(start, end)
    if selected not in by_day:
        selected = None
    if selected:
        text = day_text(selected, by_day[selected])
    else:
        text = overview_text(start, end, by_day)
    if len(text) > 1000:
        text = text[:990].rstrip() + '…'
    png = render_png(start, end, by_day, selected)
    return text, keyboard(start, end, by_day, selected=selected), start, end, selected, png


def plan_chat_id() -> str:
    """Куда слать картинку плана.

    TG_DIG_PLAN_CHAT_ID — отдельный чат для проверки. Пока переменная задана,
    группа продаж это сообщение не получает. Пусто — обычный чат отгрузок.
    """
    return (os.environ.get('TG_DIG_PLAN_CHAT_ID') or '').strip()


def send_week() -> tuple[bool, str]:
    from app.telegram import send_photo_bytes
    text, markup, _start, _end, _sel, png = render('week', None)
    target = plan_chat_id()
    if target:
        return send_photo_bytes(
            png, filename='plan.png', caption=text, reply_markup=markup, chat_id=target,
        )
    return send_photo_bytes(
        png, filename='plan.png', caption=text, chat_type='digging', reply_markup=markup,
    )


def handle_callback(cb: dict) -> None:
    from app.telegram import answer_callback_query, edit_chat_message, edit_chat_photo
    data = str(cb.get('data') or '')
    cb_id = cb.get('id')
    msg = cb.get('message') or {}
    chat = msg.get('chat') or {}
    chat_id = chat.get('id')
    message_id = msg.get('message_id')
    parts = data.split(':')
    mode = 'week'
    raw = None
    anchor = None
    if len(parts) >= 3 and parts[0] == 'dp' and parts[1] in ('d', 'w'):
        mode = 'day' if parts[1] == 'd' else 'week'
        raw = parts[2]
        if len(parts) >= 4:
            try:
                anchor = date.fromisoformat(parts[3])
            except ValueError:
                anchor = None
        elif parts[1] == 'w':
            try:
                anchor = date.fromisoformat(parts[2])
            except ValueError:
                anchor = None
    try:
        text, markup, _s, _e, _sel, png = render(mode, raw, anchor=anchor)
        if chat_id and message_id:
            ok, err = edit_chat_photo(chat_id, message_id, png, caption=text, reply_markup=markup)
            if not ok:
                from flask import current_app
                current_app.logger.warning('dig plan edit photo: %s', err)
                edit_chat_message(chat_id, message_id, text, reply_markup=markup)
    except Exception:
        db.session.rollback()
    answer_callback_query(cb_id)
