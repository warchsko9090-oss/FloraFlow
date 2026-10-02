"""Снимок плана выкопки в чат продаж.

Картинка — календарь плана на сегодня и 15 дней вперёд: клиенты, количества,
отгрузки и выходные. Дни стоят по колонкам недели. Без подписи и без кнопок.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from datetime import date, timedelta
from html import escape
from io import BytesIO
from pathlib import Path

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


# Сегодня и ещё 14 дней: 15 календарных дней вперёд, считая сегодняшний.
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
        .options(
            joinedload(ShipmentPlan.order).joinedload(Order.client),
            joinedload(ShipmentPlan.order).joinedload(Order.items),
        )
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


def _order_plants(order) -> int:
    if not order:
        return 0
    return sum(int(it.quantity or 0) for it in (order.items or []))


def _day_cards(bucket: dict) -> tuple[list, list]:
    """Строки клетки: номер заказа и штуки на копку, отдельно на отгрузку."""
    digging = {}
    for task in bucket['tasks']:
        item = task.item
        order = item.order if item else None
        if not order:
            continue
        row = digging.setdefault(order.id, {
            'order_id': order.id,
            'client': (order.client.name if order.client else '') or '',
            'qty': 0,
        })
        row['qty'] += int(task.planned_qty or 0)
    ships = []
    for ship in bucket['ships']:
        order = ship.order
        ships.append({
            'order_id': order.id if order else 0,
            'client': (order.client.name if order and order.client else '') or '',
            'qty': _order_plants(order),
        })
    dig_rows = sorted(digging.values(), key=lambda row: (-row['qty'], row['order_id']))
    return dig_rows, ships


def _order_label(order) -> str:
    if not order:
        return 'заказ'
    name = ''
    if order.client and order.client.name:
        name = order.client.name.strip()
    return f'№{order.id}' + (f' · {name}' if name else '')


def _public_base() -> str:
    """Адрес сайта, чтобы ссылка из Telegram открывала заказ, а не относительный путь."""
    env = (os.environ.get('APP_BASE_URL') or '').strip().rstrip('/')
    if env:
        return env
    try:
        from app.telegram import default_miniapp_url
        mini = default_miniapp_url()
    except Exception:
        mini = ''
    if mini.startswith('https://'):
        return mini.split('/tg/', 1)[0].rstrip('/')
    try:
        from flask import has_request_context, request
        if has_request_context():
            return request.url_root.rstrip('/')
    except Exception:
        pass
    return ''


def _order_link(order) -> str:
    label = escape(_order_label(order))
    order_id = getattr(order, 'id', None)
    if not order_id:
        return label
    href = escape(f'{_public_base()}/order/{order_id}', quote=True)
    return f'<a href="{href}">{label}</a>'


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
    crew_days = 0
    brig_days = 0
    day = start
    while day <= end:
        bucket = by_day[day]
        n, q = _day_bits(bucket)
        orders += n
        qty += q
        ships += len(bucket['ships'])
        if bucket['crew_off']:
            crew_days += 1
        if bucket['brig_off']:
            brig_days += 1
        day += timedelta(days=1)
    lines = [
        '<b>План выкопки</b>',
        f'{_fmt_day(start)} — {_fmt_day(end)}',
        '',
        '<b>Копка</b>',
        f'{orders} зак. · {qty} шт' if orders else 'нет',
        '',
        '<b>Отгрузка</b>',
        str(ships) if ships else 'нет',
        '',
        '<b>Выходные</b>',
        f'Бригада: {crew_days} дн.' if crew_days else 'Бригада: нет',
        f'Бригадир: {brig_days} дн.' if brig_days else 'Бригадир: нет',
        '',
        'Нажмите день — состав отдельно по копке и отгрузке.',
    ]
    return '\n'.join(lines)


def day_text(day: date, bucket: dict) -> str:
    n_orders, qty = _day_bits(bucket)
    lines = [f'<b>{_WD_LONG[day.weekday()]}, {_fmt_day(day)}</b>', '']

    lines.append('<b>Выходные</b>')
    if bucket['crew_off']:
        lines.append('• Бригада')
    if bucket['brig_off']:
        lines.append('• Бригадир')
    if not bucket['crew_off'] and not bucket['brig_off']:
        lines.append('нет')
    lines.append('')

    head = '<b>Копка</b>'
    if n_orders:
        head += f' · {n_orders} зак. · {qty} шт'
    lines.append(head)
    grouped = {}
    for task in bucket['tasks']:
        item = task.item
        order = item.order if item else None
        grouped.setdefault(order.id if order else 0, {'order': order, 'lines': []})
        grouped[order.id if order else 0]['lines'].append(_task_line(task))
    if not grouped:
        lines.append('нет')
    for pack in grouped.values():
        lines.append(_order_link(pack['order']))
        for line in pack['lines'][:8]:
            lines.append('• ' + escape(line))
        extra = len(pack['lines']) - 8
        if extra > 0:
            lines.append(f'• ещё {extra}')
    lines.append('')

    ships = bucket['ships']
    ship_head = '<b>Отгрузка</b>'
    if ships:
        ship_head += f' · {len(ships)}'
    lines.append(ship_head)
    if not ships:
        lines.append('нет')
    for ship in ships:
        note = f' — {ship.comment.strip()}' if (ship.comment or '').strip() else ''
        lines.append('• ' + _order_link(ship.order) + escape(note))
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
    cell_w = 168
    busiest = 1
    probe = grid_start
    while probe <= grid_end:
        bucket = by_day.get(probe)
        if bucket:
            dig_rows, ship_rows = _day_cards(bucket)
            busiest = max(busiest, len(dig_rows) + len(ship_rows))
        probe += timedelta(days=1)
    cell_h = 58 + busiest * 24 + 12
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
            crew = bool(bucket and bucket['crew_off'])
            brig = bool(bucket and bucket['brig_off'])
            if crew and brig:
                fill = '#fce7f3'
            elif crew:
                fill = '#fce7f3'
            elif brig:
                fill = '#fef9c3'
            elif in_range:
                fill = '#FFFFFF'
            else:
                fill = '#EFEBE1'
            outline = '#1f7a3a' if day == selected else '#d7e3d9'
            width_px = 3 if day == selected else 1
            box = (x + 4, y + 4, x + cell_w - 6, y + cell_h - 8)
            draw.rounded_rectangle(box, radius=10, fill=fill, outline=outline, width=width_px)
            if crew and brig:
                mid = (box[1] + box[3]) // 2
                draw.rectangle((box[0] + 8, mid, box[2] - 8, box[3] - 12), fill='#fef9c3')
                draw.rounded_rectangle(box, radius=10, outline=outline, width=width_px)
            if not in_range:
                day += timedelta(days=1)
                continue
            num_color = '#111814' if day >= today else '#9aa396'
            draw.text((x + 14, y + 12), str(day.day), font=num_f, fill=num_color)
            if day == today:
                draw.rounded_rectangle((x + 52, y + 18, x + 118, y + 38), radius=6, fill='#eaf4eb')
                draw.text((x + 58, y + 20), 'сегодня', font=tiny_f, fill='#1f7a3a')
            dig_rows, ship_rows = _day_cards(bucket)
            yy = y + 48

            def _chip(row, fill, qty_fill, text_fill):
                nonlocal yy
                label = f"#{row['order_id']}"
                if row.get('client'):
                    label += ' ' + row['client']
                qty_s = str(row['qty'])
                draw.rounded_rectangle((x + 8, yy, x + cell_w - 12, yy + 20), radius=4, fill=fill)
                draw.text((x + 12, yy + 2), label[:18], font=tiny_f, fill=text_fill)
                qty_w = 8 + 7 * len(qty_s)
                draw.rounded_rectangle(
                    (x + cell_w - 16 - qty_w, yy + 2, x + cell_w - 16, yy + 18),
                    radius=3, fill=qty_fill,
                )
                draw.text((x + cell_w - 14 - qty_w, yy + 2), qty_s, font=tiny_f, fill='#111814')
                yy += 24

            for row in dig_rows:
                _chip(row, '#dbeafe', '#eff6ff', '#1e3a8a')
            for row in ship_rows:
                _chip(row, '#e11d48', '#ffffff', '#ffffff')
            day += timedelta(days=1)

    ly = height - pad - 28
    draw.rounded_rectangle((pad, ly, pad + 18, ly + 18), radius=4, fill='#1d4ed8')
    draw.text((pad + 26, ly - 1), 'копка', font=small_f, fill='#111814')
    draw.ellipse((pad + 110, ly + 2, pad + 126, ly + 18), fill='#e11d48')
    draw.text((pad + 134, ly - 1), 'отгрузка', font=small_f, fill='#111814')
    draw.rounded_rectangle((pad + 250, ly, pad + 268, ly + 18), radius=4, fill='#f472b6')
    draw.text((pad + 276, ly - 1), 'бригада', font=small_f, fill='#111814')
    draw.rounded_rectangle((pad + 390, ly, pad + 408, ly + 18), radius=4, fill='#facc15')
    draw.text((pad + 416, ly - 1), 'бригадир', font=small_f, fill='#111814')

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
        text = text[:999]
        open_tag = text.rfind('<')
        if open_tag > text.rfind('>'):
            text = text[:open_tag]
        text = text.rstrip() + '…'
    png = render_png(start, end, by_day, selected)
    return text, keyboard(start, end, by_day, selected=selected), start, end, selected, png


def plan_chat_id() -> str:
    """Куда слать картинку плана.

    TG_DIG_PLAN_CHAT_ID — отдельный чат для проверки. Пока переменная задана,
    группа продаж это сообщение не получает. Пусто — обычный чат отгрузок.
    """
    return (os.environ.get('TG_DIG_PLAN_CHAT_ID') or '').strip()


def _range_label(start: date, end: date) -> str:
    if start.year == end.year and start.month == end.month:
        return f'{start.day}–{end.day} {_MONTHS[start.month]} {start.year}'
    if start.year == end.year:
        return f'{start.day} {_MONTHS[start.month]} — {end.day} {_MONTHS[end.month]} {start.year}'
    return f'{_fmt_day(start)} {start.year} — {_fmt_day(end)} {end.year}'


def _horizon_weeks(start: date, end: date) -> list:
    """Три недели сетки: колонки пн–вс, данные только с сегодня на 15 дней."""
    from app.digging import build_planning_weeks
    center = start - timedelta(days=start.weekday())
    weeks = build_planning_weeks(center, weeks_before=0, weeks_after=2)
    out = []
    for week in weeks:
        row = dict(week)
        days = []
        for day in week['days']:
            cell = dict(day)
            cell['in_range'] = start <= day['date_obj'] <= end
            days.append(cell)
        row['days'] = days
        out.append(row)
    return out


def _browser_exe() -> str:
    env = (os.environ.get('PLAN_SHOT_BROWSER') or '').strip()
    pf = os.environ.get('PROGRAMFILES', r'C:\Program Files')
    pfx = os.environ.get('PROGRAMFILES(X86)', r'C:\Program Files (x86)')
    local = os.environ.get('LOCALAPPDATA', '')
    candidates = [
        env,
        os.path.join(pf, r'Microsoft\Edge\Application\msedge.exe'),
        os.path.join(pfx, r'Microsoft\Edge\Application\msedge.exe'),
        os.path.join(pf, r'Google\Chrome\Application\chrome.exe'),
        os.path.join(local, r'Google\Chrome\Application\chrome.exe'),
        '/usr/bin/google-chrome',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return ''


def _crop_shot(raw: bytes) -> bytes:
    from PIL import Image
    im = Image.open(BytesIO(raw)).convert('RGB')
    w, h = im.size
    px = im.load()

    def is_margin(x, y):
        r, g, b = px[x, y]
        return r > 240 and g < 40 and 140 < b < 200

    def row_margin(y):
        return all(is_margin(x, y) for x in range(0, w, 4))

    top = 0
    while top < h and row_margin(top):
        top += 1
    bottom = h - 1
    while bottom > top and row_margin(bottom):
        bottom -= 1
    if bottom <= top:
        raise RuntimeError('Снимок календаря пустой')
    cropped = im.crop((0, top, w, bottom + 1))
    buf = BytesIO()
    cropped.save(buf, format='PNG', optimize=True)
    return buf.getvalue()


def _screenshot_html(html: str, height: int) -> bytes:
    browser = _browser_exe()
    if not browser:
        raise RuntimeError('Не найден Edge или Chrome, чтобы снять календарь')
    folder = tempfile.mkdtemp(prefix='ff-plan-')
    try:
        html_path = os.path.join(folder, 'plan.html')
        png_path = os.path.join(folder, 'plan.png')
        with open(html_path, 'w', encoding='utf-8') as handle:
            handle.write(html)
        cmd = [
            browser,
            '--headless=new',
            '--disable-gpu',
            '--hide-scrollbars',
            '--force-device-scale-factor=1',
            '--allow-file-access-from-files',
            f'--window-size=1440,{max(height, 400)}',
            f'--screenshot={png_path}',
            Path(html_path).as_uri(),
        ]
        subprocess.run(cmd, cwd=folder, timeout=45, check=False, capture_output=True)
        if not os.path.isfile(png_path):
            alt = os.path.join(folder, 'screenshot.png')
            if os.path.isfile(alt):
                png_path = alt
            else:
                raise RuntimeError('Браузер не сохранил снимок календаря')
        with open(png_path, 'rb') as handle:
            return _crop_shot(handle.read())
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def _shot_height(weeks: list) -> int:
    """Высота окна браузера: строка растёт от числа заказов на копку и отгрузку."""
    height = 140
    for week in weeks:
        chips = 1
        for day in week['days']:
            if not day.get('in_range'):
                continue
            n = len(day.get('clients_summary') or []) + len(day.get('shipments') or [])
            chips = max(chips, n)
        height += 56 + chips * 24 + 16
    return height + 48


def render_plan_shot() -> bytes:
    """PNG календаря: сегодня и 15 дней вперёд, дни по колонкам недели.

    На машине без Edge и Chrome (контейнер Amvera) уходит прежняя картинка
    того же окна, чтобы чат не остался без плана.
    """
    from flask import render_template
    start, end = horizon()
    weeks = _horizon_weeks(start, end)
    height = _shot_height(weeks)
    html = render_template(
        'digging/plan_shot.html',
        weeks=weeks,
        range_label=_range_label(start, end),
    )
    try:
        return _screenshot_html(html, height)
    except RuntimeError as exc:
        import logging
        logging.getLogger(__name__).warning('plan shot fallback: %s', exc)
        return render_png(start, end, _load(start, end), None)


def send_week() -> tuple[bool, str]:
    from app.telegram import send_photo_bytes
    png = render_plan_shot()
    target = plan_chat_id()
    if target:
        return send_photo_bytes(png, filename='plan.png', caption='', chat_id=target)
    return send_photo_bytes(png, filename='plan.png', caption='', chat_type='digging')


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
    # Ответить сразу: пока загрузка фото висит, Telegram шлёт тот же тап снова.
    answer_callback_query(cb_id)
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
