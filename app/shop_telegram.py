"""Telegram-уведомления оператору витрины по заявкам с сайта."""
import html

from flask import current_app

from app.telegram import send_message as _tg_send_message


def _esc(value) -> str:
    return html.escape(str(value or '').strip()) or '—'


def _fmt_money(amount) -> str:
    try:
        n = int(round(float(amount or 0)))
    except (TypeError, ValueError):
        n = 0
    return f'{n:,}'.replace(',', ' ')


def build_shop_kp_telegram_message(payload: dict, doc_id: int) -> str:
    """HTML-сообщение для Telegram Bot API."""
    lines = [
        '🛒 <b>Новый запрос КП с сайта</b>',
        f'Заявка №{int(doc_id)}',
        '',
        f'<b>Клиент:</b> {_esc(payload.get("customer_name"))}',
        f'<b>Телефон:</b> {_esc(payload.get("phone"))}',
    ]
    email = (payload.get('email') or '').strip()
    if email:
        lines.append(f'<b>E-mail:</b> {_esc(email)}')

    lines.append('')
    lines.append('<b>Состав:</b>')
    for line in payload.get('lines') or []:
        plant = _esc(line.get('plant_name'))
        size = (line.get('size_name') or '').strip()
        if size and size.lower() in ('саженцы', 'товарный'):
            size_label = 'Саженец'
        else:
            size_label = _esc(size) if size else '—'
        qty = int(line.get('qty') or 0)
        price = float(line.get('price') or 0)
        row_sum = float(line.get('sum') or qty * price)
        lines.append(
            f'• {plant} · {size_label} — {qty} шт × {_fmt_money(price)} ₽ = {_fmt_money(row_sum)} ₽'
        )

    lines.append('')
    lines.append(f'<b>Итого:</b> {_fmt_money(payload.get("total_sum"))} ₽')
    return '\n'.join(lines)


def notify_shop_operator_kp(payload: dict, doc_id: int) -> bool:
    """Отправляет КП менеджеру сайта в личку (TG_CHAT_ID_SHOP). Ошибки только в лог."""
    text = build_shop_kp_telegram_message(payload, doc_id)
    ok, err = _tg_send_message(text, chat_type='shop')
    if not ok:
        current_app.logger.warning('Shop KP Telegram notify failed (doc #%s): %s', doc_id, err)
    return ok


def build_shop_on_request_telegram_message(row, source: str = 'shop') -> str:
    source_label = 'Витрина /shop' if source == 'shop' else 'Главная страница'
    plant_name = 'Главная страница'
    size_name = 'Прайс / консультация'
    if getattr(row, 'plant', None):
        plant_name = _esc(row.plant.name)
    if getattr(row, 'size', None):
        size_name = _esc(row.size.name)

    lines = [
        '📩 <b>Новая заявка с сайта</b>',
        f'Заявка #{int(getattr(row, "id", 0) or 0)}',
        f'<b>Источник:</b> {source_label}',
        '',
        f'<b>Клиент:</b> {_esc(getattr(row, "customer_name", ""))}',
        f'<b>Телефон:</b> {_esc(getattr(row, "phone", ""))}',
        f'<b>Позиция:</b> {plant_name}',
        f'<b>Размер:</b> {size_name}',
    ]
    message = (getattr(row, 'message', '') or '').strip()
    if message:
        lines.extend(['', f'<b>Комментарий:</b> {_esc(message)}'])
    return '\n'.join(lines)


def notify_shop_operator_on_request(row, source: str = 'shop') -> bool:
    text = build_shop_on_request_telegram_message(row, source=source)
    ok, err = _tg_send_message(text, chat_type='shop')
    if not ok:
        current_app.logger.warning(
            'Shop on-request Telegram notify failed (request #%s): %s',
            getattr(row, 'id', '?'),
            err,
        )
    return ok
