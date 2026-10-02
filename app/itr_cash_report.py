"""Пятничная сводка поступлений ДС для группы ИТР.

Каждую пятницу в 9:00 МСК бот пишет в чат TG_CHAT_ID_ITR факт и план
поступлений за текущий календарный месяц (по Москве).

План — CashflowPlan на этот месяц (тот же, что на вкладке кешфлоу).
Факт — оплаты по заказам (Payment) с начала месяца по сегодня,
без списаний долга (writeoff). Нал, безнал и бартер входят в факт.

Перенос недовыполнения задаётся вручную в админке и живёт отдельно:
в CashflowPlan и на экране кешфлоу он не пишется, виден только в этом сообщении.
"""
from __future__ import annotations

import calendar
import json
import os
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from html import escape

from sqlalchemy import func

from app.models import db, AppSetting, CashflowPlan, Payment
from app.utils import msk_today

_LAST_SENT_KEY = 'itr_cash_in_last_sent'
_CARRY_KEY = 'itr_cash_carry'
_TYPE_ORDER = ('cashless', 'cash', 'barter')
_MONTH_NOM = {
    1: 'январь', 2: 'февраль', 3: 'март', 4: 'апрель',
    5: 'май', 6: 'июнь', 7: 'июль', 8: 'август',
    9: 'сентябрь', 10: 'октябрь', 11: 'ноябрь', 12: 'декабрь',
}
_MONTH_GEN = {
    1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля',
    5: 'мая', 6: 'июня', 7: 'июля', 8: 'августа',
    9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря',
}
_MONTH_PREP = {
    1: 'январе', 2: 'феврале', 3: 'марте', 4: 'апреле',
    5: 'мае', 6: 'июне', 7: 'июле', 8: 'августе',
    9: 'сентябре', 10: 'октябре', 11: 'ноябре', 12: 'декабре',
}


def _money(value) -> str:
    n = int(Decimal(value or 0).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    sign = '−' if n < 0 else ''
    digits = f'{abs(n):,}'.replace(',', ' ')
    return f'{sign}{digits} ₽'


def _dec(value) -> Decimal:
    return Decimal(value or 0)


def parse_money(raw: str) -> Decimal:
    """Сумма из админки: «1 200 000», «1200000,50», «1,2 млн», «500 тыс»."""
    text = (raw or '').strip().lower().replace('\xa0', ' ').replace('₽', '')
    text = text.replace('руб.', '').replace('руб', '').strip()
    mult = Decimal(1)
    if text.endswith('млн'):
        mult = Decimal(1_000_000)
        text = text[:-3].strip()
    elif text.endswith('тыс'):
        mult = Decimal(1000)
        text = text[:-3].strip()
    text = text.replace(' ', '')
    if not text:
        raise ValueError('Укажите сумму недовыполнения')
    if text.count(',') == 1 and text.count('.') == 0:
        text = text.replace(',', '.')
    elif text.count(',') == 1 and text.count('.') >= 1:
        text = text.replace('.', '').replace(',', '.')
    elif text.count('.') > 1:
        text = text.replace('.', '')
    try:
        value = Decimal(text) * mult
    except InvalidOperation as exc:
        raise ValueError('Не разобрал сумму. Пример: 1 200 000 или 1,2 млн') from exc
    if value <= 0:
        raise ValueError('Сумма должна быть больше нуля. Чтобы убрать перенос, нажмите «Убрать».')
    return value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def get_carry() -> dict | None:
    """Ручной перенос на конкретный месяц сообщения. None — не задан."""
    row = AppSetting.query.get(_CARRY_KEY)
    if row is None or not (row.value or '').strip():
        return None
    try:
        data = json.loads(row.value)
        year = int(data['year'])
        month = int(data['month'])
        amount = Decimal(str(data['amount']))
    except (TypeError, ValueError, KeyError, InvalidOperation, json.JSONDecodeError):
        return None
    if not (1 <= month <= 12) or amount <= 0 or year < 2000 or year > 2100:
        return None
    return {'year': year, 'month': month, 'amount': amount}


def carry_for_month(year: int, month: int) -> Decimal:
    saved = get_carry()
    if not saved or saved['year'] != int(year) or saved['month'] != int(month):
        return Decimal(0)
    return saved['amount']


def set_carry(year: int, month: int, amount: Decimal) -> str:
    """Пишет перенос в настройку. CashflowPlan не трогает."""
    year = int(year)
    month = int(month)
    if not (1 <= month <= 12) or year < 2000 or year > 2100:
        raise ValueError('Укажите месяц и год, на сообщения которого вешаем перенос')
    amount = _dec(amount).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    if amount <= 0:
        raise ValueError('Сумма должна быть больше нуля')
    payload = json.dumps({
        'year': year,
        'month': month,
        'amount': format(amount, 'f'),
    }, ensure_ascii=False)
    row = AppSetting.query.get(_CARRY_KEY)
    if row is None:
        row = AppSetting(key=_CARRY_KEY, value=payload)
        db.session.add(row)
    else:
        row.value = payload
    db.session.commit()
    return (
        f'Перенос {_money(amount)} вошёл в план сообщений за '
        f'{_MONTH_NOM[month]} {year}. Отдельной строкой в чат не пишется и в кешфлоу не попадает.'
    )


def clear_carry() -> None:
    row = AppSetting.query.get(_CARRY_KEY)
    if row is not None:
        db.session.delete(row)
        db.session.commit()


def carry_admin_state(today: date | None = None) -> dict:
    """Поля формы: если перенос уже есть — его месяц, иначе текущий."""
    today = today or msk_today()
    saved = get_carry()
    year = saved['year'] if saved else today.year
    month = saved['month'] if saved else today.month
    summary = ''
    amount_input = ''
    if saved:
        src = 12 if saved['month'] == 1 else saved['month'] - 1
        src_name = _MONTH_GEN[src]
        if saved['month'] == 1:
            src_name = f'{src_name} {saved["year"] - 1}'
        amount_input = f'{int(saved["amount"]):,}'.replace(',', ' ')
        if saved['amount'] != saved['amount'].to_integral_value():
            amount_input = format(saved['amount'], 'f').replace('.', ',')
        summary = (
            f'Сейчас в плане сообщений за {_MONTH_NOM[saved["month"]]} {saved["year"]} '
            f'уже сидит перенос {_money(saved["amount"])} с {src_name}.'
        )
    return {
        'year': year,
        'month': month,
        'amount_input': amount_input,
        'summary': summary,
        'active': saved is not None,
        'months': [{'n': n, 'name': _MONTH_NOM[n]} for n in range(1, 13)],
    }


def month_snapshot(as_of: date | None = None) -> dict:
    """Факт и план поступлений за месяц даты as_of (по умолчанию сегодня МСК)."""
    as_of = as_of or msk_today()
    year, month, day = as_of.year, as_of.month, as_of.day
    days = calendar.monthrange(year, month)[1]
    start = date(year, month, 1)

    plan_row = CashflowPlan.query.filter_by(year=year, month=month).first()
    plan = _dec(plan_row.amount) if plan_row is not None else Decimal(0)
    plan_set = plan_row is not None and plan > 0

    rows = db.session.query(
        func.coalesce(Payment.payment_type, 'cashless'),
        func.sum(Payment.amount),
    ).filter(
        Payment.date >= start,
        Payment.date <= as_of,
        Payment.cash_inflow_filter(),
    ).group_by(func.coalesce(Payment.payment_type, 'cashless')).all()

    by_type = []
    fact = Decimal(0)
    for ptype, amount in rows:
        amt = _dec(amount)
        if amt == 0:
            continue
        fact += amt
        by_type.append((str(ptype or 'cashless'), amt))

    def _sort_key(item):
        name = item[0]
        try:
            return (_TYPE_ORDER.index(name), name)
        except ValueError:
            return (len(_TYPE_ORDER), name)

    by_type.sort(key=_sort_key)
    carry = carry_for_month(year, month)
    return {
        'as_of': as_of,
        'year': year,
        'month': month,
        'day': day,
        'days': days,
        'plan': plan,
        'plan_set': plan_set,
        'carry': carry,
        'fact': fact,
        'by_type': by_type,
    }


def _message_parts(snap: dict) -> tuple[str, str, str]:
    """Заголовок, факт и план. План — месяц из кешфлоу плюс ручной перенос."""
    prep = _MONTH_PREP.get(snap['month'], str(snap['month']))
    gen = _MONTH_GEN.get(snap['month'], prep)
    carry = _dec(snap.get('carry') or 0)
    target = _dec(snap['plan']) + carry
    if snap.get('plan_set') or carry > 0:
        plan_text = _money(target)
    else:
        plan_text = 'не задан'
    title = f'Поступления ДС в {prep} на {snap["day"]} {gen}'
    return title, _money(snap['fact']), plan_text


def render_lines(snap: dict, *, manual: bool = False) -> list[str]:
    """Две строки: месяц и дата, затем факт / план."""
    del manual
    title, fact_text, plan_text = _message_parts(snap)
    return [title, f'факт {fact_text} / план {plan_text}']


def render_plain(snap: dict, *, manual: bool = False) -> str:
    return '\n'.join(render_lines(snap, manual=manual))


def render_html(snap: dict, *, manual: bool = False) -> str:
    """Тот же короткий текст. Суммы жирным."""
    del manual
    title, fact_text, plan_text = _message_parts(snap)
    return (
        f'{escape(title)}\n'
        f'факт <b>{escape(fact_text)}</b> / план <b>{escape(plan_text)}</b>'
    )


def example_snapshots() -> list[dict]:
    """Типовые пятницы — для примеров на странице админки."""
    return [
        {
            'title': 'Середина месяца, факт ниже ровного темпа',
            'as_of': date(2026, 10, 16),
            'year': 2026, 'month': 10, 'day': 16, 'days': 31,
            'plan': Decimal(8000000), 'plan_set': True,
            'fact': Decimal(2100000),
            'by_type': [('cashless', Decimal(1900000)), ('cash', Decimal(200000))],
        },
        {
            'title': 'Ближе к концу месяца, почти план',
            'as_of': date(2026, 10, 23),
            'year': 2026, 'month': 10, 'day': 23, 'days': 31,
            'plan': Decimal(8000000), 'plan_set': True,
            'fact': Decimal(6400000),
            'by_type': [('cashless', Decimal(6100000)), ('cash', Decimal(300000))],
        },
        {
            'title': 'План закрыт с запасом',
            'as_of': date(2026, 10, 30),
            'year': 2026, 'month': 10, 'day': 30, 'days': 31,
            'plan': Decimal(8000000), 'plan_set': True,
            'fact': Decimal(8450000),
            'by_type': [('cashless', Decimal(7900000)), ('cash', Decimal(400000)), ('barter', Decimal(150000))],
        },
        {
            'title': 'План на месяц не заведён',
            'as_of': date(2026, 7, 3),
            'year': 2026, 'month': 7, 'day': 3, 'days': 31,
            'plan': Decimal(0), 'plan_set': False,
            'fact': Decimal(640000),
            'by_type': [('cashless', Decimal(640000))],
        },
        {
            'title': 'Недовыполнение сентября перенесено на октябрь',
            'as_of': date(2026, 10, 2),
            'year': 2026, 'month': 10, 'day': 2, 'days': 31,
            'plan': Decimal(8000000), 'plan_set': True,
            'carry': Decimal(1200000),
            'fact': Decimal(900000),
            'by_type': [('cashless', Decimal(900000))],
        },
    ]


def example_messages() -> list[dict]:
    return [
        {'title': snap['title'], 'text': render_plain(snap)}
        for snap in example_snapshots()
    ]


def destination_note() -> dict:
    """Куда уйдёт сообщение. TG_TEST_CHAT_ID перехватывает все чаты, как и остальные рассылки."""
    test_id = (os.environ.get('TG_TEST_CHAT_ID') or '').strip()
    itr_id = (os.environ.get('TG_CHAT_ID_ITR') or '').strip()
    token = bool((os.environ.get('TG_BOT_TOKEN') or '').strip())
    if test_id:
        where = 'test'
        hint = 'Сейчас задан TG_TEST_CHAT_ID: отправка уйдёт в тестовый чат, не в рабочую группу ИТР.'
    elif itr_id:
        where = 'itr'
        hint = 'Уйдёт в группу из TG_CHAT_ID_ITR.'
    else:
        where = 'missing'
        hint = 'TG_CHAT_ID_ITR не задана — бот не знает, куда писать. Рабочая пятничная отправка тоже пропустится.'
    if not token:
        hint += ' Нет TG_BOT_TOKEN — Telegram сообщение не примет.'
    return {'where': where, 'hint': hint, 'has_token': token}


def _already_sent_today(as_of: date) -> bool:
    row = AppSetting.query.get(_LAST_SENT_KEY)
    return bool(row and (row.value or '').strip() == as_of.isoformat())


def _mark_sent(as_of: date) -> None:
    row = AppSetting.query.get(_LAST_SENT_KEY)
    if row is None:
        row = AppSetting(key=_LAST_SENT_KEY, value=as_of.isoformat())
        db.session.add(row)
    else:
        row.value = as_of.isoformat()
    db.session.commit()


def send_itr_cash_report(*, manual: bool = False, as_of: date | None = None) -> tuple[bool, str]:
    """Отправить сводку. manual=True — кнопка в админке, без отметки «уже слали сегодня»."""
    snap = month_snapshot(as_of)
    if not manual and _already_sent_today(snap['as_of']):
        return True, f'За {snap["as_of"].isoformat()} сводка уже уходила, повтор пропускаю'
    dest = destination_note()
    if dest['where'] == 'missing' or not dest['has_token']:
        return False, 'Не отправилось. ' + dest['hint']
    from app.telegram import send_message
    ok, err = send_message(render_html(snap, manual=manual), chat_type='itr')
    if not ok:
        return False, f'Не отправилось: {err}'
    if not manual:
        _mark_sent(snap['as_of'])
    if dest['where'] == 'test':
        return True, 'Тестовое сообщение ушло в чат TG_TEST_CHAT_ID'
    return True, 'Сообщение ушло в группу ИТР'
