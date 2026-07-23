from datetime import datetime, timedelta

from app.models import db, PatentPeriod, PatentPayment, PatentReminderLog, ForeignEmployeeProfile
from app.utils import msk_today
from app.telegram import send_message as _send_tg_message_impl

REMINDER_WINDOWS = {
    'day7': (4, 7),
    'day3': (0, 3),
}
REMINDER_LABEL_DAYS = {
    'day7': 7,
    'day3': 3,
}


def _send_tg_message(text):
    return _send_tg_message_impl(text, chat_type="patents")


def _employee_name(period):
    profile = ForeignEmployeeProfile.query.filter_by(employee_id=period.employee_id).first()
    if profile and profile.full_name:
        return profile.full_name
    if period.employee and period.employee.name:
        return period.employee.name
    return 'Сотрудник'


def _reminder_already_sent(period_id: int, reminder_type: str) -> bool:
    return PatentReminderLog.query.filter_by(
        patent_period_id=period_id,
        reminder_type=reminder_type,
    ).first() is not None


def _pending_reminder_type(delta: int, period_id: int) -> str | None:
    """Определяет, какое напоминание нужно сейчас (с догоном в своём окне)."""
    if REMINDER_WINDOWS['day3'][0] <= delta <= REMINDER_WINDOWS['day3'][1]:
        if not _reminder_already_sent(period_id, 'day3'):
            return 'day3'
        return None
    if REMINDER_WINDOWS['day7'][0] <= delta <= REMINDER_WINDOWS['day7'][1]:
        if not _reminder_already_sent(period_id, 'day7'):
            return 'day7'
        return None
    return None


def _is_patent_paid_for_reminder(period, reminder_type: str, today) -> bool:
    """Пропускаем, если в окне напоминания уже зафиксирована оплата."""
    if not period.end_date:
        return True
    horizon = REMINDER_LABEL_DAYS.get(reminder_type, 0)
    if horizon <= 0:
        return False
    window_start = period.end_date - timedelta(days=horizon)
    return PatentPayment.query.filter(
        PatentPayment.patent_period_id == period.id,
        PatentPayment.payment_date >= window_start,
        PatentPayment.payment_date <= today,
    ).first() is not None


def run_patent_reminders_job():
    today = msk_today()
    notify_rows = []

    for period in PatentPeriod.query.filter_by(is_current=True, status='active').all():
        if not period.end_date:
            continue
        delta = (period.end_date - today).days
        if delta < 0:
            continue

        reminder_type = _pending_reminder_type(delta, period.id)
        if not reminder_type:
            continue
        if _is_patent_paid_for_reminder(period, reminder_type, today):
            continue

        notify_rows.append((period, reminder_type, _employee_name(period)))

    if not notify_rows:
        return 0, "nothing_to_send"

    lines = ["<b>Напоминание по оплате патентов</b>", ""]
    for period, reminder_type, employee_name in notify_rows:
        label_days = REMINDER_LABEL_DAYS[reminder_type]
        lines.append(
            f"• {employee_name} — срок патента до {period.end_date.strftime('%d.%m.%Y')} "
            f"(напоминание за {label_days} дн.)"
        )
    message_text = "\n".join(lines)

    ok, send_result = _send_tg_message(message_text)
    if not ok:
        return 0, f"send_failed: {send_result}"

    for period, reminder_type, _name in notify_rows:
        db.session.add(PatentReminderLog(
            patent_period_id=period.id,
            reminder_type=reminder_type,
            target_date=today,
            sent_at=datetime.utcnow(),
            message_text=message_text,
        ))
    db.session.commit()
    return len(notify_rows), "sent"
