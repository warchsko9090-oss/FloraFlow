from datetime import datetime

from app.models import db, PatentPeriod, PatentReminderLog, ForeignEmployeeProfile
from app.utils import msk_today
from app.telegram import send_message as _send_tg_message_impl


def _send_tg_message(text):
    return _send_tg_message_impl(text, chat_type="patents")


def run_patent_reminders_job():
    today = msk_today()
    reminder_map = {
        14: 'day14',
        7: 'day7',
    }

    notify_rows = []
    for period in PatentPeriod.query.filter_by(is_current=True, status='active').all():
        if not period.end_date:
            continue
        delta = (period.end_date - today).days
        reminder_type = reminder_map.get(delta)
        if not reminder_type:
            continue
        already_sent = PatentReminderLog.query.filter_by(
            patent_period_id=period.id,
            reminder_type=reminder_type,
            target_date=today
        ).first()
        if already_sent:
            continue
        profile = ForeignEmployeeProfile.query.filter_by(employee_id=period.employee_id).first()
        employee_name = profile.full_name if profile and profile.full_name else period.employee.name
        notify_rows.append((period, reminder_type, delta, employee_name))

    if not notify_rows:
        return 0, "nothing_to_send"

    lines = ["<b>Напоминание по оплате патентов</b>", ""]
    for _period, _rtype, delta, employee_name in notify_rows:
        lines.append(f"• {employee_name} — срок патента до {_period.end_date.strftime('%d.%m.%Y')} (осталось {delta} дн.)")
    message_text = "\n".join(lines)

    ok, send_result = _send_tg_message(message_text)
    if not ok:
        return 0, f"send_failed: {send_result}"

    for period, reminder_type, _delta, _name in notify_rows:
        db.session.add(PatentReminderLog(
            patent_period_id=period.id,
            reminder_type=reminder_type,
            target_date=today,
            sent_at=datetime.utcnow(),
            message_text=message_text
        ))
    db.session.commit()
    return len(notify_rows), "sent"
