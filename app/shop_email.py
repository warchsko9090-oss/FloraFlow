"""Email-уведомления по заявкам с сайта (/shop и главная)."""

from __future__ import annotations

import html
import json
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from flask import current_app

from app.models import AppSetting, db

SETTINGS_KEY = 'shop_email_notifications'
ALL_CATEGORIES = ('kp_draft', 'on_request', 'landing_inquiry')

DEFAULT_SETTINGS = {
    'enabled': False,
    'smtp': {
        'host': '',
        'port': 587,
        'tls': True,
        'username': '',
        'password': '',
        'from_email': '',
        'from_name': 'Княжество',
    },
    'recipients': [],
}


def _deepcopy_json(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False))


def _esc(value: Any) -> str:
    return html.escape(str(value or '').strip()) or '—'


def _fmt_money(amount: Any) -> str:
    try:
        n = int(round(float(amount or 0)))
    except (TypeError, ValueError):
        n = 0
    return f'{n:,}'.replace(',', ' ')


def _normalize_categories(values: Any) -> list[str]:
    vals = values or []
    if isinstance(vals, str):
        vals = [vals]
    out = []
    for item in vals:
        s = str(item or '').strip()
        if s in ALL_CATEGORIES and s not in out:
            out.append(s)
    return out


def _normalize_recipients(items: Any) -> list[dict]:
    out = []
    src = items if isinstance(items, list) else []
    for item in src:
        if not isinstance(item, dict):
            continue
        email = str(item.get('email') or '').strip()
        if not email:
            continue
        categories = _normalize_categories(item.get('categories') or ALL_CATEGORIES)
        out.append({
            'email': email,
            'name': str(item.get('name') or '').strip(),
            'enabled': bool(item.get('enabled', True)),
            'categories': categories or list(ALL_CATEGORIES),
        })
    return out


def normalize_settings(raw: Any) -> dict:
    cfg = _deepcopy_json(DEFAULT_SETTINGS)
    if isinstance(raw, dict):
        smtp_in = raw.get('smtp') if isinstance(raw.get('smtp'), dict) else {}
        cfg['enabled'] = bool(raw.get('enabled', False))
        cfg['smtp']['host'] = str(smtp_in.get('host') or '').strip()
        cfg['smtp']['port'] = int(smtp_in.get('port') or 587)
        cfg['smtp']['tls'] = bool(smtp_in.get('tls', True))
        cfg['smtp']['username'] = str(smtp_in.get('username') or '').strip()
        cfg['smtp']['password'] = str(smtp_in.get('password') or '').strip()
        cfg['smtp']['from_email'] = str(smtp_in.get('from_email') or '').strip()
        cfg['smtp']['from_name'] = str(smtp_in.get('from_name') or 'Княжество').strip() or 'Княжество'
        cfg['recipients'] = _normalize_recipients(raw.get('recipients'))
    return cfg


def load_email_settings() -> dict:
    row = AppSetting.query.get(SETTINGS_KEY)
    if not row or not (row.value or '').strip():
        return normalize_settings({})
    try:
        parsed = json.loads(row.value)
    except Exception:
        parsed = {}
    return normalize_settings(parsed)


def save_email_settings(cfg: dict) -> dict:
    clean = normalize_settings(cfg)
    row = AppSetting.query.get(SETTINGS_KEY)
    payload = json.dumps(clean, ensure_ascii=False)
    if row:
        row.value = payload
    else:
        row = AppSetting(key=SETTINGS_KEY, value=payload)
        db.session.add(row)
    return clean


def _recipient_emails(cfg: dict, category: str) -> list[str]:
    emails = []
    for row in cfg.get('recipients', []):
        if not row.get('enabled', True):
            continue
        if category not in (row.get('categories') or []):
            continue
        email = str(row.get('email') or '').strip()
        if email and email not in emails:
            emails.append(email)
    return emails


def _smtp_ready(cfg: dict) -> bool:
    smtp = cfg.get('smtp', {})
    return bool(
        cfg.get('enabled')
        and smtp.get('host')
        and smtp.get('port')
        and smtp.get('username')
        and smtp.get('password')
        and smtp.get('from_email')
    )


def _send_mail(cfg: dict, to_emails: list[str], subject: str, html_body: str, text_body: str) -> tuple[bool, str]:
    smtp = cfg.get('smtp', {})
    if not _smtp_ready(cfg):
        return False, 'email notifications not configured'
    if not to_emails:
        return False, 'no recipients for category'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'{smtp.get("from_name") or "Княжество"} <{smtp.get("from_email")}>'
    msg['To'] = ', '.join(to_emails)
    msg.attach(MIMEText(text_body, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    host = smtp.get('host')
    port = int(smtp.get('port') or 587)
    username = smtp.get('username')
    password = smtp.get('password')
    use_tls = bool(smtp.get('tls', True))
    timeout = 12

    try:
        if use_tls:
            server = smtplib.SMTP(host, port, timeout=timeout)
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        else:
            server = smtplib.SMTP_SSL(host, port, timeout=timeout, context=ssl.create_default_context())
        try:
            server.login(username, password)
            server.sendmail(smtp.get('from_email'), to_emails, msg.as_string())
        finally:
            server.quit()
    except Exception as exc:
        return False, str(exc)
    return True, 'ok'


def _build_html(subject: str, lines: list[tuple[str, str]], items: list[str] | None = None) -> str:
    rows = ''.join(
        f'<tr><td style="padding:6px 10px;color:#6b7280;">{_esc(k)}</td>'
        f'<td style="padding:6px 10px;color:#111827;"><b>{_esc(v)}</b></td></tr>'
        for k, v in lines
    )
    list_html = ''
    if items:
        list_html = '<ul style="margin:10px 0 0 0;padding-left:18px;">' + ''.join(
            f'<li style="margin:4px 0;">{item}</li>' for item in items
        ) + '</ul>'
    return (
        '<div style="font-family:Arial,sans-serif;max-width:760px;">'
        f'<h2 style="margin:0 0 10px 0;">{_esc(subject)}</h2>'
        '<table style="border-collapse:collapse;">'
        f'{rows}'
        '</table>'
        f'{list_html}'
        '</div>'
    )


def _build_text(subject: str, lines: list[tuple[str, str]], items: list[str] | None = None) -> str:
    body = [subject, '']
    for k, v in lines:
        body.append(f'{k}: {v}')
    if items:
        body.append('')
        body.append('Состав:')
        for item in items:
            body.append(f'- {item}')
    return '\n'.join(body)


def send_site_email(category: str, subject: str, lines: list[tuple[str, str]], items: list[str] | None = None) -> bool:
    cfg = load_email_settings()
    targets = _recipient_emails(cfg, category)
    html_body = _build_html(subject, lines, items)
    text_body = _build_text(subject, lines, items)
    ok, err = _send_mail(cfg, targets, subject, html_body, text_body)
    if not ok:
        current_app.logger.warning('Shop email notify failed (%s): %s', category, err)
    return ok


def notify_kp_draft(payload: dict, doc_id: int) -> bool:
    lines = [
        ('Заявка', f'#{int(doc_id)}'),
        ('Источник', 'Витрина /shop'),
        ('Клиент', payload.get('customer_name') or '—'),
        ('Телефон', payload.get('phone') or '—'),
        ('E-mail', payload.get('email') or '—'),
        ('Итого', f'{_fmt_money(payload.get("total_sum"))} ₽'),
        ('Дата', datetime.now().strftime('%d.%m.%Y %H:%M')),
    ]
    items = []
    for line in payload.get('lines') or []:
        plant = _esc(line.get('plant_name'))
        size = _esc(line.get('size_name') or '—')
        qty = int(line.get('qty') or 0)
        price = _fmt_money(line.get('price'))
        row_sum = _fmt_money(line.get('sum'))
        items.append(f'{plant} · {size} — {qty} шт × {price} ₽ = {row_sum} ₽')
    return send_site_email('kp_draft', f'Новый запрос КП с сайта #{int(doc_id)}', lines, items)


def notify_on_request(row, source: str = 'shop') -> bool:
    source_label = 'Витрина /shop' if source == 'shop' else 'Главная страница'
    plant_name = getattr(getattr(row, 'plant', None), 'name', None) or 'Главная страница'
    size_name = getattr(getattr(row, 'size', None), 'name', None) or 'Прайс / консультация'
    lines = [
        ('Заявка', f'#{int(getattr(row, "id", 0) or 0)}'),
        ('Источник', source_label),
        ('Клиент', getattr(row, 'customer_name', '') or '—'),
        ('Телефон', getattr(row, 'phone', '') or '—'),
        ('Растение', plant_name),
        ('Размер', size_name),
        ('Комментарий', getattr(row, 'message', '') or '—'),
        ('Дата', datetime.now().strftime('%d.%m.%Y %H:%M')),
    ]
    category = 'on_request' if source == 'shop' else 'landing_inquiry'
    subject = 'Новая заявка «По запросу» с сайта' if source == 'shop' else 'Новая заявка с главной страницы'
    return send_site_email(category, subject, lines)


def send_test_email() -> tuple[bool, str]:
    cfg = load_email_settings()
    to_emails = []
    for category in ALL_CATEGORIES:
        for email in _recipient_emails(cfg, category):
            if email not in to_emails:
                to_emails.append(email)
    subject = 'Тест почтовых уведомлений сайта'
    lines = [
        ('Статус', 'Тестовая отправка из админки'),
        ('Категории', ', '.join(ALL_CATEGORIES)),
        ('Дата', datetime.now().strftime('%d.%m.%Y %H:%M')),
    ]
    html_body = _build_html(subject, lines)
    text_body = _build_text(subject, lines)
    return _send_mail(cfg, to_emails, subject, html_body, text_body)
