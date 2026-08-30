"""Разбор PDF-счёта для Mini App: позиции + одна строка назначения + сумма."""
from __future__ import annotations

import json
import logging
import os
import re
from decimal import Decimal, InvalidOperation

log = logging.getLogger(__name__)

_MONEY_NUM = r'(\d{1,3}(?:\s\d{3})+(?:[.,]\d{1,2})?|\d+[.,]\d{2}|\d+)'
_TOTAL_LABEL = re.compile(
    r'(?is)(?:всего\s+к\s+оплате|к\s+оплате|на\s+сумму|итого)'
    r'(?!\s*(?:ндс|без\s+налога))'
    r'[^\d]{0,48}' + _MONEY_NUM
)


def _money(value) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return Decimal(str(value).replace('\xa0', '').replace(' ', '').replace(',', '.'))
    except (InvalidOperation, TypeError, ValueError):
        return None


def extract_document_total(text: str) -> Decimal | None:
    """Итог с подвала счёта («Итого», «на сумму»), не сумма строк и не догадка LLM."""
    if not text:
        return None
    body = text.replace('\xa0', ' ')
    found: list[Decimal] = []
    for m in _TOTAL_LABEL.finditer(body):
        chunk = m.group(0).lower()
        if 'ндс' in chunk and 'на сумму' not in chunk and 'к оплате' not in chunk:
            continue
        val = _money(m.group(1))
        if val and val > 0:
            found.append(val)
    if not found:
        return None
    # Подвал обычно в конце; при нескольких «Итого» (по страницам) берём последнее.
    return found[-1]


def summarize_lines(lines: list[dict], fallback_name: str = '') -> tuple[str, Decimal]:
    """Сумма по строкам и короткая расшифровка без LLM."""
    total = Decimal('0')
    names: list[str] = []
    for ln in lines or []:
        row_sum = _money(ln.get('total'))
        if row_sum is None:
            qty = _money(ln.get('qty')) or Decimal('0')
            price = _money(ln.get('unit_price')) or Decimal('0')
            row_sum = qty * price
        if row_sum:
            total += row_sum
        desc = (ln.get('description') or '').strip()
        if desc and desc not in names:
            names.append(desc)
    if total == 0 and not names:
        summary = (fallback_name or 'Счёт на оплату').strip()[:500]
        return summary, total
    if len(names) == 1:
        summary = names[0]
    elif len(names) <= 3:
        summary = ', '.join(names)
    elif names:
        summary = f"{names[0]} и ещё {len(names) - 1} поз."
    else:
        summary = (fallback_name or 'Счёт на оплату').strip()
    return summary[:500], total


def _groq_purpose(text: str, lines: list[dict], fallback: str) -> tuple[str, Decimal | None]:
    api_key = os.environ.get('GROQ_API_KEY', '').strip()
    if not api_key or not text:
        return fallback, None
    try:
        from groq import Groq
    except ImportError:
        return fallback, None
    try:
        client = Groq(api_key=api_key)
        payload = {
            'text': text[:6000],
            'lines': lines[:40],
        }
        resp = client.chat.completions.create(
            model=os.environ.get('GROQ_MODEL', 'llama-3.3-70b-versatile'),
            messages=[
                {
                    'role': 'system',
                    'content': (
                        'Ты бухгалтер питомника. По тексту счёта верни JSON: '
                        '{"summary": "одна строка за что платим, без номера счёта", '
                        '"amount": число к оплате (итого с НДС если есть)}. '
                        'summary — по-русски, до 120 символов, без кавычек вокруг.'
                    ),
                },
                {'role': 'user', 'content': json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.0,
            max_tokens=400,
            response_format={'type': 'json_object'},
        )
        data = json.loads(resp.choices[0].message.content or '{}')
        summary = (data.get('summary') or fallback).strip()[:500]
        amount = _money(data.get('amount'))
        return summary or fallback, amount
    except Exception:
        log.exception('tg_pay groq purpose failed')
        return fallback, None


def parse_invoice_file(path: str, original_name: str = '') -> dict:
    """Возвращает {summary, amount, lines, error, raw_text_len}."""
    name = (original_name or os.path.basename(path) or 'Счёт').strip()
    ext = os.path.splitext(path)[1].lower()
    if ext not in ('.pdf',):
        return {
            'summary': name[:500],
            'amount': Decimal('0'),
            'lines': [],
            'error': 'Это не PDF — сумму и назначение впишите вручную.',
            'raw_text_len': 0,
        }

    lines: list[dict] = []
    raw_text = ''
    try:
        from app.vium_pdf_parser import extract_invoice_lines, _extract_pdf_text
        raw_text, _tables = _extract_pdf_text(path)
        lines = extract_invoice_lines(path)
    except Exception as exc:
        log.exception('tg_pay parse pdf failed')
        return {
            'summary': name[:500],
            'amount': Decimal('0'),
            'lines': [],
            'error': f'Не разобрал PDF: {exc}',
            'raw_text_len': 0,
        }

    summary, line_sum = summarize_lines(lines, name)
    printed = extract_document_total(raw_text)
    groq_summary, groq_amount = _groq_purpose(raw_text, lines, summary)
    if groq_summary:
        summary = groq_summary
    # Печатный «Итого» важнее суммы строк и Groq: LLM видит обрезанный текст
    # и на длинных счетах (50+ позиций) часто ошибается на сотни рублей.
    amount = printed or line_sum or groq_amount or Decimal('0')
    return {
        'summary': summary,
        'amount': amount,
        'lines': lines,
        'error': None if (lines or amount or groq_summary) else 'Позиции не найдены — проверьте вручную.',
        'raw_text_len': len(raw_text or ''),
    }
