"""Разбор выписки Альфа-Банка, квитанции или платёжного поручения (0401060)."""
from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from io import BytesIO

log = logging.getLogger(__name__)

_SYSTEM = (
    'Это фото или текст российской банковской выписки, квитанции об оплате '
    'или платёжного поручения (форма 0401060). Верни JSON:\n'
    '{"kind":"statement_list|payment_order|receipt|invoice_bill|other",'
    '"payments":[{"payee":"","amount":0,"invoice_no":"","date":"YYYY-MM-DD",'
    '"purpose":"","vat":""}]}\n'
    'Правила:\n'
    '- Только исходящие платежи (списания). amount — сумма операции справа, '
    'положительное число, без минуса и без пробелов. Не путать с суммой внутри '
    'назначения платежа, если они различаются: бери сумму операции.\n'
    '- На одном скрине списка может быть несколько платежей — верни все, сверху вниз.\n'
    '- payee — получатель (юрлицо / ИП), даже если имя обрезано многоточием.\n'
    '- invoice_no — номер счёта, счёта-оферты или договора из назначения '
    '(цб-1932, 0249305723-0042, 174, 001731802). Не путать с номером платёжки '
    'банка слева (№ 194) и не путать с ИНН/счётом 20 цифр.\n'
    '- date — дата счёта или платежа, DD.MM.YYYY на фото → YYYY-MM-DD.\n'
    '- purpose — полный текст назначения, если видно.\n'
    '- vat: "none" если НДС не облагается, иначе как на фото.\n'
    '- Если это неоплаченный счёт поставщика (счёт на оплату), а не квитанция '
    'и не платёжка — kind=invoice_bill и payments=[].\n'
    '- Пустые строки если поля нет. Без комментариев.'
)

_IMAGE_EXT = {'.jpg', '.jpeg', '.png', '.webp', '.jfif', '.bmp', '.tif', '.tiff', '.heic'}


def _as_money(value) -> Decimal | None:
    if value is None or value == '':
        return None
    if isinstance(value, Decimal):
        return abs(value)
    raw = str(value).strip().replace('\xa0', ' ')
    raw = raw.replace('₽', '').replace('руб.', '').replace('руб', '')
    raw = raw.replace('−', '-').replace('–', '-')
    raw = raw.replace(' ', '').replace(',', '.')
    raw = re.sub(r'[^\d.\-]', '', raw)
    if not raw or raw in ('.', '-', '-.'):
        return None
    if raw.count('.') > 1:
        parts = raw.split('.')
        raw = ''.join(parts[:-1]) + '.' + parts[-1]
    try:
        amt = Decimal(raw)
    except (InvalidOperation, ValueError):
        return None
    amt = abs(amt)
    if amt <= 0:
        return None
    return amt.quantize(Decimal('0.01'))


def _as_date(value) -> date | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    raw = raw.replace('г.', '').replace('г', '').strip()
    for fmt, n in (('%Y-%m-%d', 10), ('%d.%m.%Y', 10), ('%d.%m.%y', 8)):
        try:
            return datetime.strptime(raw[:n], fmt).date()
        except ValueError:
            continue
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', raw)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return None


def _clean_payment(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None
    amount = _as_money(row.get('amount'))
    if amount is None:
        return None
    payee = re.sub(r'\s+', ' ', str(row.get('payee') or '')).strip()[:300]
    invoice_no = str(row.get('invoice_no') or '').strip()
    invoice_no = re.sub(r'^[№N#.\s]+', '', invoice_no)[:80]
    purpose = re.sub(r'\s+', ' ', str(row.get('purpose') or '')).strip()[:800]
    vat = str(row.get('vat') or '').strip()[:120]
    return {
        'payee': payee,
        'amount': amount,
        'invoice_no': invoice_no,
        'date': _as_date(row.get('date')),
        'purpose': purpose,
        'vat': vat,
    }


def _clean_payload(data: dict) -> dict:
    kind = str((data or {}).get('kind') or '').strip() or 'other'
    payments = []
    seen = set()
    for row in (data or {}).get('payments') or []:
        item = _clean_payment(row)
        if not item:
            continue
        key = (str(item['amount']), (item['invoice_no'] or '').lower(), (item['payee'] or '').lower())
        if key in seen:
            continue
        seen.add(key)
        payments.append(item)
    return {'kind': kind[:40], 'payments': payments}


def prepare_image(data: bytes) -> tuple[bytes, str] | None:
    """JPEG для Groq: RGB, длинная сторона ≤ 1600 px."""
    if not data:
        return None
    try:
        from PIL import Image
        im = Image.open(BytesIO(data))
        if im.mode not in ('RGB', 'L'):
            im = im.convert('RGB')
        elif im.mode == 'L':
            im = im.convert('RGB')
        im.thumbnail((1600, 1600))
        buf = BytesIO()
        im.save(buf, format='JPEG', quality=82, optimize=True)
        return buf.getvalue(), 'image/jpeg'
    except Exception:
        log.exception('bank slip image prepare failed')
        return None


def extract_pdf_text(data: bytes) -> str:
    if not data or not data[:8].startswith(b'%PDF'):
        return ''
    try:
        import pdfplumber
    except ImportError:
        return ''
    try:
        chunks = []
        with pdfplumber.open(BytesIO(data)) as pdf:
            for page in pdf.pages[:4]:
                chunks.append(page.extract_text() or '')
        return '\n'.join(chunks).strip()
    except Exception:
        log.exception('bank slip pdf text failed')
        return ''


def heuristic_from_text(text: str) -> dict:
    """Запасной разбор цифрового ПП, если зрение недоступно."""
    if not (text or '').strip():
        return {'kind': 'other', 'payments': []}
    amount = None
    m_sum = re.search(
        r'Сумма[^\d]{0,40}(\d[\d\s]{0,12})[,\-](\d{2})',
        text,
        re.IGNORECASE,
    )
    if m_sum:
        amount = _as_money(m_sum.group(1) + '.' + m_sum.group(2))
    if amount is None:
        m_rub = re.search(r'(\d[\d\s]{2,12})[,\-](\d{2})\s*(?:₽|руб)', text)
        if m_rub:
            amount = _as_money(m_rub.group(1) + '.' + m_rub.group(2))
    invoice_no = ''
    m_inv = re.search(
        r'(?:сч\.?\s*-?\s*оферте\.?|сч(?:ё|е)?т[ау]?\.?|сч\.?)\s*№?\s*([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-]{0,40})',
        text,
        re.IGNORECASE,
    )
    if not m_inv:
        m_inv = re.search(
            r'договор[ау]?\s*№\s*([A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9\-]{0,40})',
            text,
            re.IGNORECASE,
        )
    if m_inv:
        invoice_no = m_inv.group(1).strip().rstrip('.')
    paid_on = None
    m_dt = re.search(r'от\s+(\d{2}\.\d{2}\.\d{4})', text)
    if m_dt:
        paid_on = _as_date(m_dt.group(1))
    payee = ''
    m_pay = re.search(
        r'Получатель[:\s]+(.{5,200}?)(?:\n|ИНН|Сч\.|Счет)',
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m_pay:
        payee = re.sub(r'\s+', ' ', m_pay.group(1)).strip()
    purpose = ''
    m_pur = re.search(
        r'Назначение платежа[:\s]+(.{10,500}?)(?:\n\s*\n|М\.П\.|Подпись|$)',
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if m_pur:
        purpose = re.sub(r'\s+', ' ', m_pur.group(1)).strip()
    vat = 'none' if re.search(r'НДС не облагается', text, re.IGNORECASE) else ''
    kind = 'payment_order' if re.search(r'ПЛАТ[ЕЁ]ЖНОЕ ПОРУЧЕНИЕ', text, re.IGNORECASE) else 'other'
    if amount is None:
        return {'kind': kind, 'payments': []}
    return _clean_payload({
        'kind': kind,
        'payments': [{
            'payee': payee,
            'amount': amount,
            'invoice_no': invoice_no,
            'date': paid_on.isoformat() if paid_on else '',
            'purpose': purpose,
            'vat': vat,
        }],
    })


def _groq_json(messages: list, *, max_tokens: int = 2500) -> dict:
    api_key = os.environ.get('GROQ_API_KEY', '').strip()
    if not api_key:
        return {}
    try:
        from groq import Groq
        client = Groq(api_key=api_key, timeout=90)
        resp = client.chat.completions.create(
            model=os.environ.get(
                'GROQ_VISION_MODEL',
                'meta-llama/llama-4-scout-17b-16e-instruct',
            ) if any(
                isinstance(m.get('content'), list) for m in messages
            ) else os.environ.get('GROQ_MODEL', 'llama-3.3-70b-versatile'),
            messages=messages,
            temperature=0.0,
            max_tokens=max_tokens,
            response_format={'type': 'json_object'},
        )
        return json.loads(resp.choices[0].message.content or '{}')
    except Exception:
        log.exception('bank slip groq parse failed')
        return {}


def groq_from_text(text: str) -> dict:
    if not (text or '').strip():
        return {}
    return _groq_json([
        {'role': 'system', 'content': _SYSTEM},
        {'role': 'user', 'content': text[:12000]},
    ], max_tokens=2000)


def groq_from_image(data: bytes, mime: str) -> dict:
    if not data:
        return {}
    b64 = base64.b64encode(data).decode('ascii')
    url = f'data:{mime};base64,{b64}'
    return _groq_json([{
        'role': 'user',
        'content': [
            {'type': 'text', 'text': _SYSTEM},
            {'type': 'image_url', 'image_url': {'url': url}},
        ],
    }])


def looks_like_image(filename: str, data: bytes = b'') -> bool:
    ext = os.path.splitext(filename or '')[1].lower()
    if ext in _IMAGE_EXT:
        return True
    if data[:3] == b'\xff\xd8\xff' or data[:8] == b'\x89PNG\r\n\x1a\n':
        return True
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return True
    return False


def parse_bank_file(data: bytes, filename: str = '') -> dict:
    """Возвращает {kind, payments, error?}."""
    name = filename or ''
    ext = os.path.splitext(name)[1].lower()
    is_pdf = ext == '.pdf' or (data or b'')[:5] == b'%PDF-'
    if is_pdf:
        text = extract_pdf_text(data)
        parsed = groq_from_text(text) if text else {}
        result = _clean_payload(parsed) if parsed else heuristic_from_text(text)
        if not result.get('payments') and text:
            heur = heuristic_from_text(text)
            if heur.get('payments'):
                result = heur
        if not result.get('payments'):
            result['error'] = (
                'Не нашёл платежи в PDF. Если это скан — сфотографируйте экран.'
            )
        return result

    prepared = prepare_image(data)
    if not prepared:
        return {'kind': 'other', 'payments': [], 'error': 'Не удалось открыть изображение.'}
    jpeg, mime = prepared
    parsed = groq_from_image(jpeg, mime)
    result = _clean_payload(parsed) if parsed else {'kind': 'other', 'payments': []}
    if not result.get('payments'):
        if not os.environ.get('GROQ_API_KEY', '').strip():
            result['error'] = 'Нет GROQ_API_KEY — нечем прочитать фото.'
        elif (result.get('kind') or '') == 'invoice_bill':
            result['error'] = 'Это похоже на счёт поставщика, не на квитанцию. Загрузите через «Файл».'
        else:
            result['error'] = 'Не разобрал платежи на фото. Попробуйте более крупный кадр.'
    return result
