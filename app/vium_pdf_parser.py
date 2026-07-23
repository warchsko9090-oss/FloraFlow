"""Парсер PDF-счетов для инбокса ВиУМ.

Извлекает позиции из текстового PDF и (опционально) приводит их к
структурированному виду через Groq LLM. Возвращает список словарей:

    [
        {
            "description": "Мешковина джут 50х80",
            "qty":         200,
            "unit":        "шт",
            "unit_price":  45.0,
            "total":       9000.0,
        },
        ...
    ]

Если `pdfplumber` или Groq недоступны — поднимает понятную ошибку,
которую вызывающий код запишет в `ViumInvoiceQueue.error`.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Iterable

log = logging.getLogger(__name__)

NUM_RE = re.compile(r'(\d+[\d\s]*[.,]?\d*)')


def _to_float(s: str | None) -> float | None:
    if not s:
        return None
    s = str(s).replace('\xa0', ' ').replace(' ', '').replace(',', '.')
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _extract_pdf_text(path: str) -> tuple[str, list[list[list[str]]]]:
    """Возвращает (полный текст PDF, список страниц с таблицами)."""
    try:
        import pdfplumber
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            'Не установлен pdfplumber. Добавьте `pdfplumber` в requirements.txt.'
        ) from e

    full_text_chunks: list[str] = []
    tables_per_page: list[list[list[str]]] = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            try:
                t = page.extract_text() or ''
            except Exception:
                t = ''
            full_text_chunks.append(t)
            try:
                tbls = page.extract_tables() or []
            except Exception:
                tbls = []
            for tbl in tbls:
                tables_per_page.append([
                    [(c if c is not None else '') for c in row] for row in tbl
                ])
    return ('\n'.join(full_text_chunks), tables_per_page)


# ---------------------------------------------------------------------------
# Эвристический парсер таблиц
# ---------------------------------------------------------------------------

_HEADER_HINTS = {
    'name': ('наименование', 'товар', 'описание', 'предмет', 'позиция'),
    'qty':  ('кол', 'количество', 'qty'),
    'unit': ('ед.изм', 'ед. изм', 'единица', 'ед'),
    'price': ('цена', 'цена за', 'unit price'),
    'total': ('сумма', 'итого', 'total', 'amount'),
}


def _classify_columns(header: list[str]) -> dict:
    out = {}
    for idx, h in enumerate(header):
        h_low = (h or '').strip().lower()
        if not h_low:
            continue
        for role, hints in _HEADER_HINTS.items():
            if any(hint in h_low for hint in hints):
                out.setdefault(role, idx)
                break
    return out


def _parse_table(table: list[list[str]]) -> list[dict]:
    """Эвристика: ищем строку-заголовок, дальше парсим строки."""
    if not table:
        return []
    header_idx = None
    for i, row in enumerate(table[:5]):
        cls = _classify_columns(row)
        if 'name' in cls and ('qty' in cls or 'total' in cls):
            header_idx = i
            break
    if header_idx is None:
        return []
    cls = _classify_columns(table[header_idx])
    rows = table[header_idx + 1:]
    out: list[dict] = []
    for r in rows:
        if not r or not any((c or '').strip() for c in r):
            continue
        name = r[cls['name']] if 'name' in cls and cls['name'] < len(r) else ''
        qty  = _to_float(r[cls['qty']])    if 'qty' in cls    and cls['qty']    < len(r) else None
        unit = (r[cls['unit']] if 'unit' in cls and cls['unit'] < len(r) else '') or ''
        price = _to_float(r[cls['price']]) if 'price' in cls and cls['price']  < len(r) else None
        total = _to_float(r[cls['total']]) if 'total' in cls and cls['total']  < len(r) else None
        name = (name or '').strip()
        if not name or len(name) < 3:
            continue
        # Выкидываем строки «итого/всего/НДС».
        if any(s in name.lower() for s in ('итого', 'всего', 'ндс', 'к оплате')):
            continue
        out.append({
            'description': name,
            'qty': qty,
            'unit': unit.strip() or None,
            'unit_price': price,
            'total': total,
        })
    return out


# ---------------------------------------------------------------------------
# LLM-нормализация (Groq)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    'Ты помогаешь оцифровать товарный счёт российской компании. '
    'На вход — текст счёта в произвольной форме (PDF). '
    'Извлеки позиции: для каждой верни {description, qty, unit, unit_price, total}. '
    'Не придумывай товары, бери только то, что явно есть в тексте. '
    'Если поле не найдено — оставь null. '
    'Верни строго JSON-массив без какого-либо обрамления.'
)


def _normalize_with_groq(raw_text: str) -> list[dict] | None:
    api_key = os.environ.get('GROQ_API_KEY', '').strip()
    if not api_key:
        return None
    try:
        from groq import Groq
    except ImportError:
        return None
    try:
        client = Groq(api_key=api_key)
        # Усечём слишком длинный текст, чтобы влезть в окно.
        text = raw_text[:8000]
        resp = client.chat.completions.create(
            model=os.environ.get('GROQ_MODEL', 'llama-3.3-70b-versatile'),
            messages=[
                {'role': 'system', 'content': _SYSTEM_PROMPT},
                {'role': 'user', 'content': text},
            ],
            temperature=0.0,
            max_tokens=2000,
            response_format={'type': 'json_object'},
        )
        content = resp.choices[0].message.content or ''
        # Groq в json_object иногда отдаёт {"items":[...]}. Берём как есть.
        data = json.loads(content)
        if isinstance(data, dict):
            for key in ('items', 'lines', 'positions', 'data'):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
        if not isinstance(data, list):
            return None
        out = []
        for ln in data:
            if not isinstance(ln, dict):
                continue
            desc = (ln.get('description') or ln.get('name') or '').strip()
            if not desc:
                continue
            out.append({
                'description': desc,
                'qty':        _to_float(ln.get('qty')),
                'unit':       (ln.get('unit') or '').strip() or None,
                'unit_price': _to_float(ln.get('unit_price') or ln.get('price')),
                'total':      _to_float(ln.get('total') or ln.get('sum')),
            })
        return out or None
    except Exception:
        log.exception('groq normalize failed')
        return None


# ---------------------------------------------------------------------------
# Публичная функция
# ---------------------------------------------------------------------------

def extract_invoice_lines(path: str) -> list[dict]:
    """Главная функция: вернуть список позиций из PDF-счёта."""
    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    text, tables = _extract_pdf_text(path)

    # 1) Эвристика по таблицам
    parsed: list[dict] = []
    for tbl in tables:
        parsed.extend(_parse_table(tbl))

    # 2) Если эвристика не дала результатов или дала <2 позиций — пробуем LLM.
    if len(parsed) < 1 and text:
        ai = _normalize_with_groq(text)
        if ai:
            parsed = ai

    # 3) Финальная очистка
    cleaned: list[dict] = []
    seen = set()
    for ln in parsed:
        desc = (ln.get('description') or '').strip()
        if not desc or len(desc) < 3:
            continue
        key = (desc.lower(), ln.get('qty'), ln.get('unit_price'))
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(ln)
    return cleaned
