"""Валидатор строк «Анализа рынка».

Задача: отделить мусорные строки (горшки, ниваки вместо обычной формы,
ценовые выбросы, левые растения) от чистых совпадений. Строка всё равно
сохраняется в CompetitorRow, но с is_rejected=True и списком причин,
чтобы админ мог просмотреть и при желании вернуть в основной список.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


# --- Паттерны ---------------------------------------------------------------

# Горшки / контейнеры. "\bC10\b", "\bP9\b", "10 л", "контейнер", "горшок", "pot".
# Избегаем ложных срабатываний на одиночные числа — требуем префикс C/P или
# явные слова.
_POT_PATTERNS = [
    re.compile(r"\b[CP]\s?\d{1,2}\b", re.IGNORECASE),
    re.compile(r"\bP\s?9\b", re.IGNORECASE),
    re.compile(r"\bконтейнер[а-я]*\b", re.IGNORECASE),
    re.compile(r"\bгорш(ок|ке|ка|ков)\b", re.IGNORECASE),
    re.compile(r"\bв\s*горшк[ае]\b", re.IGNORECASE),
    re.compile(r"\bpot\b", re.IGNORECASE),
    re.compile(r"\bлитр(аж|ов|а)?\b", re.IGNORECASE),
    re.compile(r"\b\d{1,3}\s*л(итр)?\b", re.IGNORECASE),
]

# Явные признаки декоративных форм (кроме обычного дерева/куста).
_NIVAKI_PATTERNS = re.compile(
    r"\b(ниваки|niwaki|бонсай|bonsai|топиар(?:ий|ная)?|topiary|помпон|pompon|поник(ающ|л))\b",
    re.IGNORECASE,
)
_BALL_PATTERNS = re.compile(r"\b(шар(ом|ик|ы|а)?|ball|sphere|полусфер|стриж(ен|к))\b", re.IGNORECASE)
_STAMM_PATTERNS = re.compile(r"\b(штамб(овая|овое)?|stamm|привит)\b", re.IGNORECASE)

# Паттерны «упаковка-ком» (для sanity-проверки: если pack_type пуст, но в цитате
# явно указано «ком» / «сетка» / «мешковина» / «RB» — считаем совпадение ок).
_RB_PATTERNS = re.compile(r"\b(ком|сетк[аеи]|мешковин|RB|WRB|B&B|grunt|грунт)\b", re.IGNORECASE)


# --- Защита от галлюцинаций LLM без браузера --------------------------------

# Маркетплейсы / магазины оборудования / тулы. Эти сайты не являются
# питомниками посадочного материала. Если LLM «нашла» там цену — это
# почти наверняка выдумка.
_BAD_DOMAIN_SUBSTR = (
    'ozon.', 'wildberries.', 'wb.ru', 'beru.', 'sbermarket.',
    'market.yandex', 'yandex.market',
    'avito.', 'youla.', 'drom.',
    'aliexpress.', 'ebay.', 'amazon.',
    'leroymerlin.', 'castorama.', 'obi.ru', 'petrovich.ru', 'maxidom.',
    # Инструмент / оборудование для ландшафтного дизайна — НЕ питомники:
    'instrument', 'stihl.', 'husqvarna.', 'makita.',
    'gardena.', 'karcher.',
    # Генераторы контента / агрегаторы / каталоги без цен:
    'wikipedia.', 'wikiwand.', 'yandex.ru/images', 'google.com/search',
    'pinterest.', 'dzen.ru', 'vk.com', 'ok.ru', 't.me',
    # Явно сгенерированные «left» домены, замеченные в галлюцинациях модели.
    # Держим список коротким — расширяем по мере наблюдений.
)

# Регэксп — «http(s)://hostname/...». Пустая или localhost-овая ссылка = отбрак.
_URL_RX = re.compile(r"^https?://([^/\s]+)", re.IGNORECASE)


def _extract_hostname(url: str) -> str:
    if not url:
        return ''
    m = _URL_RX.match(str(url).strip())
    if not m:
        return ''
    host = m.group(1).lower()
    if host.startswith('www.'):
        host = host[4:]
    return host


def _is_bad_domain(url: str) -> Optional[str]:
    """Если домен явно НЕ из питомников посадочного материала — вернёт
    совпавшую подстроку. Иначе None."""
    host = _extract_hostname(url)
    if not host:
        return None
    for bad in _BAD_DOMAIN_SUBSTR:
        if bad in host:
            return bad
    return None


def _has_digits(text: str) -> bool:
    return bool(re.search(r"\d", text or ''))


# --- Хелперы ----------------------------------------------------------------

def _first_number(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"\d+(?:\.\d+)?", str(text))
    if not m:
        return None
    try:
        return float(m.group())
    except (ValueError, TypeError):
        return None


def _haystack(raw: Dict[str, Any]) -> str:
    parts = [
        raw.get('plant_name') or '',
        raw.get('size') or '',
        raw.get('source_excerpt') or '',
        raw.get('url') or '',
    ]
    return " ".join(str(p) for p in parts)


def _has_pot_signal(raw: Dict[str, Any]) -> Optional[str]:
    """Возвращает строку-улику (паттерн, который сработал), либо None."""
    # 1. Явное поле pack_type от ИИ.
    pack = (raw.get('pack_type') or '').strip().lower()
    if pack:
        if pack in ('c2', 'c3', 'c5', 'c7', 'c10', 'c15', 'c20', 'p9'):
            return f"pack_type={pack.upper()}"
    # 2. Текстовый эвристический поиск в описании/ссылке.
    hay = _haystack(raw)
    for rx in _POT_PATTERNS:
        m = rx.search(hay)
        if m:
            return f"match={m.group(0)}"
    return None


def _detect_form_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    if _NIVAKI_PATTERNS.search(text):
        return 'niwaki'
    if _STAMM_PATTERNS.search(text):
        return 'stamm'
    if _BALL_PATTERNS.search(text):
        return 'ball'
    return None


def _normalize_haircut(haircut: str) -> str:
    """Приводит строку из шаблона (шар/стрижка/ball/...) к одному из:
    'free' или 'ball'."""
    if not haircut:
        return 'free'
    h = str(haircut).strip().lower()
    if h in ('', 'free', 'свободн', 'свободная', '-', 'нет'):
        return 'free'
    if 'шар' in h or 'ball' in h or 'стриж' in h or 'trim' in h or 'sphere' in h:
        return 'ball'
    return 'free'


def _fuzzy_ratio(a: str, b: str) -> int:
    """Возвращает 0..100. Используем rapidfuzz, если установлен; иначе
    простейший token-sort на пересечении множества слов."""
    a = (a or '').lower().strip()
    b = (b or '').lower().strip()
    if not a or not b:
        return 0
    try:
        from rapidfuzz import fuzz
        return int(fuzz.token_set_ratio(a, b))
    except Exception:
        # Fallback: простая метрика — пересечение токенов / объединение.
        ta = set(re.findall(r"[a-zа-я0-9]+", a))
        tb = set(re.findall(r"[a-zа-я0-9]+", b))
        if not ta or not tb:
            return 0
        inter = ta & tb
        union = ta | tb
        return int(round(100 * len(inter) / max(1, len(union))))


# --- Публичный API -----------------------------------------------------------

def validate_competitor_row(raw: Dict[str, Any], our_item: Dict[str, Any]) -> Dict[str, Any]:
    """Проверяет строку конкурента относительно нашей позиции.

    raw: {plant_name, size, price, pack_type?, form?, source_excerpt?, url?}
    our_item: {plant_name, size, haircut, our_price}

    Возвращает: {ok: bool, reasons: [str...]}
    """
    reasons: List[str] = []

    # 1. Горшки / контейнеры.
    pot = _has_pot_signal(raw)
    if pot:
        reasons.append(f"pot:{pot}")

    # 2. Форма: сравниваем требуемую и найденную.
    required_form = _normalize_haircut(our_item.get('haircut') or '')
    raw_form = (raw.get('form') or '').strip().lower()
    detected_form = raw_form or _detect_form_from_text(_haystack(raw)) or 'free'
    if required_form == 'free':
        # Нужно обычное растение, а нашли ниваки/стамб/шар/топиар — отбраковать.
        if detected_form in ('niwaki', 'topiary', 'pompon', 'stamm', 'ball'):
            reasons.append(f"form_mismatch:want=free,got={detected_form}")
    elif required_form == 'ball':
        # Нужен шар / топиар / помпон. «free» без упоминания «шар» в цитате — отбрак.
        if detected_form == 'free':
            hay = _haystack(raw)
            if not _BALL_PATTERNS.search(hay):
                reasons.append("form_mismatch:want=ball,got=free")
        elif detected_form == 'niwaki':
            # Ниваки слишком дорогой, не конкурент обычному шару.
            reasons.append("form_mismatch:want=ball,got=niwaki")

    # 3. Ценовой выброс.
    try:
        price = float(raw.get('price') or 0)
    except (ValueError, TypeError):
        price = 0.0
    try:
        our_price = float(our_item.get('our_price') or 0)
    except (ValueError, TypeError):
        our_price = 0.0
    if our_price > 0 and price > 0:
        ratio = price / our_price
        if ratio > 3.0:
            reasons.append(f"price_outlier:x{ratio:.1f}")
        elif ratio < 0.3:
            reasons.append(f"price_outlier:x{ratio:.2f}")

    # 4. Несовпадение размера (первое число).
    a = _first_number(raw.get('size'))
    b = _first_number(our_item.get('size'))
    if a is not None and b is not None and b > 0:
        diff_pct = abs(a - b) / b
        if diff_pct > 0.5:
            reasons.append(f"size_mismatch:{int(a)}vs{int(b)}")

    # 5. Fuzzy-сравнение наименования.
    score = _fuzzy_ratio(raw.get('plant_name') or '', our_item.get('plant_name') or '')
    if score < 60:
        reasons.append(f"name_fuzzy:{score}")

    # 6. Анти-галлюцинация: ссылка.
    url = (raw.get('url') or '').strip()
    if not url:
        reasons.append("no_url:empty")
    elif not _URL_RX.match(url):
        reasons.append("no_url:invalid")
    else:
        bad = _is_bad_domain(url)
        if bad:
            reasons.append(f"bad_domain:{bad}")

    # 7. Анти-галлюцинация: цитата-источник должна быть и содержать цифры
    # (иначе скорее всего модель придумала — у реальной карточки товара
    # всегда есть либо цена, либо размер).
    excerpt = (raw.get('source_excerpt') or '').strip()
    if not excerpt:
        reasons.append("no_excerpt:empty")
    elif len(excerpt) < 20:
        reasons.append(f"no_excerpt:short({len(excerpt)})")
    elif not _has_digits(excerpt):
        # Цитата без цифр — подозрительно: нет ни размера, ни цены, ни артикула.
        reasons.append("no_excerpt:no_digits")

    return {'ok': not reasons, 'reasons': reasons}


# --- Бейджи для UI ---------------------------------------------------------

_REASON_LABELS = {
    'pot': ('Горшок', 'danger'),
    'form_mismatch': ('Форма не та', 'warning'),
    'price_outlier': ('Выброс цены', 'warning'),
    'size_mismatch': ('Размер', 'warning'),
    'name_fuzzy': ('Другое растение', 'danger'),
    'no_url': ('Нет ссылки', 'danger'),
    'bad_domain': ('Не питомник', 'danger'),
    'no_excerpt': ('Нет цитаты', 'danger'),
}


def format_reason_badges(reason_list) -> List[Dict[str, str]]:
    """Преобразует список причин (из JSON) в список бейджей для шаблона:
    [{text, color, detail}, ...]."""
    out = []
    if isinstance(reason_list, str):
        try:
            import json as _json
            reason_list = _json.loads(reason_list)
        except Exception:
            reason_list = []
    for r in (reason_list or []):
        if not isinstance(r, str):
            continue
        tag, _, detail = r.partition(':')
        label, color = _REASON_LABELS.get(tag, (tag, 'secondary'))
        out.append({'text': label, 'color': color, 'detail': detail or ''})
    return out
