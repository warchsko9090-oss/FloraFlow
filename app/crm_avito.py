"""Поиск предложений на Avito — отдельный «канал» анализа рынка.

ВАЖНО: у Avito нет публичного API. Мы парсим HTML публичной выдачи.
Это хрупко и может отвалиться, если:
  - Avito поменяет вёрстку;
  - сработает Cloudflare/WAF (обычно при большом количестве запросов
    с одного IP, особенно датацентрового);
  - IP сервера уйдёт в чёрный список у них.

Поэтому работаем в режиме best-effort:
  - Одна страница выдачи на запрос (до ~50 карточек).
  - Короткий таймаут, строгие заголовки как у браузера.
  - При первой же HTTP 403/429/503 — возвращаем специальный блок
    ошибки, чтобы вызывающий код мог остановить сканирование.
  - Кэш на 15 минут (в памяти процесса), чтобы не бить по Avito
    дважды за одним запросом.
"""
from __future__ import annotations

import json
import re
import time
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import requests

log = logging.getLogger(__name__)


# --- Заголовки, имитирующие Chrome на Windows ------------------------------
# Без этого Avito сразу возвращает 403/редиректит на капчу.
_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/125.0.0.0 Safari/537.36'
)
_DEFAULT_HEADERS = {
    'User-Agent': _UA,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}


# In-memory кэш результатов поиска: key -> (ts, listings).
_SEARCH_CACHE: Dict[str, "tuple[float, List[AvitoListing]]"] = {}
_CACHE_TTL_SEC = 15 * 60


@dataclass
class AvitoListing:
    title: str
    url: str
    price: float
    excerpt: str
    location: str = ''


class AvitoBlockedError(RuntimeError):
    """Avito заблокировал запрос (403/429/503 или капча)."""


# --- Сам поиск --------------------------------------------------------------

def _build_search_url(query: str, region_slug: str = 'rossiya') -> str:
    """Собирает URL поиска. region_slug — часть пути, например 'rossiya',
    'moskva', 'moskovskaya_oblast_i_moskva', 'sankt_peterburg', 'tula'."""
    q = quote_plus(query.strip())
    # cd=1 — сохранять только объявления; s=104 — по дате.
    return f"https://www.avito.ru/{region_slug}?q={q}&cd=1&s=104"


# Прайс может быть в <meta itemprop="price" content="1500"> — самый надёжный
# индикатор карточки товара.
_RX_ITEM_BLOCK = re.compile(
    r'data-marker="item"[^>]*>(.*?)(?=data-marker="item"|</div></div></div></div>)',
    re.DOTALL | re.IGNORECASE,
)
_RX_HREF = re.compile(r'href="(/[^"#?]+[_/]\d{6,})', re.IGNORECASE)
_RX_TITLE = re.compile(
    r'(?:itemprop="name"[^>]*>|<h3[^>]*>)\s*(?:<a[^>]*>)?([^<]{5,200})',
    re.IGNORECASE,
)
_RX_PRICE = re.compile(r'itemprop="price"[^>]*content="(\d{2,9})"', re.IGNORECASE)
_RX_LOCATION = re.compile(
    r'data-marker="item-address"[^>]*>\s*<[^>]+>\s*([^<]{2,80})',
    re.IGNORECASE,
)
# Параметры/описание из блока «Специфики» или title.
_RX_PARAMS = re.compile(
    r'data-marker="item-specific-params"[^>]*>\s*<[^>]+>\s*([^<]{5,400})',
    re.IGNORECASE,
)


def _strip_html(s: str) -> str:
    return re.sub(r'<[^>]+>', ' ', s).strip()


def _clean(s: str) -> str:
    return re.sub(r'\s+', ' ', s or '').strip()


def _parse_listings(html: str) -> List[AvitoListing]:
    out: List[AvitoListing] = []
    for block_match in _RX_ITEM_BLOCK.finditer(html):
        block = block_match.group(1)
        href_m = _RX_HREF.search(block)
        title_m = _RX_TITLE.search(block)
        price_m = _RX_PRICE.search(block)
        if not (href_m and title_m and price_m):
            continue
        url = 'https://www.avito.ru' + href_m.group(1)
        title = _clean(_strip_html(title_m.group(1)))
        try:
            price = float(price_m.group(1))
        except (ValueError, TypeError):
            continue
        loc = ''
        loc_m = _RX_LOCATION.search(block)
        if loc_m:
            loc = _clean(_strip_html(loc_m.group(1)))
        params = ''
        params_m = _RX_PARAMS.search(block)
        if params_m:
            params = _clean(_strip_html(params_m.group(1)))
        # source_excerpt — это максимум того, что видим: заголовок + параметры.
        excerpt_parts = [title]
        if params:
            excerpt_parts.append(params)
        if loc:
            excerpt_parts.append(loc)
        excerpt = ' | '.join(excerpt_parts)[:300]
        out.append(AvitoListing(
            title=title, url=url, price=price, excerpt=excerpt, location=loc,
        ))
    return out


def search(query: str, region_slug: str = 'rossiya', timeout: float = 10.0,
           session: Optional[requests.Session] = None) -> List[AvitoListing]:
    """Выполняет одну операцию поиска на Avito. Возвращает список AvitoListing.

    Кидает AvitoBlockedError при явном блоке (403/429/503 или капче).
    На timeout/сетевых ошибках — возвращает пустой список (и логирует).
    """
    key = f"{region_slug}|{query.strip().lower()}"
    now = time.time()
    cached = _SEARCH_CACHE.get(key)
    if cached and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    url = _build_search_url(query, region_slug)
    sess = session or requests.Session()
    try:
        resp = sess.get(url, headers=_DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    except requests.RequestException as e:
        log.warning('avito.search: network error for %r: %s', query, e)
        _SEARCH_CACHE[key] = (now, [])
        return []

    status = resp.status_code
    body = resp.text or ''
    # Явные блоки.
    if status in (403, 429, 503):
        log.warning('avito.search: blocked status=%s for %r url=%s', status, query, url)
        raise AvitoBlockedError(f'Avito вернул HTTP {status}. Попробуйте позже или через VPN.')
    # Неявная капча.
    if ('captcha' in body.lower() and 'input' in body.lower()) or 'firewall' in body.lower():
        log.warning('avito.search: captcha/firewall wall for %r', query)
        raise AvitoBlockedError('Avito показал капчу — временно заблокировали сервер. Попробуйте позже.')
    if status != 200:
        log.warning('avito.search: unexpected status=%s for %r', status, query)
        _SEARCH_CACHE[key] = (now, [])
        return []

    listings = _parse_listings(body)
    log.info('avito.search: query=%r → %s items', query, len(listings))
    _SEARCH_CACHE[key] = (now, listings)
    return listings


def build_query_for_item(plant_name: str, size: str = '') -> str:
    """Умеренно-узкий поисковый запрос. Оставляем только ключевые токены
    (латиница + кириллица длиннее 2 символов) + размер."""
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9]{3,}", plant_name or '')
    q = ' '.join(words[:4]) if words else (plant_name or '')
    if size:
        q = f"{q} {size}".strip()
    return q.strip()


# --- Регион → slug Avito ----------------------------------------------------

# Маппинг наших регионов к пути Avito. Неполный — для остальных используем 'rossiya'.
REGION_TO_SLUG = {
    'цфо': 'rossiya',
    'москва': 'moskva',
    'московская область': 'moskovskaya_oblast',
    'московская область и москва': 'moskva_i_mo',
    'санкт-петербург': 'sankt-peterburg',
    'ленинградская область': 'leningradskaya_oblast',
    'тула': 'tula',
    'тульская область': 'tulskaya_oblast',
    'калуга': 'kaluga',
    'калужская область': 'kaluzhskaya_oblast',
    'воронеж': 'voronezh',
    'воронежская область': 'voronezhskaya_oblast',
    'тверь': 'tver',
    'тверская область': 'tverskaya_oblast',
}


def pick_region_slug(regions_list: List[str]) -> str:
    """По списку человекочитаемых регионов выбирает самый узкий слаг Avito.
    Если ничего не распознано — 'rossiya'."""
    if not regions_list:
        return 'rossiya'
    # Ищем первое явное совпадение.
    for raw in regions_list:
        low = (raw or '').lower()
        for key, slug in REGION_TO_SLUG.items():
            if key in low:
                return slug
    return 'rossiya'
