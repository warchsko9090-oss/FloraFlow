"""Настройки каталога витрины: горячие позиции, видимость, «По запросу»."""

from __future__ import annotations

from sqlalchemy.orm import joinedload

from app.models import AppSetting, Plant, PriceHistory, ShopCatalogItem, ShopPlantCard, Size, StockBalance, db
from app.shop_prices import get_shop_price_map, resolve_shop_price
from app.stock_helpers import get_reserved_map
from app.utils import msk_now

SEEDLING_HIDDEN_SETTING_KEY = 'shop_seedling_hidden_plant_ids'


def get_seedling_hidden_ids() -> set[int]:
    """Plant IDs, чьи саженцы принудительно скрыты с сайта/PDF (без ALTER схемы)."""
    import json
    try:
        row = AppSetting.query.get(SEEDLING_HIDDEN_SETTING_KEY)
    except Exception:
        db.session.rollback()
        return set()
    if not row or not row.value:
        return set()
    try:
        data = json.loads(row.value)
        if isinstance(data, list):
            return {int(x) for x in data}
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    return set()


def set_seedling_hidden_ids(plant_ids: set[int]) -> None:
    import json
    payload = json.dumps(sorted({int(x) for x in plant_ids}))
    row = AppSetting.query.get(SEEDLING_HIDDEN_SETTING_KEY)
    if row is None:
        db.session.add(AppSetting(key=SEEDLING_HIDDEN_SETTING_KEY, value=payload))
    else:
        row.value = payload


def catalog_site_bucket(item: dict) -> str:
    """Секция витрины: грунт или контейнерная площадка (по полю остатка)."""
    b = item.get('catalog_bucket')
    if b in ('ground', 'container'):
        return b
    if item.get('is_seedling'):
        return 'container'
    return 'ground'


def _field_catalog_bucket(field_id: int, container_ids: set[int] | None = None) -> str:
    from app.services import get_container_field_ids
    cids = container_ids if container_ids is not None else get_container_field_ids()
    return 'container' if int(field_id or 0) in cids else 'ground'


def _buckets_for_plant_size(plant_id: int, size_id: int, container_ids: set[int] | None = None) -> set[str]:
    """На каких секциях витрины может быть пара — по полям в остатках."""
    from app.services import get_container_field_ids
    cids = container_ids if container_ids is not None else get_container_field_ids()
    buckets: set[str] = set()
    for sb in StockBalance.query.filter_by(plant_id=plant_id, size_id=size_id).all():
        buckets.add(_field_catalog_bucket(sb.field_id, cids))
    return buckets or {'ground'}


def _aggregate_stock_by_pair_and_bucket(reserved_map):
    """{(plant_id, size_id, bucket): {free_qty, price, plant, size, ...}} — bucket по полю."""
    from app.services import get_container_field_ids

    eligible = set(get_eligible_size_ids())
    container_ids = get_container_field_ids()
    grouped = {}
    all_stocks = (
        StockBalance.query
        .options(joinedload(StockBalance.plant), joinedload(StockBalance.size))
        .all()
    )
    for st in all_stocks:
        if st.size_id not in eligible:
            continue
        free = max(
            0,
            int(st.quantity or 0)
            - int(reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0) or 0),
        )
        bucket = _field_catalog_bucket(st.field_id, container_ids)
        key = (st.plant_id, st.size_id, bucket)
        if key not in grouped:
            grouped[key] = {
                'plant_id': st.plant_id,
                'size_id': st.size_id,
                'catalog_bucket': bucket,
                'plant': st.plant,
                'size': st.size,
                'free_qty': 0,
                'price': None,
            }
        grouped[key]['free_qty'] += free

    for st in all_stocks:
        if st.size_id not in eligible:
            continue
        bucket = _field_catalog_bucket(st.field_id, container_ids)
        key = (st.plant_id, st.size_id, bucket)
        if key not in grouped:
            grouped[key] = {
                'plant_id': st.plant_id,
                'size_id': st.size_id,
                'catalog_bucket': bucket,
                'plant': st.plant,
                'size': st.size,
                'free_qty': 0,
                'price': None,
            }

    _apply_history_prices(grouped, lambda key, _g: (key[0], key[1]))
    return grouped


def _is_excluded_size_name(name: str) -> bool:
    from app.seedlings import is_excluded_from_product_stock
    return is_excluded_from_product_stock(name)


def _is_seedling_size_name(name: str) -> bool:
    """Вкладка «Саженцы» на сайте: голый «Товарный» / legacy «Саженцы» (без контейнера)."""
    from app.seedlings import STAGE_SAZHENCY, parse_seedling_size
    parsed = parse_seedling_size(name)
    return bool(parsed and parsed.get('bare') and parsed.get('stage') == STAGE_SAZHENCY)


def get_seedling_size_ids():
    """ID размеров группы «Саженцы» (голые, без контейнерной линейки)."""
    return [s.id for s in Size.query.all() if _is_seedling_size_name(s.name)]


def get_measured_seedling_size_ids():
    """ID промеренных саженцев (Саженцы · контейнер · размер)."""
    from app.seedlings import is_measured_seedling_size_name
    return [s.id for s in Size.query.all() if is_measured_seedling_size_name(s.name)]


def get_eligible_size_ids():
    """Размеры, допустимые на витрине (нетоварные/непромеренные саженцы скрыты)."""
    return [s.id for s in Size.query.all() if not _is_excluded_size_name(s.name)]


def _load_shop_plant_cards() -> list:
    """Безопасная загрузка карточек: при отсутствии колонки/ошибке БД — пустой список."""
    try:
        return list(ShopPlantCard.query.all())
    except Exception:
        db.session.rollback()
        return []


def get_plant_display_map() -> dict[int, dict]:
    """{plant_id: {is_hot, display_order, is_hidden, ...}}"""
    result = {}
    hidden_ids = get_seedling_hidden_ids()
    for card in _load_shop_plant_cards():
        result[card.plant_id] = {
            'is_hot': bool(card.is_hot),
            'display_order': int(card.display_order or 0),
            'is_hidden': bool(card.is_hidden),
            'seedling_visible': bool(card.seedling_visible),
            'seedling_on_request': bool(card.seedling_on_request),
            'seedling_hidden': card.plant_id in hidden_ids,
            'root_system': (card.root_system or '').strip(),
            'pruning': (card.pruning or '').strip(),
            'seedling_root_system': (card.seedling_root_system or '').strip(),
            'seedling_pruning': (card.seedling_pruning or '').strip(),
            'seedling_is_hot': bool(card.seedling_is_hot),
            'seedling_display_order': int(card.seedling_display_order or 0),
        }
    return result


def get_catalog_item_map() -> dict[tuple[int, int], ShopCatalogItem]:
    return {(r.plant_id, r.size_id): r for r in ShopCatalogItem.query.all()}


def _price_history_map() -> dict[tuple[int, int], float]:
    """Оптовая цена с витрины/PDF: только из «История цен» (без StockBalance / себестоимости).

    Для пары (plant, size) берём минимальную положительную цену за текущий год;
    если за текущий год нет — за последний год, где есть запись.
    """
    try:
        rows = PriceHistory.query.all()
    except Exception:
        db.session.rollback()
        return {}
    if not rows:
        return {}

    by_pair_year: dict[tuple[int, int], dict[int, list[float]]] = {}
    for h in rows:
        p = float(h.price or 0)
        if p <= 0:
            continue
        key = (int(h.plant_id), int(h.size_id))
        by_pair_year.setdefault(key, {}).setdefault(int(h.year), []).append(p)

    curr_year = msk_now().year
    result = {}
    for key, years in by_pair_year.items():
        if curr_year in years:
            result[key] = min(years[curr_year])
        else:
            y = max(years.keys())
            result[key] = min(years[y])
    return result


def _apply_history_prices(grouped: dict, key_fn) -> None:
    """Проставляет price из истории цен; без записи — None (на витрине станет 0)."""
    hist = _price_history_map()
    for key, g in grouped.items():
        pair = key_fn(key, g)
        g['price'] = hist.get(pair)


def _aggregate_stock_by_pair(reserved_map):
    """{(plant_id, size_id): {free_qty, price, plant, size, ...}} — сумма по всем полям (админка)."""
    by_bucket = _aggregate_stock_by_pair_and_bucket(reserved_map)
    grouped = {}
    for (pid, sid, _bucket), g in by_bucket.items():
        key = (pid, sid)
        if key not in grouped:
            grouped[key] = {
                'plant_id': pid,
                'size_id': sid,
                'plant': g.get('plant'),
                'size': g.get('size'),
                'free_qty': 0,
                'price': g.get('price'),
            }
        grouped[key]['free_qty'] += int(g.get('free_qty') or 0)
        if grouped[key]['price'] is None and g.get('price') is not None:
            grouped[key]['price'] = g['price']
    return grouped


def _aggregate_seedling_stock_by_plant(reserved_map):
    """{plant_id: {free_qty, price, size_id, plant, size}} — голый «Саженец» на контейнерной площадке."""
    from app.services import get_container_field_ids

    seedling_ids = set(get_seedling_size_ids())
    container_ids = get_container_field_ids()
    if not seedling_ids or not container_ids:
        return {}

    grouped = {}
    all_stocks = (
        StockBalance.query
        .options(joinedload(StockBalance.plant), joinedload(StockBalance.size))
        .filter(
            StockBalance.size_id.in_(seedling_ids),
            StockBalance.field_id.in_(container_ids),
        )
        .all()
    )
    for st in all_stocks:
        free = max(
            0,
            int(st.quantity or 0)
            - int(reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0) or 0),
        )
        pid = st.plant_id
        if pid not in grouped:
            grouped[pid] = {
                'plant_id': pid,
                'plant': st.plant,
                'size_id': st.size_id,
                'size': st.size,
                'free_qty': 0,
                'price': None,
            }
        grouped[pid]['free_qty'] += free
        if free > 0:
            grouped[pid]['size_id'] = st.size_id
            grouped[pid]['size'] = st.size

    for st in all_stocks:
        pid = st.plant_id
        if pid not in grouped:
            grouped[pid] = {
                'plant_id': pid,
                'plant': st.plant,
                'size_id': st.size_id,
                'size': st.size,
                'free_qty': 0,
                'price': None,
            }

    _apply_history_prices(
        grouped,
        lambda _pid, g: (int(g['plant_id']), int(g['size_id'])),
    )
    return grouped


def collect_seedlings_for_admin():
    """Растения с саженцами (голые «Саженцы» и/или промеренные) для вкладки админки."""
    reserved_map = get_reserved_map()
    stock_by_plant = _aggregate_seedling_stock_by_plant(reserved_map)
    cards = {c.plant_id: c for c in _load_shop_plant_cards()}
    plant_display = get_plant_display_map()

    # Промеренные саженцы тоже нужно уметь принудительно скрывать с сайта/PDF.
    measured_ids = set(get_measured_seedling_size_ids())
    measured_free: dict[int, int] = {}
    measured_size: dict[int, int] = {}
    if measured_ids:
        for st in (
            StockBalance.query
            .options(joinedload(StockBalance.plant), joinedload(StockBalance.size))
            .filter(StockBalance.size_id.in_(measured_ids))
            .all()
        ):
            free = max(
                0,
                int(st.quantity or 0)
                - int(reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0) or 0),
            )
            measured_free[st.plant_id] = measured_free.get(st.plant_id, 0) + free
            if free > 0 or st.plant_id not in measured_size:
                measured_size[st.plant_id] = st.size_id

    plant_ids = set(stock_by_plant.keys()) | set(measured_free.keys())
    hidden_ids = get_seedling_hidden_ids()
    # Карточки с уже включённым скрытием / показом — даже без остатка.
    for pid, card in cards.items():
        if card.seedling_visible or card.seedling_on_request or pid in hidden_ids:
            plant_ids.add(pid)
    plant_ids |= hidden_ids

    hist = _price_history_map()
    plants = {p.id: p for p in Plant.query.filter(Plant.id.in_(plant_ids)).all()} if plant_ids else {}

    rows = []
    for plant_id in sorted(plant_ids, key=lambda pid: (plants.get(pid).name if plants.get(pid) else '').lower()):
        plant = plants.get(plant_id) or Plant.query.get(plant_id)
        if not plant:
            continue
        stock = stock_by_plant.get(plant_id, {})
        bare_free = int(stock.get('free_qty') or 0)
        meas_free = int(measured_free.get(plant_id) or 0)
        free_qty = bare_free + meas_free
        size_id = int(stock.get('size_id') or measured_size.get(plant_id) or 0)
        if not size_id:
            continue
        card = cards.get(plant_id)
        pd = plant_display.get(plant_id, {})
        base_price = float(hist.get((plant_id, size_id), 0) or 0)
        rows.append({
            'plant_id': plant_id,
            'size_id': size_id,
            'plant_name': plant.name,
            'free_qty': free_qty,
            'base_price': base_price,
            'wholesale_price': base_price,
            'seedling_visible': bool(card.seedling_visible) if card else False,
            'seedling_on_request': bool(card.seedling_on_request) if card else False,
            'seedling_hidden': plant_id in hidden_ids,
            'is_hot': pd.get('seedling_is_hot', False),
            'display_order': pd.get('seedling_display_order', 0),
            'root_system': pd.get('seedling_root_system', ''),
            'pruning': pd.get('seedling_pruning', ''),
        })
    return rows


def save_seedling_settings_from_form(form):
    """Сохраняет видимость саженцев и «По запросу» из вкладки «Саженцы»."""
    rows = collect_seedlings_for_admin()
    existing_cards = {c.plant_id: c for c in _load_shop_plant_cards()}
    changed = 0
    hidden_ids = set(get_seedling_hidden_ids())

    for row in rows:
        pid = row['plant_id']
        vis = bool(form.get(f'seedling_visible_{pid}'))
        on_req = bool(form.get(f'seedling_on_request_{pid}'))
        hidden = bool(form.get(f'seedling_hidden_{pid}'))
        is_hot = bool(form.get(f'seedling_hot_{pid}'))
        root_system = (form.get(f'seedling_root_system_{pid}') or '').strip()
        pruning = (form.get(f'seedling_pruning_{pid}') or '').strip()
        try:
            display_order = int(form.get(f'seedling_order_{pid}') or 0)
        except (TypeError, ValueError):
            display_order = 0

        if hidden and pid not in hidden_ids:
            hidden_ids.add(pid)
            changed += 1
        elif not hidden and pid in hidden_ids:
            hidden_ids.discard(pid)
            changed += 1

        card = existing_cards.get(pid)
        needs_card = (
            vis or on_req or is_hot or display_order != 0 or root_system or pruning
        )
        if card is None and needs_card:
            card = ShopPlantCard(plant_id=pid)
            db.session.add(card)
            existing_cards[pid] = card
            changed += 1

        if card is None:
            continue

        if bool(card.seedling_visible) != vis:
            card.seedling_visible = vis
            changed += 1
        if bool(card.seedling_on_request) != on_req:
            card.seedling_on_request = on_req
            changed += 1
        if bool(card.seedling_is_hot) != is_hot:
            card.seedling_is_hot = is_hot
            changed += 1
        if int(card.seedling_display_order or 0) != display_order:
            card.seedling_display_order = display_order
            changed += 1
        if (card.seedling_root_system or '').strip() != root_system:
            card.seedling_root_system = root_system
            changed += 1
        if (card.seedling_pruning or '').strip() != pruning:
            card.seedling_pruning = pruning
            changed += 1

    set_seedling_hidden_ids(hidden_ids)
    return changed


def _wholesale_price_for_pair(plant_id, size_id, stock_group):
    """Оптовая цена из «Истории цен» (без розничных переопределений и без StockBalance)."""
    if stock_group and stock_group.get('price') is not None:
        return float(stock_group['price'] or 0)
    return 0.0


def _base_price_for_pair(plant_id, size_id, stock_group, price_overrides):
    """Оставлено для совместимости — возвращает оптовую цену."""
    return _wholesale_price_for_pair(plant_id, size_id, stock_group)


def collect_catalog_pairs_for_admin():
    """Все пары (растение, размер) для вкладки «Каталог» в админке."""
    reserved_map = get_reserved_map()
    stock_groups = _aggregate_stock_by_pair(reserved_map)
    catalog_map = get_catalog_item_map()
    price_keys = set(get_shop_price_map().keys())

    keys = set(stock_groups.keys()) | set(catalog_map.keys()) | price_keys
    rows = []
    plants = {p.id: p for p in Plant.query.all()}
    sizes = {s.id: s for s in Size.query.all()}
    plant_display = get_plant_display_map()
    price_overrides = get_shop_price_map()

    for plant_id, size_id in sorted(keys, key=lambda k: (
        (plants.get(k[0]).name if plants.get(k[0]) else '').lower(),
        (sizes.get(k[1]).name if sizes.get(k[1]) else '').lower(),
    )):
        plant = plants.get(plant_id)
        size = sizes.get(size_id)
        if not plant or not size or _is_excluded_size_name(size.name):
            continue
        stock = stock_groups.get((plant_id, size_id), {})
        free_qty = int(stock.get('free_qty') or 0)
        settings = catalog_map.get((plant_id, size_id))
        pd = plant_display.get(plant_id, {})
        rows.append({
            'plant_id': plant_id,
            'size_id': size_id,
            'plant_name': plant.name,
            'size_name': size.name,
            'free_qty': free_qty,
            'base_price': _wholesale_price_for_pair(plant_id, size_id, stock),
            'is_visible': bool(settings.is_visible) if settings else True,
            'show_on_request': bool(settings.show_on_request) if settings else False,
            'is_hot': pd.get('is_hot', False),
            'display_order': pd.get('display_order', 0),
            'is_hidden': pd.get('is_hidden', False),
            'root_system': pd.get('root_system', ''),
            'pruning': pd.get('pruning', ''),
            'has_settings': settings is not None,
        })
    return rows


def save_catalog_settings_from_form(form):
    """Сохраняет чекбоксы и порядок из POST вкладки «Каталог»."""
    rows = collect_catalog_pairs_for_admin()
    existing_items = get_catalog_item_map()
    existing_cards = {c.plant_id: c for c in _load_shop_plant_cards()}
    changed = 0

    seen_plants = set()
    for row in rows:
        pid, sid = row['plant_id'], row['size_id']
        key = (pid, sid)

        vis = bool(form.get(f'visible_{pid}_{sid}'))
        on_req = bool(form.get(f'on_request_{pid}_{sid}'))
        default_vis = True
        default_on_req = False

        item = existing_items.get(key)
        if vis != default_vis or on_req != default_on_req:
            if item is None:
                item = ShopCatalogItem(plant_id=pid, size_id=sid)
                db.session.add(item)
                existing_items[key] = item
                changed += 1
            if bool(item.is_visible) != vis:
                item.is_visible = vis
                changed += 1
            if bool(item.show_on_request) != on_req:
                item.show_on_request = on_req
                changed += 1
        elif item is not None:
            db.session.delete(item)
            existing_items.pop(key, None)
            changed += 1

        if pid in seen_plants:
            continue
        seen_plants.add(pid)

        is_hot = bool(form.get(f'hot_{pid}'))
        is_hidden = bool(form.get(f'hidden_{pid}'))
        root_system = (form.get(f'root_system_{pid}') or '').strip()
        pruning = (form.get(f'pruning_{pid}') or '').strip()
        try:
            display_order = int(form.get(f'order_{pid}') or 0)
        except (TypeError, ValueError):
            display_order = 0

        card = existing_cards.get(pid)
        needs_card = (
            is_hot or is_hidden or display_order != 0 or root_system or pruning
            or bool(card and (card.seedling_visible or card.seedling_on_request
                              or card.seedling_is_hot or card.seedling_display_order
                              or (card.seedling_root_system or '').strip()
                              or (card.seedling_pruning or '').strip()))
        )
        if card is None and needs_card:
            card = ShopPlantCard(plant_id=pid)
            db.session.add(card)
            existing_cards[pid] = card
            changed += 1

        if card is not None:
            if bool(card.is_hot) != is_hot:
                card.is_hot = is_hot
                changed += 1
            if bool(card.is_hidden) != is_hidden:
                card.is_hidden = is_hidden
                changed += 1
            if int(card.display_order or 0) != display_order:
                card.display_order = display_order
                changed += 1
            if (card.root_system or '').strip() != root_system:
                card.root_system = root_system
                changed += 1
            if (card.pruning or '').strip() != pruning:
                card.pruning = pruning
                changed += 1
            if not needs_card:
                db.session.delete(card)
                existing_cards.pop(pid, None)
                changed += 1

    return changed


def _catalog_card_attrs(plant_display: dict, bucket: str) -> dict:
    """Атрибуты карточки витрины: грунт и контейнерная площадка раздельно."""
    if bucket == 'container':
        return {
            'root_system': plant_display.get('seedling_root_system', ''),
            'pruning': plant_display.get('seedling_pruning', ''),
            'is_hot': bool(plant_display.get('seedling_is_hot')),
            'display_order': int(plant_display.get('seedling_display_order') or 0),
        }
    return {
        'root_system': plant_display.get('root_system', ''),
        'pruning': plant_display.get('pruning', ''),
        'is_hot': bool(plant_display.get('is_hot')),
        'display_order': int(plant_display.get('display_order') or 0),
    }


def _catalog_photo_rel(root: str, plant_id: int, plant_name: str, bucket: str, cache: dict) -> str | None:
    from app.photo_storage import PHOTO_VARIANT_CONTAINER, PHOTO_VARIANT_GROUND, resolve_photo_source

    key = (plant_id, bucket)
    if key not in cache:
        variant = PHOTO_VARIANT_CONTAINER if bucket == 'container' else PHOTO_VARIANT_GROUND
        rel_dir, files = resolve_photo_source(root, plant_id, plant_name, variant=variant)
        cache[key] = f'{rel_dir}/{files[0]}' if files else None
    return cache[key]


def build_visible_catalog_items(apply_shop_prices=True):
    """Собирает позиции для публичной витрины с учётом настроек."""
    from app.shop_images import SHOP_IMG_CARD, shop_image_url
    from app.seedlings import size_name_export_label
    from flask import current_app

    reserved_map = get_reserved_map()
    stock_groups = _aggregate_stock_by_pair_and_bucket(reserved_map)
    catalog_map = get_catalog_item_map()
    plant_display = get_plant_display_map()
    price_overrides = get_shop_price_map()

    keys = set(stock_groups.keys())
    for (plant_id, size_id), settings in catalog_map.items():
        if not settings.show_on_request:
            continue
        for bucket in _buckets_for_plant_size(plant_id, size_id):
            keys.add((plant_id, size_id, bucket))

    root = current_app.config['UPLOAD_FOLDER']
    photo_cache = {}
    items = []

    for plant_id, size_id, bucket in keys:
        pd = plant_display.get(plant_id, {})
        if pd.get('is_hidden'):
            continue

        settings = catalog_map.get((plant_id, size_id))
        if settings is not None and not settings.is_visible:
            continue

        stock = stock_groups.get((plant_id, size_id, bucket), {})
        free_qty = int(stock.get('free_qty') or 0)
        show_on_request = bool(settings and settings.show_on_request)

        if free_qty <= 0 and not show_on_request:
            continue

        plant = stock.get('plant') or Plant.query.get(plant_id)
        size = stock.get('size') or Size.query.get(size_id)
        if not plant or not size or _is_excluded_size_name(size.name):
            continue

        # Принудительно скрытые саженцы (промеренные) — не на сайте и не в PDF.
        from app.seedlings import is_measured_seedling_size_name
        if pd.get('seedling_hidden') and is_measured_seedling_size_name(size.name):
            continue

        on_request = free_qty <= 0 and show_on_request

        card_attrs = _catalog_card_attrs(pd, bucket)
        photo_rel = _catalog_photo_rel(root, plant_id, plant.name, bucket, photo_cache)
        row = {
            'plant_id': plant_id,
            'size_id': size_id,
            'plant_name': plant.name,
            'size_name': size_name_export_label(size.name),
            'latin_name': (plant.latin_name or ''),
            'characteristic': (plant.characteristic or ''),
            'root_system': card_attrs['root_system'],
            'pruning': card_attrs['pruning'],
            'free_qty': free_qty,
            'on_request': on_request,
            'is_hot': card_attrs['is_hot'],
            'display_order': card_attrs['display_order'],
            'price': _wholesale_price_for_pair(plant_id, size_id, stock),
            'has_photos': bool(photo_rel),
            'photo_rel': photo_rel,
            'photo_url': shop_image_url(photo_rel, SHOP_IMG_CARD),
            'catalog_bucket': bucket,
        }
        items.append(row)

    if apply_shop_prices:
        from app.shop_prices import apply_shop_prices_to_catalog
        apply_shop_prices_to_catalog(items)

    return items


def build_visible_seedling_items(apply_shop_prices=True):
    """Позиции-саженцы для витрины (без размеров в UI, один plant_id)."""
    from app.shop_images import SHOP_IMG_CARD, shop_image_url
    from app.seedlings import size_name_export_label
    from flask import current_app

    if not get_seedling_size_ids():
        return []

    reserved_map = get_reserved_map()
    stock_by_plant = _aggregate_seedling_stock_by_plant(reserved_map)
    plant_display = get_plant_display_map()
    price_overrides = get_shop_price_map()
    cards = {c.plant_id: c for c in _load_shop_plant_cards()}
    hidden_ids = get_seedling_hidden_ids()

    root = current_app.config['UPLOAD_FOLDER']
    photo_cache = {}
    items = []

    for plant_id, stock in stock_by_plant.items():
        card = cards.get(plant_id)
        if not card or not card.seedling_visible:
            continue
        if plant_id in hidden_ids:
            continue

        free_qty = int(stock.get('free_qty') or 0)
        on_request = free_qty <= 0 and bool(card.seedling_on_request)
        if free_qty <= 0 and not on_request:
            continue

        plant = stock.get('plant') or Plant.query.get(plant_id)
        size = stock.get('size') or Size.query.get(stock['size_id'])
        if not plant or not size:
            continue

        size_id = int(stock['size_id'])
        pd = plant_display.get(plant_id, {})

        photo_rel = _catalog_photo_rel(root, plant_id, plant.name, 'container', photo_cache)
        row = {
            'plant_id': plant_id,
            'size_id': size_id,
            'plant_name': plant.name,
            'size_name': size_name_export_label(size.name) or 'Саженцы',
            'latin_name': (plant.latin_name or ''),
            'characteristic': (plant.characteristic or ''),
            'root_system': pd.get('seedling_root_system', ''),
            'pruning': pd.get('seedling_pruning', ''),
            'free_qty': free_qty,
            'on_request': on_request,
            'is_hot': pd.get('seedling_is_hot', False),
            'display_order': pd.get('seedling_display_order', 0),
            'is_seedling': True,
            'price': _wholesale_price_for_pair(plant_id, size_id, stock),
            'has_photos': bool(photo_rel),
            'photo_rel': photo_rel,
            'photo_url': shop_image_url(photo_rel, SHOP_IMG_CARD),
        }
        row['catalog_bucket'] = 'container'
        items.append(row)

    if apply_shop_prices:
        from app.shop_prices import apply_shop_prices_to_catalog
        apply_shop_prices_to_catalog(items)

    return items


def sort_catalog_items(items):
    """Горячие выше (по display_order), затем по названию."""
    return sorted(
        items,
        key=lambda x: (
            0 if x.get('is_hot') else 1,
            int(x.get('display_order') or 0),
            (x.get('plant_name') or '').lower(),
            (x.get('size_name') or '').lower(),
        ),
    )


def sort_plant_groups(plants):
    """Сортировка карточек растений на главной витрины."""
    return sorted(
        plants,
        key=lambda x: (
            0 if x.get('is_hot') else 1,
            int(x.get('display_order') or 0),
            (x.get('name') or '').lower(),
        ),
    )
