"""Цены витрины /shop — розница поверх оптовой цены в ERP."""

from copy import deepcopy
from decimal import Decimal, InvalidOperation

from flask_login import current_user

from app.models import ShopPrice, db

RETAIL_PRICE_MULTIPLIER = 2


def _to_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def get_shop_price_map():
    """{(plant_id, size_id): retail override} — только явные переопределения."""
    rows = ShopPrice.query.all()
    return {(r.plant_id, r.size_id): _to_float(r.price) for r in rows}


def default_retail_price(wholesale_price):
    """Розница по умолчанию: опт × 2."""
    return _to_float(wholesale_price) * RETAIL_PRICE_MULTIPLIER


def order_default_price(plant_id, size_id, wholesale_price, overrides=None):
    """Цена новой позиции заказа: розница (override или опт×2)."""
    return resolve_shop_price(plant_id, size_id, wholesale_price, overrides)


def resolve_shop_price(plant_id, size_id, wholesale_price, overrides=None):
    """Розничная цена для витрины: явное переопределение (> опта) или опт×2."""
    wholesale = _to_float(wholesale_price)
    default_retail = default_retail_price(wholesale)
    ov = overrides if overrides is not None else get_shop_price_map()
    key = (int(plant_id), int(size_id))
    if key in ov:
        custom = _to_float(ov[key])
        # Игнорируем устаревшие записи, где в ShopPrice попал опт (равен оптовой).
        if custom > wholesale + 0.004:
            return custom
    return default_retail


def apply_shop_prices_to_catalog(catalog_items, overrides=None):
    """Подставляет розничные цены в элементы каталога (in-place)."""
    ov = overrides if overrides is not None else get_shop_price_map()
    for item in catalog_items:
        wholesale = _to_float(item.get('wholesale_price', item.get('base_price', item.get('price'))))
        item['wholesale_price'] = wholesale
        item['base_price'] = wholesale
        item['default_retail_price'] = default_retail_price(wholesale)
        item['price'] = resolve_shop_price(
            item['plant_id'], item['size_id'], wholesale, ov,
        )
        custom = ov.get((item['plant_id'], item['size_id']))
        item['has_shop_override'] = custom is not None and _to_float(custom) > wholesale + 0.004
    return catalog_items


def build_admin_price_rows(catalog_items):
    """Строки для вкладки «Цены» в shop-admin."""
    ov = get_shop_price_map()
    rows = []
    for item in catalog_items:
        wholesale = _to_float(item.get('wholesale_price', item.get('base_price', item.get('price'))))
        default_retail = default_retail_price(wholesale)
        site = resolve_shop_price(item['plant_id'], item['size_id'], wholesale, ov)
        custom = ov.get((item['plant_id'], item['size_id']))
        rows.append({
            'plant_id': item['plant_id'],
            'size_id': item['size_id'],
            'plant_name': item['plant_name'],
            'size_name': item['size_name'],
            'wholesale_price': wholesale,
            'default_retail_price': default_retail,
            'site_price': site,
            'has_override': custom is not None and _to_float(custom) > wholesale + 0.004,
        })
    return rows


def save_shop_prices_from_form(form, catalog_items):
    """
    Сохраняет розничные цены из POST (price_{plant_id}_{size_id}).
    Пустое или равное опт×2 — удаляет переопределение.
    Нельзя ниже оптовой.
    """
    catalog_map = {
        (i['plant_id'], i['size_id']): i
        for i in catalog_items
    }
    existing = {
        (r.plant_id, r.size_id): r
        for r in ShopPrice.query.all()
    }
    user_id = current_user.id if current_user and current_user.is_authenticated else None
    changed = 0
    rejected_below_wholesale = 0

    for key, item in catalog_map.items():
        field = f"price_{key[0]}_{key[1]}"
        raw = (form.get(field) or '').strip().replace(' ', '').replace(',', '.')
        wholesale = _to_float(
            item.get('wholesale_price', item.get('base_price', item.get('price'))),
        )
        default_retail = default_retail_price(wholesale)

        if not raw:
            row = existing.pop(key, None)
            if row:
                db.session.delete(row)
                changed += 1
            continue

        try:
            new_price = float(Decimal(raw))
        except (InvalidOperation, ValueError):
            continue
        if new_price < wholesale - 0.004:
            rejected_below_wholesale += 1
            continue

        if abs(new_price - default_retail) < 0.005:
            row = existing.pop(key, None)
            if row:
                db.session.delete(row)
                changed += 1
            continue

        row = existing.get(key)
        if row:
            if abs(_to_float(row.price) - new_price) >= 0.005:
                row.price = new_price
                row.updated_by_user_id = user_id
                changed += 1
        else:
            db.session.add(ShopPrice(
                plant_id=key[0],
                size_id=key[1],
                price=new_price,
                updated_by_user_id=user_id,
            ))
            changed += 1

    return changed, rejected_below_wholesale


def transform_stock_report_price_mode(sorted_groups, price_mode='wholesale'):
    """Для выгрузки остатков: wholesale — как в отчёте, retail — прайсовая (сайт)."""
    if price_mode != 'retail':
        return sorted_groups

    ov = get_shop_price_map()
    out = []
    for group in sorted_groups:
        gd = deepcopy(group)
        if gd.get('is_section'):
            out.append(gd)
            continue
        # Поддерживаем оба формата групп:
        # 1) admin stock: {'name': ..., 'data': {'rows': [...], 'totals': {...}}}
        # 2) public stock pdf: {'name': ..., 'rows': [...], 'totals': {...}}
        data = gd.get('data')
        if isinstance(data, dict):
            rows = data.get('rows') or []
            totals = dict(data.get('totals') or {})
        else:
            rows = gd.get('rows') or []
            totals = dict(gd.get('totals') or {})

        new_rows = []
        for row in rows:
            nr = dict(row)
            wholesale = _to_float(nr.get('price'))
            retail = resolve_shop_price(nr.get('plant_id') or 0, nr.get('size_id') or 0, wholesale, ov)
            nr['price'] = retail
            free = _to_float(nr.get('free'))
            qty = _to_float(nr.get('quantity'))
            nr['free_sum'] = retail * free
            nr['sum'] = retail * qty
            new_rows.append(nr)

        totals['sum'] = sum(_to_float(r.get('sum')) for r in new_rows)
        totals['free_sum'] = sum(_to_float(r.get('free_sum')) for r in new_rows)
        if isinstance(data, dict):
            data['rows'] = new_rows
            data['totals'] = totals
            gd['data'] = data
        else:
            gd['rows'] = new_rows
            gd['totals'] = totals

        out.append(gd)
    return out


def draft_line_prices_from_payload(payload):
    """{(plant_id, size_id): price} из сохранённой заявки с сайта."""
    result = {}
    for line in payload.get('lines') or []:
        try:
            pid = int(line['plant_id'])
            sid = int(line['size_id'])
            result[(pid, sid)] = _to_float(line.get('price'))
        except (KeyError, TypeError, ValueError):
            continue
    return result


def draft_row_price(payload, plant_id, size_id, fallback=None):
    prices = draft_line_prices_from_payload(payload)
    key = (int(plant_id), int(size_id))
    if key in prices:
        return prices[key]
    return fallback


def wholesale_for_order_item(item, hist_map=None):
    """Оптовая цена позиции из прайса (История цен / остатки)."""
    from app.utils import get_actual_price
    from app.shop_catalog import _price_history_map

    pid = int(item.plant_id or 0)
    sid = int(item.size_id or 0)
    if not pid or not sid:
        return 0.0
    wholesale = _to_float(get_actual_price(pid, sid, item.field_id, item.year))
    if wholesale > 0:
        return wholesale
    hist = hist_map if hist_map is not None else _price_history_map()
    return _to_float(hist.get((pid, sid), 0))


def list_prices_for_order_item(item, overrides=None, hist_map=None):
    """(wholesale, retail) для позиции заказа."""
    wholesale = wholesale_for_order_item(item, hist_map=hist_map)
    retail = resolve_shop_price(item.plant_id, item.size_id, wholesale, overrides)
    return wholesale, _to_float(retail)


def price_with_discount(base_price, discount_pct):
    """База минус скидка %. Не уходит ниже 0."""
    base = _to_float(base_price)
    try:
        pct = float(discount_pct or 0)
    except (TypeError, ValueError):
        pct = 0.0
    if pct < 0:
        pct = 0.0
    if pct > 100:
        pct = 100.0
    return round(max(0.0, base * (1.0 - pct / 100.0)), 2)


def build_order_price_editor_rows(items):
    """Строки для UI редактора цен в заказе."""
    ov = get_shop_price_map()
    try:
        from app.shop_catalog import _price_history_map
        hist = _price_history_map()
    except Exception:
        hist = {}
    rows = []
    for it in items or []:
        wholesale, retail = list_prices_for_order_item(it, overrides=ov, hist_map=hist)
        rows.append({
            'id': it.id,
            'plant_name': it.plant.name if it.plant else '',
            'size_name': it.size.name if it.size else '',
            'qty': int(it.quantity or 0),
            'current_price': _to_float(it.price),
            'wholesale': wholesale,
            'retail': retail,
        })
    return rows
