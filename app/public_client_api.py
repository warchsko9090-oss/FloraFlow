import json
import os
import re
from datetime import datetime
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request, url_for, render_template, abort, redirect, make_response, send_from_directory
from sqlalchemy.orm import joinedload

from app.models import AppSetting, Document, DocumentRow, Field, Plant, Size, StockBalance, db
from app.photo_storage import resolve_photo_source, list_sidebar_files, SIDEBAR_REL_DIR
from app.shop_contacts import (
    contact_display_label,
    contact_href,
    get_shop_contacts_for_kp,
    get_shop_contacts_for_site,
)
from app.shop_contacts_page import (
    get_company_requisites,
    get_contacts_page_settings,
    map_embed_markup,
)
from app.shop_images import (
    SHOP_IMG_BG,
    SHOP_IMG_CARD,
    SHOP_IMG_PREVIEW,
    SHOP_IMG_THUMB,
    build_resized_image_response,
    shop_image_url,
)
from app.shop_plant_cards import get_plant_card_map
from app.shop_prices import apply_shop_prices_to_catalog, draft_line_prices_from_payload, get_shop_price_map, resolve_shop_price, transform_stock_report_price_mode
from app.shop_catalog import (
    build_visible_catalog_items,
    build_visible_seedling_items,
    sort_catalog_items,
    sort_plant_groups,
)
from app.stock_helpers import get_reserved_map
from app.utils import msk_now, create_pdf_response, natural_key
from app.rate_limit import rate_limit, check_rate_limit
from app.shop_security import shop_kp_access_token, verify_shop_kp_access
from app.shop_landing_page import get_landing_context

bp = Blueprint("public_client_api", __name__)


_UPLOAD_SUFFIX_RE = re.compile(r"_(\d{10,})(_\d+)?$")


def _display_name_from_filename(filename: str) -> str:
    stem, _ = os.path.splitext(filename or "")
    stem = (stem or "").strip()
    stem = _UPLOAD_SUFFIX_RE.sub("", stem)
    return stem or "photo"


def _is_token_valid():
    expected = os.environ.get("ERP_PUBLIC_TOKEN", "").strip()
    if not expected:
        # If token is not configured, block public API by default.
        return False
    given = (request.headers.get("X-Public-Token") or "").strip()
    return given == expected


def _forbidden():
    return jsonify({"status": "error", "message": "forbidden"}), 403


# ==========================================================
# 1. СТАРЫЙ API ДЛЯ ВНЕШНИХ ИНТЕГРАЦИЙ (ПО ТОКЕНУ)
# ==========================================================

@bp.get("/public/client/catalog")
def public_catalog():
    if not _is_token_valid():
        return _forbidden()

    reserved_map = get_reserved_map()
    price_overrides = get_shop_price_map()

    all_stocks = (
        StockBalance.query
        .options(joinedload(StockBalance.plant), joinedload(StockBalance.size))
        .all()
    )
    grouped = {}
    for st in all_stocks:
        free = max(
            0,
            int(st.quantity or 0)
            - int(reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0) or 0),
        )
        if free <= 0:
            continue

        key = (st.plant_id, st.size_id)
        if key not in grouped:
            grouped[key] = {
                "plant_id": st.plant_id,
                "size_id": st.size_id,
                "plant_name": st.plant.name if st.plant else "-",
                "size_name": st.size.name if st.size else "-",
                "free_qty": 0,
                "price": None,
            }
        grouped[key]["free_qty"] += free
        wholesale = float(st.price or 0)
        retail = resolve_shop_price(st.plant_id, st.size_id, wholesale, price_overrides)
        cur = grouped[key]["price"]
        grouped[key]["price"] = retail if cur is None else min(cur, retail)

    items = []
    for row in grouped.values():
        root = current_app.config["UPLOAD_FOLDER"]
        rel_dir, files = resolve_photo_source(root, row["plant_id"], row["plant_name"])
        photo_url = None
        if files:
            photo_url = url_for("main.serve_uploaded_file", filename=f"{rel_dir}/{files[0]}")

        items.append(
            {
                "plant_id": row["plant_id"],
                "size_id": row["size_id"],
                "plant_name": row["plant_name"],
                "size_name": row["size_name"],
                "free_qty": int(row["free_qty"]),
                "price": float(row["price"] or 0),
                "photo_url": photo_url,
            }
        )
    items.sort(key=lambda x: (x["plant_name"].lower(), x["size_name"].lower()))
    return jsonify({"status": "ok", "items": items})


@bp.post("/public/client/draft-order")
def public_draft_order():
    if not _is_token_valid():
        return _forbidden()

    data = request.get_json(silent=True) or {}
    customer_name = (data.get("customer_name") or "").strip()
    phone = (data.get("phone") or "").strip()
    comment = (data.get("comment") or "").strip()
    items = data.get("items") or []

    if len(customer_name) < 2:
        return jsonify({"status": "error", "message": "Укажите имя/компанию"}), 400
    if len(customer_name) > 200:
        return jsonify({"status": "error", "message": "Слишком длинное имя"}), 400
    if len(comment) > 500:
        return jsonify({"status": "error", "message": "Комментарий слишком длинный"}), 400
    if not items:
        return jsonify({"status": "error", "message": "Не выбраны позиции"}), 400

    valid_rows = []
    for item in items:
        try:
            plant_id = int(item.get("plant_id"))
            size_id = int(item.get("size_id"))
            qty = int(item.get("quantity"))
        except Exception:
            continue
        if qty <= 0:
            continue
        if not Plant.query.get(plant_id) or not Size.query.get(size_id):
            continue
        valid_rows.append((plant_id, size_id, qty))

    if not valid_rows:
        return jsonify({"status": "error", "message": "Нет валидных позиций"}), 400

    payload = {
        "customer_name": customer_name,
        "phone": phone,
        "comment": comment,
        "created_at": msk_now().isoformat(),
        "source": "public_client_api",
    }

    try:
        doc = Document(
            doc_type="client_draft",
            date=msk_now(),
            user_id=None,
            comment=json.dumps(payload, ensure_ascii=False),
        )
        db.session.add(doc)
        db.session.flush()

        current_year = datetime.utcnow().year
        for plant_id, size_id, qty in valid_rows:
            db.session.add(
                DocumentRow(
                    document_id=doc.id,
                    plant_id=plant_id,
                    size_id=size_id,
                    quantity=qty,
                    year=current_year,
                )
            )

        db.session.commit()
        try:
            from app.shop_telegram import notify_shop_operator_kp

            notify_shop_operator_kp(payload, doc.id)
        except Exception:
            current_app.logger.exception('Draft-order Telegram notify failed (doc #%s)', doc.id)
        try:
            from app.shop_email import notify_kp_draft

            notify_kp_draft(payload, doc.id)
        except Exception:
            current_app.logger.exception('Draft-order email notify failed (doc #%s)', doc.id)
        try:
            from app.shop_push import notify_kp_draft as notify_kp_draft_push

            notify_kp_draft_push(payload, doc.id)
        except Exception:
            current_app.logger.exception('Draft-order push notify failed (doc #%s)', doc.id)
        return jsonify({"status": "ok", "draft_id": doc.id})
    except Exception as exc:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(exc)}), 500


# ==========================================================
# 2. НОВАЯ БЕЗОПАСНАЯ ПУБЛИЧНАЯ ВИТРИНА (/shop) И ГАЛЕРЕЯ
# ==========================================================

def _render_landing_page(redirect_endpoint: str):
    """Общая логика главной страницы (prod `/` на shop, `/landing-preview` на ERP)."""
    if request.method == "POST":
        if not check_rate_limit("shop_landing_inquiry", 5, 3600):
            abort(429)
        if not (request.form.get("pd_consent") or "").strip():
            return redirect(url_for(redirect_endpoint, err="consent") + "#action")

        from app.shop_on_request import create_landing_inquiry

        try:
            row = create_landing_inquiry(
                request.form.get("customer_name", ""),
                request.form.get("phone", ""),
                request.form.get("message", ""),
            )
            db.session.commit()
            try:
                from app.shop_telegram import notify_shop_operator_on_request

                notify_shop_operator_on_request(row, source='landing')
            except Exception:
                current_app.logger.exception('Landing inquiry Telegram notify failed')
            try:
                from app.shop_email import notify_on_request

                notify_on_request(row, source='landing')
            except Exception:
                current_app.logger.exception('Landing inquiry email notify failed')
            try:
                from app.shop_push import notify_on_request as notify_on_request_push

                notify_on_request_push(row, source='landing')
            except Exception:
                current_app.logger.exception('Landing inquiry push notify failed')
            return redirect(url_for(redirect_endpoint, sent=1))
        except ValueError as exc:
            db.session.rollback()
            return redirect(url_for(redirect_endpoint, err=str(exc)) + "#action")
        except Exception:
            db.session.rollback()
            current_app.logger.exception("landing inquiry failed")
            return redirect(url_for(redirect_endpoint, err="server") + "#action")

    ctx = get_landing_context(
        form_sent=request.args.get("sent") == "1",
        form_error=(request.args.get("err") or "").strip(),
    )
    resp = make_response(render_template("public_landing.html", **ctx))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@bp.route("/", methods=["GET", "POST"])
def public_landing():
    """Главная посадочная страница knyajestvo.ru."""
    return _render_landing_page("public_client_api.public_landing")


@bp.route("/landing-preview", methods=["GET", "POST"])
def landing_preview():
    """Локальный предпросмотр главной на ERP (корневой ``/`` → дашборд)."""
    return _render_landing_page("public_client_api.landing_preview")


@bp.get("/yandex_779ccc68e1d1e3fd.html")
def yandex_webmaster_verify():
    """Файл подтверждения прав Яндекс.Вебмастер для knyajestvo.ru."""
    return send_from_directory(
        current_app.static_folder,
        "yandex_779ccc68e1d1e3fd.html",
        mimetype="text/html; charset=utf-8",
    )


@bp.get("/robots.txt")
def public_robots_txt():
    base = (request.url_root or "https://knyajestvo.ru/").rstrip("/")
    body = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /public/\n"
        "Disallow: /shop/kp/\n"
        "Disallow: /shop/on-request\n"
        f"Sitemap: {base}/sitemap.xml\n"
    )
    resp = make_response(body)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    return resp


@bp.get("/sitemap.xml")
def public_sitemap_xml():
    """Карта основных публичных страниц для Яндекс/Google."""
    from datetime import timezone

    base = (request.url_root or "https://knyajestvo.ru/").rstrip("/")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urls = [
        ("/", "1.0", "weekly"),
        ("/shop", "0.9", "daily"),
        ("/shop/contacts", "0.7", "monthly"),
        ("/shop/privacy", "0.3", "yearly"),
        ("/shop/consent", "0.3", "yearly"),
        ("/shop/stock.pdf", "0.6", "daily"),
    ]
    try:
        for item in get_aggregated_catalog(apply_shop_prices=False):
            pid = item.get("plant_id")
            if pid:
                urls.append((f"/shop/plant/{int(pid)}", "0.8", "weekly"))
    except Exception:
        current_app.logger.exception("sitemap: catalog plants skipped")

    # уникальные пути с сохранением порядка
    seen = set()
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path, priority, changefreq in urls:
        if path in seen:
            continue
        seen.add(path)
        loc = f"{base}{path}"
        lines.append("  <url>")
        lines.append(f"    <loc>{loc}</loc>")
        lines.append(f"    <lastmod>{today}</lastmod>")
        lines.append(f"    <changefreq>{changefreq}</changefreq>")
        lines.append(f"    <priority>{priority}</priority>")
        lines.append("  </url>")
    lines.append("</urlset>")
    resp = make_response("\n".join(lines) + "\n")
    resp.headers["Content-Type"] = "application/xml; charset=utf-8"
    return resp


@bp.get("/public/client/image/<int:width>/<path:filename>")
@rate_limit("public_image", 120, 60)
def serve_resized_image(width, filename):
    return build_resized_image_response(filename, width)


@bp.get("/public/client/photos/<int:plant_id>")
def public_photos(plant_id):
    """Универсальная выдача фотографий для витрины и старого API"""
    from app.photo_storage import PHOTO_VARIANT_CONTAINER, PHOTO_VARIANT_GROUND, resolve_photo_source
    from flask import request

    variant = PHOTO_VARIANT_CONTAINER if request.args.get('seedling') == '1' or request.args.get('variant') == 'container' else PHOTO_VARIANT_GROUND

    erp = (os.environ.get('ERP_PUBLIC_BASE_URL') or '').strip().rstrip('/')
    if erp:
        import requests
        try:
            q = f'?seedling=1' if variant == PHOTO_VARIANT_CONTAINER else ''
            r = requests.get(f'{erp}/public/client/photos/{plant_id}{q}', timeout=15)
            if r.ok:
                return jsonify(r.json())
        except Exception:
            pass

    plant = Plant.query.get_or_404(plant_id)
    root = current_app.config["UPLOAD_FOLDER"]
    rel_dir, filenames = resolve_photo_source(root, plant.id, plant.name, variant=variant)

    if not filenames:
        return jsonify({"status": "ok", "items": []})

    files = []
    for name in filenames:
        rel = f"{rel_dir}/{name}"
        files.append({
            "thumb_url": shop_image_url(rel, SHOP_IMG_THUMB),
            "preview_url": shop_image_url(rel, SHOP_IMG_PREVIEW),
            "image_url": shop_image_url(rel),
            "display_name": _display_name_from_filename(name),
        })
    return jsonify({"status": "ok", "items": files})


@bp.get("/public/client/photo-map")
def public_photo_map():
    """Список фото всех растений (для shop-контейнера без локальных файлов)."""
    erp = (os.environ.get('ERP_PUBLIC_BASE_URL') or '').strip().rstrip('/')
    if erp:
        import requests
        try:
            r = requests.get(f'{erp}/public/client/photo-map', timeout=30)
            if r.ok:
                return jsonify(r.json())
        except Exception:
            pass

    root = current_app.config["UPLOAD_FOLDER"]
    from app.photo_storage import PHOTO_VARIANT_CONTAINER, PHOTO_VARIANT_GROUND, resolve_photo_source

    photos = {}
    container_photos = {}
    for plant in Plant.query.all():
        rel_dir, files = resolve_photo_source(root, plant.id, plant.name, variant=PHOTO_VARIANT_GROUND)
        if files:
            photos[str(plant.id)] = f"{rel_dir}/{files[0]}"
        c_rel, c_files = resolve_photo_source(
            root, plant.id, plant.name, variant=PHOTO_VARIANT_CONTAINER,
        )
        if c_files:
            container_photos[str(plant.id)] = f"{c_rel}/{c_files[0]}"
    return jsonify({
        "status": "ok",
        "photos": photos,
        "container_photos": container_photos,
    })


@bp.get("/public/client/sidebar-slides")
def public_sidebar_slides():
    """Фоновое слайд-шоу витрины: /data/data-sidebar (Amvera) или UPLOAD_FOLDER/data-sidebar локально."""
    from app.shop_sidebar import sidebar_slides_payload

    return jsonify(sidebar_slides_payload(SHOP_IMG_BG))


@bp.get("/public/client/landing-desire-slides")
def public_landing_desire_slides():
    """Слайды блока «Материал…» на главной."""
    from app.shop_landing_page import landing_desire_slides_payload

    return jsonify(landing_desire_slides_payload(SHOP_IMG_BG))


def _format_money(value) -> str:
    try:
        n = int(round(float(value)))
    except (TypeError, ValueError):
        n = 0
    return f"{n:,}".replace(",", " ")


_RU_MONTHS = [
    "", "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def _format_legal_date(dt) -> str:
    return f"{dt.day} {_RU_MONTHS[dt.month]} {dt.year} г."


def _requisite_lookup(requisites, *keywords) -> str:
    for req in requisites:
        label = (req.label or "").lower()
        if any(kw in label for kw in keywords):
            val = (req.value or "").strip()
            if val:
                return val
    return ""


def _legal_context() -> dict:
    """Данные оператора для правовых страниц (из реквизитов и контактов)."""
    requisites = get_company_requisites()
    contacts = get_shop_contacts_for_site()
    op_email = next((c.value for c in contacts if c.contact_type == "email" and c.value), "")
    op_phone = next((c.value for c in contacts if c.contact_type == "phone" and c.value), "")
    return {
        "op_name": _requisite_lookup(requisites, "наимен") or "ООО «Княжество»",
        "op_inn": _requisite_lookup(requisites, "инн"),
        "op_ogrn": _requisite_lookup(requisites, "огрн"),
        "op_addr": _requisite_lookup(requisites, "юридическ") or _requisite_lookup(requisites, "адрес"),
        "op_email": op_email,
        "op_phone": op_phone,
        "updated_date": _format_legal_date(msk_now()),
    }


def _parse_shop_doc_payload(doc: Document) -> dict:
    try:
        return json.loads(doc.comment or "{}")
    except json.JSONDecodeError:
        return {}


def _shop_kp_details_from_doc(doc: Document):
    payload = _parse_shop_doc_payload(doc)
    line_prices = draft_line_prices_from_payload(payload)
    card_map = get_plant_card_map()
    details = []
    total_sum = 0.0
    for row in doc.rows:
        key = (row.plant_id, row.size_id)
        if key in line_prices:
            price = line_prices[key]
        else:
            cat_map = {(item["plant_id"], item["size_id"]): item for item in get_aggregated_catalog()}
            cat = cat_map.get(key)
            price = float(cat["price"]) if cat else 0.0
        row_sum = row.quantity * price
        card = card_map.get(row.plant_id, {})
        details.append({
            "plant": row.plant.name if row.plant else "—",
            "size": row.size.name if row.size else "—",
            "root_system": card.get("root_system", ""),
            "pruning": card.get("pruning", ""),
            "qty": row.quantity,
            "price": price,
            "sum": row_sum,
        })
        total_sum += row_sum
    if payload.get("total_sum") is not None and line_prices:
        total_sum = float(payload["total_sum"])
    return details, total_sum


@bp.get("/shop/kp/<int:doc_id>.pdf")
@rate_limit("shop_kp_pdf", 30, 3600)
def shop_kp_pdf(doc_id):
    if not verify_shop_kp_access(doc_id, request.args.get('token')):
        return jsonify({"status": "error", "message": "forbidden"}), 403
    doc = Document.query.filter_by(id=doc_id, doc_type="client_draft").first_or_404()
    payload = _parse_shop_doc_payload(doc)
    details, total_sum = _shop_kp_details_from_doc(doc)
    doc_date = doc.date or msk_now()

    html = render_template(
        "shop_kp_pdf.html",
        doc_id=doc.id,
        customer_name=payload.get("customer_name") or "—",
        phone=payload.get("phone") or "",
        email=payload.get("email") or "",
        doc_date=doc_date,
        details=details,
        total_sum=total_sum,
        positions_count=len(details),
        generated_at=msk_now(),
        fmt=_format_money,
        shop_contacts=get_shop_contacts_for_kp(),
        contact_display_label=contact_display_label,
    )
    return create_pdf_response(
        html,
        f"KP_{doc_id}.pdf",
        page_bg="#111814",
        page_margin="0",
    )


def get_aggregated_catalog(apply_shop_prices=True):
    """Собирает позиции для витрины с учётом остатков, видимости и «По запросу»."""
    regular = build_visible_catalog_items(apply_shop_prices=apply_shop_prices)
    seedlings = build_visible_seedling_items(apply_shop_prices=apply_shop_prices)
    return sort_catalog_items(regular + seedlings)


def catalog_item_to_shop_row(item: dict) -> dict:
    on_request = bool(item.get('on_request'))
    free_qty = int(item.get('free_qty') or 0)
    return {
        "id": f"{item['plant_id']}_{item['size_id']}",
        "plant_id": item["plant_id"],
        "size_id": item["size_id"],
        "cat": item["plant_name"],
        "name": item["plant_name"],
        "latin": item.get("latin_name") or "",
        "spec": item["size_name"],
        "stock": free_qty,
        "free_qty": free_qty,
        "on_request": on_request,
        "price": item["price"],
        "img": item.get("photo_url") or "",
        "has_photos": bool(item.get("has_photos")),
        "root_system": item.get("root_system") or "",
        "pruning": item.get("pruning") or "",
        "is_seedling": bool(item.get("is_seedling")),
    }


def catalog_size_hint(size_names: list[str]) -> str:
    """Подпись размеров на карточке: один размер как есть, несколько — «от …»."""
    from app.utils import natural_key

    names = sorted(
        [n.strip() for n in (size_names or []) if (n or '').strip()],
        key=natural_key,
    )
    if not names:
        return ''
    if len(names) == 1:
        return names[0]
    return f'от {names[0]}'


def group_catalog_by_plant(catalog_items):
    from app.shop_catalog import catalog_site_bucket

    plants = {}
    for item in catalog_items:
        pid = item["plant_id"]
        bucket = item.get('catalog_bucket') or catalog_site_bucket(item)
        is_container = bucket == 'container'
        key = f"{bucket}_{pid}"
        if key not in plants:
            plants[key] = {
                "plant_id": pid,
                "name": item["plant_name"],
                "latin": item.get("latin_name") or "",
                "characteristic": item.get("characteristic") or "",
                "root_system": item.get("root_system") or "",
                "pruning": item.get("pruning") or "",
                "img": item.get("photo_url") or "",
                "has_photos": bool(item.get("has_photos")),
                "variant_count": 0,
                "total_stock": 0,
                "min_price": None,
                "is_hot": bool(item.get("is_hot")),
                "display_order": int(item.get("display_order") or 0),
                "has_on_request": False,
                "is_seedling": bool(item.get("is_seedling")),
                "is_container": is_container,
                "catalog_bucket": bucket,
                "size_names": [],
            }
        plant = plants[key]
        if not item.get("is_seedling"):
            size_label = (item.get("size_name") or "").strip()
            if size_label and size_label not in plant["size_names"]:
                plant["size_names"].append(size_label)
        if item.get("is_seedling"):
            plant["variant_count"] = 0
            plant["total_stock"] = int(item.get("free_qty") or 0)
            if item.get("on_request"):
                plant["has_on_request"] = True
        else:
            plant["variant_count"] += 1
            if item.get("on_request"):
                plant["has_on_request"] = True
            else:
                plant["total_stock"] += int(item.get("free_qty") or 0)
        price = float(item["price"] or 0)
        if price > 0 and (plant["min_price"] is None or price < plant["min_price"]):
            plant["min_price"] = price
        if not plant["img"] and item.get("photo_url"):
            plant["img"] = item["photo_url"]
        if item.get("has_photos"):
            plant["has_photos"] = True
        if item.get("is_hot"):
            plant["is_hot"] = True
    for plant in plants.values():
        plant["size_names"] = sorted(plant.get("size_names") or [], key=lambda s: s.lower())
        plant["size_hint"] = catalog_size_hint(plant["size_names"])
    return sort_plant_groups(plants.values())


def _shop_page_context(catalog_items):
    shop_items = [catalog_item_to_shop_row(item) for item in catalog_items]
    return {
        "catalog_json": json.dumps(shop_items, ensure_ascii=False),
        "shop_contacts": get_shop_contacts_for_site(),
        "contact_display_label": contact_display_label,
        "contact_href": contact_href,
    }


def _empty_stock_pdf_totals():
    return {
        'sum': 0, 'free_sum': 0, 'income': 0, 'reserved': 0,
        'free': 0, 'shipped': 0, 'qty': 0,
    }


def _catalog_item_to_pdf_row(item: dict) -> dict:
    """Строка прайса PDF из позиции витрины."""
    from app.seedlings import size_name_export_label
    on_request = bool(item.get('on_request'))
    free_qty = int(item.get('free_qty') or 0)
    if item.get('is_seedling'):
        size_label = 'Саженец'
    else:
        size_label = size_name_export_label(item.get('size_name') or '') or '—'
    return {
        'plant_id': item['plant_id'],
        'size_id': item['size_id'],
        'size': size_label,
        'price': float(item.get('price') or 0),
        'free': free_qty,
        'pdf_on_request': on_request,
    }


def _site_pdf_bucket_for_item(item: dict) -> str:
    """ground | container — товарные саженцы (Саженцы · контейнер) в отдельной секции."""
    from app.shop_catalog import catalog_site_bucket
    return catalog_site_bucket(item)


def _build_site_stock_pdf_groups(catalog_items: list) -> list:
    """Группы для PDF: только позиции, включённые в отображение на сайте."""
    buckets = {'ground': {}, 'container': {}}

    for item in catalog_items:
        plant_name = item.get('plant_name') or '—'
        section = _site_pdf_bucket_for_item(item)
        section_buckets = buckets[section]
        if plant_name not in section_buckets:
            section_buckets[plant_name] = {
                'name': plant_name,
                'latin_name': item.get('latin_name') or '',
                'root_system': item.get('root_system') or '',
                'pruning': item.get('pruning') or '',
                'rows': [],
                'totals': _empty_stock_pdf_totals(),
            }
        section_buckets[plant_name]['rows'].append(_catalog_item_to_pdf_row(item))

    def _finalize(section_buckets: dict) -> list:
        groups = []
        for plant_name in sorted(section_buckets.keys(), key=lambda n: n.lower()):
            group = section_buckets[plant_name]
            group['rows'].sort(key=lambda r: (r.get('size') or '').lower())
            group['totals']['free'] = sum(
                int(r.get('free') or 0) for r in group['rows'] if not r.get('pdf_on_request')
            )
            groups.append(group)
        return groups

    sorted_groups = []
    ground_groups = _finalize(buckets['ground'])
    container_groups = _finalize(buckets['container'])

    if ground_groups:
        sorted_groups.append({
            'is_section': True,
            'section_key': 'ground',
            'print_include': True,
            'name': 'Растения в грунте',
            'data': {'latin_name': '', 'rows': [], 'totals': _empty_stock_pdf_totals()},
        })
        sorted_groups.extend(ground_groups)
    if container_groups:
        sorted_groups.append({
            'is_section': True,
            'section_key': 'containers',
            'print_include': True,
            'name': 'Саженцы',
            'data': {'latin_name': '', 'rows': [], 'totals': _empty_stock_pdf_totals()},
        })
        sorted_groups.extend(container_groups)
    return sorted_groups


def get_public_product_stock_report():
    """PDF товарных остатков на сайте — только позиции с витрины (как в каталоге)."""
    end_date = msk_now()
    regular = build_visible_catalog_items(apply_shop_prices=False)
    seedlings = build_visible_seedling_items(apply_shop_prices=False)
    catalog_items = regular + seedlings
    return end_date, _build_site_stock_pdf_groups(catalog_items)


@bp.route("/shop/stock.pdf")
@rate_limit("shop_stock_pdf", 15, 3600)
def public_shop_stock_pdf():
    end_date, groups = get_public_product_stock_report()
    groups = transform_stock_report_price_mode(groups, 'retail')
    ground_groups = []
    container_groups = []
    current = None
    for g in groups:
        if g.get('is_section'):
            current = g.get('section_key')
            continue
        if current == 'containers':
            container_groups.append(g)
        else:
            ground_groups.append(g)

    print_settings = {}
    setting = AppSetting.query.get('print_header_settings')
    if setting and setting.value:
        try:
            print_settings = json.loads(setting.value)
        except Exception:
            print_settings = {}
    promo_photos = [
        print_settings.get(k) for k in (
            'promo_photo_1_url', 'promo_photo_2_url', 'promo_photo_3_url',
        ) if print_settings.get(k)
    ]

    template_ctx = dict(
        groups=groups,
        ground_groups=ground_groups,
        container_groups=container_groups,
        promo_photos=promo_photos,
        print_settings=print_settings,
        doc_date=end_date,
        generated_at=msk_now(),
        fmt=_format_money,
        shop_contacts=get_shop_contacts_for_site(),
        contact_display_label=contact_display_label,
    )
    filename = f"Tovarnye_ostatki_{end_date.strftime('%d.%m.%Y')}.pdf"
    pdf_kwargs = dict(page_bg="#111814", page_margin="0")

    if ground_groups and container_groups:
        html_ground = render_template(
            "shop_stock_pdf.html",
            pdf_part="ground",
            **template_ctx,
        )
        html_containers = render_template(
            "shop_stock_pdf.html",
            pdf_part="containers",
            **template_ctx,
        )
        return create_pdf_response(
            "",
            filename,
            pdf_parts=[html_ground, html_containers],
            **pdf_kwargs,
        )

    html = render_template("shop_stock_pdf.html", pdf_part="full", **template_ctx)
    return create_pdf_response(html, filename, **pdf_kwargs)


@bp.route("/shop/contacts")
def public_shop_contacts():
    page = get_contacts_page_settings()
    ctx = _shop_page_context(get_aggregated_catalog())
    ctx["contacts_page"] = page
    ctx["company_requisites"] = get_company_requisites()
    ctx["map_embed"] = map_embed_markup(page)
    return render_template("public_contacts.html", **ctx)


@bp.route("/shop/privacy")
def public_privacy():
    ctx = _shop_page_context(get_aggregated_catalog())
    ctx.update(_legal_context())
    return render_template("public_privacy.html", **ctx)


@bp.route("/shop/consent")
def public_consent():
    ctx = _shop_page_context(get_aggregated_catalog())
    ctx.update(_legal_context())
    return render_template("public_consent.html", **ctx)


@bp.route("/shop/plant/<int:plant_id>")
def public_shop_plant(plant_id):
    from flask import abort, request
    from app.shop_catalog import catalog_site_bucket

    seedling_mode = request.args.get('seedling') == '1'
    want_bucket = 'container' if seedling_mode else 'ground'
    catalog_items = get_aggregated_catalog()
    plant_rows = [
        item for item in catalog_items
        if item["plant_id"] == plant_id
        and (item.get('catalog_bucket') or catalog_site_bucket(item)) == want_bucket
    ]
    if not plant_rows:
        abort(404)

    first = plant_rows[0]
    photo_rel = first.get("photo_rel")
    is_bare_seedling = len(plant_rows) == 1 and bool(plant_rows[0].get('is_seedling'))
    plant = {
        "plant_id": plant_id,
        "name": first["plant_name"],
        "latin": first.get("latin_name") or "",
        "characteristic": first.get("characteristic") or "",
        "root_system": first.get("root_system") or "",
        "pruning": first.get("pruning") or "",
        "img": shop_image_url(photo_rel, SHOP_IMG_PREVIEW) or "",
        "has_photos": any(item.get("has_photos") for item in plant_rows),
        "is_seedling": is_bare_seedling,
        "is_container": want_bucket == 'container',
    }
    variants = [catalog_item_to_shop_row(item) for item in plant_rows]
    ctx = _shop_page_context(catalog_items)
    ctx["plant"] = plant
    ctx["variants_json"] = json.dumps(variants, ensure_ascii=False)
    return render_template("public_plant.html", **ctx)


@bp.route("/shop/on-request", methods=["POST"])
@rate_limit("shop_on_request", 10, 3600)
def public_shop_on_request():
    """Заявка на позицию «По запросу» с витрины."""
    from app.shop_on_request import create_on_request

    data = request.get_json(silent=True) if request.is_json else request.form
    data = data or {}

    if not (str(data.get("pd_consent") or "").strip()):
        return jsonify({"status": "error", "message": "Необходимо согласие на обработку персональных данных"}), 400

    try:
        plant_id = data.get("plant_id")
        size_id = data.get("size_id")
        customer_name = data.get("customer_name", "")
        phone = data.get("phone", "")
        message = data.get("message", "")
        row = create_on_request(plant_id, size_id, customer_name, phone, message)
        db.session.commit()
        try:
            from app.shop_telegram import notify_shop_operator_on_request

            notify_shop_operator_on_request(row, source='shop')
        except Exception:
            current_app.logger.exception('Shop on-request Telegram notify failed (request #%s)', row.id)
        try:
            from app.shop_email import notify_on_request

            notify_on_request(row, source='shop')
        except Exception:
            current_app.logger.exception('Shop on-request email notify failed (request #%s)', row.id)
        try:
            from app.shop_push import notify_on_request as notify_on_request_push

            notify_on_request_push(row, source='shop')
        except Exception:
            current_app.logger.exception('Shop on-request push notify failed (request #%s)', row.id)
        return jsonify({"status": "ok", "request_id": row.id})
    except ValueError as exc:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(exc)}), 400
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("shop on-request failed")
        return jsonify({"status": "error", "message": "Не удалось отправить заявку"}), 500


@bp.route("/shop", methods=["GET", "POST"])
def public_shop():
    """Защищенная страница витрины (не требует авторизации, не светит админку)"""
    catalog_items = get_aggregated_catalog()

    if request.method == "POST":
        if not check_rate_limit('shop_checkout', 8, 3600):
            abort(429)

        customer_name = request.form.get("customer_name", "").strip()
        phone = request.form.get("phone", "").strip()
        email = request.form.get("email", "").strip()
        pd_consent = request.form.get("pd_consent", "").strip()

        if not pd_consent:
            return "Ошибка: Необходимо согласие на обработку персональных данных", 400
        if len(customer_name) < 2:
            return "Ошибка: Укажите имя или название компании", 400

        # Собираем корзину из POST запроса и сверяем цены с каталогом
        valid_rows = []
        ordered_details = []
        total_sum = 0
        
        # Быстрый поиск по каталогу для проверки цены и наличия
        catalog_map = {(item['plant_id'], item['size_id']): item for item in catalog_items}

        for key, val in request.form.items():
            if key.startswith("qty_") and val.isdigit() and int(val) > 0:
                try:
                    _, plant_id, size_id = key.split("_")
                    pid, sid, qty = int(plant_id), int(size_id), int(val)
                    
                    cat_item = catalog_map.get((pid, sid))
                    if cat_item:
                        if cat_item.get("on_request"):
                            continue
                        # Защита: клиент не может заказать больше, чем есть
                        final_qty = min(qty, cat_item["free_qty"])
                        if final_qty > 0:
                            valid_rows.append((pid, sid, final_qty))
                            row_sum = final_qty * cat_item["price"]
                            total_sum += row_sum
                            ordered_details.append({
                                'plant': cat_item["plant_name"],
                                'size': cat_item["size_name"],
                                'qty': final_qty,
                                'price': cat_item["price"],
                                'sum': row_sum
                            })
                except Exception:
                    pass

        if not valid_rows:
            return "Ошибка: Корзина пуста или товаров нет в наличии", 400

        payload = {
            "customer_name": customer_name,
            "phone": phone,
            "email": email,
            "pd_consent": True,
            "pd_consent_at": msk_now().isoformat(),
            "created_at": msk_now().isoformat(),
            "source": "public_shop",
            "lines": [
                {
                    "plant_id": pid,
                    "size_id": sid,
                    "qty": final_qty,
                    "price": cat_item["price"],
                    "sum": final_qty * cat_item["price"],
                    "plant_name": cat_item["plant_name"],
                    "size_name": cat_item["size_name"],
                }
                for pid, sid, final_qty in valid_rows
                for cat_item in [catalog_map[(pid, sid)]]
            ],
            "total_sum": total_sum,
        }

        try:
            doc = Document(
                doc_type="client_draft",
                date=msk_now(),
                user_id=None,
                comment=json.dumps(payload, ensure_ascii=False),
            )
            db.session.add(doc)
            db.session.flush()

            current_year = datetime.utcnow().year
            for pid, sid, qty in valid_rows:
                db.session.add(
                    DocumentRow(
                        document_id=doc.id,
                        plant_id=pid,
                        size_id=sid,
                        quantity=qty,
                        year=current_year,
                    )
                )

            db.session.commit()

            try:
                from app.shop_telegram import notify_shop_operator_kp
                notify_shop_operator_kp(payload, doc.id)
            except Exception:
                current_app.logger.exception('Shop KP Telegram notify error (doc #%s)', doc.id)
            try:
                from app.shop_email import notify_kp_draft

                notify_kp_draft(payload, doc.id)
            except Exception:
                current_app.logger.exception('Shop KP email notify error (doc #%s)', doc.id)
            try:
                from app.shop_push import notify_kp_draft as notify_kp_draft_push

                notify_kp_draft_push(payload, doc.id)
            except Exception:
                current_app.logger.exception('Shop KP push notify error (doc #%s)', doc.id)

            ctx = _shop_page_context(get_aggregated_catalog())
            ctx.update({
                "doc_id": doc.id,
                "kp_pdf_token": shop_kp_access_token(doc.id),
                "customer_name": customer_name,
                "details": ordered_details,
                "total_sum": total_sum,
            })
            return render_template("public_success.html", **ctx)
            
        except Exception as exc:
            db.session.rollback()
            return f"Внутренняя ошибка: {str(exc)}", 500

    # GET запрос: Отдаем витрину
    ctx = _shop_page_context(catalog_items)
    ctx["plants_json"] = json.dumps(group_catalog_by_plant(catalog_items), ensure_ascii=False)
    return render_template("public_catalog.html", **ctx)