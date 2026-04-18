import json
import os
import re
from datetime import datetime

from flask import Blueprint, current_app, jsonify, request, url_for, render_template
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from app.models import Document, DocumentRow, Order, OrderItem, Plant, Size, StockBalance, db
from app.photo_storage import resolve_photo_source
from app.utils import msk_now

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

    reserved_rows = (
        db.session.query(
            OrderItem.plant_id,
            OrderItem.size_id,
            OrderItem.field_id,
            OrderItem.year,
            func.sum(OrderItem.quantity - OrderItem.shipped_quantity),
        )
        .join(Order)
        .filter(
            Order.status != "canceled",
            Order.status != "ghost",
            Order.is_deleted == False,
        )
        .group_by(OrderItem.plant_id, OrderItem.size_id, OrderItem.field_id, OrderItem.year)
        .all()
    )
    reserved_map = {(r[0], r[1], r[2], r[3]): int(r[4] or 0) for r in reserved_rows}

    all_stocks = (
        StockBalance.query
        .options(joinedload(StockBalance.plant), joinedload(StockBalance.size))
        .all()
    )
    grouped = {}
    for st in all_stocks:
        free = int(st.quantity or 0) - reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0)
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
        p = float(st.price or 0)
        grouped[key]["price"] = p if grouped[key]["price"] is None else min(grouped[key]["price"], p)

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
        return jsonify({"status": "ok", "draft_id": doc.id})
    except Exception as exc:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(exc)}), 500


# ==========================================================
# 2. НОВАЯ БЕЗОПАСНАЯ ПУБЛИЧНАЯ ВИТРИНА (/shop) И ГАЛЕРЕЯ
# ==========================================================

@bp.get("/public/client/photos/<int:plant_id>")
def public_photos(plant_id):
    """Универсальная выдача фотографий для витрины и старого API"""
    # Здесь мы больше не требуем токен, чтобы клиент мог смотреть фото
    plant = Plant.query.get_or_404(plant_id)
    root = current_app.config["UPLOAD_FOLDER"]
    rel_dir, filenames = resolve_photo_source(root, plant.id, plant.name)

    if not filenames:
        return jsonify({"status": "ok", "items": []})

    files = []
    for name in filenames:
        rel = f"{rel_dir}/{name}"
        files.append({
            "thumb_url": url_for("main.serve_uploaded_file", filename=rel),
            "image_url": url_for("main.serve_uploaded_file", filename=rel),
            "display_name": _display_name_from_filename(name),
        })
    return jsonify({"status": "ok", "items": files})


def get_aggregated_catalog():
    """Собирает доступные остатки для витрины, игнорируя поля и партии"""
    reserved_rows = (
        db.session.query(
            OrderItem.plant_id,
            OrderItem.size_id,
            OrderItem.field_id,
            OrderItem.year,
            func.sum(OrderItem.quantity - OrderItem.shipped_quantity),
        )
        .join(Order)
        .filter(
            Order.status != "canceled",
            Order.status != "ghost",
            Order.is_deleted == False,
        )
        .group_by(OrderItem.plant_id, OrderItem.size_id, OrderItem.field_id, OrderItem.year)
        .all()
    )
    reserved_map = {(r[0], r[1], r[2], r[3]): int(r[4] or 0) for r in reserved_rows}

    all_stocks = (
        StockBalance.query
        .options(joinedload(StockBalance.plant), joinedload(StockBalance.size))
        .all()
    )

    root = current_app.config['UPLOAD_FOLDER']
    photo_cache = {}
    grouped = {}
    for st in all_stocks:
        size_name_lower = st.size.name.lower() if st.size else ""
        if "нетов" in size_name_lower or "саженцы" in size_name_lower:
            continue

        free = int(st.quantity or 0) - reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0)
        if free <= 0:
            continue

        key = (st.plant_id, st.size_id)
        if key not in grouped:
            plant_name = st.plant.name if st.plant else "-"
            pid = st.plant_id
            if pid not in photo_cache:
                rel_dir, files = resolve_photo_source(root, pid, plant_name)
                if files:
                    photo_cache[pid] = url_for("main.serve_uploaded_file", filename=f"{rel_dir}/{files[0]}")
                else:
                    photo_cache[pid] = None

            grouped[key] = {
                "plant_id": pid,
                "size_id": st.size_id,
                "plant_name": plant_name,
                "size_name": st.size.name if st.size else "-",
                "free_qty": 0,
                "price": None,
                "has_photos": bool(photo_cache[pid]),
                "photo_url": photo_cache[pid],
            }

        grouped[key]["free_qty"] += free
        p = float(st.price or 0)
        grouped[key]["price"] = p if grouped[key]["price"] is None else min(grouped[key]["price"], p)

    items = list(grouped.values())
    items.sort(key=lambda x: (x["plant_name"].lower(), x["size_name"].lower()))
    return items


@bp.route("/shop", methods=["GET", "POST"])
def public_shop():
    """Защищенная страница витрины (не требует авторизации, не светит админку)"""
    catalog_items = get_aggregated_catalog()

    if request.method == "POST":
        customer_name = request.form.get("customer_name", "").strip()

        if not customer_name:
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
            "created_at": msk_now().isoformat(),
            "source": "public_shop"
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
            
            # Отдаем страницу успеха с деталями заказа
            return render_template(
                "public_success.html", 
                doc_id=doc.id, 
                customer_name=customer_name,
                details=ordered_details, 
                total_sum=total_sum
            )
            
        except Exception as exc:
            db.session.rollback()
            return f"Внутренняя ошибка: {str(exc)}", 500

    # GET запрос: Отдаем витрину
    return render_template("public_catalog.html", items=catalog_items)