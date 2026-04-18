import json
import io
import os
import requests
import time
from decimal import Decimal
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file, current_app
from werkzeug.utils import secure_filename
from flask_login import login_required, current_user
from sqlalchemy import func, or_, and_
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from app.models import db, Order, OrderItem, Client, Plant, Size, Field, Payment, StockBalance, Document, DocumentRow, OrderItemHistory, DiggingLog, ActionLog
from app.utils import msk_now, check_stock_availability, get_actual_price, log_action, natural_key, dateru
from app.models import Project

bp = Blueprint('orders', __name__)

from app.telegram import send_message as _tg_send_msg, send_photo as _tg_send_photo, send_photo_album as _tg_send_photo_album

def send_tg_message_orders(text):
    _tg_send_msg(text, chat_type="orders")

def send_tg_photo_orders(photo_path, caption=""):
    _tg_send_photo(photo_path, caption, chat_type="orders")

def send_tg_photo_album_orders(photo_paths):
    _tg_send_photo_album(photo_paths, chat_type="orders")
# ---------------------------------


def _build_order_item_snapshot(item):
    return {
        'plant_id': item.plant_id,
        'size_id': item.size_id,
        'field_id': item.field_id,
        'year': item.year,
        'price': float(item.price or 0),
        'plant_name': item.plant.name if item.plant else '',
        'size_name': item.size.name if item.size else '',
        'field_name': item.field.name if item.field else '',
    }


def _record_order_item_history(order_id, item, action_type, before_qty, after_qty):
    before_val = int(before_qty) if before_qty is not None else None
    after_val = int(after_qty) if after_qty is not None else None
    delta = 0
    if before_val is not None and after_val is not None:
        delta = after_val - before_val
    history = OrderItemHistory(
        order_id=order_id,
        order_item_id=item.id if item else None,
        action_type=action_type,
        before_quantity=before_val,
        after_quantity=after_val,
        delta_quantity=delta,
        snapshot_payload=json.dumps(_build_order_item_snapshot(item), ensure_ascii=False) if item else None,
        changed_by_user_id=current_user.id if current_user.is_authenticated else None,
        created_at=msk_now()
    )
    db.session.add(history)


def _available_batches_for_product(plant_id, size_id):
    stocks = StockBalance.query.filter_by(plant_id=plant_id, size_id=size_id).all()
    if not stocks:
        return []
    # Single aggregated query for all reserved quantities for this plant+size
    reserved_rows = (
        db.session.query(
            OrderItem.field_id, OrderItem.year,
            func.sum(OrderItem.quantity - OrderItem.shipped_quantity)
        )
        .join(Order)
        .filter(
            OrderItem.plant_id == plant_id,
            OrderItem.size_id == size_id,
            Order.status != 'canceled',
            Order.status != 'ghost',
            Order.is_deleted == False
        )
        .group_by(OrderItem.field_id, OrderItem.year)
        .all()
    )
    reserved_map = {(r[0], r[1]): int(r[2] or 0) for r in reserved_rows}
    
    result = []
    for st in stocks:
        reserved_qty = reserved_map.get((st.field_id, st.year), 0)
        free_qty = int(st.quantity or 0) - reserved_qty
        if free_qty > 0:
            result.append({
                'field_id': st.field_id,
                'field_name': st.field.name if st.field else '-',
                'year': st.year,
                'free': free_qty,
                'price': float(st.price or 0)
            })
    result.sort(key=lambda x: x['free'], reverse=True)
    return result


def _get_client_catalog_rows():
    """Каталог остатков для клиента (как на /shop)."""
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

    grouped = {}
    for st in StockBalance.query.all():
        size_name_lower = st.size.name.lower() if st.size else ""
        if "нетов" in size_name_lower or "саженцы" in size_name_lower:
            continue

        free = int(st.quantity or 0) - reserved_map.get((st.plant_id, st.size_id, st.field_id, st.year), 0)
        if free <= 0:
            continue

        key = (st.plant_id, st.size_id)
        if key not in grouped:
            grouped[key] = {
                "plant_name": st.plant.name if st.plant else "-",
                "size_name": st.size.name if st.size else "-",
                "free_qty": 0,
                "price": None,
            }

        grouped[key]["free_qty"] += free
        p = float(st.price or 0)
        grouped[key]["price"] = p if grouped[key]["price"] is None else min(grouped[key]["price"], p)

    items = list(grouped.values())
    items.sort(key=lambda x: (natural_key(x["plant_name"]), natural_key(x["size_name"])))
    return items


def _parse_client_draft_comment(comment):
    if not comment:
        return {'customer_name': '-', 'phone': '', 'comment': ''}
    try:
        data = json.loads(comment)
        return {
            'customer_name': data.get('customer_name') or '-',
            'phone': data.get('phone') or '',
            'comment': data.get('comment') or '',
        }
    except Exception:
        return {'customer_name': str(comment), 'phone': '', 'comment': ''}


@bp.route('/orders', methods=['GET', 'POST'])
@login_required
def orders_list():
    from app.digging import ensure_payment_file_column
    ensure_payment_file_column()
    mode = request.args.get('mode', 'active')
    f_client = request.args.get('filter_client')
    f_status = request.args.get('filter_status')
    f_date_start = request.args.get('start_date')
    f_date_end = request.args.get('end_date')
    sort = request.args.get('sort', 'name')  # По умолчанию сортировка по названию (имени клиента)
    
    if request.method == 'POST' and current_user.role == 'admin':
        oid = request.form.get('order_id')
        act = request.form.get('action')
        o = Order.query.get(oid)
        if o:
            if act == 'delete':
                o.is_deleted = True
                flash('Заказ скрыт')
            elif act == 'restore':
                o.is_deleted = False
                flash('Заказ восстановлен')
            elif act == 'purge':
                # Полное удаление из БД (только для админа)
                db.session.delete(o)
                flash('Заказ удален навсегда')
            db.session.commit()
            log_action(f"Спрятал/показал/удалил заказ {o.id}")
        return redirect(url_for('orders.orders_list', mode=mode))
        
    # Автоматически скрываем старые отмененные заказы (через ~4 месяца)
    cutoff = msk_now() - timedelta(days=120)
    Order.query.filter(
        Order.status == 'canceled',
        Order.is_deleted == False,
        or_(
            and_(Order.canceled_at != None, Order.canceled_at < cutoff),
            and_(Order.canceled_at == None, Order.date < cutoff)
        )
    ).update({Order.is_deleted: True}, synchronize_session=False)
    db.session.commit()

    # флаг скрытых (old 'trash')
    show_hidden = mode in ('hidden', 'trash')
    q = Order.query.filter_by(is_deleted=show_hidden)
        
    if f_client: q = q.filter(Order.client_id == int(f_client))
    if f_status: q = q.filter(Order.status == f_status)
    if not f_status: q = q.filter(Order.status != 'ghost') 
    if f_date_start: q = q.filter(func.date(Order.date) >= f_date_start)
    if f_date_end: q = q.filter(func.date(Order.date) <= f_date_end)
    
    # Применяем сортировку
    if sort == 'date':
        orders = q.order_by((Order.status == 'canceled'), Order.date.desc()).all()
    else:  # sort == 'name' (по умолчанию)
        orders = q.order_by((Order.status == 'canceled')).all()
        # Сортируем по имени клиента в памяти, чтобы использовать natural_key
        orders.sort(key=lambda x: natural_key(x.client.name))
    
    total_stats = {'sum': 0, 'shipped_fact': 0}
    orders_view = []
    for o in orders:
        if not o.items: 
            orders_view.append({'order': o, 'rowspan': 1, 'order_items': []})
            continue
        if o.status != 'canceled' and not o.is_deleted: 
            total_stats['sum'] += o.total_sum
            total_stats['shipped_fact'] += sum(i.shipped_quantity for i in o.items)
            
        o.items.sort(key=lambda x: natural_key(x.size.name))
        orders_view.append({'order': o, 'rowspan': len(o.items), 'order_items': o.items})
        
    return render_template('orders/orders.html', 
                           orders=orders_view, 
                           stats=total_stats,
                           show_hidden=show_hidden, 
                           all_clients=Client.query.all(), 
                           filters={'clients': [int(f_client)] if f_client else [], 'statuses': [f_status] if f_status else [], 'start': f_date_start, 'end': f_date_end}, 
                           sort=sort,
                           mode=mode)


@bp.route('/orders/client_drafts')
@login_required
def client_drafts_list():
    if current_user.role == 'user2':
        flash('Доступ запрещен')
        return redirect(url_for('orders.orders_list'))

    mode = request.args.get('mode', 'new')
    q = Document.query.filter(Document.doc_type.in_(['client_draft', 'client_draft_approved', 'client_draft_rejected']))
    if mode == 'new':
        q = q.filter(Document.doc_type == 'client_draft')
    elif mode == 'approved':
        q = q.filter(Document.doc_type == 'client_draft_approved')
    elif mode == 'rejected':
        q = q.filter(Document.doc_type == 'client_draft_rejected')

    docs = q.order_by(Document.date.desc(), Document.id.desc()).all()
    view = []
    for d in docs:
        meta = _parse_client_draft_comment(d.comment)
        view.append({
            'doc': d,
            'meta': meta,
            'items_count': len(d.rows or []),
            'items_qty': sum((r.quantity or 0) for r in (d.rows or []))
        })
    return render_template('orders/client_drafts.html', drafts=view, mode=mode)


@bp.route('/orders/client_draft/<int:doc_id>', methods=['GET', 'POST'])
@login_required
def client_draft_detail(doc_id):
    if current_user.role == 'user2':
        flash('Доступ запрещен')
        return redirect(url_for('orders.orders_list'))

    doc = Document.query.get_or_404(doc_id)
    if doc.doc_type not in ('client_draft', 'client_draft_approved', 'client_draft_rejected'):
        flash('Это не клиентский черновик')
        return redirect(url_for('orders.client_drafts_list'))

    meta = _parse_client_draft_comment(doc.comment)

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'reject':
            if doc.doc_type != 'client_draft':
                flash('Черновик уже обработан')
                return redirect(url_for('orders.client_draft_detail', doc_id=doc.id))
            doc.doc_type = 'client_draft_rejected'
            db.session.commit()
            flash('Черновик отклонен')
            return redirect(url_for('orders.client_drafts_list', mode='rejected'))

        if action == 'approve':
            if doc.doc_type != 'client_draft':
                flash('Черновик уже обработан')
                return redirect(url_for('orders.client_draft_detail', doc_id=doc.id))

            client_id = request.form.get('client_id', type=int)
            if client_id:
                client = Client.query.get(client_id)
            else:
                new_name = (request.form.get('new_client_name') or meta.get('customer_name') or '').strip()
                client = Client.query.filter(func.lower(Client.name) == new_name.lower()).first() if new_name else None
                if not client and new_name:
                    client = Client(name=new_name)
                    db.session.add(client)
                    db.session.flush()

            if not client:
                flash('Выберите или укажите клиента')
                return redirect(url_for('orders.client_draft_detail', doc_id=doc.id))

            alloc_row_ids = request.form.getlist('alloc_row_id[]')
            alloc_fields = request.form.getlist('alloc_field_id[]')
            alloc_years = request.form.getlist('alloc_year[]')
            alloc_qtys = request.form.getlist('alloc_qty[]')

            allocations = {}
            for i in range(len(alloc_row_ids)):
                try:
                    row_id = int(alloc_row_ids[i])
                    field_id = int(alloc_fields[i])
                    year = int(alloc_years[i])
                    qty = int(alloc_qtys[i])
                except Exception:
                    continue
                if qty <= 0:
                    continue
                allocations.setdefault(row_id, []).append({'field_id': field_id, 'year': year, 'qty': qty})

            for r in doc.rows:
                alloc_sum = sum(x['qty'] for x in allocations.get(r.id, []))
                if alloc_sum != int(r.quantity or 0):
                    flash(f'Для позиции "{r.plant.name} {r.size.name}" сумма распределения должна быть {r.quantity}')
                    return redirect(url_for('orders.client_draft_detail', doc_id=doc.id))

            try:
                order = Order(client_id=client.id, date=msk_now(), status='reserved')
                db.session.add(order)
                db.session.flush()

                created_items = []
                for r in doc.rows:
                    for al in allocations.get(r.id, []):
                        ok, free = check_stock_availability(r.plant_id, r.size_id, al['field_id'], al['year'], al['qty'])
                        if not ok:
                            raise ValueError(
                                f'Недостаточно остатка для "{r.plant.name} {r.size.name}" '
                                f'(поле {al["field_id"]}, {al["year"]}), доступно: {free}'
                            )
                        price = get_actual_price(r.plant_id, r.size_id, al['field_id'])
                        oi = OrderItem(
                            order_id=order.id,
                            plant_id=r.plant_id,
                            size_id=r.size_id,
                            field_id=al['field_id'],
                            year=al['year'],
                            quantity=al['qty'],
                            price=price
                        )
                        db.session.add(oi)
                        db.session.flush()
                        created_items.append(oi)

                for created_item in created_items:
                    _record_order_item_history(
                        order_id=order.id,
                        item=created_item,
                        action_type='initial_item',
                        before_qty=0,
                        after_qty=created_item.quantity
                    )

                doc.doc_type = 'client_draft_approved'
                doc.order_id = order.id
                db.session.commit()
                flash(f'Черновик подтвержден. Создан заказ #{order.id}')
                return redirect(url_for('orders.order_detail', order_id=order.id))
            except Exception as e:
                db.session.rollback()
                flash(f'Ошибка подтверждения: {e}')
                return redirect(url_for('orders.client_draft_detail', doc_id=doc.id))

    rows_view = []
    for r in doc.rows:
        rows_view.append({
            'row': r,
            'batches': _available_batches_for_product(r.plant_id, r.size_id)
        })

    return render_template(
        'orders/client_draft_detail.html',
        doc=doc,
        meta=meta,
        rows_view=rows_view,
        clients=sorted(Client.query.all(), key=lambda x: natural_key(x.name))
    )

@bp.route('/order/create', methods=['GET', 'POST'])
@login_required
def order_create():
    if request.method == 'POST':
        c_id = request.form.get('client')
        is_barter = request.form.get('is_barter') == 'on'
        o = Order(client_id=c_id, date=msk_now(), is_barter=is_barter)
        db.session.add(o)
        # Пока не коммитим, чтобы если будет ошибка товара, можно было откатить
        
        p_ids = request.form.getlist('plant[]')
        s_ids = request.form.getlist('size[]')
        f_ids = request.form.getlist('field[]')
        y_ids = request.form.getlist('year[]')
        q_ids = request.form.getlist('quantity[]')
        
        created_items = []
        for i in range(len(p_ids)):
            if int(q_ids[i]) > 0:
                ok, free = check_stock_availability(p_ids[i], s_ids[i], f_ids[i], int(y_ids[i]), int(q_ids[i]))
                if not ok: 
                    # ИСПРАВЛЕНО: Откат транзакции перед редиректом
                    db.session.rollback()
                    flash(f"Ошибка! Недостаточно товара (доступно: {free}). Заказ не создан.")
                    return redirect(url_for('orders.order_create'))
                
                p = get_actual_price(int(p_ids[i]), int(s_ids[i]), int(f_ids[i]))
                new_item = OrderItem(
                    order_id=o.id,
                    plant_id=int(p_ids[i]),
                    size_id=int(s_ids[i]),
                    field_id=int(f_ids[i]),
                    year=int(y_ids[i]),
                    quantity=int(q_ids[i]),
                    price=p
                )
                db.session.add(new_item)
                created_items.append(new_item)

        db.session.flush()
        for created_item in created_items:
            _record_order_item_history(
                order_id=o.id,
                item=created_item,
                action_type='initial_item',
                before_qty=0,
                after_qty=created_item.quantity
            )
        
        db.session.commit()
        log_action(f"Создал заказ #{o.id}")
        return redirect(url_for('orders.orders_list'))
        
    return render_template('orders/order_form.html', clients=Client.query.all(), plants=Plant.query.all(), sizes=Size.query.all(), fields=Field.query.all())

@bp.route('/api/stock_info')
@login_required
def api_stock_info():
    p, s, f = request.args.get('plant'), request.args.get('size'), request.args.get('field')
    # Добавил проверку на наличие параметров
    if not p or not s or not f:
        return jsonify({'batches': []})
        
    stocks = StockBalance.query.filter_by(plant_id=p, size_id=s, field_id=f).order_by(StockBalance.year.asc()).all()
    # Single query for all reserved quantities
    reserved_rows = (
        db.session.query(OrderItem.year, func.sum(OrderItem.quantity - OrderItem.shipped_quantity))
        .join(Order)
        .filter(OrderItem.plant_id == p, OrderItem.size_id == s, OrderItem.field_id == f,
                Order.status != 'canceled', Order.is_deleted == False)
        .group_by(OrderItem.year).all()
    )
    reserved_map = {r[0]: int(r[1] or 0) for r in reserved_rows}
    
    res = []
    for st in stocks:
        free_qty = st.quantity - reserved_map.get(st.year, 0)
        if free_qty > 0: 
            res.append({'year': st.year, 'free': free_qty, 'price': float(st.price)})
            
    return jsonify({'batches': res})

@bp.route('/api/stock_availability')
@login_required
def api_stock_availability():
    """
    Возвращает доступные остатки по всем полям для конкретного Растения и Размера.
    """
    p_id = request.args.get('plant_id')
    s_id = request.args.get('size_id')
    
    if not p_id or not s_id:
        return jsonify({'items': []})
        
    stocks = StockBalance.query.filter_by(plant_id=p_id, size_id=s_id).all()
    if not stocks:
        return jsonify({'items': []})
    
    reserved_rows = (
        db.session.query(OrderItem.field_id, OrderItem.year, func.sum(OrderItem.quantity - OrderItem.shipped_quantity))
        .join(Order)
        .filter(
            OrderItem.plant_id == p_id, OrderItem.size_id == s_id,
            Order.status != 'canceled', Order.status != 'ghost', Order.is_deleted == False
        )
        .group_by(OrderItem.field_id, OrderItem.year).all()
    )
    reserved_map = {(r[0], r[1]): int(r[2] or 0) for r in reserved_rows}
    
    result = []
    for st in stocks:
        free_qty = st.quantity - reserved_map.get((st.field_id, st.year), 0)
        if free_qty > 0:
            result.append({
                'field_id': st.field_id,
                'field_name': st.field.name if st.field else '-',
                'year': st.year,
                'free': free_qty,
                'price': float(st.price)
            })
            
    result.sort(key=lambda x: x['free'], reverse=True)
    return jsonify({'items': result})

@bp.route('/api/turnover_years')
@login_required
def api_turnover_years():
    p = request.args.get('plant')
    f = request.args.get('field')
    if not p or not f: 
        return jsonify(list())
    
    unique_years = set()
    stocks = db.session.query(StockBalance).filter_by(plant_id=p, field_id=f).all()
    for st in stocks: 
        if st.year: unique_years.add(st.year)
    ghosts = db.session.query(OrderItem).filter_by(plant_id=p, field_id=f).all()
    for g in ghosts: 
        if g.year: unique_years.add(g.year)
    docs_from = db.session.query(DocumentRow).filter_by(plant_id=p, field_from_id=f).all()
    for d in docs_from: 
        if d.year: unique_years.add(d.year)
    docs_to = db.session.query(DocumentRow).filter_by(plant_id=p, field_to_id=f).all()
    for d in docs_to: 
        if d.year: unique_years.add(d.year)
            
    return jsonify(sorted(list(unique_years)))

@bp.route('/order/<int:order_id>', methods=['GET', 'POST'])
@login_required
def order_detail(order_id):
    from app.digging import ensure_payment_file_column
    ensure_payment_file_column()
    o = Order.query.get_or_404(order_id)
    
    return_to = request.form.get('return_to') or request.args.get('return_to')
    if return_to == 'None': return_to = None
    if current_user.role == 'user2' and request.method == 'POST': 
        flash('Доступ запрещен')
        if return_to:
            return redirect(return_to)
        return redirect(url_for('orders.order_detail', order_id=order_id))
    
    if request.method == 'POST':
        if 'link_project' in request.form:
            pid = request.form.get('project_id')
            if pid:
                o.project_id = int(pid)
            else:
                o.project_id = None
            db.session.commit()
            flash('Привязка к проекту обновлена')

        if 'update_info' in request.form:
            new_client_id = request.form.get('client_id')
            if new_client_id: o.client_id = int(new_client_id)
            o.invoice_number = request.form.get('invoice_number')
            d = request.form.get('invoice_date')
            o.invoice_date = datetime.strptime(d, '%Y-%m-%d') if d else None
            
            if current_user.role in ['admin', 'executive']:
                o.is_barter = request.form.get('is_barter') == 'on'
                
            db.session.commit()
            flash('Обновлено')

        elif 'delete_payment_file' in request.form and current_user.role in ['admin', 'executive', 'user']:
            p = Payment.query.get(request.form.get('payment_id'))
            if p and p.file_path:
                try:
                    if os.path.exists(p.file_path):
                        os.remove(p.file_path)
                    p.file_path = None
                    db.session.commit()
                    flash('Файл удален')
                    log_action(f"Удалил файл оплаты в заказе #{o.id}")
                except Exception as e:
                    flash(f'Ошибка удаления файла: {e}')
                    
        elif 'upload_payment_file' in request.form and current_user.role in ['admin', 'executive', 'user']:
            p = Payment.query.get(request.form.get('payment_id'))
            file = request.files.get('payment_file')
            if p and file and file.filename:
                # Удаляем старый файл, если был
                if p.file_path and os.path.exists(p.file_path):
                    try: os.remove(p.file_path)
                    except OSError: pass
                
                date_str = msk_now().strftime('%Y-%m-%d')
                upload_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'payment', date_str)
                os.makedirs(upload_dir, exist_ok=True)
                fname = secure_filename(file.filename)
                base, ext = os.path.splitext(fname)
                unique_fname = f"pay_{p.id}_{int(time.time())}{ext}"
                fpath = os.path.join(upload_dir, unique_fname)
                file.save(fpath)
                p.file_path = fpath
                db.session.commit()
                flash('Файл загружен')
                log_action(f"Загрузил файл оплаты в заказ #{o.id}")
                
        elif 'delete_payment' in request.form and current_user.role in ['admin', 'executive', 'user']:
            p = Payment.query.get(request.form.get('payment_id'))
            if p:
                db.session.delete(p)
                db.session.commit()
                flash('Оплата удалена')
                log_action(f"Удалил оплату в заказе #{o.id}")

        elif 'edit_payment' in request.form and current_user.role in ['admin', 'executive', 'user']:
            p = Payment.query.get(request.form.get('payment_id'))
            if p:
                old_amount = p.amount
                old_date = p.date
                old_comment = p.comment
                
                p.amount = float(request.form.get('amount'))
                p.date = datetime.strptime(request.form.get('date'), '%Y-%m-%d').date()
                p.payment_type = request.form.get('payment_type') or 'cashless'
                p.comment = request.form.get('comment')
                db.session.commit()
                flash('Оплата изменена')
                log_action(f"Изменил оплату в заказе #{o.id}: {old_amount}→{p.amount} ({old_date}→{p.date})")
                
        elif 'add_payment' in request.form:
            p_type = request.form.get('payment_type') or 'cashless'
            p = Payment(order_id=o.id, amount=float(request.form.get('amount')), date=datetime.strptime(request.form.get('date'), '%Y-%m-%d'), payment_type=p_type, comment=request.form.get('comment'))
            db.session.add(p)
            db.session.commit() # Сохраняем, чтобы получить ID

            file = request.files.get('payment_file')
            if file and file.filename:
                date_str = msk_now().strftime('%Y-%m-%d')
                upload_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'payment', date_str)
                os.makedirs(upload_dir, exist_ok=True)
                fname = secure_filename(file.filename)
                base, ext = os.path.splitext(fname)
                unique_fname = f"pay_{p.id}_{int(time.time())}{ext}"
                fpath = os.path.join(upload_dir, unique_fname)
                file.save(fpath)
                p.file_path = fpath
                db.session.commit()

            flash('Оплата добавлена')
            log_action(f"Добавил оплату в заказ #{o.id}")

        elif 'cancel_order' in request.form: 
            o.status = 'canceled'
            o.canceled_at = msk_now()
            db.session.commit()
            flash('Отменен')
            log_action(f"Отменил заказ #{o.id}")
            if return_to:
                return redirect(return_to)
            if return_to:
                return redirect(return_to)
            return redirect(url_for('orders.orders_list'))

        elif 'restore_order' in request.form: 
            o.status = 'reserved'
            o.canceled_at = None
            db.session.commit()
            flash('Восстановлен')
            log_action(f"Восстановил заказ #{o.id}")

        elif 'save_items' in request.form:
            ids = request.form.getlist('item_id[]')
            prices = request.form.getlist('price[]')
            qtys = request.form.getlist('quantity[]')
            flds = request.form.getlist('field_id[]')
            yrs = request.form.getlist('year[]')
            prepared_updates = []

            for i, iid in enumerate(ids):
                it = OrderItem.query.get(int(iid))
                if not it:
                    continue

                new_qty = int(qtys[i])
                new_field_id = int(flds[i])
                new_year = int(yrs[i])

                # Нельзя уменьшать план ниже уже отгруженного количества.
                if new_qty < int(it.shipped_quantity or 0):
                    flash(
                        f'Нельзя установить кол-во меньше отгруженного для позиции '
                        f'"{it.plant.name} {it.size.name}". '
                        f'Отгружено: {it.shipped_quantity}, попытка: {new_qty}.'
                    )
                    if return_to:
                        return redirect(return_to)
                    return redirect(url_for('orders.order_detail', order_id=order_id))

                # Проверка остатка с учетом новой партии и исключением текущей строки.
                ok, free = check_stock_availability(
                    it.plant_id,
                    it.size_id,
                    new_field_id,
                    new_year,
                    new_qty,
                    exclude_item_id=it.id
                )
                if not ok:
                    field_obj = Field.query.get(new_field_id)
                    field_name = field_obj.name if field_obj else new_field_id
                    flash(
                        f'Недостаточно остатка для позиции "{it.plant.name} {it.size.name}" '
                        f'(поле: {field_name}, год: {new_year}). Доступно: {free}, запрошено: {new_qty}.'
                    )
                    if return_to:
                        return redirect(return_to)
                    return redirect(url_for('orders.order_detail', order_id=order_id))

                prepared_updates.append({
                    'item': it,
                    'new_qty': new_qty,
                    'new_price': float(prices[i]),
                    'new_field_id': new_field_id,
                    'new_year': new_year
                })

            for upd in prepared_updates:
                it = upd['item']
                old_qty = int(it.quantity or 0)
                it.quantity = upd['new_qty']
                it.price = upd['new_price']
                it.field_id = upd['new_field_id']
                it.year = upd['new_year']
                if old_qty != upd['new_qty']:
                    _record_order_item_history(
                        order_id=o.id,
                        item=it,
                        action_type='qty_change',
                        before_qty=old_qty,
                        after_qty=upd['new_qty']
                    )
            db.session.commit()
            flash('Сохранено')
            log_action(f"Обновил товары в заказе #{o.id}")

        elif 'delete_item' in request.form:
            it = OrderItem.query.get(request.form.get('item_id'))
            if it and it.shipped_quantity == 0:
                try:
                    _record_order_item_history(
                        order_id=o.id,
                        item=it,
                        action_type='delete_item',
                        before_qty=int(it.quantity or 0),
                        after_qty=0
                    )

                    # FK-safe: история и логи выкопки могут ссылаться на удаляемую позицию.
                    # Перед удалением отвязываем ссылки (order_item_id -> NULL), сохраняя сами записи.
                    OrderItemHistory.query.filter_by(order_item_id=it.id).update(
                        {OrderItemHistory.order_item_id: None},
                        synchronize_session=False
                    )

                    related_logs = DiggingLog.query.filter_by(order_item_id=it.id).all()
                    for log_entry in related_logs:
                        if log_entry.plant_id is None:
                            log_entry.plant_id = it.plant_id
                        if log_entry.size_id is None:
                            log_entry.size_id = it.size_id
                        if log_entry.field_id is None:
                            log_entry.field_id = it.field_id
                        if log_entry.year is None:
                            log_entry.year = it.year
                        log_entry.order_item_id = None

                    db.session.delete(it)
                    db.session.commit()
                    flash('Позиция удалена')
                except Exception as exc:
                    db.session.rollback()
                    flash(f'Ошибка удаления позиции: {exc}')

        elif 'add_new_item' in request.form:
            p, s, f, y, q = request.form.get('plant'), request.form.get('size'), request.form.get('field'), request.form.get('year'), int(request.form.get('quantity'))
            ok, free = check_stock_availability(p, s, f, int(y), q)
            if ok: 
                new_item = OrderItem(
                    order_id=o.id,
                    plant_id=p,
                    size_id=s,
                    field_id=f,
                    year=int(y),
                    quantity=q,
                    price=get_actual_price(p, s, f)
                )
                db.session.add(new_item)
                db.session.flush()
                _record_order_item_history(
                    order_id=o.id,
                    item=new_item,
                    action_type='add_item',
                    before_qty=0,
                    after_qty=q
                )
                db.session.commit()
            else: 
                flash(f'Недостаточно товара. Доступно: {free}')
                
        if return_to:
            return redirect(return_to)
        return redirect(url_for('orders.order_detail', order_id=order_id))
    
    # Получаем связанные документы (отгрузки)
    # --- ЛОГИКА СБОРА ДЕРЕВА ДОКУМЕНТОВ (СЧЕТ -> ЗАКАЗЫ -> ОПЛАТЫ/ОТГРУЗКИ) ---
    
    # 1. Определяем список заказов для отображения
    # Если есть номер счета - ищем все заказы этого клиента с таким счетом
    if o.invoice_number:
        related_orders = Order.query.filter_by(
            client_id=o.client_id, 
            invoice_number=o.invoice_number,
            is_deleted=False
        ).order_by(Order.date).all()
    else:
        # Если счета нет, показываем только текущий заказ
        related_orders = [o]

    # 2. Собираем данные (группы отгрузок) для КАЖДОГО заказа из списка
    orders_data = []
    
    for ord_obj in related_orders:
        ship_groups = []
        
        # А. Если Ghost
        if ord_obj.status == 'ghost':
            total_qty = sum(i.quantity for i in ord_obj.items)
            if total_qty > 0:
                ship_groups.append({
                    'date': ord_obj.date,
                    'type': 'ghost',
                    'doc_ids': [],
                    'total_qty': total_qty,
                    'comment': 'Историческая запись'
                })
        else:
            # Б. Если обычный заказ
            docs = Document.query.filter_by(order_id=ord_obj.id, doc_type='shipment').order_by(Document.date).all()
            groups_map = {}
            for d in docs:
                date_key = d.date.strftime('%Y-%m-%d')
                if date_key not in groups_map:
                    groups_map[date_key] = {
                        'date': d.date,
                        'type': 'real',
                        'doc_ids': [],
                        'total_qty': 0,
                        'comment': set()
                    }
                group = groups_map[date_key]
                group['doc_ids'].append(str(d.id))
                group['total_qty'] += sum(r.quantity for r in d.rows)
                if d.comment: group['comment'].add(d.comment)
            
            for val in groups_map.values():
                val['comment'] = ", ".join(val['comment'])
                ship_groups.append(val)
            ship_groups.sort(key=lambda x: x['date'])
            
        orders_data.append({
            'order': ord_obj,
            'is_current': (ord_obj.id == o.id),
            'shipments': ship_groups
        })

       # ... в конце функции ...
    active_projects = Project.query.filter_by(status='active').order_by(Project.name).all()

    return render_template('orders/order_detail.html', 
                           order=o, 
                           orders_data=orders_data, 
                           active_projects=active_projects, # <--- Передаем проекты
                           plants=sorted(Plant.query.all(), key=lambda x: x.name), 
                           sizes=Size.query.all(), 
                           fields=Field.query.all(), 
                           clients=sorted(Client.query.all(), key=lambda x: x.name),
                           current_year=msk_now().year,
                           return_to=return_to)

@bp.route('/order/download_payment_file/<int:payment_id>')
@login_required
def download_payment_file(payment_id):
    p = Payment.query.get_or_404(payment_id)
    if p.file_path and os.path.exists(p.file_path):
        return send_file(p.file_path, as_attachment=True)
    flash("Файл не найден")
    return redirect(request.referrer)

@bp.route('/order/<int:order_id>/ship', methods=['POST'])
@login_required
def order_ship(order_id):
    o = Order.query.get_or_404(order_id)
    return_to = request.form.get('return_to') or request.args.get('return_to')
    if return_to == 'None': return_to = None
    item_id = int(request.form.get('item_id'))
    qty = int(request.form.get('quantity'))
    item = OrderItem.query.get(item_id)
    
    if qty > (item.quantity - item.shipped_quantity): 
        flash('Ошибка кол-ва')
    else:
        # Создаем документ
        doc = Document(doc_type='shipment', user_id=current_user.id, order_id=o.id, comment=f"Отгрузка {o.id}", date=msk_now())
        db.session.add(doc)
        db.session.flush() # Получаем ID документа
        
        # Добавляем строку документа
        db.session.add(DocumentRow(document_id=doc.id, plant_id=item.plant_id, size_id=item.size_id, field_from_id=item.field_id, year=item.year, quantity=qty))
        
        # ВАЖНО: Списываем остаток со склада!
        from app.utils import get_or_create_stock
        stock = get_or_create_stock(item.plant_id, item.size_id, item.field_id, item.year)
        stock.quantity -= qty
        
        # Обновляем прогресс в позиции заказа
        item.shipped_quantity += qty
        
        # Обновляем статус заказа
        if o.status == 'reserved': o.status = 'in_progress'
        if all(i.shipped_quantity >= i.quantity for i in o.items): o.status = 'shipped'
        
        db.session.commit()
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'status': 'ok', 'shipped': item.shipped_quantity, 'plan': item.quantity})
            
        flash('Отгружено (остатки обновлены)')
        log_action(f"Отгрузил из заказа #{o.id}")
        
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({'status': 'error', 'message': 'Ошибка кол-ва'})
        
    if return_to:
        return redirect(return_to)
    return redirect(url_for('orders.order_detail', order_id=order_id))

@bp.route('/order/<int:order_id>/send_shipment_report', methods=['POST'])
@login_required
def send_shipment_report(order_id):
    o = Order.query.get_or_404(order_id)
    return_to = request.form.get('return_to') or request.args.get('return_to')
    if return_to == 'None': return_to = None
    
    # --- ЗАЩИТА ОТ ДУБЛИКАТОВ (5 МИНУТ) ---
    cutoff_time = msk_now() - timedelta(minutes=5)
    recent_log = ActionLog.query.filter(
        ActionLog.user_id == current_user.id,
        ActionLog.action == f"Отправил отчет об отгрузке в ТГ по заказу #{o.id}",
        ActionLog.date >= cutoff_time
    ).first()
    
    if recent_log:
        flash('Отчет по этой отгрузке уже был отправлен недавно. Пожалуйста, подождите.', 'info')
        if return_to:
            return redirect(return_to)
        return redirect(url_for('orders.order_detail', order_id=order_id))
    # --------------------------------------
    
    comment = request.form.get('comment', '').strip()
    photos = request.files.getlist('photos')
    
    # 1. Сохранение фотографий
    saved_photos = []
    if photos and any(p.filename for p in photos):
        date_str = msk_now().strftime('%Y-%m-%d')
        upload_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'shipment', date_str)
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir, exist_ok=True)
        for p in photos:
            if p.filename:
                fname = secure_filename(p.filename)
                base, ext = os.path.splitext(fname)
                unique_fname = f"ord{o.id}_{int(time.time()*1000)}{ext}"
                fpath = os.path.join(upload_dir, unique_fname)
                p.save(fpath)
                saved_photos.append(fpath)
    
    # 2. Формирование текста отчета
    report_lines = []
    report_lines.append(f"🚚 <b>Отгрузка по заказу #{o.id}</b>")
    report_lines.append(f"👤 <b>Клиент:</b> {o.client.name}")
    
    # Считаем общую сумму по текущему состоянию заказа
    total_sum = sum(item.sum for item in o.items)
    report_lines.append(f"💰 <b>Сумма заказа:</b> {total_sum:,.0f} ₽".replace(',', ' '))
    
    if comment:
        import html
        safe_comment = html.escape(comment)
        report_lines.append(f"\n💬 <b>Комментарий:</b> {safe_comment}")
        
    report_lines.append("\n📦 <b>Позиции в заказе (отгружено из плана):</b>")
    
    has_report_rows = False
    
    # Сначала агрегируем данные по (Растение, Размер)
    aggregated_items = {}
    for item in o.items:
        p_name = item.plant.name if item.plant else "Неизвестно"
        s_name = item.size.name if item.size else "-"
        
        key = (p_name, s_name)
        if key not in aggregated_items:
            aggregated_items[key] = {'plan': 0, 'fact': 0}
            
        aggregated_items[key]['plan'] += item.quantity
        aggregated_items[key]['fact'] += item.shipped_quantity

    grouped_items = {}
    for (p_name, s_name), totals in aggregated_items.items():
        plan = totals['plan']
        fact = totals['fact']
        
        if plan > 0 or fact > 0:
            has_report_rows = True
            icon = "✅" if fact >= plan else ("⏳" if fact > 0 else "❌")
            line = f"{icon} {s_name} ➔ {fact} из {plan} шт."
            grouped_items.setdefault(p_name, []).append(line)

    for plant_name, lines in grouped_items.items():
        report_lines.append(f"\n🌲 <b>{plant_name}</b>")
        report_lines.extend(lines)
            
    if not has_report_rows:
        report_lines.append("<i>Пока ничего не отгружено.</i>")
        
    msg = "\n".join(report_lines)
    
    # 3. Отправка в ТГ
    # Сначала отправляем фото АЛЬБОМОМ
    if saved_photos:
        send_tg_photo_album_orders(saved_photos)
        time.sleep(1)
        
    # Затем отправляем текстовый отчет
    send_tg_message_orders(msg)
    
    flash('Отчет об отгрузке успешно отправлен в Telegram!', 'success')
    log_action(f"Отправил отчет об отгрузке в ТГ по заказу #{o.id}")
    
    if return_to:
        return redirect(return_to)
    return redirect(url_for('orders.order_detail', order_id=order_id))

@bp.route('/invoice/<int:client_id>/<path:invoice_number>')
@login_required
def invoice_detail(client_id, invoice_number):
    # 1. Ищем все заказы
    from urllib.parse import unquote
    invoice_number = unquote(invoice_number)
    
    orders = Order.query.filter(
        Order.client_id == client_id,
        Order.invoice_number == invoice_number,
        Order.is_deleted == False
    ).order_by(Order.date).all()
    
    if not orders:
        flash(f'Счет "{invoice_number}" не найден или заказы удалены')
        return redirect(url_for('orders.orders_list'))
        
    client = orders[0].client
    invoice_date = orders[0].invoice_date
    return_to = request.args.get('return_to') or request.referrer
    
    # 2. Считаем общую математику (используем Decimal из базы, не превращая в float)
    from decimal import Decimal
    
    stats = {
        'total_sum': Decimal(0),
        'paid_sum': Decimal(0),
        'shipped_sum': Decimal(0),
        'total_items_qty': 0,
        'orders_count': len(orders)
    }
    
    # 3. Сбор данных для "Товарной Матрицы"
    matrix = {}
    
    all_payments = []
    all_shipments = []
    
    for o in orders:
        stats['total_sum'] += o.total_sum
        stats['paid_sum'] += o.paid_sum
        
        # Собираем платежи
        for p in o.payments:
            all_payments.append({'date': p.date, 'amount': p.amount, 'comment': p.comment, 'order_id': o.id})
            
        # Собираем товары
        for item in o.items:
            stats['total_items_qty'] += item.quantity
            
            key = (item.plant.name, item.size.name)
            if key not in matrix:
                matrix[key] = {
                    'plant': item.plant.name,
                    'size': item.size.name,
                    'total_qty': 0,
                    'shipped_qty': 0,
                    'dug_qty': 0, # <--- Добавили поле
                    'avg_price': 0,
                    'total_money': 0,
                    'orders_links': []
                }
            
            row = matrix[key]
            row['total_qty'] += item.quantity
            row['shipped_qty'] += item.shipped_quantity
            row['dug_qty'] += (item.dug_quantity or 0) # <--- Суммируем выкопанное
            
            # ВАЖНО: item.price - это Decimal, умножаем напрямую
            # Если price вдруг None, заменяем на 0
            price = item.price if item.price is not None else Decimal(0)
            
            row['total_money'] += price * item.quantity
            
            if o.id not in row['orders_links']: row['orders_links'].append(o.id)
            
            # Считаем сумму отгруженного по факту (без float!)
            stats['shipped_sum'] += price * item.shipped_quantity

        # Собираем документы отгрузки
        docs = Document.query.filter_by(order_id=o.id, doc_type='shipment').all()
        for d in docs:
            qty = sum(r.quantity for r in d.rows)
            all_shipments.append({'date': d.date, 'id': d.id, 'qty': qty, 'order_id': o.id, 'comment': d.comment})

    # Досчитываем средние цены
    sorted_matrix = []
    for k, v in matrix.items():
        if v['total_qty'] > 0:
            v['avg_price'] = v['total_money'] / Decimal(v['total_qty'])
        sorted_matrix.append(v)
    
    # Сортировка
    sorted_matrix.sort(key=lambda x: x['plant'])
    all_payments.sort(key=lambda x: x['date'])
    all_shipments.sort(key=lambda x: x['date'])

    return render_template('orders/invoice_detail.html', 
                           invoice_number=invoice_number,
                           invoice_date=invoice_date,
                           client=client,
                           orders=orders,
                           stats=stats,
                           matrix=sorted_matrix,
                           payments=all_payments,
                           shipments=all_shipments,
                           return_to=return_to)

@bp.route('/orders/export')
@login_required
def export_orders():
    mode = request.args.get('mode', 'active')
    f_client = request.args.get('filter_client')
    f_status = request.args.get('filter_status')
    f_date_start = request.args.get('start_date')
    f_date_end = request.args.get('end_date')

    # 1. Получаем заказы с учетом фильтров
    q = Order.query.filter_by(is_deleted=(mode == 'trash'))
    if f_client: q = q.filter(Order.client_id == int(f_client))
    if f_status: q = q.filter(Order.status == f_status)
    if not f_status: q = q.filter(Order.status != 'ghost')
    if f_date_start: q = q.filter(func.date(Order.date) >= f_date_start)
    if f_date_end: q = q.filter(func.date(Order.date) <= f_date_end)
    
    # Сортируем: сначала новые
    orders = q.order_by(Order.date.desc()).all()

    # 2. Создаем Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "Заказы"
    
    # --- СТИЛИ ---
    # Стиль для шапки заказа (Темно-зеленый фон, белый текст)
    style_order_header = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")
    font_order_header = Font(bold=True, color="FFFFFF", size=12)
    
    # Стиль для заголовков таблицы товаров (Светло-серый)
    style_table_header = PatternFill(start_color="E0E0E0", end_color="E0E0E0", fill_type="solid")
    font_table_header = Font(bold=True, color="000000")
    
    # Стиль для итогов (Жирный, границы сверху)
    font_total = Font(bold=True)
    border_total = Border(top=Side(style='thick'))
    
    # Обычные границы
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    align_center = Alignment(horizontal="center", vertical="center")
    align_right = Alignment(horizontal="right", vertical="center")

    # Текущая строка в Excel (курсор)
    row_idx = 1

    # Заголовки столбцов для товаров
    columns = ["Растение", "Размер", "Поле", "Год", "Цена", "Кол-во", "Сумма"]
    # Ширина колонок (примерно)
    col_widths = [30, 15, 15, 10, 15, 10, 20]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[chr(64 + i)].width = w

    # 3. ПРОХОДИМ ПО КАЖДОМУ ЗАКАЗУ
    for o in orders:
        if not o.items: continue # Пропускаем пустые заказы

        # --- ШАПКА ЗАКАЗА ---
        # Объединяем ячейки для красоты заголовка
        ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=7)
        header_text = f"Заказ №{o.id} от {o.date.strftime('%d.%m.%Y')} | Клиент: {o.client.name} | Статус: {o.status}"
        
        cell = ws.cell(row=row_idx, column=1, value=header_text)
        cell.fill = style_order_header
        cell.font = font_order_header
        cell.alignment = Alignment(horizontal="left", vertical="center")
        ws.row_dimensions[row_idx].height = 25 # Чуть выше строка
        row_idx += 1

        # --- ЗАГОЛОВКИ ТАБЛИЦЫ ТОВАРОВ ---
        for col_num, col_name in enumerate(columns, 1):
            cell = ws.cell(row=row_idx, column=col_num, value=col_name)
            cell.fill = style_table_header
            cell.font = font_table_header
            cell.border = thin_border
            cell.alignment = align_center
        row_idx += 1

        # --- ПОЗИЦИИ ТОВАРОВ ---
        total_qty = 0
        total_sum = 0

        grouped_rows = {}
        for item in o.items:
            plant_name = item.plant.name
            size_name = item.size.name
            key = (plant_name, size_name)
            if key not in grouped_rows:
                grouped_rows[key] = {
                    'plant': plant_name,
                    'size': size_name,
                    'fields': set(),
                    'years': set(),
                    'qty': 0,
                    'sum': Decimal(0),
                }
            row = grouped_rows[key]
            row['fields'].add(item.field.name)
            row['years'].add(item.year)
            row['qty'] += int(item.quantity or 0)
            row['sum'] += Decimal(item.sum or 0)

        for row in grouped_rows.values():
            fields_str = ", ".join(sorted(row['fields'], key=natural_key))
            years_str = ", ".join(str(y) for y in sorted(row['years']) if y is not None)
            unit_price = (row['sum'] / row['qty']) if row['qty'] else Decimal(0)

            # Данные строки
            ws.cell(row=row_idx, column=1, value=row['plant']).border = thin_border
            ws.cell(row=row_idx, column=2, value=row['size']).border = thin_border
            ws.cell(row=row_idx, column=3, value=fields_str).border = thin_border

            c_year = ws.cell(row=row_idx, column=4, value=years_str)
            c_year.border = thin_border
            c_year.alignment = align_center
            
            c_price = ws.cell(row=row_idx, column=5, value=float(unit_price))
            c_price.border = thin_border
            c_price.number_format = '#,##0.00'
            
            c_qty = ws.cell(row=row_idx, column=6, value=row['qty'])
            c_qty.border = thin_border
            c_qty.alignment = align_center
            
            c_sum = ws.cell(row=row_idx, column=7, value=float(row['sum']))
            c_sum.border = thin_border
            c_sum.number_format = '#,##0.00'
            c_sum.font = Font(bold=True)

            total_qty += row['qty']
            total_sum += row['sum']
            row_idx += 1

        # --- ИТОГИ ПО ЗАКАЗУ ---
        # Пишем слово "ИТОГО"
        c_label = ws.cell(row=row_idx, column=5, value="ИТОГО ПО ЗАКАЗУ:")
        c_label.font = font_total
        c_label.alignment = align_right
        c_label.border = border_total

        # Итого кол-во
        c_t_qty = ws.cell(row=row_idx, column=6, value=total_qty)
        c_t_qty.font = font_total
        c_t_qty.alignment = align_center
        c_t_qty.border = border_total

        # Итого сумма
        c_t_sum = ws.cell(row=row_idx, column=7, value=float(total_sum))
        c_t_sum.font = font_total
        c_t_sum.number_format = '#,##0.00 "₽"'
        c_t_sum.border = border_total
        
        # Добавляем пустую строку-разделитель между заказами
        row_idx += 2 

    # Отдача файла
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f'Orders_Report_{msk_now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
    
    return send_file(
        buf, 
        download_name=filename, 
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@bp.route('/order/<int:order_id>/export_history')
@login_required
def export_order_history(order_id):
    if current_user.role == 'user2':
        flash('Доступ запрещен')
        return redirect(url_for('orders.order_detail', order_id=order_id))
    o = Order.query.get_or_404(order_id)
    history_rows = OrderItemHistory.query.filter_by(order_id=o.id).order_by(OrderItemHistory.created_at.asc(), OrderItemHistory.id.asc()).all()

    wb = Workbook()
    ws = wb.active
    ws.title = f"Заказ_{o.id}"

    style_order_header = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")
    style_sub_header = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    style_table_header = PatternFill(start_color="C8E6C9", end_color="C8E6C9", fill_type="solid")
    style_total = PatternFill(start_color="F1F8E9", end_color="F1F8E9", fill_type="solid")
    font_order_header = Font(bold=True, color="FFFFFF", size=13)
    font_sub_header = Font(bold=True, color="1B5E20", size=11)
    font_table_header = Font(bold=True, color="000000")
    font_total = Font(bold=True)
    border_total = Border(top=Side(style='thick'))
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    align_center = Alignment(horizontal="center", vertical="center")
    align_right = Alignment(horizontal="right", vertical="center")

    columns = ["Растение", "Размер", "Цена", "Первоначально", "Изменения", "Итог", "Сумма"]
    col_widths = [33, 16, 14, 16, 14, 12, 18]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[chr(64 + i)].width = w

    row_idx = 1
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=7)
    header_text = f"Статус заказа №{o.id} от {o.date.strftime('%d.%m.%Y')}"
    cell = ws.cell(row=row_idx, column=1, value=header_text)
    cell.fill = style_order_header
    cell.font = font_order_header
    cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[row_idx].height = 28
    row_idx += 1

    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=7)
    client_line = f"Клиент: {o.client.name}    |    Статус: {o.status}"
    c_client = ws.cell(row=row_idx, column=1, value=client_line)
    c_client.fill = style_sub_header
    c_client.font = font_sub_header
    c_client.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[row_idx].height = 22
    row_idx += 2

    for col_num, col_name in enumerate(columns, 1):
        c = ws.cell(row=row_idx, column=col_num, value=col_name)
        c.fill = style_table_header
        c.font = font_table_header
        c.border = thin_border
        c.alignment = align_center
    row_idx += 1

    # Сводим историю в агрегат по позиции (изменение количества).
    # Если у позиции нет событий, считаем её "без изменений": первоначально == итог.
    lines = {}

    def _line_key(payload):
        return (
            payload.get('plant_id'),
            payload.get('size_id'),
            payload.get('field_id'),
            payload.get('year'),
        )

    for h in history_rows:
        payload = {}
        if h.snapshot_payload:
            try:
                payload = json.loads(h.snapshot_payload)
            except Exception:
                payload = {}
        if not payload:
            continue

        key = _line_key(payload)
        if key not in lines:
            lines[key] = {
                'plant_name': payload.get('plant_name') or '-',
                'size_name': payload.get('size_name') or '-',
                'field_name': payload.get('field_name') or '-',
                'year': payload.get('year'),
                'price': Decimal(str(payload.get('price') or 0)),
                'initial_qty': None,
                'delta_qty': 0,
                'final_qty': None,
            }

        line = lines[key]

        # Первичная загрузка при создании заказа должна попадать в "Первоначально",
        # а не в "Изменения".
        if h.action_type == 'initial_item':
            if line['initial_qty'] is None:
                line['initial_qty'] = int(h.after_quantity if h.after_quantity is not None else 0)
            if h.after_quantity is not None:
                line['final_qty'] = int(h.after_quantity)
        else:
            if line['initial_qty'] is None:
                line['initial_qty'] = int(h.before_quantity if h.before_quantity is not None else 0)
            line['delta_qty'] += int(h.delta_quantity or 0)
            if h.after_quantity is not None:
                line['final_qty'] = int(h.after_quantity)

        if payload.get('price') is not None:
            line['price'] = Decimal(str(payload.get('price')))

    for item in o.items:
        key = (item.plant_id, item.size_id, item.field_id, item.year)
        if key not in lines:
            lines[key] = {
                'plant_name': item.plant.name if item.plant else '-',
                'size_name': item.size.name if item.size else '-',
                'field_name': item.field.name if item.field else '-',
                'year': item.year,
                'price': Decimal(str(item.price or 0)),
                'initial_qty': int(item.quantity),
                'delta_qty': 0,
                'final_qty': int(item.quantity),
            }
        else:
            lines[key]['final_qty'] = int(item.quantity)
            lines[key]['price'] = Decimal(str(item.price or 0))
            if lines[key]['initial_qty'] is None:
                lines[key]['initial_qty'] = int(item.quantity) - int(lines[key]['delta_qty'])

    for data in lines.values():
        if data['initial_qty'] is None:
            data['initial_qty'] = 0
        if data['final_qty'] is None:
            data['final_qty'] = data['initial_qty']

    total_final_qty = 0
    total_final_sum = Decimal('0')
    for data in sorted(lines.values(), key=lambda x: (x['plant_name'], x['size_name'], x['field_name'], x['year'] or 0)):
        final_sum = data['price'] * Decimal(data['final_qty'])
        ws.cell(row=row_idx, column=1, value=data['plant_name']).border = thin_border
        ws.cell(row=row_idx, column=2, value=data['size_name']).border = thin_border
        c_price = ws.cell(row=row_idx, column=3, value=float(data['price']))
        c_price.border = thin_border
        c_price.number_format = '#,##0.00 "₽"'

        c_initial = ws.cell(row=row_idx, column=4, value=int(data['initial_qty']))
        c_initial.border = thin_border
        c_initial.alignment = align_center

        c_delta = ws.cell(row=row_idx, column=5, value=int(data['delta_qty']))
        c_delta.border = thin_border
        c_delta.alignment = align_center

        c_final = ws.cell(row=row_idx, column=6, value=int(data['final_qty']))
        c_final.border = thin_border
        c_final.alignment = align_center

        c_sum = ws.cell(row=row_idx, column=7, value=float(final_sum))
        c_sum.border = thin_border
        c_sum.number_format = '#,##0.00 "₽"'
        c_sum.font = Font(bold=True)

        total_final_qty += int(data['final_qty'])
        total_final_sum += final_sum
        row_idx += 1

    c_label = ws.cell(row=row_idx, column=5, value="ИТОГО:")
    c_label.font = font_total
    c_label.alignment = align_right
    c_label.border = border_total
    c_label.fill = style_total

    c_t_qty = ws.cell(row=row_idx, column=6, value=total_final_qty)
    c_t_qty.font = font_total
    c_t_qty.alignment = align_center
    c_t_qty.border = border_total
    c_t_qty.fill = style_total

    c_t_sum = ws.cell(row=row_idx, column=7, value=float(total_final_sum))
    c_t_sum.font = font_total
    c_t_sum.number_format = '#,##0.00 "₽"'
    c_t_sum.border = border_total
    c_t_sum.fill = style_total

    row_idx += 2
    debt = Decimal(str(o.total_sum)) - Decimal(str(o.paid_sum))
    summary_titles = ["Итоговая сумма заказа", "Оплачено", "Остаток"]
    summary_values = [float(o.total_sum), float(o.paid_sum), float(debt)]
    summary_cols = [(1, 2), (3, 4), (5, 7)]

    for idx, title in enumerate(summary_titles):
        start_col, end_col = summary_cols[idx]
        ws.merge_cells(start_row=row_idx, start_column=start_col, end_row=row_idx, end_column=end_col)
        t_cell = ws.cell(row=row_idx, column=start_col, value=title)
        t_cell.fill = style_sub_header
        t_cell.font = font_sub_header
        t_cell.alignment = align_center
        t_cell.border = thin_border
        for c in range(start_col + 1, end_col + 1):
            ws.cell(row=row_idx, column=c).border = thin_border
    row_idx += 1

    for idx, value in enumerate(summary_values):
        start_col, end_col = summary_cols[idx]
        ws.merge_cells(start_row=row_idx, start_column=start_col, end_row=row_idx, end_column=end_col)
        v_cell = ws.cell(row=row_idx, column=start_col, value=value)
        v_cell.font = Font(bold=True, size=12, color="1B5E20")
        v_cell.alignment = align_center
        v_cell.number_format = '#,##0.00 "₽"'
        v_cell.border = thin_border
        for c in range(start_col + 1, end_col + 1):
            ws.cell(row=row_idx, column=c).border = thin_border

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f'Order_{o.id}_History_{msk_now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
    return send_file(
        buf,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@bp.route('/orders/export_client_catalog')
@login_required
def export_client_catalog():
    rows = _get_client_catalog_rows()

    wb = Workbook()
    ws = wb.active
    ws.title = "Прайс для заказа"

    ws.merge_cells("A1:E1")
    ws["A1"] = "ПРАЙС-ЛИСТ ДЛЯ ЗАКАЗА"
    ws["A1"].font = Font(bold=True, size=16, color="FFFFFF")
    ws["A1"].fill = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 30

    ws.merge_cells("A2:E2")
    ws["A2"] = f"Дата выгрузки: {msk_now().strftime('%d.%m.%Y %H:%M')}  |  Внесите количество в столбец 'Заказать, шт'"
    ws["A2"].font = Font(size=11, color="1B5E20")
    ws["A2"].fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    ws["A2"].alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[2].height = 22

    ws.merge_cells("A3:E3")
    ws["A3"] = "Показаны только доступные позиции. Размеры 'Нетов.' и 'Саженцы' исключены."
    ws["A3"].font = Font(size=10, color="455A64", italic=True)
    ws["A3"].alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[3].height = 20

    header_row = 5
    headers = ["Наименование", "Размер", "Цена, ₽", "Свободный остаток, шт", "Заказать, шт"]
    for col, title in enumerate(headers, 1):
        c = ws.cell(row=header_row, column=col, value=title)
        c.font = Font(bold=True, color="000000")
        c.fill = PatternFill(start_color="C8E6C9", end_color="C8E6C9", fill_type="solid")
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        c.border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )
    ws.row_dimensions[header_row].height = 30

    widths = {1: 40, 2: 14, 3: 13, 4: 20, 5: 16}
    for idx, width in widths.items():
        ws.column_dimensions[get_column_letter(idx)].width = width

    data_start = header_row + 1
    row_idx = data_start

    thin = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))
    zebra_fill = PatternFill(start_color="F9FBE7", end_color="F9FBE7", fill_type="solid")
    qty_fill = PatternFill(start_color="FFF9C4", end_color="FFF9C4", fill_type="solid")

    for i, item in enumerate(rows):
        ws.cell(row=row_idx, column=1, value=item["plant_name"])
        ws.cell(row=row_idx, column=2, value=item["size_name"])
        c_price = ws.cell(row=row_idx, column=3, value=float(item["price"] or 0))
        c_free = ws.cell(row=row_idx, column=4, value=int(item["free_qty"] or 0))
        c_order = ws.cell(row=row_idx, column=5, value=None)

        c_price.number_format = '#,##0.00'
        c_free.number_format = '#,##0'
        c_order.number_format = '#,##0'

        for col in range(1, 6):
            c = ws.cell(row=row_idx, column=col)
            c.border = thin
            c.font = Font(size=12)
            if col in (3, 4, 5):
                c.alignment = Alignment(horizontal="center", vertical="center")
            else:
                c.alignment = Alignment(horizontal="left", vertical="center")

        if i % 2 == 1:
            for col in range(1, 6):
                ws.cell(row=row_idx, column=col).fill = zebra_fill

        c_order.fill = qty_fill
        row_idx += 1

    data_end = max(row_idx - 1, data_start)

    qty_validation = DataValidation(type="whole", operator="greaterThanOrEqual", formula1="0", allow_blank=True)
    qty_validation.error = "Введите целое число 0 или больше."
    qty_validation.errorTitle = "Некорректное количество"
    ws.add_data_validation(qty_validation)
    if data_end >= data_start:
        qty_validation.add(f"E{data_start}:E{data_end}")
        ws.auto_filter.ref = f"A{header_row}:E{data_end}"

    ws.freeze_panes = f"A{data_start}"
    ws.sheet_view.zoomScale = 120

    footer_row = data_end + 2
    ws.merge_cells(start_row=footer_row, start_column=1, end_row=footer_row, end_column=5)
    ws.cell(row=footer_row, column=1, value="Подсказка: в столбце 'Заказать, шт' клиент заполняет нужное количество.").font = Font(
        italic=True, color="1B5E20"
    )
    ws.cell(row=footer_row, column=1).alignment = Alignment(horizontal="left", vertical="center")

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f'Client_Catalog_{msk_now().strftime("%Y-%m-%d_%H-%M")}.xlsx'
    log_action("Выгрузил клиентский каталог (Excel)")
    return send_file(
        buf,
        download_name=filename,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )