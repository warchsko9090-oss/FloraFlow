import io
import re
from datetime import datetime, timedelta
from decimal import Decimal
from flask import make_response
from flask_login import current_user
from sqlalchemy import func
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from xhtml2pdf import pisa 

from app.models import (
    db, StockBalance, Order, OrderItem, BudgetItem, ActionLog, PriceHistory,
    Plant, Size, Field, Client, Supplier
)

# --- КОНСТАНТЫ ---
MONTH_NAMES = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 
               7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}
RATE_TYPES_LABELS = {'norm': 'Обычный', 'norm_over': 'Обычный (Перераб)', 'spec': 'Копка/Стрижка', 'spec_over': 'Копка/Стрижка (Перераб)'}
EMPLOYEE_ROLES = {'worker': 'Рабочий', 'brigadier': 'Бригадир', 'assistant': 'Помощник', 'manager': 'Менеджер', 'mechanic': 'Механик'}

# --- ФУНКЦИЯ ВРЕМЕНИ (MSK UTC+3) ---
def msk_now():
    return datetime.utcnow() + timedelta(hours=3)

def msk_today():
    return msk_now().date()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def log_action(text):
    """Записывает действие текущего пользователя в лог."""
    try:
        if current_user.is_authenticated:
            log = ActionLog(user_id=current_user.id, action=text, date=msk_now())
            db.session.add(log)
            db.session.commit()
    except Exception as e:
        print(f"Log error: {e}")

def get_or_create_stock(plant_id, size_id, field_id, year):
    """Ищет запись остатка, если нет - создает нулевую."""
    stock = StockBalance.query.filter_by(plant_id=plant_id, size_id=size_id, field_id=field_id, year=year).first()
    if not stock:
        stock = StockBalance(plant_id=plant_id, size_id=size_id, field_id=field_id, year=year, quantity=0, price=0, purchase_price=0)
        db.session.add(stock)
        db.session.flush()
    return stock

def natural_key(obj):
    """Для сортировки (C1, C2, C10...)"""
    text = obj.name if hasattr(obj, 'name') else str(obj)
    if isinstance(obj, BudgetItem): text = obj.code
    return [int(s) if s.isdigit() else s.lower() for s in re.split('([0-9]+)', text)]

def check_stock_availability(plant_id, size_id, field_id, year, quantity_needed, exclude_item_id=None):
    """Проверяет, хватает ли свободного остатка (с учетом резервов)."""
    stock = StockBalance.query.filter_by(plant_id=plant_id, size_id=size_id, field_id=field_id, year=year).first()
    fact_qty = stock.quantity if stock else 0
    q = db.session.query(func.sum(OrderItem.quantity - OrderItem.shipped_quantity)).join(Order).filter(
        OrderItem.plant_id == plant_id, OrderItem.size_id == size_id,
        OrderItem.field_id == field_id, OrderItem.year == year,
        Order.status != 'canceled', Order.status != 'ghost', Order.is_deleted == False
    )
    if exclude_item_id: q = q.filter(OrderItem.id != exclude_item_id)
    reserved_qty = q.scalar() or 0
    free_qty = fact_qty - reserved_qty
    if free_qty < quantity_needed: return False, free_qty
    return True, free_qty

def get_actual_price(plant_id, size_id, field_id, check_year=None):
    """Получает цену товара из истории или текущих остатков"""
    curr_year = check_year if check_year else msk_now().year
    hist = PriceHistory.query.filter_by(plant_id=plant_id, size_id=size_id, field_id=field_id, year=curr_year).first()
    if hist: return hist.price
    sb = StockBalance.query.filter_by(plant_id=plant_id, size_id=size_id, field_id=field_id).order_by(StockBalance.year.desc()).first()
    return sb.price if sb else Decimal('0.00')

def create_pdf_response(html_content, filename):
    result = io.BytesIO()
    pdf = pisa.pisaDocument(io.BytesIO(html_content.encode("UTF-8")), result, encoding='UTF-8')
    if not pdf.err:
        response = make_response(result.getvalue())
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
        return response
    return "Error generating PDF"

def apply_excel_styles(ws):
    header_fill = PatternFill(start_color="E0F2F1", end_color="E0F2F1", fill_type="solid")
    header_font = Font(bold=True, color="004D40")
    thin_border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    for cell in ws[1]:
        cell.fill = header_fill; cell.font = header_font; cell.alignment = Alignment(horizontal="center"); cell.border = thin_border
    for col in ws.columns:
        max_length = 0; column = col[0].column_letter
        for cell in col:
            cell.border = thin_border
            try:
                if len(str(cell.value)) > max_length: max_length = len(str(cell.value))
            except Exception: pass
        ws.column_dimensions[column].width = max_length + 2

# --- ФУНКЦИИ ФОРМАТИРОВАНИЯ ---
def format_money(value):
    if value is None: return ""
    try: return "{:,.2f} ₽".format(float(value)).replace(',', ' ')
    except (ValueError, TypeError): return str(value)

def format_money_int(value):
    if value is None: return ""
    try: return "{:,.0f} ₽".format(float(value)).replace(',', ' ')
    except (ValueError, TypeError): return str(value)

def dateru(value):
    if value is None: return ""
    if isinstance(value, str): return value 
    return value.strftime('%d.%m.%Y')

# --- EXCEL IMPORT MAP HELPERS ---
def get_plant_map():
    return {p.name.strip().lower(): p.id for p in Plant.query.all()}

def get_size_map():
    return {s.name.strip().lower(): s.id for s in Size.query.all()}

def get_field_map():
    return {f.name.strip().lower(): f.id for f in Field.query.all()}

def get_client_map():
    return {c.name.strip().lower(): c.id for c in Client.query.all()}

def get_supplier_map():
    return {s.name.strip().lower(): s.id for s in Supplier.query.all()}