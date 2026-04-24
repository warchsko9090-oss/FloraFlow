from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from datetime import datetime
from sqlalchemy import func
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

# --- Пользователи и Настройки ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False) 
    role = db.Column(db.String(20), nullable=False) # 'admin', 'user', 'user2'

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class AppSetting(db.Model):
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.Text) 

# --- Справочники ---
class Plant(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False, unique=True)
    characteristic = db.Column(db.String(200))
    latin_name = db.Column(db.String(200)) # Добавлено поле

class Size(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)

class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    fixed_balance = db.Column(db.Numeric(15, 2), nullable=True)
    fixed_balance_date = db.Column(db.Date, nullable=True)

class Field(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    investor_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    planting_year = db.Column(db.Integer, default=2017)
    investor = db.relationship('Client', foreign_keys=[investor_id])

    # --- Визуальная карта (Журнал документов → вид «Карта»).
    # Координаты/размеры в процентах от канваса (0..100), чтобы оставаться
    # независимыми от размера фонового фото и от размера окна.
    # map_shape — JSON-массив вершин полигона в процентах от бокса поля
    # (например [[0,0],[100,0],[100,100],[0,100]] = прямоугольник).
    # Если любое из полей пустое — фронт нарисует поле в автоматической сетке.
    map_x = db.Column(db.Float, nullable=True)
    map_y = db.Column(db.Float, nullable=True)
    map_w = db.Column(db.Float, nullable=True)
    map_h = db.Column(db.Float, nullable=True)
    map_shape = db.Column(db.Text, nullable=True)
    map_color = db.Column(db.String(20), nullable=True)
    map_z = db.Column(db.Integer, nullable=True)
    # Раскладка содержимого карточки поля на карте: 'auto' | 'stack' |
    # 'row' | 'compact' | 'number-only'. Если NULL — fronend применит 'stack'
    # (вертикально: номер/итого/состав), это максимально читаемо на любом
    # размере поля. Админ может переключить в редакторе карты для конкретного поля.
    map_layout = db.Column(db.String(20), nullable=True)


class MapSettings(db.Model):
    """Глобальные настройки визуальной карты полей. В таблице всегда одна строка (id=1)."""
    id = db.Column(db.Integer, primary_key=True)
    background_path = db.Column(db.String(255), nullable=True)   # относительный путь в UPLOAD_FOLDER
    bg_width = db.Column(db.Integer, nullable=True)              # натуральные размеры фото (для aspect-ratio)
    bg_height = db.Column(db.Integer, nullable=True)
    bg_opacity = db.Column(db.Float, default=1.0)                # 0..1
    # --- Трансформация фонового изображения внутри канваса карты.
    # bg_fit:
    #   'contain' — вписать целиком (по умолчанию);
    #   'cover'   — заполнить весь канвас, обрезая лишнее;
    #   'stretch' — растянуть до краёв канваса, игнорируя пропорции;
    #   'custom'  — использовать ручные offset_x/y + scale.
    # bg_offset_x/y — смещение центра фото относительно центра канваса в процентах от канваса (−100..100).
    # bg_scale — множитель масштаба поверх fit'а (0.1..5.0), 1.0 — без изменений.
    # bg_rotation — поворот фото в градусах (−180..180).
    bg_fit = db.Column(db.String(20), default='cover')
    bg_offset_x = db.Column(db.Float, default=0.0)
    bg_offset_y = db.Column(db.Float, default=0.0)
    bg_scale = db.Column(db.Float, default=1.0)
    bg_rotation = db.Column(db.Float, default=0.0)
    # --- Размер рабочей зоны (канваса).
    # canvas_width  — «физическая» ширина канваса в px. Поля позиционируются в % от неё,
    #                 а визуальный размер грида в DOM зависит от этой величины и от zoom'а.
    # canvas_aspect — соотношение сторон канваса. Строка:
    #                   'auto'  — от натуральных размеров фонового фото (либо 16/9 если фото нет);
    #                   '16/9', '4/3', '1/1', '21/9', '3/1', '2/1' — фиксированный ratio.
    #                 Независимость от фото полезна, если снимок не покрывает всю плантацию.
    canvas_width = db.Column(db.Integer, default=1600)
    canvas_aspect = db.Column(db.String(20), default='auto')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @classmethod
    def get(cls):
        row = cls.query.get(1)
        if row is None:
            row = cls(id=1, bg_opacity=1.0, bg_fit='cover',
                      bg_offset_x=0.0, bg_offset_y=0.0, bg_scale=1.0, bg_rotation=0.0,
                      canvas_width=1600, canvas_aspect='auto')
            db.session.add(row)
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                row = cls.query.get(1)
        return row

class Supplier(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)

class FileArchive(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)  # Имя файла на диске
    original_name = db.Column(db.String(255), nullable=False) # Оригинальное имя
    category = db.Column(db.String(50)) # Поступление, Инвентаризация и т.д.
    comment = db.Column(db.String(255))
    uploaded_at = db.Column(db.DateTime, default=datetime.now)
    size_bytes = db.Column(db.Integer, default=0)

# --- Учет ---
class StockBalance(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    plant_id = db.Column(db.Integer, db.ForeignKey('plant.id'), nullable=False)
    size_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=False)
    year = db.Column(db.Integer, nullable=False, default=2025) 
    
    price = db.Column(db.Numeric(10, 2), default=0.0)
    purchase_price = db.Column(db.Numeric(10, 2), default=0.0)
    # НОВОЕ ПОЛЕ:
    current_total_cost = db.Column(db.Numeric(10, 2), default=0.0) 
    
    quantity = db.Column(db.Integer, default=0)
    
    plant = db.relationship('Plant')
    size = db.relationship('Size')
    field = db.relationship('Field')
    __table_args__ = (
        db.UniqueConstraint('plant_id', 'size_id', 'field_id', 'year', name='_plant_size_field_year_uc'),
        db.Index('idx_stock_plant', 'plant_id'),
        db.Index('idx_stock_field', 'field_id'),
    )

class PriceHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    plant_id = db.Column(db.Integer, db.ForeignKey('plant.id'), nullable=False)
    size_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    price = db.Column(db.Numeric(10, 2), default=0.0)
    plant = db.relationship('Plant')
    size = db.relationship('Size')
    field = db.relationship('Field')
    __table_args__ = (db.UniqueConstraint('plant_id', 'size_id', 'field_id', 'year', name='_price_hist_uc'),)

class Document(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    doc_type = db.Column(db.String(50), nullable=False) 
    date = db.Column(db.DateTime, default=datetime.now)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    comment = db.Column(db.String(500))
    order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=True)
    user = db.relationship('User')
    rows = db.relationship('DocumentRow', backref='document', cascade="all, delete-orphan")
    order = db.relationship('Order', foreign_keys=[order_id]) 

class DocumentRow(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    document_id = db.Column(db.Integer, db.ForeignKey('document.id'), nullable=False)
    plant_id = db.Column(db.Integer, db.ForeignKey('plant.id'), nullable=False)
    size_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=False)
    field_from_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=True)
    field_to_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=True)
    year = db.Column(db.Integer, nullable=False, default=2025)
    quantity = db.Column(db.Integer, nullable=False)
    size_to_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=True)
    
    plant = db.relationship('Plant')
    size = db.relationship('Size', foreign_keys=[size_id])
    size_to = db.relationship('Size', foreign_keys=[size_to_id])
    field_from = db.relationship('Field', foreign_keys=[field_from_id])
    field_to = db.relationship('Field', foreign_keys=[field_to_id])
    __table_args__ = (
        db.Index('idx_docrow_document', 'document_id'),
    )

class ActionLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.now)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    action = db.Column(db.String(500))
    user = db.relationship('User')

# --- Продажи ---
class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.now)
    client_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=False)
    status = db.Column(db.String(20), default='reserved') 
    canceled_at = db.Column(db.DateTime, nullable=True)
    is_deleted = db.Column(db.Boolean, default=False) 
    invoice_number = db.Column(db.String(50))
    invoice_date = db.Column(db.Date, nullable=True)
    
    # НОВОЕ ПОЛЕ
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'), nullable=True)
    is_barter = db.Column(db.Boolean, default=False)

    # Метка «менеджер подтвердил актуальность резерва» — обнуляет таймер
    # детектора «резерв без движения 14 дней». NULL для старых заказов.
    reserve_ack_at = db.Column(db.DateTime, nullable=True)

    client = db.relationship('Client')
    items = db.relationship('OrderItem', backref='order', cascade="all, delete-orphan")
    payments = db.relationship('Payment', backref='order', cascade="all, delete-orphan")

    __table_args__ = (
        db.Index('idx_order_date', 'date'),
        db.Index('idx_order_client', 'client_id'),
        db.Index('idx_order_status', 'status'),
    )

    @property
    def total_sum(self):
        return sum(item.sum for item in self.items)
    
    @property
    def paid_sum(self):
        return sum(p.amount for p in self.payments)

    @property
    def payment_status(self):
        total = self.total_sum
        paid = self.paid_sum
        if paid >= total and total > 0: return 'paid'
        if paid > 0: return 'partial'
        return 'unpaid'

    def refresh_status_by_dug(self):
        """Пересчитывает статус заказа по факту выкопки.

        - Если ничего не выкопано -> оставляем "reserved"
        - Если хоть что-то выкопано -> ставим "in_progress"
        - Если полностью выкопано (по всем позициям) -> ставим "ready"
        """
        if self.status in ('canceled', 'ghost', 'shipped'):
            return

        total_qty = sum(item.quantity for item in self.items)
        if total_qty == 0:
            return

        total_dug = sum(item.dug_total for item in self.items)
        if total_dug >= total_qty:
            self.status = 'ready'
        elif total_dug > 0:
            self.status = 'in_progress'
        else:
            self.status = 'reserved'


class OrderItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=False)
    plant_id = db.Column(db.Integer, db.ForeignKey('plant.id'), nullable=False)
    size_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=False)
    field_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=False) 
    year = db.Column(db.Integer, nullable=False, default=2025)
    price = db.Column(db.Numeric(10, 2), default=0.0)
    quantity = db.Column(db.Integer, default=0) 
    shipped_quantity = db.Column(db.Integer, default=0) 
    plant = db.relationship('Plant')
    size = db.relationship('Size')
    field = db.relationship('Field')
    dug_quantity = db.Column(db.Integer, default=0)

    @property
    def dug_total(self):
        """Фактическое количество выкопано по этой позиции (с учетом лога выкопки).

        Если поле dug_quantity заполнено — используем его (для совместимости старых данных).
        Иначе считаем сумму по связанным записям DiggingLog.
        """
        if self.dug_quantity and self.dug_quantity > 0:
            return self.dug_quantity
        total = db.session.query(func.sum(DiggingLog.quantity)) \
            .filter(DiggingLog.order_item_id == self.id, DiggingLog.status != 'rejected') \
            .scalar() or 0
        return total

    __table_args__ = (
        db.Index('idx_orderitem_order', 'order_id'),
        db.Index('idx_orderitem_plant', 'plant_id'),
    )

    @property
    def sum(self):
        return self.price * self.quantity

class Payment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=False)
    date = db.Column(db.Date, default=datetime.now)
    amount = db.Column(db.Numeric(10, 2), nullable=False)
    payment_type = db.Column(db.String(20), default='cashless') # cashless, cash, barter
    comment = db.Column(db.String(200))
    file_path = db.Column(db.String(255), nullable=True) # Путь к файлу платежки

class OrderItemHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=False)
    order_item_id = db.Column(db.Integer, db.ForeignKey('order_item.id'), nullable=True)
    action_type = db.Column(db.String(30), nullable=False)  # add_item, delete_item, qty_change
    before_quantity = db.Column(db.Integer, nullable=True)
    after_quantity = db.Column(db.Integer, nullable=True)
    delta_quantity = db.Column(db.Integer, nullable=False, default=0)
    snapshot_payload = db.Column(db.Text, nullable=True)
    changed_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)

    order = db.relationship('Order', foreign_keys=[order_id])
    item = db.relationship('OrderItem', foreign_keys=[order_item_id])
    changed_by = db.relationship('User', foreign_keys=[changed_by_user_id])

    __table_args__ = (
        db.Index('idx_order_item_history_order', 'order_id'),
        db.Index('idx_order_item_history_item', 'order_item_id'),
        db.Index('idx_order_item_history_created', 'created_at'),
    )

# --- ФИНАНСЫ ---
class BudgetItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(20), nullable=False)
    name = db.Column(db.String(200), nullable=False)
    is_amortization = db.Column(db.Boolean, default=False)

class BudgetPlan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    budget_item_id = db.Column(db.Integer, db.ForeignKey('budget_item.id'), nullable=False)
    month = db.Column(db.Integer, nullable=False)
    amount = db.Column(db.Numeric(10, 2), default=0.0)
    item = db.relationship('BudgetItem')

# Плановые поступления (поступления денежных средств по месяцам)
class CashflowPlan(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.Integer, nullable=False)
    amount = db.Column(db.Numeric(10, 2), default=0.0)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(150), nullable=False)
    is_salary = db.Column(db.Boolean, default=False)
    fixed_salary = db.Column(db.Numeric(10, 2), default=0.0)
    is_active = db.Column(db.Boolean, default=True) 
    role = db.Column(db.String(50), default='worker') 
    # Персональные статьи ЗП (если None — берётся глобальная настройка hr_official_item / hr_unofficial_item)
    official_budget_item_id = db.Column(db.Integer, db.ForeignKey('budget_item.id'), nullable=True)
    unofficial_budget_item_id = db.Column(db.Integer, db.ForeignKey('budget_item.id'), nullable=True)
    foreign_profile = db.relationship('ForeignEmployeeProfile', backref='employee', uselist=False, cascade="all, delete-orphan")
    foreign_documents = db.relationship('ForeignEmployeeDocument', backref='employee', cascade="all, delete-orphan")
    patent_periods = db.relationship('PatentPeriod', backref='employee', cascade="all, delete-orphan")
    patent_payments = db.relationship('PatentPayment', backref='employee', cascade="all, delete-orphan")


class ForeignEmployeeProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False, unique=True)
    full_name = db.Column(db.String(200), nullable=False)
    phone = db.Column(db.String(50))
    citizenship = db.Column(db.String(100))
    date_of_birth = db.Column(db.Date, nullable=True)
    passport_number = db.Column(db.String(100))
    passport_issued_by = db.Column(db.String(255))
    migration_card_number = db.Column(db.String(100))
    registration_address = db.Column(db.String(255))
    inn = db.Column(db.String(50))
    snils = db.Column(db.String(50))
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)


class ForeignEmployeeDocument(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    category = db.Column(db.String(50), nullable=False)  # passport, patent, check, migration_card ...
    title = db.Column(db.String(200))
    original_name = db.Column(db.String(255), nullable=False)
    stored_name = db.Column(db.String(255), nullable=False)
    file_rel_path = db.Column(db.String(500), nullable=False)
    uploaded_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    uploaded_at = db.Column(db.DateTime, default=datetime.now)
    size_bytes = db.Column(db.Integer, default=0)
    uploaded_by = db.relationship('User', foreign_keys=[uploaded_by_user_id])
    __table_args__ = (
        db.Index('idx_foreign_doc_employee', 'employee_id'),
    )


class PatentPeriod(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=False)
    status = db.Column(db.String(20), default='active')  # active, archived, canceled
    is_current = db.Column(db.Boolean, default=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    created_by = db.relationship('User', foreign_keys=[created_by_user_id])
    payments = db.relationship('PatentPayment', backref='period', cascade="all, delete-orphan")
    reminder_logs = db.relationship('PatentReminderLog', backref='period', cascade="all, delete-orphan")
    __table_args__ = (
        db.Index('idx_patent_period_employee', 'employee_id'),
        db.Index('idx_patent_period_end_date', 'end_date'),
    )


class PatentPayment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    patent_period_id = db.Column(db.Integer, db.ForeignKey('patent_period.id'), nullable=False)
    payment_date = db.Column(db.Date, nullable=False)
    months_paid = db.Column(db.Integer, nullable=False, default=1)  # 1/2/3
    amount = db.Column(db.Numeric(10, 2), nullable=False, default=0)
    period_end_after_payment = db.Column(db.Date, nullable=True)
    check_file_rel_path = db.Column(db.String(500), nullable=True)
    check_original_name = db.Column(db.String(255), nullable=True)
    comment = db.Column(db.String(500))
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    created_by = db.relationship('User', foreign_keys=[created_by_user_id])
    __table_args__ = (
        db.Index('idx_patent_payment_employee', 'employee_id'),
        db.Index('idx_patent_payment_date', 'payment_date'),
    )


class PatentReminderLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    patent_period_id = db.Column(db.Integer, db.ForeignKey('patent_period.id'), nullable=False)
    reminder_type = db.Column(db.String(20), nullable=False)  # day14 / day7
    target_date = db.Column(db.Date, nullable=False)
    sent_at = db.Column(db.DateTime, default=datetime.now)
    message_text = db.Column(db.Text)
    __table_args__ = (
        db.UniqueConstraint('patent_period_id', 'reminder_type', 'target_date', name='uq_patent_reminder_once'),
        db.Index('idx_patent_reminder_target', 'target_date'),
    )

class Expense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    date = db.Column(db.Date, nullable=False)
    budget_item_id = db.Column(db.Integer, db.ForeignKey('budget_item.id'), nullable=False) # Общая статья
    description = db.Column(db.String(500))
    amount = db.Column(db.Numeric(10, 2), nullable=False)
    payment_type = db.Column(db.String(10), nullable=False) 
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=True)
    
    # ПЕРИОД ДЛЯ КАДРОВ
    target_month = db.Column(db.Integer, nullable=True)
    target_year = db.Column(db.Integer, nullable=True)
    
    # СВЯЗКИ
    order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=True)
    barter_order_id = db.Column(db.Integer, db.ForeignKey('order.id'), nullable=True) # Заказ для взаимозачета
    invoice_id = db.Column(db.Integer, db.ForeignKey('payment_invoice.id'), nullable=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'), nullable=True) 
    project_budget_id = db.Column(db.Integer, db.ForeignKey('project_budget.id'), nullable=True) 
    
    item = db.relationship('BudgetItem')
    employee = db.relationship('Employee')
    invoice = db.relationship('PaymentInvoice', backref='expenses')
    order = db.relationship('Order', foreign_keys=[order_id], backref='project_expenses')
    barter_order = db.relationship('Order', foreign_keys=[barter_order_id], backref='barter_expenses')
    project_budget_item = db.relationship('ProjectBudget')
    __table_args__ = (
        db.Index('idx_expense_date_budget', 'date', 'budget_item_id'),
        db.Index('idx_expense_employee', 'employee_id'),
        db.Index('idx_expense_invoice', 'invoice_id'),
    )

class UnitCostOverride(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, unique=True, nullable=False)
    amount = db.Column(db.Numeric(10, 2), nullable=True) 
    amortization = db.Column(db.Numeric(10, 2), nullable=True)

class SalaryRate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    role = db.Column(db.String(50), default='worker', nullable=False)
    rate_type = db.Column(db.String(20), nullable=False) 
    rate_value = db.Column(db.Numeric(10, 2), default=0.0)
    __table_args__ = (db.UniqueConstraint('year', 'role', 'rate_type', name='_year_role_rate_type_uc'),)

class TimeLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    hours_norm = db.Column(db.Float, default=0.0)
    hours_norm_over = db.Column(db.Float, default=0.0)
    hours_spec = db.Column(db.Float, default=0.0)
    hours_spec_over = db.Column(db.Float, default=0.0)
    is_day_off = db.Column(db.Boolean, default=False) # НОВОЕ ПОЛЕ: Флаг выходного дня
    employee = db.relationship('Employee')
    __table_args__ = (
        db.UniqueConstraint('employee_id', 'date', name='_emp_date_uc'),
        db.Index('idx_timelog_date', 'date'),
    )

class EmployeePayment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    employee_id = db.Column(db.Integer, db.ForeignKey('employee.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    amount = db.Column(db.Numeric(10, 2), nullable=False)
    payment_type = db.Column(db.String(20), nullable=False)
    comment = db.Column(db.String(200))
    employee = db.relationship('Employee')

# --- Анализ конкурентов (CRM) ---
class CompetitorSnapshot(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.now)
    name = db.Column(db.String(100)) # Например "Мониторинг Апрель"
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    # Сырой ответ LLM для отладки «почему ИИ вернул это» (при автоматическом запуске).
    raw_ai_response = db.Column(db.Text, nullable=True)
    rows = db.relationship('CompetitorRow', backref='snapshot', cascade="all, delete-orphan")

class CompetitorRow(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    snapshot_id = db.Column(db.Integer, db.ForeignKey('competitor_snapshot.id'), nullable=False)
    plant_name = db.Column(db.String(150))
    size_name = db.Column(db.String(50))
    competitor_name = db.Column(db.String(100))
    competitor_price = db.Column(db.Numeric(10, 2), default=0.0)
    source_link = db.Column(db.String(500))
    # Кэшируем наши данные на момент загрузки, чтобы история не менялась при смене наших цен
    our_price_at_moment = db.Column(db.Numeric(10, 2), nullable=True)
    our_cost_at_moment = db.Column(db.Numeric(10, 2), nullable=True)
    # Поля качества совпадения (заполняются валидатором/ИИ).
    pack_type = db.Column(db.String(16), nullable=True)      # RB/WRB/grunt/C2..C20/P9/other
    form = db.Column(db.String(16), nullable=True)           # free/ball/niwaki/topiary/pompon/stamm/other
    source_excerpt = db.Column(db.Text, nullable=True)       # цитата с сайта конкурента
    confidence = db.Column(db.Float, nullable=True)          # 0..1 от модели
    is_rejected = db.Column(db.Boolean, default=False, nullable=False)
    reject_reasons = db.Column(db.Text, nullable=True)       # JSON-строка со списком причин

# --- ЧАТ И AI ---
class KnowledgeBase(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    question = db.Column(db.String(255), nullable=False) 
    keywords = db.Column(db.String(255), nullable=False) 
    answer = db.Column(db.Text, nullable=False)          
    link = db.Column(db.String(200), nullable=True)      

# История чата для обучения (RAG)
class ChatLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.now)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    user_message = db.Column(db.String(500))
    ai_response = db.Column(db.Text)
    is_helpful = db.Column(db.Boolean, nullable=True) # True=Like, False=Dislike
    user = db.relationship('User')

# Примеры правильных SQL запросов (Few-Shot Learning)
class SQLExample(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    question = db.Column(db.String(255), nullable=False)
    sql_query = db.Column(db.Text, nullable=False)

class ChangeLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.String(50), nullable=False) # Например v1.02
    date = db.Column(db.Date, default=datetime.now)    # Дата релиза
    content = db.Column(db.Text, nullable=False)       # Текст изменений
    created_at = db.Column(db.DateTime, default=datetime.now)

# --- Счета на оплату (Task System) ---
class PaymentInvoice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    
    filename = db.Column(db.String(255), nullable=False)  # Имя файла на диске
    original_name = db.Column(db.String(255), nullable=False)
    
    budget_item_id = db.Column(db.Integer, db.ForeignKey('budget_item.id'), nullable=True)
    amount = db.Column(db.Numeric(10, 2), default=0.0)
    due_date = db.Column(db.Date, nullable=True) # Срок оплаты
    
    priority = db.Column(db.String(20), default='normal') # high, normal, low
    comment = db.Column(db.String(500))
    status = db.Column(db.String(20), default='new') # new, paid
    
    item = db.relationship('BudgetItem')

# --- ВЫКОПКА (DIGGING) ---
class DiggingLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, default=datetime.now)
    created_at = db.Column(db.DateTime, default=datetime.now)
    
    # Если бригадир вводит выкопку без привязки к конкретному заказу,
    # сохраняем plant/size/field/year и оставляем order_item_id пустым.
    order_item_id = db.Column(db.Integer, db.ForeignKey('order_item.id'), nullable=True)
    plant_id = db.Column(db.Integer, db.ForeignKey('plant.id'), nullable=True)
    size_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=True)
    field_id = db.Column(db.Integer, db.ForeignKey('field.id'), nullable=True)
    year = db.Column(db.Integer, nullable=True)

    user_id = db.Column(db.Integer, db.ForeignKey('user.id')) # Кто внес (Бригадир)
    
    quantity = db.Column(db.Integer, nullable=False)
    
    # Статус записи: 
    # 'pending' - внес бригадир, ждет распределения/проверки
    # 'approved' - менеджер подтвердил (влияет на итоги)
    # 'rejected' - отклонено (ошибка)
    status = db.Column(db.String(20), default='pending') 
    
    item = db.relationship('OrderItem')
    plant = db.relationship('Plant')
    size = db.relationship('Size')
    field = db.relationship('Field')
    user = db.relationship('User')

# --- ПРОЕКТЫ (НОВЫЕ КЛАССЫ) ---
class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    description = db.Column(db.String(500))
    status = db.Column(db.String(20), default='active') # active, closed
    created_at = db.Column(db.DateTime, default=datetime.now)
    
    # Связи
    items = db.relationship('ProjectItem', backref='project', cascade="all, delete-orphan")
    expenses = db.relationship('Expense', backref='project_link') 
    orders = db.relationship('Order', backref='project')

    @property
    def total_expenses(self):
        return sum(e.amount for e in self.expenses)

    @property
    def total_revenue(self):
        # Сумма всех заказов, привязанных к проекту
        return sum(o.total_sum for o in self.orders if o.status != 'canceled')

    def get_economics(self):
        """
        Возвращает полную экономику проекта:
        - Выручка (План по заказам)
        - Себестоимость растений (Закупка + Накопленные за (Год-1))
        - Прямые расходы (Факт)
        - Прибыль и Рентабельность
        """
        from decimal import Decimal
        from sqlalchemy import func
        from app.models import Expense, StockBalance, Order
        # Локальный импорт сервиса, чтобы избежать ошибки circular import
        from app.services import calculate_cost_data 

        # 1. Выручка (Сумма активных заказов)
        revenue = sum(o.total_sum for o in self.orders if o.status != 'canceled')

        # 2. Прямые расходы (Total Fact)
        fact_expenses = sum(e.amount for e in self.expenses)

        # 3. Себестоимость растений
        plants_cost_total = Decimal(0)
        costs_cache = {} # Кэш для накопленных расходов по годам
        
        # Кэш цен закупки (чтобы не дергать базу 1000 раз)
        stock_prices = { 
            (sb.plant_id, sb.size_id, sb.field_id, sb.year): sb.purchase_price 
            for sb in StockBalance.query.all() 
        }

        for o in self.orders:
            if o.status == 'canceled': continue
            
            # ВАЖНО: Берем расходы за ПРЕДЫДУЩИЙ год от даты заказа
            calc_basis_year = o.date.year - 1
            
            if calc_basis_year not in costs_cache:
                # Считаем сложную математику через сервис
                c_data = calculate_cost_data(calc_basis_year)
                costs_cache[calc_basis_year] = c_data['accumulated_costs_map']
                
            for item in o.items:
                qty = Decimal(item.quantity) # Берем плановое количество (ведь выручка тоже плановая)
                
                # Цена закупки партии
                purch = stock_prices.get((item.plant_id, item.size_id, item.field_id, item.year), Decimal(0))
                
                # Накопленная себестоимость (уход) за прошлые годы
                accum = costs_cache[calc_basis_year].get(item.year, Decimal(0))
                
                plants_cost_total += (purch + accum) * qty

        # 4. Итоговые показатели
        profit = revenue - plants_cost_total - fact_expenses
        margin = (profit / revenue * 100) if revenue > 0 else 0
        
        # 5. Считаем распределение бюджета (для детализации)
        # Сумма расходов, которые удалось привязать к конкретным статьям бюджета проекта
        budgeted_fact = Decimal(0)
        for item in self.budget_items:
            # Сумма расходов по этой статье
            item_fact = sum(e.amount for e in self.expenses if e.project_budget_id == item.id)
            budgeted_fact += item_fact
            
        # Нераспределенный факт = Всего расходов - Расходы по статьям
        unallocated = fact_expenses - budgeted_fact

        return {
            'revenue': revenue,
            'plants_cost': plants_cost_total,
            'direct_expenses': fact_expenses,
            'profit': profit,
            'margin': margin,
            'unallocated_fact': unallocated
        }

class ProjectItem(db.Model):
    """Растения, включенные в проект (План производства/выращивания)"""
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'), nullable=False)
    plant_id = db.Column(db.Integer, db.ForeignKey('plant.id'), nullable=False)
    size_id = db.Column(db.Integer, db.ForeignKey('size.id'), nullable=True)
    quantity = db.Column(db.Integer, default=0)
    comment = db.Column(db.String(200))
    
    plant = db.relationship('Plant')
    size = db.relationship('Size')

class ProjectBudget(db.Model):
    """Плановые расходы по проекту (Бюджет)"""
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'), nullable=False)
    name = db.Column(db.String(150), nullable=False) # Название статьи (напр. "Логистика")
    amount = db.Column(db.Numeric(10, 2), default=0.0) # Плановая сумма
    
    project_rel = db.relationship('Project', backref=db.backref('budget_items', cascade="all, delete-orphan"))

class BlockedIP(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ip_address = db.Column(db.String(50), unique=True, nullable=False)
    failed_attempts = db.Column(db.Integer, default=0)
    locked_at = db.Column(db.DateTime, nullable=True)

# --- НОВОЕ: ЗАДАНИЯ НА ВЫКОПКУ (Для ленты) ---
class DiggingTask(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_item_id = db.Column(db.Integer, db.ForeignKey('order_item.id'), nullable=False)
    planned_date = db.Column(db.Date, nullable=False)
    planned_qty = db.Column(db.Integer, nullable=False)
    comment = db.Column(db.String(500))
    status = db.Column(db.String(20), default='pending') # pending, done
    created_at = db.Column(db.DateTime, default=datetime.now)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'))

    item = db.relationship('OrderItem', backref=db.backref('digging_tasks', lazy=True))
    created_by = db.relationship('User')

# --- ЗАДАЧИ ИЗ TELEGRAM (AI ПАРСИНГ) ---
class TgTask(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.now)
    raw_text = db.Column(db.Text, nullable=False) # Оригинальное сообщение
    title = db.Column(db.String(200)) # Суть задачи
    details = db.Column(db.Text) # Детали, извлеченные AI
    
    assignee_role = db.Column(db.String(50)) # Роль (если задача на отдел)
    assignee_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True) # Конкретный исполнитель
    deadline = db.Column(db.Date, nullable=True) # Дедлайн
    
    action_type = db.Column(db.String(50)) # 'create_order', 'digging', 'info'
    action_payload = db.Column(db.Text) # JSON с параметрами
    status = db.Column(db.String(20), default='new') # new, done
    sender_name = db.Column(db.String(100)) # Имя руководителя в ТГ

    # Для аналитики по поручениям — проставляется при смене статуса на 'done'.
    completed_at = db.Column(db.DateTime, nullable=True)
    completed_by_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)

    # Ключ дедупликации для авто-аномалий и дайджестов. Стабильный между запусками.
    # Пример: 'debt_new_order:client=128' или 'ready_stale:order=217'.
    dedup_key = db.Column(db.String(255), nullable=True, index=True)
    # Временные метки для отслеживания «как давно длится» и «исчезла ли аномалия».
    first_seen_at = db.Column(db.DateTime, nullable=True)
    last_seen_at = db.Column(db.DateTime, nullable=True)
    severity = db.Column(db.String(20), default='info')  # info / warning / danger

    # --- Расширение жизненного цикла задачи (v2) ---
    # Автор задачи: для ручного создания = current_user, для AI = None (пришло из чата).
    created_by_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    # Последняя модификация (reassign / complete). Для сортировки в аналитике.
    updated_at = db.Column(db.DateTime, nullable=True)
    # История «передачи» на один шаг назад: кто и когда передал задачу.
    # Для полноценного audit-log позже можно завести отдельную таблицу.
    reassigned_from_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    reassigned_at = db.Column(db.DateTime, nullable=True)
    # Источник задачи — для фильтров в аналитике:
    #   'tg'       — AI распарсил сообщение из Telegram;
    #   'manual'   — руководитель/менеджер создал через UI;
    #   'anomaly'  — автокарточка из anomaly_engine;
    #   'digest'   — еженедельный дайджест;
    #   'fallback' — сбой AI, оставили «сырую» карточку.
    source = db.Column(db.String(20), nullable=True)

    assignee = db.relationship('User', foreign_keys=[assignee_id])
    completed_by = db.relationship('User', foreign_keys=[completed_by_id])
    created_by = db.relationship('User', foreign_keys=[created_by_id])
    reassigned_from = db.relationship('User', foreign_keys=[reassigned_from_id])


class WeeklyDigest(db.Model):
    """История еженедельных дайджестов для admin/executive.

    Один дайджест = одна неделя (понедельник) × один пользователь.
    Хранит и готовый HTML (для быстрого показа), и структурированные числа
    (summary_json) — чтобы можно было сравнить с прошлой неделей.
    """
    id = db.Column(db.Integer, primary_key=True)
    week_start = db.Column(db.Date, nullable=False, index=True)  # понедельник недели
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    content_html = db.Column(db.Text, nullable=False)
    summary_json = db.Column(db.Text)  # сериализованный словарь с метриками
    created_at = db.Column(db.DateTime, default=datetime.now)
    user = db.relationship('User')
    __table_args__ = (
        db.UniqueConstraint('week_start', 'user_id', name='_digest_week_user_uc'),
    )


# --- TG-мониторинг чата расходов (app/expense_chat.py) ---
# Каждое сообщение из чата «Расходы Жемчужниково» сохраняем здесь, чтобы
# (а) исключить двойную обработку при ретраях webhook,
# (б) иметь готовое состояние для карточки на дашборде,
# (в) собрать обучающую выборку {raw_text -> budget_item_id} для авто-режима.
class ChatExpenseMessage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tg_chat_id = db.Column(db.String(64), nullable=False)
    tg_message_id = db.Column(db.Integer, nullable=False)
    tg_date = db.Column(db.DateTime, nullable=True)  # время сообщения в МСК
    raw_text = db.Column(db.Text, nullable=False)
    sender_name = db.Column(db.String(150), nullable=True)

    parsed_amount = db.Column(db.Numeric(12, 2), nullable=True)
    parsed_description = db.Column(db.String(500), nullable=True)
    # 'cash' | 'cashless' | None
    parsed_payment_type = db.Column(db.String(20), nullable=True)

    # Подсказка классификатора до подтверждения админом (может меняться).
    suggested_budget_item_id = db.Column(
        db.Integer, db.ForeignKey('budget_item.id'), nullable=True
    )
    # 'pending' | 'matched' | 'imported' | 'rejected' | 'unparseable'
    status = db.Column(db.String(20), nullable=False, default='pending', index=True)
    # Создан ли по факту Expense и какой TgTask сейчас висит на эту карточку.
    expense_id = db.Column(db.Integer, db.ForeignKey('expense.id'), nullable=True)
    task_id = db.Column(db.Integer, db.ForeignKey('tg_task.id'), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.now)
    updated_at = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)

    suggested_item = db.relationship('BudgetItem', foreign_keys=[suggested_budget_item_id])
    expense = db.relationship('Expense', foreign_keys=[expense_id])
    task = db.relationship('TgTask', foreign_keys=[task_id])

    __table_args__ = (
        db.UniqueConstraint('tg_chat_id', 'tg_message_id', name='_tg_chat_msg_uc'),
        db.Index('idx_chat_expense_status', 'status'),
    )


class ChatExpenseAlias(db.Model):
    """Обучение классификатора: запомненные связки «короткий ключ описания»
    → статья бюджета. Заполняется при каждом подтверждении админа в фиде.

    `alias_key` — нормализованные первые ~3-4 значимых слова описания
    (lower, без знаков препинания). Индекс по нему.
    """
    id = db.Column(db.Integer, primary_key=True)
    alias_key = db.Column(db.String(200), nullable=False, index=True)
    budget_item_id = db.Column(
        db.Integer, db.ForeignKey('budget_item.id'), nullable=False
    )
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)
    hit_count = db.Column(db.Integer, default=1)
    last_used_at = db.Column(db.DateTime, default=datetime.now)
    created_at = db.Column(db.DateTime, default=datetime.now)

    budget_item = db.relationship('BudgetItem')
    created_by = db.relationship('User')

    __table_args__ = (
        db.UniqueConstraint('alias_key', 'budget_item_id', name='_alias_item_uc'),
    )
