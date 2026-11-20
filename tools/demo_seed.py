# -*- coding: utf-8 -*-
"""Локальная demo-БД ERP: пустая схема + синтетика за выбранный год.

Не читает nursery.db / прод. Структура статей — типичный питомник
(как у прода), имена клиентов/сотрудников и суммы — случайные.

Примеры:
  python tools/demo_seed.py --reset
  python tools/demo_seed.py --reset --year 2026 --seed 42
"""
from __future__ import annotations

import argparse
import calendar
import os
import random
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEMO_DB = ROOT / 'demo_nursery.db'

# --- справочники (анонимные, без копирования прод-имён) ---

BUDGET_ITEMS = [
    # code, name, is_amortization, is_vium_source, is_container_purchase
    ('1', 'Отсыпка дороги / щебень', True, False, False),
    ('1.1', 'Ограждение погрузочной площадки', True, False, False),
    ('2', 'Офис: интернет, расходники', True, False, False),
    ('2.1', 'Косметический ремонт офиса', True, False, False),
    ('3', 'Растения С3 / ОКС', False, False, False),
    ('4', 'Электроинструмент', False, False, False),
    ('4.1', 'Ручной инструмент', True, False, False),
    ('5', 'Грядки под посев', True, False, False),
    ('5.1', 'Капельный полив', False, False, False),
    ('5.2', 'Магистральные трубы', False, False, False),
    ('5.3', 'Насос: покупка и ремонт', True, False, False),
    ('6', 'Торф / субстрат', False, False, False),
    ('6.1', 'Удобрения и пестициды', False, False, False),
    ('6.2', 'Сетка, мешковина', False, False, False),
    ('7', 'Хозрасходы', False, False, False),
    ('7.1', 'ГСМ и транспорт', False, False, False),
    ('8', 'Мотоблок / техника', True, False, False),
    ('8.1', 'Ремонт дизельной техники', True, False, False),
    ('8.2', 'Ремонт бензиновой техники', True, False, False),
    ('9', 'Электричество', False, False, False),
    ('9.1', 'Увеличение мощности', True, False, False),
    ('10', 'Зарплата менеджер', False, False, False),
    ('10.1', 'Зарплата бухгалтер', False, False, False),
    ('10.2', 'Зарплата руководитель', False, False, False),
    ('10.3', 'Зарплата рабочие (неофиц.)', False, False, False),
    ('10.4', 'Зарплата рабочие (офиц.)', False, False, False),
    ('10.5', 'Зарплата руководитель (офиц.)', False, False, False),
    ('11', 'Налоги рабочие', False, False, False),
    ('11.1', 'Налоги общие', False, False, False),
    ('11.2', 'Оформление документов', False, False, False),
    ('12', 'Найм техники', False, False, False),
    ('13', 'ЧОП / охрана', False, False, False),
    ('14', 'Представительские', False, False, False),
    ('15', 'Контейнеры', False, True, True),
    ('UNASSIGNED', 'К разнесению (без статьи)', False, False, False),
]

# Веса статей расходов (без ЗП) — rough like prod share
EXPENSE_WEIGHTS = {
    '7': 18, '7.1': 12, '12': 10, '6.1': 8, '6.2': 6, '6': 5,
    '9': 8, '2': 7, '5.1': 6, '3': 5, '11': 6, '11.1': 3,
    '13': 4, '14': 3, '15': 4, '8.1': 3, '8.2': 3, '4': 2, '4.1': 2,
}

# Сезонность расходов по месяцам (1..12), относительно «среднего»
MONTH_EXPENSE_FACTOR = {
    1: 0.35, 2: 0.40, 3: 0.55, 4: 1.35, 5: 1.55, 6: 1.45,
    7: 1.20, 8: 1.10, 9: 1.25, 10: 0.90, 11: 1.40, 12: 0.70,
}
MONTH_ORDER_FACTOR = {
    1: 0.15, 2: 0.20, 3: 0.45, 4: 1.20, 5: 1.60, 6: 1.40,
    7: 0.90, 8: 0.70, 9: 1.10, 10: 1.30, 11: 0.80, 12: 0.35,
}

PLANTS = [
    ('Туя западная «Smaragd»', 'Thuja occidentalis'),
    ('Туя западная «Brabant»', 'Thuja occidentalis'),
    ('Ель обыкновенная', 'Picea abies'),
    ('Ель колючая «Glauca»', 'Picea pungens'),
    ('Сосна горная', 'Pinus mugo'),
    ('Можжевельник казацкий', 'Juniperus sabina'),
    ('Можжевельник скальный «Blue Arrow»', 'Juniperus scopulorum'),
    ('Гортензия метельчатая «Limelight»', 'Hydrangea paniculata'),
    ('Гортензия метельчатая «Vanilla Fraze»', 'Hydrangea paniculata'),
    ('Гортензия древовидная «Annabelle»', 'Hydrangea arborescens'),
    ('Спирея японская', 'Spiraea japonica'),
    ('Барбарис Тунберга', 'Berberis thunbergii'),
    ('Дерен белый «Elegantissima»', 'Cornus alba'),
    ('Пузыреплодник калинолистный', 'Physocarpus opulifolius'),
    ('Кизильник блестящий', 'Cotoneaster lucidus'),
    ('Боярышник сливолистный', 'Crataegus × prunifolia'),
    ('Рябина обыкновенная', 'Sorbus aucuparia'),
    ('Клён остролистный', 'Acer platanoides'),
    ('Липа мелколистная', 'Tilia cordata'),
    ('Берёза повислая', 'Betula pendula'),
    ('Ива белая', 'Salix alba'),
    ('Сирень обыкновенная', 'Syringa vulgaris'),
    ('Чубушник венечный', 'Philadelphus coronarius'),
    ('Лапчатка кустарниковая', 'Potentilla fruticosa'),
    ('Вейгела цветущая', 'Weigela florida'),
    ('Форзиция промежуточная', 'Forsythia × intermedia'),
    ('Роза морщинистая', 'Rosa rugosa'),
    ('Самшит вечнозелёный', 'Buxus sempervirens'),
    ('Тисс ягодный', 'Taxus baccata'),
    ('Пихта бальзамическая', 'Abies balsamea'),
    ('Лиственница европейская', 'Larix decidua'),
    ('Калина обыкновенная', 'Viburnum opulus'),
    ('Жимолость татарская', 'Lonicera tatarica'),
    ('Снежноягодник белый', 'Symphoricarpos albus'),
    ('Арония черноплодная', 'Aronia melanocarpa'),
    ('Ирга канадская', 'Amelanchier canadensis'),
    ('Яблоня декоративная', 'Malus hybrid'),
    ('Груша уссурийская', 'Pyrus ussuriensis'),
    ('Вишня войлочная', 'Prunus tomentosa'),
    ('Слива растопыренная', 'Prunus cerasifera'),
]

SIZES = [
    '40-60', '60-80', '80-100', '100-120', '120-140', '140-160',
    '160-180', '180-200', '200-225', '225-250', '250-275', '275-300',
    'C3', 'C5', 'C7.5', 'C10', 'C15', 'C20', 'C25', 'C35',
]

RU_MONTHS = [
    '', 'январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
    'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь',
]

FIRST_NAMES = [
    'Алексей', 'Дмитрий', 'Сергей', 'Иван', 'Андрей', 'Николай', 'Павел',
    'Михаил', 'Роман', 'Артём', 'Егор', 'Кирилл', 'Максим', 'Олег',
    'Анна', 'Мария', 'Елена', 'Ольга', 'Наталья', 'Ирина',
]
LAST_NAMES = [
    'Иванов', 'Петров', 'Сидоров', 'Козлов', 'Новиков', 'Морозов',
    'Волков', 'Соколов', 'Лебедев', 'Ковалев', 'Смирнов', 'Кузнецов',
    'Попов', 'Васильев', 'Михайлов', 'Фёдоров', 'Орлов', 'Белов',
]

CLIENT_ORGS = [
    'ООО «Зелёный Квартал»', 'ООО «Ландшафт Про»', 'ИП Соколов А.В.',
    'ООО «Сады Мещеры»', 'ООО «ПаркСтрой»', 'ИП Орлова Е.Н.',
    'ООО «ДендроСнаб»', 'ООО «Аллея»', 'ООО «НордГрин»',
    'ИП Белов К.С.', 'ООО «ЭкоДвор»', 'ООО «ФлораТрейд»',
    'ООО «ГринЛайн»', 'ИП Кузнецова М.А.', 'ООО «Питомник Партнёр»',
    'ООО «СитиПлант»', 'ООО «Хвойный Дом»', 'ООО «МегаЛандшафт»',
    'ИП Волков Д.И.', 'ООО «Верста Садовая»',
]

SUPPLIERS = [
    'ООО «ТорфСнаб»', 'ООО «АгроХим»', 'ИП РемонтТех',
    'ООО «КонтейнерМаркет»', 'ООО «СеткаПлюс»',
]


def _money(x) -> Decimal:
    return Decimal(str(round(float(x), 2)))


def _configure_env(db_path: Path) -> None:
    uri = 'sqlite:///' + str(db_path.resolve()).replace('\\', '/')
    os.environ['DATABASE_URL'] = uri
    os.environ['DISABLE_SCHEDULER'] = '1'
    os.environ['DISABLE_AI_SEED'] = '1'
    os.environ['TG_USE_POLLING'] = '0'
    os.environ['DEMO_MODE'] = '1'
    os.environ['FLASK_ENV'] = 'development'
    os.environ['SECRET_KEY'] = 'demo-floraflow-client-preview'
    # Не цепляемся к прод-Telegram / AI
    for key in (
        'TG_BOT_TOKEN', 'TG_WEBHOOK_URL', 'GROQ_API_KEY', 'GEMINI_API_KEY',
        'AMVERA', 'ERP_PUBLIC_TOKEN',
    ):
        os.environ.pop(key, None)


def _unlink_db(db_path: Path) -> None:
    for p in (db_path, Path(str(db_path) + '-wal'), Path(str(db_path) + '-shm')):
        if p.exists():
            p.unlink()
            print(f'  removed {p.name}')


def _person(rng: random.Random) -> str:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    female = first in ('Анна', 'Мария', 'Елена', 'Ольга', 'Наталья', 'Ирина')
    if female and last.endswith(('ов', 'ев', 'ин')):
        last = last + 'а'
    return f'{last} {first}'


def seed_all(year: int, rng: random.Random) -> dict:
    from app.models import (
        AppSetting, BudgetItem, BudgetPlan, CashflowPlan, CashHolder, CashMove,
        Client, Employee, Expense, Field, Order, OrderItem, Payment, Plant,
        SalaryRate, Size, StockBalance, Supplier, TimeLog, User, db,
    )
    from app.expense_splits import apply_budget_lines

    stats = {}

    # --- users (все роли для демо клиенту) ---
    demo_users = (
        ('demo', 'admin', 'demo2026'),
        ('boss', 'executive', 'boss2026'),
        ('manager', 'user', 'manager2026'),
        ('sales', 'shop_manager', 'sales2026'),
        ('hr', 'user2', 'hr2026'),
        ('brigadier', 'brigadier', 'brig2026'),
    )
    users_by_role: dict[str, User] = {}
    for username, role, password in demo_users:
        u = User.query.filter_by(username=username).first()
        if u is None:
            u = User(username=username, role=role)
            u.set_password(password)
            db.session.add(u)
        else:
            u.role = role
            u.set_password(password)
        users_by_role[role] = u
    db.session.flush()
    admin_user = users_by_role['admin']
    manager_user = users_by_role['user']
    sales_user = users_by_role['shop_manager']
    exec_user = users_by_role['executive']
    brig_user = users_by_role['brigadier']
    stats['users'] = len(demo_users)

    # --- budget ---
    by_code: dict[str, BudgetItem] = {}
    for code, name, amort, vium, cont in BUDGET_ITEMS:
        item = BudgetItem.query.filter_by(code=code).first()
        if item is None:
            item = BudgetItem(
                code=code, name=name,
                is_amortization=amort,
                is_vium_source=vium,
                is_container_purchase=cont,
            )
            db.session.add(item)
        else:
            item.name = name
            item.is_amortization = amort
            item.is_vium_source = vium
            item.is_container_purchase = cont
        by_code[code] = item
    db.session.flush()
    stats['budget_items'] = len(by_code)

    unofficial = by_code['10.3']
    official = by_code['10.4']
    for key, item in (
        ('hr_unofficial_item', unofficial),
        ('hr_official_item', official),
    ):
        row = AppSetting.query.get(key)
        if row is None:
            db.session.add(AppSetting(key=key, value=str(item.id)))
        else:
            row.value = str(item.id)
    other = f"{by_code['10'].id},{by_code['10.2'].id}"
    row = AppSetting.query.get('hr_other_salary_items')
    if row is None:
        db.session.add(AppSetting(key='hr_other_salary_items', value=other))
    else:
        row.value = other

    header = (
        '{"company":"Демо Питомник «ГринВью»","inn":"7700000000",'
        '"phone":"+7 (900) 000-00-00","city":"Московская обл."}'
    )
    ph = AppSetting.query.get('print_header_settings')
    if ph is None:
        db.session.add(AppSetting(key='print_header_settings', value=header))
    else:
        ph.value = header

    # --- plants / sizes / fields / clients ---
    plants = []
    for name, latin in PLANTS:
        p = Plant.query.filter_by(name=name).first()
        if p is None:
            p = Plant(name=name, latin_name=latin, characteristic='')
            db.session.add(p)
        plants.append(p)
    db.session.flush()

    sizes = []
    for name in SIZES:
        s = Size.query.filter_by(name=name).first()
        if s is None:
            s = Size(name=name)
            db.session.add(s)
        sizes.append(s)
    db.session.flush()

    fields = []
    for i in range(1, 21):
        fname = f'Поле {i}'
        f = Field.query.filter_by(name=fname).first()
        if f is None:
            f = Field(name=fname, planting_year=2018 + (i % 6))
            db.session.add(f)
        fields.append(f)
    yards = []
    for n in (1, 2):
        yname = f'Контейнерная площадка {n}'
        y = Field.query.filter_by(name=yname).first()
        if y is None:
            y = Field(name=yname, planting_year=year - (n - 1))
            db.session.add(y)
        yards.append(y)
    db.session.flush()
    yard1, yard2 = yards[0], yards[1]
    fields.extend(yards)

    clients = []
    for name in CLIENT_ORGS:
        c = Client.query.filter_by(name=name).first()
        if c is None:
            c = Client(
                name=name,
                inn=str(rng.randint(7700000000, 7799999999)),
                phone=f'+7 9{rng.randint(10, 99)} {rng.randint(100, 999)}-{rng.randint(10, 99)}-{rng.randint(10, 99)}',
                address=f'г. Демоград, ул. Садовая, д. {rng.randint(1, 80)}',
            )
            db.session.add(c)
        clients.append(c)
    db.session.flush()
    stats['clients'] = len(clients)

    for name in SUPPLIERS:
        if Supplier.query.filter_by(name=name).first() is None:
            db.session.add(Supplier(name=name))
    db.session.flush()

    # --- employees ---
    employees: list[Employee] = []
    roles_plan = (
        [('manager', True, 120000)] * 2
        + [('brigadier', False, 0)]
        + [('mechanic', True, 90000)]
        + [('worker', False, 0)] * 8
    )
    used_names: set[str] = set()
    for role, is_salary, fixed in roles_plan:
        for _ in range(20):
            name = _person(rng)
            if name not in used_names:
                used_names.add(name)
                break
        emp = Employee(
            name=name,
            role=role,
            is_salary=is_salary,
            fixed_salary=_money(fixed) if is_salary else _money(0),
            is_active=True,
            unofficial_budget_item_id=unofficial.id,
            official_budget_item_id=official.id if is_salary else None,
        )
        db.session.add(emp)
        employees.append(emp)
    db.session.flush()
    stats['employees'] = len(employees)
    workers = [e for e in employees if e.role == 'worker']
    managers = [e for e in employees if e.role == 'manager']
    boss = next(e for e in employees if e.role == 'brigadier')

    # --- salary rates ---
    rate_rows = [
        ('worker', 'norm', 250), ('worker', 'norm_over', 300),
        ('worker', 'spec', 300), ('worker', 'spec_over', 360),
        ('brigadier', 'norm', 350), ('brigadier', 'norm_over', 420),
        ('brigadier', 'spec', 420), ('brigadier', 'spec_over', 500),
        ('mechanic', 'norm', 2500), ('mechanic', 'norm_over', 0),
        ('mechanic', 'spec', 0), ('mechanic', 'spec_over', 0),
        ('manager', 'norm', 0), ('manager', 'norm_over', 0),
        ('manager', 'spec', 0), ('manager', 'spec_over', 0),
        ('assistant', 'norm', 0), ('assistant', 'norm_over', 0),
        ('assistant', 'spec', 0), ('assistant', 'spec_over', 0),
    ]
    for role, rtype, val in rate_rows:
        row = SalaryRate.query.filter_by(year=year, role=role, rate_type=rtype).first()
        if row is None:
            db.session.add(SalaryRate(
                year=year, role=role, rate_type=rtype, rate_value=_money(val),
            ))
        else:
            row.rate_value = _money(val)

    # --- budget + cashflow plans ---
    monthly_cash_in = {
        1: 400_000, 2: 500_000, 3: 900_000, 4: 2_200_000, 5: 3_500_000,
        6: 3_200_000, 7: 2_000_000, 8: 1_600_000, 9: 2_400_000, 10: 2_800_000,
        11: 1_800_000, 12: 700_000,
    }
    plan_codes = [c for c in EXPENSE_WEIGHTS]
    plan_codes += ['10', '10.1', '10.2', '10.3', '10.4', '1', '5', '8']
    for month in range(1, 13):
        factor = MONTH_EXPENSE_FACTOR[month]
        for code in plan_codes:
            item = by_code[code]
            base = {
                '10.3': 1_400_000, '10': 100_000, '10.1': 80_000, '10.2': 130_000,
                '10.4': 350_000, '5.1': 400_000, '7': 250_000, '6.1': 200_000,
            }.get(code, 80_000)
            amt = _money(base * factor * rng.uniform(0.75, 1.25))
            bp = BudgetPlan.query.filter_by(
                year=year, budget_item_id=item.id, month=month,
            ).first()
            if bp is None:
                db.session.add(BudgetPlan(
                    year=year, budget_item_id=item.id, month=month, amount=amt,
                ))
            else:
                bp.amount = amt
        cin = _money(monthly_cash_in[month] * rng.uniform(0.85, 1.15))
        cf = CashflowPlan.query.filter_by(year=year, month=month).first()
        if cf is None:
            db.session.add(CashflowPlan(year=year, month=month, amount=cin))
        else:
            cf.amount = cin

    # --- stock (поля + 2 контейнерные площадки с ассортиментом) ---
    stock_n = 0
    field_sizes = [s for s in sizes if not s.name.startswith('C')]
    container_sizes = [s for s in sizes if s.name.startswith('C')] or sizes[:6]

    def _add_stock(field, plant, size, qty=None):
        nonlocal stock_n
        existing = StockBalance.query.filter_by(
            plant_id=plant.id, size_id=size.id, field_id=field.id, year=year,
        ).first()
        if existing:
            return
        qty = qty if qty is not None else rng.randint(5, 120)
        price = _money(rng.choice([450, 650, 900, 1200, 1800, 2500, 3500, 5000]))
        purchase = _money(float(price) * rng.uniform(0.35, 0.55))
        db.session.add(StockBalance(
            plant_id=plant.id, size_id=size.id, field_id=field.id, year=year,
            quantity=qty, price=price, purchase_price=purchase,
            current_total_cost=_money(float(purchase) * qty),
        ))
        stock_n += 1

    for field in fields[:12]:
        for _ in range(rng.randint(8, 16)):
            _add_stock(field, rng.choice(plants), rng.choice(field_sizes or sizes))

    # Площадки 1–2: плотный ассортимент в контейнерах
    for yard, dens in ((yard1, 28), (yard2, 22)):
        for plant in rng.sample(plants, k=min(dens, len(plants))):
            for size in rng.sample(container_sizes, k=min(3, len(container_sizes))):
                _add_stock(yard, plant, size, qty=rng.randint(8, 90))
    db.session.flush()
    stats['stock_rows'] = stock_n
    stats['container_yards'] = 2

    # --- time logs (workdays) ---
    tl_n = 0
    hourly = [e for e in employees if e.role in ('worker', 'brigadier')]
    for month in range(1, 13):
        days = calendar.monthrange(year, month)[1]
        for day in range(1, days + 1):
            d = date(year, month, day)
            if d.weekday() >= 6:  # вс
                continue
            for emp in hourly:
                if rng.random() < 0.08:
                    continue
                hours = 8.0 if d.weekday() < 5 else 6.0
                if rng.random() < 0.12:
                    hours += rng.choice([1.0, 2.0])
                db.session.add(TimeLog(
                    employee_id=emp.id,
                    date=d,
                    hours_norm=hours,
                    hours_norm_over=max(0.0, hours - 8.0),
                    hours_spec=0.0,
                    hours_spec_over=0.0,
                    is_day_off=False,
                ))
                tl_n += 1
    stats['time_logs'] = tl_n

    # --- expenses ---
    exp_n = 0
    weighted = []
    for code, w in EXPENSE_WEIGHTS.items():
        weighted.extend([code] * w)

    def add_expense(d: date, item: BudgetItem, amount, desc: str, *,
                    payment_type='cashless', employee=None,
                    target_month=None, target_year=None):
        nonlocal exp_n
        amount = _money(amount)
        if amount <= 0:
            return
        ex = Expense(
            date=d,
            budget_item_id=item.id,
            description=desc[:500],
            amount=amount,
            payment_type=payment_type,
            employee_id=employee.id if employee else None,
            target_month=target_month,
            target_year=target_year,
            created_at=datetime.combine(d, datetime.min.time()) + timedelta(hours=10),
        )
        db.session.add(ex)
        db.session.flush()
        apply_budget_lines(ex, [{'budget_item_id': item.id, 'amount': amount}])
        exp_n += 1

    for month in range(1, 13):
        factor = MONTH_EXPENSE_FACTOR[month]
        n_ops = int(18 * factor + rng.randint(-3, 5))
        n_ops = max(8, n_ops)
        days = calendar.monthrange(year, month)[1]
        for _ in range(n_ops):
            d = date(year, month, rng.randint(1, days))
            code = rng.choice(weighted)
            item = by_code[code]
            if code == '7.1':
                amt = rng.uniform(3_000, 25_000)
                desc = rng.choice(['ГСМ', 'Компенсация ГСМ', 'Транспорт / доставка'])
            elif code in ('6.1', '6', '6.2', '15', '3'):
                amt = rng.uniform(15_000, 280_000)
                desc = item.name
            elif code == '9':
                amt = rng.uniform(25_000, 95_000)
                desc = f'Электричество за {month:02d}.{year}'
            elif code == '13':
                amt = rng.uniform(40_000, 90_000)
                desc = 'Охрана ЧОП'
            else:
                amt = rng.uniform(5_000, 120_000) * (0.6 + 0.8 * factor)
                desc = item.name
            pay = 'cash' if rng.random() < 0.35 else 'cashless'
            add_expense(d, item, amt * factor ** 0.3, desc, payment_type=pay)

        # ЗП выплаты раз в месяц
        pay_day = min(10, days)
        d_pay = date(year, month, pay_day)
        for emp in workers:
            hours_est = 160 * rng.uniform(0.85, 1.05)
            rate = 250
            amt = hours_est * rate * rng.uniform(0.95, 1.05)
            add_expense(
                d_pay, unofficial, amt,
                f'ЗП {emp.name} (за {RU_MONTHS[month]} {year})',
                payment_type='cash', employee=emp,
                target_month=month, target_year=year,
            )
        for emp in managers:
            add_expense(
                d_pay, by_code['10'], float(emp.fixed_salary) * rng.uniform(0.95, 1.0),
                f'ЗП {emp.name} (за {month:02d}.{year})',
                payment_type='cashless', employee=emp,
                target_month=month, target_year=year,
            )
        add_expense(
            d_pay, by_code['10.2'], rng.uniform(90_000, 160_000),
            f'ЗП руководитель (за {month:02d}.{year})',
            payment_type='cash', employee=boss,
            target_month=month, target_year=year,
        )
        add_expense(
            d_pay, by_code['10.1'], rng.uniform(60_000, 95_000),
            f'ЗП бухгалтер (за {month:02d}.{year})',
            payment_type='cashless',
            target_month=month, target_year=year,
        )
        # иногда смешанная «ЗП + ГСМ»
        if rng.random() < 0.35:
            emp = rng.choice(workers)
            gsm = by_code['7.1']
            add_expense(
                date(year, month, min(15, days)), gsm, rng.uniform(4_000, 12_000),
                f'Компенсация ГСМ {emp.name}',
                payment_type='cash',
            )

    stats['expenses'] = exp_n

    # --- orders + payments ---
    order_n = 0
    pay_n = 0
    stock_pairs = StockBalance.query.filter(StockBalance.quantity > 0).all()
    if not stock_pairs:
        stock_pairs = []

    for month in range(1, 13):
        n_orders = max(2, int(12 * MONTH_ORDER_FACTOR[month] + rng.randint(-2, 3)))
        days = calendar.monthrange(year, month)[1]
        for _ in range(n_orders):
            day = rng.randint(1, days)
            odt = datetime(year, month, day, rng.randint(9, 17), rng.randint(0, 59))
            client = rng.choice(clients)
            status = rng.choices(
                ['shipped', 'ready', 'in_progress', 'reserved', 'canceled'],
                weights=[45, 15, 15, 20, 5],
            )[0]
            order = Order(
                date=odt,
                client_id=client.id,
                status=status,
                invoice_number=f'Д-{year}-{order_n + 1:04d}',
                invoice_date=odt.date(),
            )
            db.session.add(order)
            db.session.flush()
            n_lines = rng.randint(1, 5)
            for __ in range(n_lines):
                if stock_pairs and rng.random() < 0.7:
                    sb = rng.choice(stock_pairs)
                    plant_id, size_id, field_id = sb.plant_id, sb.size_id, sb.field_id
                    price = sb.price or _money(1200)
                else:
                    plant_id = rng.choice(plants).id
                    size_id = rng.choice(sizes).id
                    field_id = rng.choice(fields[:12]).id
                    price = _money(rng.choice([500, 800, 1200, 1800, 2500, 4000]))
                qty = rng.randint(3, 40)
                shipped = qty if status == 'shipped' else (
                    rng.randint(0, qty) if status in ('ready', 'in_progress') else 0
                )
                dug = qty if status in ('shipped', 'ready') else (
                    rng.randint(0, qty) if status == 'in_progress' else 0
                )
                db.session.add(OrderItem(
                    order_id=order.id,
                    plant_id=plant_id,
                    size_id=size_id,
                    field_id=field_id,
                    year=year,
                    price=price,
                    quantity=qty,
                    shipped_quantity=shipped,
                    dug_quantity=dug,
                ))
            db.session.flush()
            total = float(order.total_sum or 0)
            if status != 'canceled' and total > 0:
                paid_ratio = rng.choice([1.0, 1.0, 1.0, 0.5, 0.0])
                if paid_ratio > 0:
                    db.session.add(Payment(
                        order_id=order.id,
                        date=odt.date() + timedelta(days=rng.randint(0, 20)),
                        amount=_money(total * paid_ratio),
                        payment_type=rng.choice(['cashless', 'cashless', 'cash']),
                        comment='Демо-оплата',
                    ))
                    pay_n += 1
            order_n += 1
    stats['orders'] = order_n
    stats['payments'] = pay_n

    # --- cash desk ---
    holder = CashHolder.query.filter_by(name='Касса демо').first()
    if holder is None:
        holder = CashHolder(name='Касса демо', opening_balance=_money(50_000), is_active=True)
        db.session.add(holder)
        db.session.flush()
    for month in (3, 5, 7, 9, 11):
        db.session.add(CashMove(
            holder_id=holder.id,
            kind='payout',
            amount=_money(rng.uniform(20_000, 80_000)),
            move_date=date(year, month, 5),
            source_kind='topup',
            note='Пополнение кассы (демо)',
        ))
        db.session.add(CashMove(
            holder_id=holder.id,
            kind='receipt',
            amount=_money(rng.uniform(8_000, 35_000)),
            move_date=date(year, month, 18),
            source_kind='spend',
            note='Расход из кассы (демо)',
        ))

    extra = _seed_demo_extras(
        year=year,
        rng=rng,
        plants=plants,
        sizes=sizes,
        fields=fields[:12],
        yards=[yard1, yard2],
        field_sizes=field_sizes or sizes,
        container_sizes=container_sizes,
        clients=clients,
        by_code=by_code,
        admin_user=admin_user,
        manager_user=manager_user,
        sales_user=sales_user,
        exec_user=exec_user,
        brig_user=brig_user,
    )
    stats.update(extra)

    db.session.commit()
    return stats


def _seed_demo_extras(
    *,
    year: int,
    rng: random.Random,
    plants,
    sizes,
    fields,
    yards,
    field_sizes,
    container_sizes,
    clients,
    by_code,
    admin_user,
    manager_user,
    sales_user,
    exec_user,
    brig_user,
) -> dict:
    """Дашборд, журнал, счета, выкопка, ВиУМ, рынок — без прод-данных."""
    import json
    from app.models import (
        AppSetting, ChatExpenseAlias, ChatExpenseMessage, CompetitorRow,
        CompetitorSnapshot, DiggingCalendarMark, DiggingLog, DiggingTask,
        Document, DocumentRow, Employee, Expense, OpsActionDraft, Order,
        OrderItem, PaymentInvoice, Project, ProjectBudget, ProjectItem,
        ProjectPottingDayMeta, ProjectPottingLog, SaleCompany, SaleInvoice,
        SaleInvoiceLine, SalesChatDigestLog, ShipmentPlan, StockBalance,
        Supplier, TgTask, ViumLot, ViumMaterial, ViumOperation,
        ViumOperationLine, ViumPurchase, ViumPurchaseLine, ViumTechCardLine,
        WeeklyDigest, db,
    )
    from app.expense_splits import apply_budget_lines

    out = {}
    # Реальная «сегодня» (MSK) — иначе дедлайны уезжают в свёрнутую «Просрочено».
    try:
        from app.utils import msk_today
        today = msk_today()
    except Exception:
        today = date.today()
    if today.year != year:
        # сид за другой год — якорим на середину сезона
        today = date(year, 11, 15)
    suppliers = Supplier.query.all()

    # Сотрудник «Алексей» — для карточки ЗП из ТГ (упоминание в тексте)
    alex = Employee.query.filter(Employee.name.ilike('%Алексей%')).first()
    if alex is None:
        alex = Employee(
            name='Козлов Алексей',
            role='worker',
            is_salary=False,
            fixed_salary=_money(0),
            is_active=True,
            unofficial_budget_item_id=by_code['10.3'].id,
        )
        db.session.add(alex)
        db.session.flush()

    def _mk_task(*, dedup, atype, source, arole, title, details, payload,
                 status='new', severity='info', deadline_days=0):
        if TgTask.query.filter_by(dedup_key=dedup).first():
            return None
        assignee = {
            'admin,executive': admin_user,
            'admin,executive,user': admin_user,
            'user': manager_user,
            'shop_manager': sales_user,
            'brigadier': brig_user,
            'executive': exec_user,
        }.get(arole)
        t = TgTask(
            raw_text=f'[демо ТГ] {title}\n{details}',
            title=title[:200],
            details=details,
            assignee_role=arole,
            assignee_id=assignee.id if assignee else None,
            deadline=today + timedelta(days=deadline_days),
            action_type=atype,
            action_payload=json.dumps(payload, ensure_ascii=False),
            status=status,
            sender_name=rng.choice(['Руководитель', 'Менеджер продаж', 'Бухгалтер', 'Бригадир']),
            completed_at=(datetime.combine(today, datetime.min.time()) if status == 'done' else None),
            completed_by_id=(admin_user.id if status == 'done' else None),
            dedup_key=dedup,
            first_seen_at=datetime.combine(today - timedelta(days=1), datetime.min.time()),
            last_seen_at=datetime.combine(today, datetime.min.time()),
            severity=severity,
            source=source,
            created_at=datetime.combine(today - timedelta(hours=6), datetime.min.time()),
        )
        db.session.add(t)
        db.session.flush()
        return t

    tasks = []
    # Дайджесты / поле / продажи — без дублей
    for i, spec in enumerate((
        ('digest', 'digest', 'admin,executive',
         'Дайджест недели: продажи и выкопка',
         'Отгружено 18 заказов, дебиторка −12%, выкопка по плану 86%.'),
        ('digest', 'digest', 'admin,executive',
         'Дайджест продаж (вечер)',
         '4 новых запроса, 2 счёта согласованы, 1 резерв без движения.'),
        ('info', 'tg', 'user',
         'Клиент просит счёт на Тую Smaragd',
         'ООО «Зелёный Квартал»: 40 шт C10, отгрузка на следующей неделе.'),
        ('digging', 'tg', 'brigadier',
         'План выкопки: заказ на Ель 120-140',
         '25 шт с поля 3 — отметить в календаре выкопки.'),
        ('anomaly', 'anomaly', 'admin,executive',
         'Резерв без движения 14+ дней',
         '1 заказ в резерве без оплаты и без выкопки.',
         'warning'),
        ('info', 'manual', 'user',
         'Подготовить УПД по отгрузке',
         'Документы для бухгалтера после отгрузки.'),
    )):
        sev = spec[5] if len(spec) > 5 else 'info'
        t = _mk_task(
            dedup=f'demo:misc:{i}',
            atype=spec[0], source=spec[1], arole=spec[2],
            title=spec[3], details=spec[4],
            payload={'demo': True},
            status='new' if i < 5 else 'done',
            severity=sev,
        )
        if t:
            tasks.append(t)

    # ---------- Расходы / ЗП из ТГ (полноценные карточки) ----------
    def _mk_chat_expense(*, mid, amount, desc, pay, status, sug_code, classifier=None):
        existing = ChatExpenseMessage.query.filter_by(
            tg_chat_id='demo-expenses', tg_message_id=mid,
        ).first()
        if existing:
            return existing
        row = ChatExpenseMessage(
            tg_chat_id='demo-expenses',
            tg_message_id=mid,
            tg_date=datetime.combine(today, datetime.min.time()) + timedelta(hours=10 + mid % 5),
            raw_text=f'{desc} {amount}',
            sender_name='Демо чат расходов',
            parsed_amount=_money(amount),
            parsed_description=desc,
            parsed_payment_type=pay,
            suggested_budget_item_id=by_code[sug_code].id,
            status=status,
        )
        db.session.add(row)
        db.session.flush()
        return row

    ce_payroll = _mk_chat_expense(
        mid=9101, amount=45100,
        desc='зарплата + компенсация ГСМ Алексею за ноябрь 2026',
        pay='cash', status='pending', sug_code='10.3', classifier='payroll',
    )
    ce_gsm = _mk_chat_expense(
        mid=9102, amount=12800,
        desc='ГСМ и транспортные расходы безнал',
        pay='cashless', status='pending', sug_code='7.1',
    )
    ce_fert = _mk_chat_expense(
        mid=9103, amount=67000,
        desc='удобрения и пестициды по накладной',
        pay='cashless', status='pending', sug_code='6.1',
    )
    # уже проведённый — для истории
    _mk_chat_expense(
        mid=9104, amount=8500, desc='хозрасходы офис',
        pay='cash', status='imported', sug_code='7',
    )

    for ce, title_amt, classifier in (
        (ce_payroll, 45100, 'payroll'),
        (ce_gsm, 12800, None),
        (ce_fert, 67000, None),
    ):
        if ce.status != 'pending':
            continue
        if ce.task_id:
            continue
        payload = {
            'chat_expense_id': ce.id,
            'amount': float(ce.parsed_amount or 0),
            'description': ce.parsed_description,
            'payment_type': ce.parsed_payment_type,
            'suggested_budget_item_id': ce.suggested_budget_item_id,
            'classifier_source': classifier or 'rule',
            'tg_chat_id': ce.tg_chat_id,
            'tg_message_id': ce.tg_message_id,
            'sender': ce.sender_name,
        }
        t = _mk_task(
            dedup=f'demo:chat_expense:{ce.id}',
            atype='chat_expense',
            source='tg',
            arole='admin,executive',
            title=f'Расход из ТГ: {title_amt:,} ₽'.replace(',', ' '),
            details=ce.parsed_description or '',
            payload=payload,
            status='new',
            deadline_days=2,
        )
        if t:
            ce.task_id = t.id
            tasks.append(t)

    # алиасы классификатора расходов
    for key, code in (('гсм транспорт', '7.1'), ('удобрения пестициды', '6.1'), ('зарплата рабочие', '10.3')):
        if ChatExpenseAlias.query.filter_by(alias_key=key).first():
            continue
        db.session.add(ChatExpenseAlias(
            alias_key=key,
            budget_item_id=by_code[code].id,
            hit_count=rng.randint(2, 8),
        ))

    db.session.flush()
    out['tg_tasks'] = len(tasks)
    out['chat_expenses_pending'] = 3

    # WeeklyDigest (2–3 недели)
    for weeks_ago in (0, 1, 2):
        monday = today - timedelta(days=today.weekday() + 7 * weeks_ago)
        for user in (admin_user, exec_user):
            if WeeklyDigest.query.filter_by(week_start=monday, user_id=user.id).first():
                continue
            summary = {
                'orders': rng.randint(8, 22),
                'shipped': rng.randint(5, 18),
                'revenue': rng.randint(800_000, 3_500_000),
                'expenses': rng.randint(600_000, 2_800_000),
                'digging_pct': rng.randint(70, 98),
            }
            html = (
                f'<h4>Дайджест за неделю с {monday.isoformat()}</h4>'
                f'<ul><li>Заказов: {summary["orders"]}</li>'
                f'<li>Отгружено: {summary["shipped"]}</li>'
                f'<li>Выручка ≈ {summary["revenue"]:,} ₽</li>'
                f'<li>Расходы ≈ {summary["expenses"]:,} ₽</li>'
                f'<li>Выкопка по плану: {summary["digging_pct"]}%</li></ul>'
                f'<p class="text-muted">Демо-сводка (синтетика).</p>'
            ).replace(',', ' ')
            db.session.add(WeeklyDigest(
                week_start=monday,
                user_id=user.id,
                content_html=html,
                summary_json=json.dumps(summary, ensure_ascii=False),
            ))
    out['weekly_digests'] = 6

    # Sales chat digest log
    for d_off in range(0, 5):
        d = today - timedelta(days=d_off)
        if SalesChatDigestLog.query.filter_by(digest_date=d).first():
            continue
        db.session.add(SalesChatDigestLog(
            digest_date=d,
            message_count=rng.randint(6, 28),
            summary_text=(
                f'Демо-сводка продаж {d.isoformat()}: '
                f'{rng.randint(1, 4)} новых клиента, '
                f'{rng.randint(1, 5)} уточнений по остаткам, '
                f'{rng.randint(0, 3)} договорённости об отгрузке.'
            ),
            recipient_ids='demo',
            sent_at=datetime.combine(d, datetime.min.time()) + timedelta(hours=19),
        ))

    # ---------- Счета на оплату (дашборд → провести оплату) ----------
    inv_pay_n = 0
    week_monday = today - timedelta(days=today.weekday())
    # priority=normal: high на дашборде принудительно кладёт карточку в «Просрочено».
    pay_specs = [
        ('Счёт за торф 80 м³', '6', 186000, 'normal', 'new', 'invoice'),
        ('Электричество ноябрь', '9', 72000, 'normal', 'new', 'invoice'),
        ('Контейнеры C5/C10', '15', 245000, 'normal', 'new', 'invoice'),
        ('ЧОП охрана', '13', 65000, 'normal', 'new', 'invoice'),
        ('План: ГСМ неделя', '7.1', 45000, 'normal', 'new', 'plan'),
        ('Без статьи — разнести', 'UNASSIGNED', 15300, 'normal', 'new', 'invoice'),
    ]
    for summary, code, amount, prio, status, kind in pay_specs:
        if PaymentInvoice.query.filter_by(summary=summary, original_name=f'demo_{summary}').first():
            continue
        item = by_code[code]
        inv = PaymentInvoice(
            filename=f'demo_{code}_{amount}.pdf',
            original_name=f'demo_{summary}',
            file_blob=b'%PDF-1.4 demo invoice',
            summary=summary,
            line_items=json.dumps([{
                'description': summary, 'qty': 1, 'unit': 'шт',
                'unit_price': amount, 'total': amount,
            }], ensure_ascii=False),
            source='tg',
            budget_item_id=item.id,
            amount=_money(amount),
            due_date=today + timedelta(days=rng.randint(0, 2)),
            priority=prio,
            comment='Демо счёт из ТГ / план оплаты',
            status=status,
            payment_type='cashless',
            kind=kind,
            week_start=week_monday if kind == 'plan' else None,
            created_by_user_id=admin_user.id,
            created_at=datetime.combine(today - timedelta(days=1), datetime.min.time()),
        )
        db.session.add(inv)
        inv_pay_n += 1
    db.session.flush()
    out['pay_invoices'] = inv_pay_n

    # Расход «без статьи» — карточка на дашборде для правки
    unassigned = by_code['UNASSIGNED']
    if not Expense.query.filter_by(description='Демо: расход без статьи (разнести)').first():
        ex = Expense(
            date=today - timedelta(days=1),
            budget_item_id=unassigned.id,
            description='Демо: расход без статьи (разнести)',
            amount=_money(15300),
            payment_type='cashless',
        )
        db.session.add(ex)
        db.session.flush()
        apply_budget_lines(ex, [{'budget_item_id': unassigned.id, 'amount': _money(15300)}])

    # ---------- AI Ops: оплата / отгрузка по заказам ----------
    debt_orders = (
        Order.query.filter(Order.status.in_(['shipped', 'ready', 'in_progress']))
        .order_by(Order.id.desc()).limit(30).all()
    )
    ops_n = 0
    for order in debt_orders[:2]:
        paid = float(sum(float(p.amount or 0) for p in (order.payments or [])))
        total = float(order.total_sum or 0)
        debt = max(0.0, total - paid)
        if debt < 100:
            # искусственно уменьшим оплату для демо-карточки
            if order.payments:
                order.payments[0].amount = _money(float(order.payments[0].amount or 0) * 0.3)
                db.session.flush()
                paid = float(sum(float(p.amount or 0) for p in order.payments))
                debt = max(5000.0, total - paid)
            else:
                debt = max(5000.0, total * 0.4)
        t = _mk_task(
            dedup=f'demo:ops:pay:{order.id}',
            atype='info',
            source='ai_ops',
            arole='admin,executive,user',
            title=f'Оплата по заказу #{order.id}',
            details=f'Клиент {(order.client.name if order.client else "")}: остаток ≈ {debt:.0f} ₽',
            payload={'order_id': order.id, 'demo': True},
            status='new',
            deadline_days=3,
        )
        if not t:
            continue
        draft = OpsActionDraft(
            task_id=t.id,
            action_type='payment_confirm',
            status='pending',
            payload_json=json.dumps({
                'amount': round(debt, 2),
                'payment_type': 'cashless',
                'client_name': order.client.name if order.client else '',
            }, ensure_ascii=False),
            channel='sales',
            quote=f'оплатили {debt:.0f} по счёту {order.invoice_number or order.id}',
            confidence=_money(0.86),
            order_id=order.id,
            client_id=order.client_id,
        )
        db.session.add(draft)
        ops_n += 1
        tasks.append(t)

    ship_orders = (
        Order.query.filter(Order.status.in_(['ready', 'in_progress']))
        .order_by(Order.id.desc()).limit(10).all()
    )
    for order in ship_orders[:2]:
        t = _mk_task(
            dedup=f'demo:ops:ship:{order.id}',
            atype='info',
            source='ai_ops',
            arole='admin,executive,user',
            title=f'Отгрузка заказа #{order.id}',
            details='Клиент забрал / машина на площадке — провести отгрузку.',
            payload={'order_id': order.id, 'demo': True},
            status='new',
            deadline_days=1,
        )
        if not t:
            continue
        db.session.add(OpsActionDraft(
            task_id=t.id,
            action_type='shipment_confirm',
            status='pending',
            payload_json=json.dumps({'order_id': order.id}, ensure_ascii=False),
            channel='sales',
            quote=f'отгрузили заказ {order.invoice_number or order.id}',
            confidence=_money(0.9),
            order_id=order.id,
            client_id=order.client_id,
        ))
        ops_n += 1
        tasks.append(t)
    out['ops_drafts'] = ops_n
    out['tg_tasks'] = len([t for t in TgTask.query.filter(TgTask.dedup_key.like('demo:%')).all()])

    # ---------- 3. Счета клиентам ----------
    company = SaleCompany.query.filter_by(short_name='ГринВью Демо').first()
    if company is None:
        company = SaleCompany(
            short_name='ГринВью Демо',
            legal_name='ООО «ГринВью Демо»',
            inn='7700000001',
            kpp='770001001',
            ogrn='1177700000001',
            legal_address='Московская обл., Демоград, ул. Питомниковая, 1',
            bank_name='АО «ДемоБанк»',
            bik='044525000',
            rs='40702810000000000001',
            ks='30101810000000000001',
            phone='+7 (900) 000-00-00',
            director='Иванов И.И.',
            vat_mode='none',
            is_active=True,
            sort_order=1,
        )
        db.session.add(company)
        db.session.flush()

    orders = Order.query.filter(Order.status != 'canceled').order_by(Order.id.desc()).limit(40).all()
    inv_n = 0
    for i, order in enumerate(orders[:12]):
        if SaleInvoice.query.filter_by(order_id=order.id).first():
            continue
        client = order.client
        status = 'approved' if i < 8 else 'draft'
        inv = SaleInvoice(
            company_id=company.id,
            client_id=client.id if client else None,
            user_id=sales_user.id,
            status=status,
            amount=_money(0),
            comment='Демо-счёт клиенту',
            buyer_name=(client.name if client else 'Клиент демо')[:300],
            buyer_inn=getattr(client, 'inn', None) if client else None,
            buyer_address=getattr(client, 'address', None) if client else None,
            buyer_phone=getattr(client, 'phone', None) if client else None,
            origin='erp',
            kind='goods',
            order_id=order.id,
            from_existing_order=True,
            approved_at=datetime.combine(today, datetime.min.time()) if status == 'approved' else None,
            doc_number=100 + i,
            doc_year=year,
            created_at=order.date or datetime.combine(today, datetime.min.time()),
        )
        db.session.add(inv)
        db.session.flush()
        total = Decimal('0')
        for it in (order.items or [])[:5]:
            line_sum = _money(float(it.price or 0) * int(it.quantity or 0))
            total += line_sum
            db.session.add(SaleInvoiceLine(
                invoice_id=inv.id,
                plant_id=it.plant_id,
                size_id=it.size_id,
                plant_name=(it.plant.name if it.plant else 'Растение')[:200],
                size_name=(it.size.name if it.size else '')[:120],
                qty=int(it.quantity or 1),
                price=_money(it.price or 0),
            ))
        if total <= 0:
            p = rng.choice(plants)
            s = rng.choice(container_sizes)
            qty = rng.randint(5, 30)
            price = _money(rng.choice([900, 1500, 2200]))
            total = _money(float(price) * qty)
            db.session.add(SaleInvoiceLine(
                invoice_id=inv.id,
                plant_id=p.id, size_id=s.id,
                plant_name=p.name, size_name=s.name,
                qty=qty, price=price,
            ))
        inv.amount = total
        inv_n += 1
    out['sale_invoices'] = inv_n

    # ---------- 4. Журнал документов ----------
    doc_n = 0
    for month in range(3, 12):
        d = date(year, month, rng.randint(3, 25))
        # income
        doc = Document(
            doc_type='income',
            date=datetime.combine(d, datetime.min.time()),
            user_id=manager_user.id,
            comment=f'Поступление саженцев (демо) {d.isoformat()}',
            supplier_id=rng.choice(suppliers).id if suppliers else None,
        )
        db.session.add(doc)
        db.session.flush()
        for _ in range(rng.randint(2, 5)):
            plant = rng.choice(plants)
            size = rng.choice(field_sizes)
            field = rng.choice(fields)
            qty = rng.randint(10, 80)
            db.session.add(DocumentRow(
                document_id=doc.id,
                plant_id=plant.id,
                size_id=size.id,
                field_to_id=field.id,
                year=year,
                quantity=qty,
                purchase_price=_money(rng.uniform(80, 400)),
            ))
        doc_n += 1

        # move field → yard
        doc_m = Document(
            doc_type='move',
            date=datetime.combine(d + timedelta(days=1), datetime.min.time()),
            user_id=manager_user.id,
            comment='Перемещение на контейнерную площадку (демо)',
        )
        db.session.add(doc_m)
        db.session.flush()
        for _ in range(rng.randint(1, 3)):
            plant = rng.choice(plants)
            db.session.add(DocumentRow(
                document_id=doc_m.id,
                plant_id=plant.id,
                size_id=rng.choice(field_sizes).id,
                field_from_id=rng.choice(fields).id,
                field_to_id=rng.choice(yards).id,
                size_to_id=rng.choice(container_sizes).id,
                year=year,
                quantity=rng.randint(5, 40),
            ))
        doc_n += 1

        # writeoff
        if month % 2 == 0:
            doc_w = Document(
                doc_type='writeoff',
                date=datetime.combine(d + timedelta(days=2), datetime.min.time()),
                user_id=brig_user.id,
                comment='Списание брака / усыхание (демо)',
            )
            db.session.add(doc_w)
            db.session.flush()
            plant = rng.choice(plants)
            db.session.add(DocumentRow(
                document_id=doc_w.id,
                plant_id=plant.id,
                size_id=rng.choice(field_sizes).id,
                field_from_id=rng.choice(fields).id,
                year=year,
                quantity=rng.randint(1, 12),
            ))
            doc_n += 1
    out['documents'] = doc_n

    # ---------- 5. Выкопка + аналитика ----------
    items = (
        OrderItem.query.join(Order)
        .filter(Order.status.in_(['reserved', 'in_progress', 'ready', 'shipped']))
        .limit(80)
        .all()
    )
    dig_task_n = dig_log_n = ship_n = 0
    for it in items[:25]:
        planned = today + timedelta(days=rng.randint(-20, 25))
        if not DiggingTask.query.filter_by(order_item_id=it.id).first():
            db.session.add(DiggingTask(
                order_item_id=it.id,
                planned_date=planned,
                planned_qty=min(int(it.quantity or 1), rng.randint(5, max(6, int(it.quantity or 10)))),
                comment='План выкопки (демо)',
                status='done' if planned < today else 'pending',
                created_by_user_id=manager_user.id,
            ))
            dig_task_n += 1
        if planned <= today and rng.random() < 0.7:
            db.session.add(DiggingLog(
                date=planned,
                created_at=datetime.combine(planned, datetime.min.time()),
                order_item_id=it.id,
                plant_id=it.plant_id,
                size_id=it.size_id,
                field_id=it.field_id,
                year=it.year or year,
                user_id=brig_user.id,
                quantity=min(int(it.quantity or 1), rng.randint(3, max(4, int(it.quantity or 8)))),
                status=rng.choice(['approved', 'approved', 'pending']),
            ))
            dig_log_n += 1
    # несколько логов без заказа (свободная выкопка)
    for _ in range(8):
        d = today - timedelta(days=rng.randint(1, 40))
        plant = rng.choice(plants)
        size = rng.choice(field_sizes)
        field = rng.choice(fields)
        db.session.add(DiggingLog(
            date=d,
            created_at=datetime.combine(d, datetime.min.time()),
            plant_id=plant.id,
            size_id=size.id,
            field_id=field.id,
            year=year,
            user_id=brig_user.id,
            quantity=rng.randint(5, 40),
            status='approved',
        ))
        dig_log_n += 1

    for order in orders[:10]:
        if ShipmentPlan.query.filter_by(order_id=order.id).first():
            continue
        db.session.add(ShipmentPlan(
            order_id=order.id,
            planned_date=today + timedelta(days=rng.randint(-5, 20)),
            comment='План отгрузки (демо)',
            created_by_user_id=manager_user.id,
        ))
        ship_n += 1

    for off, kind in ((3, DiggingCalendarMark.KIND_CREW_OFF),
                      (10, DiggingCalendarMark.KIND_BRIGADIER_OFF),
                      (17, DiggingCalendarMark.KIND_CREW_OFF)):
        d = today + timedelta(days=off)
        if DiggingCalendarMark.query.filter_by(planned_date=d, kind=kind).first():
            continue
        db.session.add(DiggingCalendarMark(
            planned_date=d,
            kind=kind,
            created_by_user_id=manager_user.id,
        ))
    out['digging_tasks'] = dig_task_n
    out['digging_logs'] = dig_log_n
    out['shipment_plans'] = ship_n

    # ---------- 5b. ВиУМ ----------
    mats_def = [
        ('Мешковина', 'м'),
        ('Сетка металлическая', 'м'),
        ('Шпагат', 'кг'),
        ('Горшок C5', 'шт'),
        ('Горшок C10', 'шт'),
        ('Субстрат торф', 'л'),
    ]
    materials = []
    for name, unit in mats_def:
        m = ViumMaterial.query.filter_by(name=name).first()
        if m is None:
            m = ViumMaterial(name=name, unit=unit, description='Демо материал')
            db.session.add(m)
        materials.append(m)
    db.session.flush()

    vium_ops = 0
    for month in (4, 6, 8, 10):
        d = date(year, month, 12)
        op = ViumOperation(
            kind='intake',
            date=d,
            comment=f'Приход ВиУМ (демо) {month}.{year}',
            created_by_user_id=admin_user.id,
        )
        db.session.add(op)
        db.session.flush()
        for mat in materials:
            qty = _money(rng.uniform(50, 400))
            price = _money(rng.uniform(15, 120))
            db.session.add(ViumOperationLine(
                operation_id=op.id,
                material_id=mat.id,
                qty=qty,
                unit_price=price,
            ))
            db.session.add(ViumLot(
                material_id=mat.id,
                received_at=d,
                qty_received=qty,
                qty_remaining=_money(float(qty) * rng.uniform(0.4, 0.95)),
                unit_price=price,
                source_operation_id=op.id,
                note='Демо партия',
            ))
        vium_ops += 1
        # расход
        op_c = ViumOperation(
            kind='consume',
            date=d + timedelta(days=5),
            comment='Расход на выкопку (демо)',
            created_by_user_id=brig_user.id,
        )
        db.session.add(op_c)
        db.session.flush()
        mat = rng.choice(materials)
        db.session.add(ViumOperationLine(
            operation_id=op_c.id,
            material_id=mat.id,
            qty=_money(rng.uniform(5, 40)),
            unit_price=_money(rng.uniform(15, 80)),
        ))
        vium_ops += 1

    # техкарты на пару популярных растений
    tc_n = 0
    for plant in plants[:6]:
        for size in container_sizes[:2]:
            mat = materials[0]
            if ViumTechCardLine.query.filter_by(
                plant_id=plant.id, size_id=size.id, material_id=mat.id,
            ).first():
                continue
            db.session.add(ViumTechCardLine(
                plant_id=plant.id,
                size_id=size.id,
                material_id=mat.id,
                qty_per_bush=_money(rng.choice([0.5, 1, 1.5, 2])),
                note='Демо норма',
                is_active=True,
            ))
            tc_n += 1
    out['vium_ops'] = vium_ops
    out['vium_techcards'] = tc_n

    # ---------- План закупок ВиУМ ----------
    purch_n = 0
    if not ViumPurchase.query.filter(ViumPurchase.title.like('Демо закупка%')).count():
        for title, lines in (
            ('Демо закупка: мешковина и сетка', [
                ('Мешковина 1.2м', 200, 'м', 45, materials[0].id if materials else None),
                ('Сетка металлическая', 150, 'м', 80, materials[1].id if len(materials) > 1 else None),
            ]),
            ('Демо закупка: горшки C5/C10', [
                ('Горшок C5', 500, 'шт', 28, materials[3].id if len(materials) > 3 else None),
                ('Горшок C10', 300, 'шт', 55, materials[4].id if len(materials) > 4 else None),
            ]),
        ):
            vp = ViumPurchase(
                title=title,
                request_date=today - timedelta(days=rng.randint(1, 10)),
                status='open',
                source='manual',
                channel='itr',
                note='Демо-план закупок',
                created_by_user_id=admin_user.id,
            )
            db.session.add(vp)
            db.session.flush()
            for i, (ltitle, qty, unit, price, mid) in enumerate(lines):
                db.session.add(ViumPurchaseLine(
                    purchase_id=vp.id,
                    title=ltitle,
                    qty_text=f'{qty} {unit}',
                    qty=_money(qty),
                    unit=unit,
                    unit_price=_money(price),
                    supplier=rng.choice(['ООО «ТорфСнаб»', 'ООО «КонтейнерМаркет»', 'ООО «СеткаПлюс»']),
                    delivery_date=today + timedelta(days=rng.randint(5, 20)),
                    status='planned',
                    material_id=mid,
                    sort_order=i,
                ))
            purch_n += 1
            # задача на дашборд
            t = _mk_task(
                dedup=f'demo:purchase:{vp.id}',
                atype='info',
                source='tg',
                arole='admin,executive',
                title=f'План закупки: {title.replace("Демо закупка: ", "")}',
                details='Согласовать позиции и сроки поставки (раздел ВиУМ → закупки).',
                payload={'vium_purchase_id': vp.id, 'demo': True},
                status='new',
                deadline_days=7,
            )
            if t:
                vp.task_id = t.id
                tasks.append(t)
    out['vium_purchases'] = purch_n

    # ---------- Проекты: контейнеры, посадка в горшки, бюджет ----------
    proj_n = 0
    for idx, (pname, yard, desc) in enumerate((
        ('Контейнеризация 2026 · площадка 1', yards[0],
         'Посадка в горшки с поля на контейнерную площадку 1'),
        ('Контейнеризация 2026 · площадка 2', yards[1],
         'Вторая очередь посадки, отдельный бюджет проекта'),
    )):
        proj = Project.query.filter_by(name=pname).first()
        if proj is None:
            proj = Project(
                name=pname,
                description=desc,
                status='active',
                potting_stock_field_id=yard.id,
                created_at=datetime(year, 4, 1) + timedelta(days=idx * 20),
            )
            db.session.add(proj)
            db.session.flush()
        else:
            proj.potting_stock_field_id = yard.id
            proj.status = 'active'

        # статьи бюджета проекта
        if not proj.budget_items:
            for bname, bamt in (
                ('Горшки и кассеты', 420_000),
                ('Субстрат / торф', 280_000),
                ('ЗП бригада посадки', 350_000),
                ('Логистика с поля', 95_000),
                ('Прочее', 40_000),
            ):
                db.session.add(ProjectBudget(
                    project_id=proj.id,
                    name=bname,
                    amount=_money(bamt * rng.uniform(0.9, 1.1)),
                ))
            db.session.flush()

        # позиции поставки → посадка
        if not proj.items:
            for plant in rng.sample(plants, k=min(8, len(plants))):
                size = rng.choice(container_sizes)
                qty = rng.randint(80, 350)
                item = ProjectItem(
                    project_id=proj.id,
                    plant_id=plant.id,
                    size_id=size.id,
                    quantity=qty,
                    comment='Поставка ОКС / пересадка (демо)',
                )
                db.session.add(item)
                db.session.flush()
                # логи посадки по дням
                planted = 0
                for day_off in range(0, 40, 3):
                    if planted >= qty:
                        break
                    chunk = min(rng.randint(15, 45), qty - planted)
                    log_d = date(year, 5, 1) + timedelta(days=day_off + idx)
                    if log_d > today:
                        break
                    db.session.add(ProjectPottingLog(
                        project_id=proj.id,
                        project_item_id=item.id,
                        log_date=log_d,
                        quantity=chunk,
                        comment='Посадка в горшки (демо)',
                        created_by_id=brig_user.id,
                    ))
                    planted += chunk
                    if not ProjectPottingDayMeta.query.filter_by(
                        project_id=proj.id, log_date=log_d,
                    ).first():
                        db.session.add(ProjectPottingDayMeta(
                            project_id=proj.id,
                            log_date=log_d,
                            workers_count=rng.randint(4, 9),
                        ))

        # факт расходов проекта по статьям бюджета
        if not Expense.query.filter_by(project_id=proj.id).count():
            for pb in proj.budget_items:
                for _ in range(rng.randint(1, 3)):
                    d = date(year, rng.randint(5, 10), rng.randint(1, 28))
                    if d > today:
                        d = today - timedelta(days=rng.randint(1, 20))
                    amt = _money(float(pb.amount or 0) * rng.uniform(0.08, 0.25))
                    code = {
                        'Горшки и кассеты': '15',
                        'Субстрат / торф': '6',
                        'ЗП бригада посадки': '10.3',
                        'Логистика с поля': '7.1',
                    }.get(pb.name, '7')
                    ex = Expense(
                        date=d,
                        budget_item_id=by_code[code].id,
                        description=f'{pb.name} · {proj.name}',
                        amount=amt,
                        payment_type=rng.choice(['cash', 'cashless']),
                        project_id=proj.id,
                        project_budget_id=pb.id,
                    )
                    db.session.add(ex)
                    db.session.flush()
                    apply_budget_lines(ex, [{'budget_item_id': by_code[code].id, 'amount': amt}])

        # заказы с позиций площадки проекта
        yard_stock = (
            StockBalance.query.filter_by(field_id=yard.id, year=year)
            .filter(StockBalance.quantity > 0).limit(20).all()
        )
        linked = Order.query.filter_by(project_id=proj.id).count()
        if linked < 3 and yard_stock:
            for j in range(3 - linked):
                client = rng.choice(clients)
                odt = datetime(year, 9 + (j % 2), 5 + j * 3, 12, 0)
                order = Order(
                    date=odt,
                    client_id=client.id,
                    status=rng.choice(['reserved', 'in_progress', 'shipped']),
                    invoice_number=f'П-{proj.id}-{j + 1:02d}',
                    invoice_date=odt.date(),
                    project_id=proj.id,
                )
                db.session.add(order)
                db.session.flush()
                for sb in rng.sample(yard_stock, k=min(2, len(yard_stock))):
                    qty = rng.randint(5, min(25, int(sb.quantity or 25)))
                    db.session.add(OrderItem(
                        order_id=order.id,
                        plant_id=sb.plant_id,
                        size_id=sb.size_id,
                        field_id=yard.id,
                        year=year,
                        price=sb.price or _money(1800),
                        quantity=qty,
                        shipped_quantity=qty if order.status == 'shipped' else 0,
                        dug_quantity=qty if order.status != 'reserved' else 0,
                    ))
        proj_n += 1

    # проекты участвуют в себесе контейнерной площадки
    proj_ids = [p.id for p in Project.query.filter(Project.name.like('Контейнеризация%')).all()]
    if proj_ids:
        row = AppSetting.query.get('cost_container_project_ids')
        val = ','.join(str(i) for i in proj_ids)
        if row is None:
            db.session.add(AppSetting(key='cost_container_project_ids', value=val))
        else:
            row.value = val
        pot = AppSetting.query.get('potting_stock_field_id')
        if pot is None:
            db.session.add(AppSetting(key='potting_stock_field_id', value=str(yards[0].id)))
        else:
            pot.value = str(yards[0].id)

    out['projects'] = proj_n

    # ---------- 6. Анализ рынка (готовые строки, без AI) ----------
    snap = CompetitorSnapshot.query.filter_by(name='Демо мониторинг осень 2026').first()
    if snap is None:
        snap = CompetitorSnapshot(
            name='Демо мониторинг осень 2026',
            date=datetime.combine(today, datetime.min.time()),
            user_id=admin_user.id,
            raw_ai_response=None,
        )
        db.session.add(snap)
        db.session.flush()
    competitors = ['Питомник «Север»', 'Сады «Восток»', 'GreenTrade', 'EcoPlant Market']
    market_n = 0
    if CompetitorRow.query.filter_by(snapshot_id=snap.id).count() == 0:
        for plant in plants[:12]:
            size = rng.choice(sizes)
            our = float(rng.choice([900, 1400, 2200, 3500]))
            for comp in rng.sample(competitors, k=2):
                their = our * rng.uniform(0.85, 1.25)
                db.session.add(CompetitorRow(
                    snapshot_id=snap.id,
                    plant_name=plant.name,
                    size_name=size.name,
                    competitor_name=comp,
                    competitor_price=_money(their),
                    source_link='https://example.com/demo-price',
                    our_price_at_moment=_money(our),
                    our_cost_at_moment=_money(our * 0.45),
                    pack_type=rng.choice(['RB', 'C5', 'C10', 'WRB']),
                    form=rng.choice(['free', 'ball', 'stamm']),
                    source_excerpt='Пример цены с сайта конкурента (демо).',
                    confidence=0.82,
                    is_rejected=False,
                ))
                market_n += 1
    out['market_rows'] = market_n

    # чуть остатков на площадках ещё раз гарантируем видимость
    for yard in yards:
        if StockBalance.query.filter_by(field_id=yard.id, year=year).count() < 10:
            for plant in plants[:10]:
                _sb = StockBalance.query.filter_by(
                    plant_id=plant.id,
                    size_id=container_sizes[0].id,
                    field_id=yard.id,
                    year=year,
                ).first()
                if _sb is None:
                    price = _money(1800)
                    db.session.add(StockBalance(
                        plant_id=plant.id,
                        size_id=container_sizes[0].id,
                        field_id=yard.id,
                        year=year,
                        quantity=rng.randint(10, 60),
                        price=price,
                        purchase_price=_money(800),
                        current_total_cost=_money(8000),
                    ))

    return out


def ensure_demo_seeded(*, year: int | None = None, seed: int = 2026) -> dict | None:
    """Для Amvera DEMO_MODE: один раз заполнить пустую БД синтетикой.

    Вызывать из run.py после create_app. Не трогает уже заполненную базу.
    """
    from app.models import BudgetItem, Expense, TgTask, db
    from app.utils import msk_today

    if Expense.query.count() > 30 and BudgetItem.query.count() > 10:
        # Подтянуть дедлайны демо-карточек на сегодня (после смены даты).
        today = msk_today()
        n = (
            TgTask.query.filter(TgTask.dedup_key.like('demo:%'), TgTask.status == 'new')
            .update({TgTask.deadline: today}, synchronize_session=False)
        )
        if n:
            db.session.commit()
        return None

    y = year or msk_today().year
    stats = seed_all(year=y, rng=random.Random(seed))
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description='Собрать demo_nursery.db с синтетикой')
    parser.add_argument('--reset', action='store_true', help='Удалить demo БД и создать заново')
    parser.add_argument('--year', type=int, default=2026)
    parser.add_argument('--seed', type=int, default=2026, help='seed генератора случайных чисел')
    parser.add_argument('--db', type=Path, default=DEMO_DB)
    args = parser.parse_args()

    db_path: Path = args.db
    print(f'Demo DB: {db_path}')
    if args.reset:
        _unlink_db(db_path)
    elif db_path.exists():
        print('БД уже есть. Для полной пересборки: --reset')
        return 1

    _configure_env(db_path)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from app import create_app

    app = create_app()
    rng = random.Random(args.seed)
    with app.app_context():
        # если БД уже с демо-данными без --reset — не дублируем
        from app.models import BudgetItem, Expense
        if BudgetItem.query.count() > 5 and Expense.query.count() > 50 and not args.reset:
            print('Похоже, демо уже заполнено. Используйте --reset.')
            return 1
        stats = seed_all(year=args.year, rng=rng)

    print('OK demo seed:')
    for k, v in stats.items():
        print(f'  {k}: {v}')
    print()
    print('Вход для клиента:')
    print('  demo / demo2026         admin')
    print('  boss / boss2026         executive (руководитель)')
    print('  manager / manager2026   менеджер питомника')
    print('  sales / sales2026       shop_manager (продажи)')
    print('  hr / hr2026             кадровик')
    print('  brigadier / brig2026    бригадир')
    print()
    print('Запуск:  .\\scripts\\run-demo.ps1')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
