import io
import re
import os
import calendar
from datetime import datetime, date, timedelta
from decimal import Decimal
from werkzeug.utils import secure_filename
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, current_app, jsonify, send_from_directory
from flask_login import login_required, current_user
from sqlalchemy import func, or_, and_
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side 
from openpyxl.utils import get_column_letter

from app.models import (
    db, Expense, BudgetItem, BudgetPlan, CashflowPlan, Employee, UnitCostOverride, 
    Plant, StockBalance, Order, OrderItem, Field, Payment, Document, DocumentRow, Size,
    Client, PaymentInvoice, Project, ProjectItem, ProjectBudget
)
from app.utils import (
    msk_now, msk_today, log_action, natural_key, create_pdf_response, MONTH_NAMES
)
from app.services import (
    calculate_cost_data, calculate_investor_debt, get_detailed_stock_at_year_end
)

bp = Blueprint('finance', __name__)

@bp.route('/expenses', methods=['GET', 'POST'])
@login_required
def expenses():
    if current_user.role not in ['admin', 'executive']: 
        return redirect(url_for('orders.orders_list'))
    
    # Текущий таб (расходы / счета)
    tab = request.args.get('tab', 'expenses')

    # Общие фильтры (для вкладки расходов)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    f_item = request.args.get('filter_item')
    f_type = request.args.get('filter_type')
    export = request.args.get('export')

    # Подготовка данных из счета для быстрого создания расхода
    invoice_id = request.args.get('invoice_id')
    prefill_invoice = None
    if invoice_id:
        prefill_invoice = PaymentInvoice.query.get(invoice_id)

    # Если открыли вкладку "Счета" — подготавливаем список счетов
    invoices = []
    invoice_summary = None
    if tab == 'invoices':
        invoices = PaymentInvoice.query.filter(PaymentInvoice.status != 'paid').order_by(
            PaymentInvoice.due_date.asc(),
            PaymentInvoice.priority.desc()
        ).all()
        invoice_summary = {
            'high': 0, 'normal': 0, 'low': 0, 'total': 0,
            'count_high': 0, 'count_normal': 0, 'count_low': 0, 'count_total': len(invoices)
        }
        # Pre-aggregate paid sums for all invoices in one query
        inv_paid_agg = db.session.query(
            Expense.invoice_id, func.sum(Expense.amount)
        ).filter(Expense.invoice_id.isnot(None)).group_by(Expense.invoice_id).all()
        inv_paid_map = {r[0]: r[1] or Decimal(0) for r in inv_paid_agg}

        for inv in invoices:
            if isinstance(inv.due_date, str):
                try:
                    inv.due_date = datetime.strptime(inv.due_date, '%Y-%m-%d').date()
                except Exception:
                    pass
            elif isinstance(inv.due_date, datetime):
                inv.due_date = inv.due_date.date()

            paid_sum = inv_paid_map.get(inv.id, Decimal(0))
            inv.paid_sum = float(paid_sum)
            inv.remaining = float(inv.amount - paid_sum)

            # Сводная статистика: считаем остаток к оплачиваению (не всю сумму счета)
            val = inv.remaining
            p = inv.priority
            invoice_summary[p] += val
            invoice_summary[f'count_{p}'] += 1
            invoice_summary['total'] += val

    # Если открыта вкладка "Расходы" — подготавливаем журнал расходов
    expenses_list = []
    if tab == 'expenses':
        query = Expense.query
        if start_date:
            query = query.filter(func.date(Expense.date) >= start_date)
        if end_date:
            query = query.filter(func.date(Expense.date) <= end_date)
        if f_item:
            query = query.filter(Expense.budget_item_id == int(f_item))
        if f_type:
            query = query.filter(Expense.payment_type == f_type)
        
        expenses_list = query.order_by(Expense.date.desc()).all()
    
    if export == 'pdf' and tab == 'expenses':
        font_path = os.path.join(current_app.root_path, 'static', 'DejaVuSans.ttf').replace('\\', '/')
        rendered = render_template('expenses_pdf.html', expenses=expenses_list, font_path=font_path)
        return create_pdf_response(rendered, "expenses.pdf")
        
    if request.method == 'POST':
        if current_user.role == 'executive':
            flash('У вас права только на просмотр')
            return redirect(url_for('finance.expenses', tab='expenses'))
        
        # 1. Удаление обычного расхода
        if 'delete_expense' in request.form:
            Expense.query.filter_by(id=request.form.get('expense_id')).delete()
            db.session.commit()
            flash('Расход удален')
            log_action("Удалил расход")
            
        # 2. НОВЫЙ БЛОК: Удаление или пометка счета как "Оплачен"
        elif request.form.get('action') in ['delete', 'mark_paid']:
            inv_id = request.form.get('id')
            inv = PaymentInvoice.query.get(inv_id)
            if inv:
                action = request.form.get('action')
                if action == 'delete':
                    # Удаляем файл с диска
                    inv_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'invoices')
                    file_path = os.path.join(inv_dir, inv.filename)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    db.session.delete(inv)
                    flash('Счет полностью удален')
                    log_action(f"Удалил счет: {inv.original_name}")
                else:
                    inv.status = 'paid'
                    flash('Счет помечен как оплаченный')
                    log_action(f"Пометил счет как оплаченный: {inv.original_name}")
                db.session.commit()
            return redirect(url_for('finance.expenses', tab='invoices'))

        elif 'due_date' in request.form or 'add_invoice' in request.form:
            # === ЭТО ЛОГИКА ЗАГРУЗКИ СЧЕТА НА ОПЛАТУ ===
            try:
                file = request.files.get('file')
                if not file or not file.filename:
                    flash('Ошибка: Файл счета не выбран')
                    return redirect(url_for('finance.expenses', tab='invoices'))

                filename = secure_filename(file.filename)
                save_name = f"inv_{int(msk_now().timestamp())}_{filename}"
                
                inv_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'invoices')
                if not os.path.exists(inv_dir):
                    os.makedirs(inv_dir)
                    
                file.save(os.path.join(inv_dir, save_name))
                
                due_date_str = request.form.get('due_date')
                d_val = datetime.strptime(due_date_str, '%Y-%m-%d').date() if due_date_str else None
                
                b_id = request.form.get('budget_item_id') or request.form.get('budget_item')
                
                inv = PaymentInvoice(
                    filename=save_name,
                    original_name=file.filename,
                    budget_item_id=int(b_id) if b_id else None,
                    amount=float(request.form.get('amount') or 0),
                    due_date=d_val,
                    priority=request.form.get('priority', 'normal'),
                    comment=request.form.get('comment', '')
                )
                db.session.add(inv)
                db.session.commit()
                flash('Счет успешно загружен')
                log_action('Загрузил новый счет на оплату')
            except Exception as e:
                db.session.rollback()
                flash(f'Ошибка при загрузке счета: {e}')
            return redirect(url_for('finance.expenses', tab='invoices'))
            
        else:
            # === ЭТО ЛОГИКА СОЗДАНИЯ ОБЫЧНОГО РАСХОДА ===
            try:
                date_str = request.form.get('date')
                if not date_str:
                    flash('Ошибка: Укажите дату расхода!')
                    return redirect(url_for('finance.expenses', tab='expenses'))
                    
                date_val = datetime.strptime(date_str, '%Y-%m-%d')
                
                # Получаем ID (если пустая строка, будет None)
                oid = request.form.get('order_id')
                pid = request.form.get('project_id')
                pbid = request.form.get('project_budget_id') # Плановая статья проекта
                inv_id = request.form.get('invoice_id')
                barter_oid = request.form.get('barter_order_id')
                
                t_month = request.form.get('target_month')
                t_year = request.form.get('target_year')

                exp = Expense(
                    date=date_val, 
                    budget_item_id=int(request.form.get('budget_item')), 
                    description=request.form.get('description'), 
                    amount=float(request.form.get('amount')), 
                    payment_type=request.form.get('payment_type'), 
                    employee_id=(int(request.form.get('employee_id')) if request.form.get('employee_id') else None),
                    order_id=(int(oid) if oid else None),
                    project_id=(int(pid) if pid else None),
                    project_budget_id=(int(pbid) if pbid else None),
                    barter_order_id=(int(barter_oid) if barter_oid else None),
                    target_month=(int(t_month) if t_month else None),
                    target_year=(int(t_year) if t_year else None)
                )

                if inv_id:
                    inv = PaymentInvoice.query.get(int(inv_id))
                    if inv:
                        exp.invoice_id = inv.id

                db.session.add(exp)
                db.session.commit()

                # Обновляем статус счета (paid/partial/new) в зависимости от суммы созданных расходов
                if inv_id:
                    inv = PaymentInvoice.query.get(int(inv_id))
                    if inv:
                        paid_sum = db.session.query(func.sum(Expense.amount)).filter(Expense.invoice_id == inv.id).scalar() or Decimal(0)
                        if paid_sum >= inv.amount:
                            inv.status = 'paid'
                        elif paid_sum > 0:
                            inv.status = 'partial'
                        else:
                            inv.status = 'new'
                        db.session.commit()

                flash('Добавлен')
                log_action("Добавил расход")
            except Exception as e: 
                flash(f'Ошибка: {e}')
        return redirect(url_for('finance.expenses', tab='expenses'))
        
    active_orders = Order.query.filter(Order.status != 'canceled', Order.is_deleted == False).order_by(Order.id.desc()).limit(50).all()
    barter_orders = Order.query.filter(Order.is_barter == True, Order.status != 'canceled', Order.is_deleted == False).order_by(Order.id.desc()).all()
    # Загружаем проекты и их бюджетные статьи (для JS)
    active_projects = Project.query.filter_by(status='active').order_by(Project.name).all()

    return render_template('finance/expenses.html', 
                           active_tab=tab,
                           expenses=expenses_list, 
                           invoices=invoices,
                           invoice_summary=invoice_summary,
                           budget_items=sorted(BudgetItem.query.all(), key=natural_key),
                           employees=sorted(Employee.query.filter_by(is_active=True).all(), key=lambda x: x.name),
                           active_orders=active_orders,
                           barter_orders=barter_orders,
                           active_projects=active_projects, 
                           today=msk_today(), 
                           prefill_invoice=prefill_invoice,
                           filters={'start': start_date, 'end': end_date, 'item': f_item, 'type': f_type})

@bp.route('/expenses/download_template')
@login_required
def expenses_download_template():
    wb = Workbook()
    ws = wb.active
    ws.append(["Дата", "Код", "Описание", "Сумма", "Тип"])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(buf, download_name='expenses_template.xlsx', as_attachment=True)

@bp.route('/expenses/import', methods=['POST'])
@login_required
def expenses_import():
    if current_user.role != 'admin':
        return redirect(url_for('finance.expenses'))
    
    file = request.files.get('file')
    if not file:
        return redirect(url_for('finance.expenses'))
    
    try:
        wb = load_workbook(file)
        ws = wb.active
        budget_map = {str(b.code).strip(): b.id for b in BudgetItem.query.all()}
        count = 0
        for row in ws.iter_rows(min_row=2, values_only=True):
            raw_date = row[0]
            code_val = str(row[1]).strip() if row[1] else None
            desc = row[2]
            amount = row[3]
            type_val = row[4]
            
            if not raw_date or not amount or not code_val:
                continue
            
            final_date = msk_today()
            if isinstance(raw_date, datetime):
                final_date = raw_date.date()
            elif isinstance(raw_date, str):
                try:
                    final_date = datetime.strptime(raw_date, '%d.%m.%Y').date()
                except:
                    pass
            
            b_item_id = budget_map.get(code_val)
            if not b_item_id and code_val.isdigit():
                 check_id = int(code_val)
                 if BudgetItem.query.get(check_id):
                     b_item_id = check_id
            
            if not b_item_id:
                continue
            
            p_type = 'cashless'
            tv_str = str(type_val).strip()
            if tv_str == '1' or tv_str.lower() == 'нал':
                p_type = 'cash'
            elif tv_str == '2' or tv_str.lower() == 'безнал':
                p_type = 'cashless'
            
            exp = Expense(date=final_date, budget_item_id=b_item_id, description=desc, amount=float(amount), payment_type=p_type, employee_id=None)
            db.session.add(exp)
            count += 1
            
        db.session.commit()
        log_action(f'Импорт {count} расходов')
        flash(f'Успешно загружено {count} расходов')
    except Exception as e: 
        db.session.rollback()
        flash(f'Ошибка: {e}')
    return redirect(url_for('finance.expenses'))

@bp.route('/expenses/export')
@login_required
def expenses_export():
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))

    # Получаем те же фильтры, что и при просмотре
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    f_item = request.args.get('filter_item')
    f_type = request.args.get('filter_type')

    query = Expense.query
    if start_date: query = query.filter(func.date(Expense.date) >= start_date)
    if end_date: query = query.filter(func.date(Expense.date) <= end_date)
    if f_item: query = query.filter(Expense.budget_item_id == int(f_item))
    if f_type: query = query.filter(Expense.payment_type == f_type)
    
    expenses = query.order_by(Expense.date.desc()).all()

    wb = Workbook()
    ws = wb.active
    ws.title = "Расходы"

    # Шапка
    headers = ["Дата", "Статья бюджета", "Код", "Сумма", "Тип", "Сотрудник", "Проект", "Заказ", "Описание"]
    ws.append(headers)
    
    # Стили шапки
    header_fill = PatternFill(start_color="E0F2F1", end_color="E0F2F1", fill_type="solid")
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = Font(bold=True)

    for e in expenses:
        emp_name = e.employee.name if e.employee else ""
        proj_name = e.project_link.name if e.project_id else ""
        ord_name = str(e.order_id) if e.order_id else ""
        
        ws.append([
            e.date.strftime('%d.%m.%Y'),
            e.item.name,
            e.item.code,
            float(e.amount),
            'Нал' if e.payment_type == 'cash' else 'Безнал',
            emp_name,
            proj_name,
            ord_name,
            e.description
        ])

    # Ширина колонок
    ws.column_dimensions['A'].width = 12
    ws.column_dimensions['B'].width = 30
    ws.column_dimensions['I'].width = 40

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    return send_file(
        buf, 
        download_name=f'expenses_{msk_now().strftime("%Y%m%d")}.xlsx', 
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

def _budget_period_to_months(tm):
    """Преобразует параметр месяца/периода в список месяцев и подпись."""
    if tm == 'all':
        return list(range(1, 13)), 'За год'
    if tm == 'q1':
        return [1, 2, 3], '1 кв.'
    if tm == 'q2':
        return [4, 5, 6], '2 кв.'
    if tm == 'q3':
        return [7, 8, 9], '3 кв.'
    if tm == 'q4':
        return [10, 11, 12], '4 кв.'
    if tm == 'spring':
        return [1, 2, 3, 4, 5, 6], 'Весна'
    if tm == 'autumn':
        return [7, 8, 9, 10, 11, 12], 'Осень'
    try:
        m = int(tm)
        if 1 <= m <= 12:
            return [m], MONTH_NAMES.get(m, f"Месяц {m}")
    except Exception:
        pass
    return list(range(1, 13)), 'За год'


@bp.route('/budget', methods=['GET', 'POST'])
@login_required
def budget():
    if current_user.role not in ['admin', 'executive']: 
        return redirect(url_for('orders.orders_list'))
    
    # Параметры фильтрации
    year = int(request.args.get('year', msk_now().year))
    # По умолчанию берем "all" (весь год) или конкретный месяц/период
    default_month = 'all'  # Или str(msk_now().month) для фокуса на текущем
    target_month = request.args.get('month', default_month)
    tab = request.args.get('tab', 'budget')

    export = request.args.get('export')

    selected_months, period_label = _budget_period_to_months(target_month)
    period_end_month = max(selected_months)
    
    # Загрузка данных
    items = sorted(BudgetItem.query.all(), key=natural_key)
    plans = BudgetPlan.query.filter_by(year=year).all()
    
    # Умное определение периода: берем target_month, если он есть, иначе берем месяц из даты
    actual_month = func.coalesce(Expense.target_month, func.extract('month', Expense.date))
    actual_year = func.coalesce(Expense.target_year, func.extract('year', Expense.date))
    
    expenses_q = db.session.query(
        Expense.budget_item_id, 
        actual_month.label('month'), 
        func.sum(Expense.amount)
    ).filter(actual_year == year).group_by(Expense.budget_item_id, actual_month).all()

    # Структура данных
    # data = { item_id: { item: Obj, months: {1:..., 2:...}, total: {...} } }
    data = {i.id: {'item': i, 'months': {m: {'plan': Decimal(0), 'fact': Decimal(0), 'diff': Decimal(0)} for m in range(1, 13)}, 'total': {'plan':Decimal(0), 'fact':Decimal(0), 'diff':Decimal(0), 'percent': 0}} for i in items}
    
    for p in plans: 
        if p.budget_item_id in data:
            data[p.budget_item_id]['months'][p.month]['plan'] = p.amount
    for e in expenses_q: 
        if e[0] in data:
            data[e[0]]['months'][e[1]]['fact'] = e[2]
        
    grand_totals = {m: {'plan':Decimal(0), 'fact':Decimal(0), 'diff':Decimal(0)} for m in range(1, 14)} # 13 - это год
    
    # Расчет итогов
    for row in data.values():
        tp, tf = Decimal(0), Decimal(0)
        for m in range(1, 13):
            d = row['months'][m]
            d['diff'] = d['plan'] - d['fact']
            tp += d['plan']
            tf += d['fact']
            
            # Глобальные итоги по месяцам
            grand_totals[m]['plan'] += d['plan']
            grand_totals[m]['fact'] += d['fact']
            grand_totals[m]['diff'] += d['diff']
            
        row['total'] = {'plan': tp, 'fact': tf, 'diff': tp - tf}

        # Итог за выбранный период (месяц/кв/сезон)
        period_plan = Decimal(0)
        period_fact = Decimal(0)
        for m in selected_months:
            period_plan += row['months'][m]['plan']
            period_fact += row['months'][m]['fact']
        row['period'] = {
            'plan': period_plan,
            'fact': period_fact,
            'diff': period_plan - period_fact
        }
        
        # Процент выполнения (для прогресс-бара)
        if tp > 0:
            pct = (tf / tp) * 100
            row['total']['percent'] = min(pct, 100) # Ограничиваем для визуализации
            row['total']['percent_raw'] = pct
        else:
            row['total']['percent'] = 0
            row['total']['percent_raw'] = 0

        grand_totals[13]['plan'] += tp
        grand_totals[13]['fact'] += tf
        grand_totals[13]['diff'] += (tp - tf)

    # Итоги для выбранного периода (месяц/кв./сезон/год)
    period_totals = {'plan': Decimal(0), 'fact': Decimal(0), 'diff': Decimal(0), 'pct': 0}
    for m in selected_months:
        period_totals['plan'] += grand_totals[m]['plan']
        period_totals['fact'] += grand_totals[m]['fact']
        period_totals['diff'] += grand_totals[m]['diff']
    if period_totals['plan'] > 0:
        period_totals['pct'] = float((period_totals['fact'] / period_totals['plan']) * 100)

    # CASHFLOW: планируемые поступления и фактические поступления
    cashflow_data = {m: {'plan': Decimal(0), 'fact': Decimal(0)} for m in range(1, 13)}
    cashflow_records = CashflowPlan.query.filter_by(year=year).all()
    for r in cashflow_records:
        if r.month in cashflow_data:
            cashflow_data[r.month]['plan'] = r.amount

    payments_q = db.session.query(func.extract('month', Payment.date).label('month'), func.sum(Payment.amount)).filter(func.extract('year', Payment.date) == year).group_by('month').all()
    for m, total in payments_q:
        if int(m) in cashflow_data:
            cashflow_data[int(m)]['fact'] = total

    cashflow_total_plan = sum(cashflow_data[m]['plan'] for m in range(1, 13))
    cashflow_total_fact = sum(cashflow_data[m]['fact'] for m in range(1, 13))

    # Итоги cashflow для выбранного периода
    cashflow_period_plan = sum(cashflow_data[m]['plan'] for m in selected_months)
    cashflow_period_fact = sum(cashflow_data[m]['fact'] for m in selected_months)

    # Накопительные (кумулятивные) показатели
    cumulative = {m: {'plan_balance': Decimal(0), 'fact_balance': Decimal(0)} for m in range(1, 13)}
    cum_in_plan = Decimal(0)
    cum_in_fact = Decimal(0)
    cum_out_plan = Decimal(0)
    cum_out_fact = Decimal(0)

    for m in range(1, 13):
        cum_in_plan += cashflow_data[m]['plan']
        cum_in_fact += cashflow_data[m]['fact']
        cum_out_plan += grand_totals[m]['plan']
        cum_out_fact += grand_totals[m]['fact']

        cumulative[m]['plan_balance'] = cum_in_fact - cum_out_plan  # фактический вход - плановый расход
        cumulative[m]['fact_balance'] = cum_in_fact - cum_out_fact  # фактический вход - фактический расход

    # Экспорт PDF (оставляем как есть, он всегда выгружает полный год)
    if export == 'pdf' and tab == 'budget':
        font_path = os.path.join(current_app.root_path, 'static', 'DejaVuSans.ttf').replace('\\', '/')
        rendered = render_template('finance/budget_pdf.html', year=year, data=data, grand_totals=grand_totals, items=items, month_names=MONTH_NAMES, font_path=font_path)
        return create_pdf_response(rendered, f"budget_{year}.pdf")
        
    # Обработка сохранения (POST)
    if request.method == 'POST':
        if current_user.role == 'executive':
            flash('Только просмотр')
            return redirect(url_for('finance.budget', year=year, month=target_month, tab=tab))

        act = request.form.get('action')
        
        if act == 'add_item': 
            db.session.add(BudgetItem(code=request.form.get('code'), name=request.form.get('name'), is_amortization=bool(request.form.get('is_amortization'))))
            db.session.commit()
            flash('Статья добавлена')
        elif act == 'edit_item':
            item = BudgetItem.query.get(request.form.get('id'))
            if item:
                item.code = request.form.get('code'); item.name = request.form.get('name'); item.is_amortization = bool(request.form.get('is_amortization'))
                db.session.commit()
                flash('Статья обновлена')
        elif act == 'delete_item':
            item_id = request.form.get('id')
            if not Expense.query.filter_by(budget_item_id=item_id).first():
                BudgetItem.query.filter_by(id=item_id).delete()
                db.session.commit()
                flash('Статья удалена')
            else: flash('Нельзя удалить статью с расходами')
        elif 'save_plan' in request.form:
            for k, v in request.form.items():
                if k.startswith('plan['):
                    match = re.findall(r'\[(\d+)\]\[(\d+)\]', k) # Ищем plan[ID][MONTH]
                    if match:
                        iid, m = int(match[0][0]), int(match[0][1])
                        p = BudgetPlan.query.filter_by(year=year, budget_item_id=iid, month=m).first()
                        if not p: 
                            p = BudgetPlan(year=year, budget_item_id=iid, month=m)
                            db.session.add(p)
                        try: p.amount = float(v.replace(' ', '').replace(',', '.') or 0)
                        except: pass
            db.session.commit()
            flash('План сохранен')
        elif 'save_cashflow' in request.form:
            for k, v in request.form.items():
                if k.startswith('cashflow['):
                    match = re.findall(r'\[(\d+)\]', k) # Ищем cashflow[MONTH]
                    if match:
                        m = int(match[0])
                        p = CashflowPlan.query.filter_by(year=year, month=m).first()
                        if not p:
                            p = CashflowPlan(year=year, month=m)
                            db.session.add(p)
                        try:
                            # Извлекаем только цифры, запятую и точку перед конвертацией
                            val_str = re.sub(r'[^\d.,-]', '', str(v)).replace(',', '.')
                            p.amount = float(val_str or 0)
                        except Exception as e:
                            print(f"Error parsing cashflow value: {v} -> {e}")
            db.session.commit()
            flash('План поступлений сохранен')
            
        return redirect(url_for('finance.budget', year=year, month=target_month, tab=tab))
        
    return render_template('finance/budget.html', 
                           year=year, 
                           data=data, 
                           grand_totals=grand_totals, 
                           items=items, 
                           month_names=MONTH_NAMES,
                           target_month=target_month,
                           period_label=period_label,
                           selected_months=selected_months,
                           period_end_month=period_end_month,
                           period_totals=period_totals,
                           active_tab=tab,
                           cashflow_data=cashflow_data,
                           cashflow_total_plan=cashflow_total_plan,
                           cashflow_total_fact=cashflow_total_fact,
                           cashflow_period_plan=cashflow_period_plan,
                           cashflow_period_fact=cashflow_period_fact,
                           cumulative=cumulative)

@bp.route('/budget/export')
@login_required
def budget_export():
    year = int(request.args.get('year', msk_now().year))
    target_month = request.args.get('month', 'all')
    selected_months, period_label = _budget_period_to_months(target_month)

    # 1. Загрузка данных (аналогично budget)
    items = sorted(BudgetItem.query.all(), key=natural_key)
    plans = BudgetPlan.query.filter_by(year=year).all()
    
    actual_month = func.coalesce(Expense.target_month, func.extract('month', Expense.date))
    actual_year = func.coalesce(Expense.target_year, func.extract('year', Expense.date))
    
    expenses_q = db.session.query(
        Expense.budget_item_id, 
        actual_month.label('month'), 
        func.sum(Expense.amount)
    ).filter(actual_year == year).group_by(Expense.budget_item_id, actual_month).all()

    data = {i.id: {'item': i, 'months': {m: {'plan': 0, 'fact': 0} for m in range(1, 13)}, 'total': {'plan':0, 'fact':0}} for i in items}
    
    for p in plans:
        if p.budget_item_id in data: data[p.budget_item_id]['months'][p.month]['plan'] = float(p.amount)
    for e in expenses_q:
        if e[0] in data: data[e[0]]['months'][e[1]]['fact'] = float(e[2])

    # Итоги строк
    for row in data.values():
        tp, tf = 0, 0
        for m in range(1, 13):
            tp += row['months'][m]['plan']
            tf += row['months'][m]['fact']
        row['total'] = {'plan': tp, 'fact': tf, 'diff': tp - tf}

    # 2. Создание Excel
    wb = Workbook()
    ws = wb.active
    ws.title = f"Бюджет {year}"
    
    # Стили
    font_bold = Font(bold=True)
    font_white = Font(bold=True, color="FFFFFF")
    fill_header = PatternFill(start_color="37474F", end_color="37474F", fill_type="solid") # Темный
    fill_total_col = PatternFill(start_color="FFF8E1", end_color="FFF8E1", fill_type="solid") # Желтоватый для Итого
    fill_months = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    align_center = Alignment(horizontal="center", vertical="center")
    
    # --- ШАПКА (СТРОКА 1) ---
    ws.cell(row=1, column=1, value="Код").font = font_bold
    ws.cell(row=1, column=2, value="Статья").font = font_bold
    
    # ИТОГО ГОД (Колонки 3-5)
    ws.merge_cells(start_row=1, start_column=3, end_row=1, end_column=5)
    c = ws.cell(row=1, column=3, value="ИТОГО ЗА ГОД")
    c.font = font_bold
    c.fill = fill_total_col
    c.alignment = align_center

    # ОТБОР ПО ПЕРИОДУ (колонки после года)
    col_idx = 6
    for m in selected_months:
        ws.merge_cells(start_row=1, start_column=col_idx, end_row=1, end_column=col_idx+2)
        c = ws.cell(row=1, column=col_idx, value=MONTH_NAMES.get(m, f"Месяц {m}"))
        c.font = font_bold
        c.fill = fill_months
        c.alignment = align_center
        col_idx += 3

    # --- ПОДЗАГОЛОВКИ (СТРОКА 2) ---
    headers = ["Код", "Статья", "План", "Факт", "Откл"]
    for _ in selected_months:
        headers.extend(["План", "Факт", "Откл"])

    for i, h in enumerate(headers, 1):
        cell = ws.cell(row=2, column=i, value=h)
        cell.border = border
        cell.alignment = align_center
        # Красим колонки ИТОГО
        if 3 <= i <= 5:
            cell.fill = fill_total_col
        else:
            cell.fill = fill_months

    # --- ДАННЫЕ (СТРОКИ) ---
    row_num = 3
    for i_id, row_data in data.items():
        # Код и Имя
        ws.cell(row=row_num, column=1, value=row_data['item'].code).border = border
        ws.cell(row=row_num, column=2, value=row_data['item'].name).border = border
        
        # ИТОГО ГОД
        tot = row_data['total']
        ws.cell(row=row_num, column=3, value=tot['plan']).fill = fill_total_col
        ws.cell(row=row_num, column=4, value=tot['fact']).fill = fill_total_col
        ws.cell(row=row_num, column=5, value=tot['diff']).fill = fill_total_col
        
        # ПЕРИОД (месяц/квартал/сезон)
        col_idx = 6
        for m in selected_months:
            d = row_data['months'][m]
            diff = d['plan'] - d['fact']
            ws.cell(row=row_num, column=col_idx, value=d['plan'])
            ws.cell(row=row_num, column=col_idx+1, value=d['fact'])
            ws.cell(row=row_num, column=col_idx+2, value=diff)
            col_idx += 3
            
        # Форматирование строки
        for c in range(1, col_idx):
            cell = ws.cell(row=row_num, column=c)
            cell.border = border
            if c > 2: cell.number_format = '#,##0.00' # Числовой формат
            
        row_num += 1

    # Ширина колонок
    ws.column_dimensions['A'].width = 8
    ws.column_dimensions['B'].width = 35
    for c in range(3, 100): ws.column_dimensions[get_column_letter(c)].width = 12

    # Сохранение
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    
    # Правильный ответ для скачивания
    from flask import make_response
    response = make_response(buf.getvalue())
    period_safe = period_label.replace(' ', '_').replace('/', '-')
    filename = f'Budget_{year}_{period_safe}.xlsx'
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return response

# ВАЖНО: Добавьте этот импорт в начале файла finance.py, если его нет
from openpyxl.utils import get_column_letter

@bp.route('/cost', methods=['GET', 'POST'])
@login_required
def cost_report():
    if current_user.role not in ['admin', 'executive']: 
        return redirect(url_for('orders.orders_list'))
    
    current_year = msk_now().year
    years = list(range(2017, 2051))
    
    if request.method == 'POST':
        if current_user.role == 'executive':
            flash('Только просмотр')
            return redirect(url_for('finance.cost_report'))

    if request.method == 'POST' and 'update_purchase_price' in request.form:
        p_id = request.form.get('plant_id')
        s_id = request.form.get('size_id')
        f_id = request.form.get('field_id')
        try:
            val = float(request.form.get('purchase_price'))
            sbs = StockBalance.query.filter_by(plant_id=p_id, size_id=s_id, field_id=f_id).all()
            if sbs:
                for sb in sbs: sb.purchase_price = val
            else: 
                db.session.add(StockBalance(plant_id=p_id, size_id=s_id, field_id=f_id, year=current_year, quantity=0, price=0, purchase_price=val))
            db.session.commit()
        except: pass
        return redirect(url_for('finance.cost_report', filter_year=request.form.get('selected_year'), tab='impact'))
    
    selected_year = int(request.args.get('filter_year', current_year))
    active_tab = request.args.get('tab', 'summary')
    selected_period = request.args.get('period', '')

    cd = calculate_cost_data(selected_year, period=selected_period)
    
    years_with_data = [y for y in years if cd['summary_totals'][y]['total'] > 0]
    visible_years = sorted(list(set(years_with_data) | {current_year, selected_year}))
    if 'hide_year' in request.args: 
        visible_years = [y for y in visible_years if y not in [int(x) for x in request.args.getlist('hide_year')]]
    
    active_amort_years = [y for y in years if cd['raw_amort_source'].get(y, Decimal(0)) > 0]
    
    raw_rows = get_detailed_stock_at_year_end(selected_year)
    
    f_plants = [int(x) for x in request.args.getlist('filter_plant')]
    f_fields = [int(x) for x in request.args.getlist('filter_field')]
    
    if f_plants: 
        raw_rows = [r for r in raw_rows if r['plant_id'] in f_plants]
    if f_fields:
        raw_rows = [r for r in raw_rows if r['field_id'] in f_fields]
    
    grouped_map = {}
    for r in raw_rows:
        key = (r['plant_id'], r['field_id'], r['year'])
        if key not in grouped_map:
            grouped_map[key] = {
                'plant_id': r['plant_id'], 'field_id': r['field_id'], 'year': r['year'],
                'name': r['name'], 'field': r['field'],
                'total_qty': 0, 'total_purchase_val': Decimal(0), 'sample_size_id': r['size_id']
            }
        grouped_map[key]['total_qty'] += r['quantity']
        grouped_map[key]['total_purchase_val'] += r['purchase_price'] * Decimal(r['quantity'])

    impact_rows = []
    impact_total_qty = 0
    impact_table_total = Decimal(0)
    
    for k, v in grouped_map.items():
        qty = v['total_qty']
        if qty == 0: continue
        avg_purchase_price = v['total_purchase_val'] / Decimal(qty)
        batch_year = v['year']
        ac_opex = cd['accum_opex_map'].get(batch_year, Decimal(0))
        ac_amort = cd['accum_amort_map'].get(batch_year, Decimal(0))
        total_unit_cost = avg_purchase_price + ac_opex + ac_amort
        total_cost_batch = total_unit_cost * Decimal(qty)
        
        impact_rows.append({
            'name': v['name'], 'field': v['field'], 'year': batch_year, 'quantity': qty,
            'purchase_price': avg_purchase_price, 'accum_opex': ac_opex, 'accum_amort': ac_amort,
            'total_unit_cost': total_unit_cost, 'total_cost': total_cost_batch,
            'plant_id': v['plant_id'], 'field_id': v['field_id'], 'size_id': v['sample_size_id']
        })
        impact_total_qty += qty
        impact_table_total += total_cost_batch

    impact_rows.sort(key=lambda x: (x['name'], x['year']))
    impact_summary_card = {'total_qty': impact_total_qty, 'year_cost': cd['summary_totals'][selected_year]['total'], 'unit_cost': cd['unit_cost_by_year'].get(selected_year, Decimal(0))}
    
    all_plants = sorted(Plant.query.all(), key=lambda x: x.name)
    all_items = sorted(cd['budget_items'].values(), key=lambda x: natural_key(x.name))
    
    op_rows, amort_rows = [], []
    for item in all_items:
        row = {'name': item.name, 'data': {}}
        for y in visible_years:
            fact = Decimal(0)
            for yr, iid, amt in cd['all_expenses']: 
                if int(yr) == y and iid == item.id: fact = amt
            qty = cd['qty_by_year'].get(y) or 1
            row['data'][y] = {'sum': fact, 'per_unit': fact / Decimal(qty) if cd['qty_by_year'].get(y) else Decimal(0)}
        (amort_rows if item.is_amortization else op_rows).append(row)
        
    if request.args.get('export') == 'excel': 
        return "Export functionality pending"
    
    # === БЛОК ЭКСПОРТА (ВСТАВИТЬ ПЕРЕД return render_template) ===
    if request.args.get('export') == 'excel':
        wb = Workbook()
        
        # Лист 1: Сводная таблица (Summary)
        ws = wb.active
        ws.title = "Свод Себестоимости"
        
        # Заголовки (Года)
        headers = ["Статья"]
        for y in visible_years:
            headers.extend([f"{y} (Сумма)", f"{y} (На ед.)"])
        ws.append(headers)
        
        # Данные операционные
        ws.append(["ОПЕРАЦИОННЫЕ РАСХОДЫ"])
        for row in op_rows:
            line = [row['name']]
            for y in visible_years:
                d = row['data'].get(y, {'sum': 0, 'per_unit': 0})
                line.extend([float(d['sum']), float(d['per_unit'])])
            ws.append(line)
            
        # Итого операционные
        ws.append([])
        line_total_op = ["ИТОГО ОПЕРАЦИОННЫЕ"]
        for y in visible_years:
            st = cd['summary_totals'][y]
            line_total_op.extend([float(st['op']), float(st['op_unit'])])
        ws.append(line_total_op)
        
        # Данные амортизация
        ws.append([])
        ws.append(["АМОРТИЗАЦИЯ"])
        for row in amort_rows:
            line = [row['name']]
            for y in visible_years:
                d = row['data'].get(y, {'sum': 0, 'per_unit': 0})
                line.extend([float(d['sum']), float(d['per_unit'])])
            ws.append(line)
            
        # Итого полная
        ws.append([])
        line_grand = ["ВСЕГО СЕБЕСТОИМОСТЬ (ЕД)"]
        for y in visible_years:
            st = cd['summary_totals'][y]
            # Пишем пустую сумму (не имеет смысла суммировать аморт+опер в абсолюте тут), пишем только на ЕД
            line_grand.extend(["", float(st['total_unit'])])
        ws.append(line_grand)

        # Лист 2: Влияние на цену (Impact)
        ws2 = wb.create_sheet("Детализация партий")
        ws2.append(["Растение", "Поле", "Год партии", "Кол-во", "Цена закупки", "Накоп. Опер", "Накоп. Аморт", "Итого Ед.", "Итого Сумма"])
        
        for r in impact_rows:
            ws2.append([
                r['name'], r['field'], r['year'], r['quantity'],
                float(r['purchase_price']), float(r['accum_opex']), float(r['accum_amort']),
                float(r['total_unit_cost']), float(r['total_cost'])
            ])

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        
        period_suffix = f"_{selected_period}" if selected_period else ""
        return send_file(
            buf, 
            download_name=f'cost_report_{selected_year}{period_suffix}.xlsx', 
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    return render_template('finance/cost.html', 
                           years=years, visible_years=visible_years, hidden_years=[], hidden_items=[], 
                           all_items=all_items, op_rows=op_rows, amort_rows=amort_rows,
                           summary_totals=cd['summary_totals'], waterfall=cd['amort_waterfall'], 
                           raw_source=cd['raw_amort_source'], active_amort_years=active_amort_years, 
                           impact_rows=impact_rows, cumulative_cost=cd['cumulative_cost'], 
                           selected_year=selected_year, selected_period=selected_period, current_year=current_year,
                           impact_summary_card=impact_summary_card, impact_table_total=impact_table_total, active_tab=active_tab, 
                           all_plants=all_plants, selected_plants=f_plants,
                           all_fields=Field.query.all(), selected_fields=f_fields)

@bp.route('/cost/save_override', methods=['POST'])
@login_required
def save_cost_override():
    if current_user.role != 'admin': return jsonify({'status': 'error'})
    try:
        year = int(request.form.get('year'))
        val_total = float(request.form.get('amount')) if request.form.get('amount') else None
        val_amort = float(request.form.get('amortization')) if request.form.get('amortization') else None

        override = UnitCostOverride.query.filter_by(year=year).first()
        if not override:
            override = UnitCostOverride(year=year)
            db.session.add(override)
        
        override.amount = val_total
        override.amortization = val_amort

        if override.amount is None and override.amortization is None:
            db.session.delete(override)
        
        db.session.commit()
        log_action(f"Обновил ручную себестоимость для {year} года")
        flash(f'Данные за {year} год обновлены')
    except Exception as e:
        db.session.rollback()
        flash(f'Ошибка: {e}')
    
    return redirect(url_for('finance.cost_report', tab='summary', filter_year=request.form.get('return_year', msk_now().year)))

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ СВЕРКИ ---
def get_reconciliation_data(c_id, f_start, f_end, mode, use_fixed_balance=False):
    rows = []
    opening_balance = Decimal(0)
    current_bal = Decimal(0)
    
    client = Client.query.get(c_id)
    fixed_balance = Decimal(str(client.fixed_balance)) if client and client.fixed_balance is not None else None
    fixed_balance_date = client.fixed_balance_date if client else None
    
    # 1. РАСЧЕТ ВХОДЯЩЕГО САЛЬДО
    if f_start:
        f_start_date = datetime.strptime(f_start, '%Y-%m-%d').date()
        
        if use_fixed_balance and fixed_balance is not None and fixed_balance_date is not None and fixed_balance_date < f_start_date:
            base_balance = fixed_balance
            base_date_filter = func.date(Document.date) > fixed_balance_date
            base_date_filter_order = func.date(Order.date) > fixed_balance_date
            base_date_filter_payment = func.date(Payment.date) > fixed_balance_date
        else:
            base_balance = Decimal(0)
            base_date_filter = True
            base_date_filter_order = True
            base_date_filter_payment = True

        past_shipments_real = db.session.query(func.sum(OrderItem.price * DocumentRow.quantity))\
            .select_from(Document)\
            .join(Order, Document.order_id == Order.id)\
            .join(DocumentRow, DocumentRow.document_id == Document.id)\
            .join(OrderItem, and_(
                OrderItem.order_id == Document.order_id,
                OrderItem.plant_id == DocumentRow.plant_id,
                OrderItem.size_id == DocumentRow.size_id,
                OrderItem.field_id == DocumentRow.field_from_id
            ))\
            .filter(Order.client_id == c_id, Document.doc_type == 'shipment', func.date(Document.date) < f_start, base_date_filter).scalar() or Decimal(0)

        past_shipments_ghost = db.session.query(func.sum(OrderItem.price * OrderItem.quantity))\
            .join(Order)\
            .filter(Order.client_id == c_id, Order.status == 'ghost', func.date(Order.date) < f_start, base_date_filter_order).scalar() or Decimal(0)

        past_payments = db.session.query(func.sum(Payment.amount))\
            .join(Order)\
            .filter(Order.client_id == c_id, func.date(Payment.date) < f_start, base_date_filter_payment).scalar() or Decimal(0)
        
        opening_balance = base_balance + (Decimal(str(past_shipments_real)) + Decimal(str(past_shipments_ghost))) - Decimal(str(past_payments))
        
        rows.append({
            'date': datetime.strptime(f_start, '%Y-%m-%d'), 
            'sort_date': datetime.strptime(f_start, '%Y-%m-%d'),
            'desc': 'Входящее сальдо', 'debit': Decimal(0), 'credit': Decimal(0), 
            'is_balance': True, 'doc_items': [], 'invoice_info': '', 'balance': opening_balance,
            'has_payments': False
        })
    elif use_fixed_balance and fixed_balance is not None and fixed_balance_date is not None:
        opening_balance = fixed_balance
        dt = datetime.combine(fixed_balance_date, datetime.min.time())
        rows.append({
            'date': dt, 
            'sort_date': dt,
            'desc': 'Фиксированное сальдо (Корректировка)', 'debit': Decimal(0), 'credit': Decimal(0), 
            'is_balance': True, 'doc_items': [], 'invoice_info': '', 'balance': opening_balance,
            'has_payments': False
        })

    # 2. СБОР ДАННЫХ
    raw_rows = []
    
    # Предварительно загружаем ID заказов, у которых есть оплаты (для кнопки гармошки)
    # Это нужно, чтобы знать, рисовать ли кнопку "показать оплаты" у отгрузки
    paid_orders_query = db.session.query(Payment.order_id).join(Order).filter(Order.client_id == c_id).distinct()
    paid_orders_ids = set(r[0] for r in paid_orders_query.all())

    # А. Документы (Отгрузки) — pre-load order items for price lookup
    ship_query = db.session.query(Document).join(Order).filter(Order.client_id == c_id, Document.doc_type == 'shipment')
    if f_start: ship_query = ship_query.filter(func.date(Document.date) >= f_start)
    if f_end: ship_query = ship_query.filter(func.date(Document.date) <= f_end)
    if use_fixed_balance and fixed_balance_date is not None:
        ship_query = ship_query.filter(func.date(Document.date) > fixed_balance_date)
    
    ship_docs = ship_query.all()
    ship_order_ids = list(set(d.order_id for d in ship_docs if d.order_id))
    oi_price_map = {}
    if ship_order_ids:
        oi_rows = OrderItem.query.filter(OrderItem.order_id.in_(ship_order_ids)).all()
        for oi in oi_rows:
            oi_price_map[(oi.order_id, oi.plant_id, oi.size_id, oi.field_id)] = oi.price

    for doc in ship_docs:
        doc_sum = Decimal(0); doc_items_list = []
        for r in doc.rows:
            raw_price = oi_price_map.get((doc.order_id, r.plant_id, r.size_id, r.field_from_id))
            price = Decimal(str(raw_price)) if raw_price is not None else Decimal(0)
            row_sum = price * Decimal(r.quantity)
            doc_sum += row_sum
            doc_items_list.append({'date': doc.date, 'name': r.plant.name if r.plant else '-', 'size': r.size.name if r.size else '-', 'field': r.field_from.name if r.field_from else '-', 'qty': r.quantity, 'price': price, 'sum': row_sum})
        
        inv_info = f"Счет № {doc.order.invoice_number} от {doc.order.invoice_date.strftime('%d.%m.%Y')}" if (doc.order and doc.order.invoice_number and doc.order.invoice_date) else (f"Счет № {doc.order.invoice_number}" if (doc.order and doc.order.invoice_number) else "")
        
        raw_rows.append({
            'date': doc.date, 
            'sort_date': doc.date, 
            'desc': f"Отгрузка (Заказ #{doc.order_id})", 
            'invoice_info': inv_info, 
            'debit': doc_sum, 
            'credit': Decimal(0), 
            'order_id': doc.order_id, 
            'doc_items': doc_items_list, 
            'is_grouped': False,
            'has_payments': (doc.order_id in paid_orders_ids) # Флаг наличия оплат
        })

    # Б. Ghost (Архивные отгрузки)
    ghost_query = Order.query.filter(Order.client_id == c_id, Order.status == 'ghost')
    if f_start: ghost_query = ghost_query.filter(func.date(Order.date) >= f_start)
    if f_end: ghost_query = ghost_query.filter(func.date(Order.date) <= f_end)
    if use_fixed_balance and fixed_balance_date is not None:
        ghost_query = ghost_query.filter(func.date(Order.date) > fixed_balance_date)
    
    for o in ghost_query.all():
        g_sum = Decimal(0); ghost_items_list = []
        for i in o.items:
            price = Decimal(str(i.price)) if i.price else Decimal(0)
            row_sum = price * Decimal(i.quantity)
            g_sum += row_sum
            ghost_items_list.append({'date': o.date, 'name': i.plant.name, 'size': i.size.name, 'field': i.field.name, 'qty': i.quantity, 'price': price, 'sum': row_sum})
        
        inv_info = f"Счет № {o.invoice_number} от {o.invoice_date.strftime('%d.%m.%Y')}" if (o.invoice_number and o.invoice_date) else (f"Счет № {o.invoice_number}" if o.invoice_number else "")
        
        raw_rows.append({
            'date': o.date, 
            'sort_date': o.date, 
            'desc': f"Архив. отгрузка #{o.id}", 
            'invoice_info': inv_info, 
            'debit': g_sum, 
            'credit': Decimal(0), 
            'order_id': o.id, 
            'doc_items': ghost_items_list, 
            'is_grouped': False,
            'has_payments': (o.id in paid_orders_ids) # Флаг наличия оплат
        })

    # В. Оплаты
    # Изменили запрос: берем Payment и Order целиком, чтобы достать invoice_number
    pay_query = db.session.query(Payment, Order).join(Order).filter(Order.client_id == c_id)
    if f_start: pay_query = pay_query.filter(func.date(Payment.date) >= f_start)
    if f_end: pay_query = pay_query.filter(func.date(Payment.date) <= f_end)
    if use_fixed_balance and fixed_balance_date is not None:
        pay_query = pay_query.filter(func.date(Payment.date) > fixed_balance_date)
    
    for p, o in pay_query.all():
        display_date = datetime.combine(p.date, datetime.min.time())
        sort_date_val = (o.date + timedelta(seconds=1)) if o.date else display_date
        
        # Формируем информацию о счете так же, как в отгрузках, для группировки
        inv_info = f"Счет № {o.invoice_number} от {o.invoice_date.strftime('%d.%m.%Y')}" if (o.invoice_number and o.invoice_date) else (f"Счет № {o.invoice_number}" if o.invoice_number else "")
        
        # Создаем элемент детализации для оплаты, чтобы показать его внутри группы
        payment_item_detail = {
            'date': display_date,
            'name': f"Оплата от {p.date.strftime('%d.%m.%Y')}",
            'size': '-',
            'field': '-',
            'qty': 1,
            'price': Decimal(str(p.amount)),
            'sum': Decimal(str(p.amount)),
            'type': 'payment' # Метка типа строки
        }

        raw_rows.append({
            'date': display_date, 
            'sort_date': sort_date_val, 
            'desc': f"Оплата (по заказу #{p.order_id})", 
            'debit': Decimal(0), 
            'credit': Decimal(str(p.amount)), 
            'order_id': p.order_id, 
            'doc_items': [payment_item_detail], # Кладем детализацию сюда
            'invoice_info': inv_info, # Теперь здесь есть номер счета!
            'is_grouped': False,
            'has_payments': False,
            'is_payment_row': True 
        })

    # 3. ГРУППИРОВКА И СОРТИРОВКА
    if mode == 'grouped':
        grouped_map = {}
        final_rows = []
        
        for r in raw_rows:
            # Группируем всё (и отгрузки, и оплаты), если есть номер счета
            if r.get('invoice_info'):
                key = r['invoice_info']
                
                if key not in grouped_map:
                    # Создаем новую группу
                    new_group = r.copy()
                    new_group['is_grouped'] = True
                    # Если первая запись была оплатой, меняем описание, иначе оставляем "Реализация..." или меняем на общее
                    new_group['desc'] = "Операции по счету" 
                    new_group['doc_items'] = list(r['doc_items'])
                    # Инициализируем суммы, так как r.copy() взяло только текущие значения
                    new_group['debit'] = r['debit']
                    new_group['credit'] = r['credit']
                    
                    grouped_map[key] = new_group
                else:
                    # Добавляем к существующей группе
                    grouped_map[key]['debit'] += r['debit']
                    grouped_map[key]['credit'] += r['credit'] # Суммируем оплаты
                    grouped_map[key]['doc_items'].extend(r['doc_items'])
                    
                    # Обновляем даты по самой ранней операции
                    if r['sort_date'] < grouped_map[key]['sort_date']: grouped_map[key]['sort_date'] = r['sort_date']
                    if r['date'] < grouped_map[key]['date']: grouped_map[key]['date'] = r['date']
                    
                    # Если это была запись отгрузки (есть order_id с флагом), сохраняем флаг
                    if r.get('has_payments'): grouped_map[key]['has_payments'] = True
            else:
                # Если счета нет - оставляем как есть (например, входящее сальдо или оплата без привязки)
                final_rows.append(r)
        
        # Сортируем детали внутри групп по дате
        for grp in grouped_map.values(): 
            grp['doc_items'].sort(key=lambda x: x['date'])
            
        final_rows.extend(grouped_map.values())
        raw_rows = final_rows

    # Сортировка по дате привязки
    raw_rows.sort(key=lambda x: x['sort_date'])
    
    # 4. РАСЧЕТ ИТОГОВОГО САЛЬДО И ОБОРОТОВ
    current_bal = opening_balance
    
    # Переменные для итогов по колонкам
    total_debit = Decimal(0)
    total_credit = Decimal(0)

    for r in raw_rows:
        # Суммируем обороты за период (не считая входящее сальдо, если оно реализовано как строка)
        # В твоем коде входящее сальдо имеет флаг 'is_balance', его в обороты включать не надо
        if not r.get('is_balance'):
            total_debit += r['debit']
            total_credit += r['credit']

        current_bal += r['debit'] - r['credit']
        r['balance'] = current_bal
        rows.append(r)
        
    return rows, current_bal, total_debit, total_credit

@bp.route('/reports/reconciliation')
@login_required
def reports_reconciliation():
    clients = Client.query.all()
    cid = request.args.get('client_id')
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    mode = request.args.get('mode', 'grouped')
    
    rows = []
    total_bal = Decimal(0)
    t_debit = Decimal(0)
    t_credit = Decimal(0)
    client_name = ''
    barter_orders = []
    
    current_url = request.url

    if cid:
        try:
            client_obj = Client.query.get(int(cid))
            client_name = client_obj.name if client_obj else "Неизвестный"
            rows, total_bal, t_debit, t_credit = get_reconciliation_data(int(cid), f_start, f_end, mode)
            
            # Получаем бартерные заказы клиента
            b_orders = Order.query.filter(Order.client_id == int(cid), Order.is_barter == True, Order.status != 'canceled', Order.is_deleted == False).all()
            for bo in b_orders:
                bo_total = bo.total_sum
                bo_paid = sum(e.amount for e in bo.barter_expenses)
                barter_orders.append({
                    'order': bo,
                    'total': bo_total,
                    'paid': bo_paid,
                    'remaining': bo_total - bo_paid
                })
        except Exception as e:
            flash(f"Ошибка: {str(e)}")
            print(f"RECONCILIATION ERROR: {e}")

    # Передаем debit и credit в totals
    return render_template('finance/reports.html', active_tab='reconciliation', clients=clients, rows=rows, 
                           totals={'balance': total_bal, 'debit': t_debit, 'credit': t_credit}, 
                           filters={'client_id': cid, 'start': f_start, 'end': f_end},
                           client_name=client_name, mode=mode,
                           current_url=current_url,
                           barter_orders=barter_orders)

@bp.route('/reports/reconciliation/export')
@login_required
def reports_reconciliation_export():
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
    
    cid = request.args.get('client_id')
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    mode = request.args.get('mode', 'grouped')
    
    if not cid: return redirect(url_for('finance.reports_reconciliation'))
    
    client_obj = Client.query.get(int(cid))
    client_name = client_obj.name if client_obj else "Неизвестный"
    
    # Получаем данные (ИСПРАВЛЕНО: принимаем 4 значения)
    rows, final_balance, t_debit, t_credit = get_reconciliation_data(int(cid), f_start, f_end, mode)
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Акт сверки"
    
    # Стили
    bold_font = Font(bold=True)
    italic_font = Font(italic=True, size=10, color="555555") # Для деталей
    header_fill = PatternFill(start_color="E0F2F1", end_color="E0F2F1", fill_type="solid")
    detail_fill = PatternFill(start_color="F9FAFB", end_color="F9FAFB", fill_type="solid") # Фон для деталей
    border_style = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    align_center = Alignment(horizontal='center', vertical='center')
    align_right = Alignment(horizontal='right', vertical='center')
    
    # Заголовок
    ws.merge_cells('A1:F1')
    ws['A1'] = f"Акт сверки с {client_name}"
    ws['A1'].font = Font(size=14, bold=True, color="2E7D32")
    ws['A1'].alignment = align_center
    
    ws.merge_cells('A2:F2')
    period_str = f"Период: {datetime.strptime(f_start, '%Y-%m-%d').strftime('%d.%m.%Y') if f_start else '...'} - {datetime.strptime(f_end, '%Y-%m-%d').strftime('%d.%m.%Y') if f_end else '...'}"
    ws['A2'] = period_str
    ws['A2'].alignment = align_center
    
    # Шапка таблицы
    headers = ["Дата", "Документ / Операция", "Основание (Счет)", "Дебет (Отгрузка)", "Кредит (Оплата)", "Сальдо"]
    ws.append([]) 
    ws.append(headers) 
    
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=4, column=col_num)
        cell.font = bold_font
        cell.fill = header_fill
        cell.border = border_style
        cell.alignment = align_center

    # Данные
    for r in rows:
        # ОСНОВНАЯ СТРОКА
        row_cells = [
            r['date'].strftime('%d.%m.%Y'),
            r['desc'],
            r['invoice_info'] or '-',
            float(r['debit']) if r['debit'] else 0,
            float(r['credit']) if r['credit'] else 0,
            float(r['balance'])
        ]
        ws.append(row_cells)
        curr_row = ws.max_row
        
        # Стилизация основной строки
        for i in range(1, 7):
            cell = ws.cell(row=curr_row, column=i)
            cell.border = border_style
            if i in [4, 5, 6]: 
                cell.number_format = '#,##0.00'
                cell.alignment = align_right
            
            if r.get('is_grouped') or r.get('is_balance'):
                cell.font = bold_font
            
            if r.get('is_balance'): cell.fill = PatternFill(start_color="FFF3E0", fill_type="solid")
            elif r.get('is_grouped'): cell.fill = PatternFill(start_color="FFF8E1", fill_type="solid") # Желтоватый для группы

        # ДЕТАЛИЗАЦИЯ (ГАРМОШКА) В EXCEL
        if r.get('is_grouped') and r.get('doc_items'):
            for item in r['doc_items']:
                # Формируем строку детализации
                # Смещаем данные: 
                # A: Дата отгрузки
                # B: Название + Размер + Поле
                # C: Пусто
                # D: Сумма (в дебет)
                # E-F: Пусто
                
                item_desc = f"   ↳ {item['name']} ({item['size']}) / {item['field']} / {item['qty']} шт x {float(item['price'])}"
                
                detail_cells = [
                    item['date'].strftime('%d.%m.%Y'), # Дата конкретной отгрузки
                    item_desc,
                    "",
                    float(item['sum']),
                    "",
                    ""
                ]
                ws.append(detail_cells)
                det_row = ws.max_row
                
                # Стилизация детали
                for i in range(1, 7):
                    cell = ws.cell(row=det_row, column=i)
                    cell.font = italic_font
                    cell.fill = detail_fill
                    cell.border = border_style # Можно убрать border, если хочется "воздуха"
                    
                    if i == 4: # Сумма
                        cell.number_format = '#,##0.00'
                        cell.alignment = align_right

    # Итоги
    ws.append([])
    # ИСПРАВЛЕНО: Добавляем t_debit и t_credit в строку итогов
    ws.append(["", "", "ИТОГО ОБОРОТЫ И САЛЬДО:", float(t_debit), float(t_credit), float(final_balance)])
    last_row = ws.max_row
    
    ws.cell(row=last_row, column=3).alignment = align_right
    ws.cell(row=last_row, column=3).font = bold_font
    
    # Стилизуем итоги оборотов (Дебет и Кредит)
    for col_idx in [4, 5]:
        cell = ws.cell(row=last_row, column=col_idx)
        cell.font = bold_font
        cell.number_format = '#,##0.00'
        cell.border = border_style
        cell.alignment = align_right

    # Стилизуем Сальдо
    bal_cell = ws.cell(row=last_row, column=6)
    bal_cell.font = Font(bold=True, color="C62828" if final_balance > 0 else "2E7D32")
    bal_cell.number_format = '#,##0.00'
    bal_cell.border = border_style

    ws.column_dimensions['A'].width = 12
    ws.column_dimensions['B'].width = 50 # Шире для деталей
    ws.column_dimensions['C'].width = 25
    ws.column_dimensions['D'].width = 15
    ws.column_dimensions['E'].width = 15
    ws.column_dimensions['F'].width = 15

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    
    filename = f"Act_{client_name}_{f_start or ''}_{f_end or ''}.xlsx"
    return send_file(buf, download_name=filename, as_attachment=True)

@bp.route('/reports/turnover')
@login_required
def reports_turnover():
    if current_user.role == 'user2':
        return redirect(url_for('main.index'))
    
    f_plant = request.args.get('plant_id', type=int)
    f_field = request.args.get('field_id', type=int)
    f_year = request.args.get('year', type=int)
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    
    rows =[]
    opening_balance = 0
    closing_balance = 0

    if f_plant and f_field and f_year:
        doc_rows = db.session.query(DocumentRow, Document).join(Document).filter(
            DocumentRow.plant_id == f_plant, DocumentRow.year == f_year, 
            or_(DocumentRow.field_from_id == f_field, DocumentRow.field_to_id == f_field)
        ).all()
        
        ghosts = db.session.query(OrderItem, Order).join(Order).filter(
            OrderItem.plant_id == f_plant, OrderItem.field_id == f_field, OrderItem.year == f_year, Order.status == 'ghost'
        ).all()
        
        all_events =[]
        for r, d in doc_rows:
            qty = r.quantity
            is_income = False
            is_expense = False
            doc_name = ""
            
            if d.doc_type in ('income', 'correction'):
                if r.field_to_id == f_field:
                    is_income = True
                    # НОВАЯ ЛОГИКА: если год <= 2025, то это "Первоначальный импорт остатков"
                    if d.date.year <= 2025:
                        doc_name = "Первоначальный импорт остатков"
                    else:
                        doc_name = "Поступление" if d.doc_type == 'income' else "Импорт"
            elif d.doc_type in ('writeoff', 'shipment'):
                if r.field_from_id == f_field:
                    is_expense = True
                    doc_name = "Списание" if d.doc_type == 'writeoff' else f"Отгрузка (Заказ #{d.order_id})"
            elif d.doc_type == 'move':
                if r.field_to_id == f_field:
                    is_income = True
                    doc_name = "Перемещение (ВХ)"
                elif r.field_from_id == f_field:
                    is_expense = True
                    doc_name = "Перемещение (ИСХ)"
            
            if is_income or is_expense:
                all_events.append({
                    'date': d.date, 
                    'type': 'real', 
                    'doc_name': doc_name, 
                    'income': qty if is_income else 0, 
                    'expense': qty if is_expense else 0, 
                    'author': d.user.username if d.user else '', 
                    'comment': d.comment, 
                    'size_name': r.size.name if r.size else ''
                })

        for item, order in ghosts:
            all_events.append({
                'date': order.date, 
                'type': 'ghost', 
                'doc_name': f"Ghost #{order.id}", 
                'income': 0, 
                'expense': item.quantity, 
                'author': 'Ghost', 
                'comment': order.client.name, 
                'size_name': item.size.name if item.size else ''
            })

        all_events.sort(key=lambda x: x.get('date'))
        
        current_balance = 0
        start_dt = datetime.strptime(f_start, '%Y-%m-%d') if f_start else datetime.min
        end_dt = datetime.strptime(f_end, '%Y-%m-%d').replace(hour=23, minute=59) if f_end else datetime.max

        for ev in all_events:
            if ev.get('type') == 'real':
                current_balance += ev.get('income', 0) - ev.get('expense', 0)
            
            ev['balance'] = current_balance
            
            if ev.get('date') < start_dt and ev.get('type') == 'real':
                opening_balance = current_balance
            
            if start_dt <= ev.get('date') <= end_dt:
                rows.append(ev)
        
        closing_balance = current_balance

    return render_template('finance/reports.html', active_tab='turnover', rows=rows, opening_balance=opening_balance, closing_balance=closing_balance, plants=Plant.query.all(), sizes=Size.query.all(), fields=Field.query.all(), filters=request.args)

@bp.route('/reports/financial')
@login_required
def reports_financial():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    year = int(request.args.get('year', msk_now().year))
    
    # Исправление: Добавлена обработка NULL (None) при суммировании
    expenses = db.session.query(Expense.payment_type, func.sum(Expense.amount)).filter(func.extract('year', Expense.date) == year).group_by(Expense.payment_type).all()
    
    # Безопасное получение сумм: если расхода нет, ставим 0
    ec = next((x[1] for x in expenses if x[0] == 'cash'), Decimal(0))
    if ec is None: ec = Decimal(0)
    
    ecl = next((x[1] for x in expenses if x[0] == 'cashless'), Decimal(0))
    if ecl is None: ecl = Decimal(0)
    
    cd_prev = calculate_cost_data(year - 1)
    debt, breakdown = calculate_investor_debt(year, cd_prev['accumulated_costs_map'])
    
    total_exp = ec + ecl + (ec * Decimal('0.15')) + debt
    
    orders = Order.query.filter(func.extract('year', Order.date) == year, Order.status != 'canceled', Order.is_deleted == False).all()
    
    val_shipped = Decimal(0); val_reserved = Decimal(0)
    for o in orders:
        for item in o.items:
            # Защита от None в цене
            price = item.price if item.price is not None else Decimal(0)
            val_shipped += Decimal(item.shipped_quantity) * price
            reserved_qty = item.quantity - item.shipped_quantity
            if reserved_qty > 0: val_reserved += Decimal(reserved_qty) * price
            
    data = {
        'exp_cash': ec, 
        'exp_cashless': ecl, 
        'tax_cash_15': ec * Decimal('0.15'), 
        'investor_debt': debt, 
        'investor_breakdown': breakdown, 
        'total_expenses': total_exp, 
        'realized_revenue': val_shipped, 
        'net_profit': val_shipped - total_exp, 
        'val_reserved': val_reserved, 
        'val_shipped': val_shipped, 
        'val_partial_shipped': 0, 
        'val_partial_unshipped': 0, 
        'total_sales_potential': val_shipped + val_reserved
    }
    return render_template('finance/reports.html', active_tab='financial', years=range(2017, 2051), selected_year=year, data=data)

@bp.route('/reports/margin')
@login_required
def reports_margin():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    year = int(request.args.get('year', msk_now().year))
    view = (request.args.get('view') or 'snapshot').lower()
    if view not in ('snapshot', 'dynamics'):
        view = 'snapshot'

    # Получаем списки ID для фильтрации
    f_plants = [int(x) for x in request.args.getlist('filter_plant')]
    f_sizes = [int(x) for x in request.args.getlist('filter_size')]
    f_fields = [int(x) for x in request.args.getlist('filter_field')]

    rows = []
    dynamics = None

    if view == 'snapshot':
        cost_data = calculate_cost_data(year)
        accum = cost_data['accumulated_costs_map']

        for r in get_detailed_stock_at_year_end(year):
            if f_plants and r['plant_id'] not in f_plants: continue
            if f_sizes and r['size_id'] not in f_sizes: continue
            if f_fields and r['field_id'] not in f_fields: continue

            tc = r['purchase_price'] + accum.get(r['year'], Decimal(0))
            margin = r['selling_price'] - tc
            margin_percent = (margin / r['selling_price'] * 100) if r['selling_price'] > 0 else 0

            rows.append({
                'name': r['name'], 'size': r['size'], 'field': r['field'],
                'price': r['selling_price'], 'cost': tc,
                'margin': margin, 'margin_percent': margin_percent,
                'size_name': r['size'],
            })
    else:
        dynamics = _build_margin_dynamics(
            year_from=int(request.args.get('year_from') or (year - 4)),
            year_to=int(request.args.get('year_to') or year),
            f_plants=f_plants, f_sizes=f_sizes, f_fields=f_fields,
        )

    return render_template('finance/reports.html', active_tab='margin', years=range(2017, 2051),
                           selected_year=year, rows=rows,
                           view=view, dynamics=dynamics,
                           all_plants=Plant.query.order_by(Plant.name).all(),
                           all_sizes=Size.query.all(),
                           all_fields=Field.query.all(),
                           filters={'plants': f_plants, 'sizes': f_sizes, 'fields': f_fields})


def _build_margin_dynamics(year_from, year_to, f_plants=None, f_sizes=None, f_fields=None):
    """Динамика фактической маржинальности в разрезе растения × года отгрузки.

    Маржа считается по фактически отгруженным позициям (shipped_quantity > 0):
    - выручка = price * shipped_quantity (из OrderItem);
    - себестоимость = (purchase_price партии + накопленная себестоимость до year_of_sale - 1) * qty;
    - год отгрузки определяем по Order.date.year (исключаем canceled/ghost/is_deleted).

    Возвращает структуру, удобную для рендера:
    {
        'years': [2020, 2021, ...],
        'plants': [ {plant_id, name, cells: {year: {qty, revenue, cost, margin, pct}},
                     total_qty, total_revenue, total_cost, total_margin, total_pct,
                     trend_delta, spark_points: [(idx, pct), ...],
                     sizes: [ { size_name, cells: {...}, total_* } ]
                   }, ...],
        'year_totals': {year: {qty, revenue, cost, margin, pct}},
        'grand_total': {qty, revenue, cost, margin, pct},
    }
    """
    from sqlalchemy.orm import joinedload

    if year_from > year_to:
        year_from, year_to = year_to, year_from
    years = list(range(year_from, year_to + 1))
    f_plants = f_plants or []
    f_sizes = f_sizes or []
    f_fields = f_fields or []

    # Кеш цен закупки по партиям
    stock_prices = {
        (sb.plant_id, sb.size_id, sb.field_id, sb.year): (sb.purchase_price or Decimal(0))
        for sb in StockBalance.query.all()
    }
    # Кеш накопленной себестоимости: {year_basis: {batch_year: Decimal}}
    costs_cache = {}

    q = db.session.query(OrderItem, Order).join(Order).options(
        joinedload(OrderItem.plant), joinedload(OrderItem.size)
    ).filter(
        Order.status != 'canceled',
        Order.status != 'ghost',
        Order.is_deleted == False,  # noqa: E712
        OrderItem.shipped_quantity > 0,
        func.extract('year', Order.date) >= year_from,
        func.extract('year', Order.date) <= year_to,
    )
    if f_plants:
        q = q.filter(OrderItem.plant_id.in_(f_plants))
    if f_sizes:
        q = q.filter(OrderItem.size_id.in_(f_sizes))
    if f_fields:
        q = q.filter(OrderItem.field_id.in_(f_fields))

    def _empty_cell():
        return {'qty': Decimal(0), 'revenue': Decimal(0), 'cost': Decimal(0),
                'margin': Decimal(0), 'pct': 0.0}

    plants_map = {}  # plant_id -> {name, cells{year:_cell}, sizes{size_name:_cell}, ...}

    for item, order in q.all():
        ysale = order.date.year
        if ysale not in years:
            continue
        qty = Decimal(item.shipped_quantity or 0)
        if qty <= 0:
            continue
        price = item.price if item.price is not None else Decimal(0)
        revenue = price * qty

        basis = ysale - 1
        if basis not in costs_cache:
            costs_cache[basis] = calculate_cost_data(basis).get('accumulated_costs_map', {})
        accum_unit = costs_cache[basis].get(item.year, Decimal(0))
        purch_unit = stock_prices.get((item.plant_id, item.size_id, item.field_id, item.year), Decimal(0))
        cost_unit = (purch_unit or Decimal(0)) + (accum_unit or Decimal(0))
        cost = cost_unit * qty

        pid = item.plant_id
        pdata = plants_map.setdefault(pid, {
            'plant_id': pid,
            'name': item.plant.name if item.plant else f'Растение #{pid}',
            'cells': {y: _empty_cell() for y in years},
            'sizes_map': {},
            'total_qty': Decimal(0), 'total_revenue': Decimal(0),
            'total_cost': Decimal(0), 'total_margin': Decimal(0),
        })

        c = pdata['cells'][ysale]
        c['qty'] += qty
        c['revenue'] += revenue
        c['cost'] += cost
        c['margin'] = c['revenue'] - c['cost']

        pdata['total_qty'] += qty
        pdata['total_revenue'] += revenue
        pdata['total_cost'] += cost

        # Детализация по размеру
        size_name = item.size.name if item.size else '—'
        sdata = pdata['sizes_map'].setdefault(size_name, {
            'size_name': size_name,
            'cells': {y: _empty_cell() for y in years},
            'total_qty': Decimal(0), 'total_revenue': Decimal(0),
            'total_cost': Decimal(0), 'total_margin': Decimal(0),
        })
        sc = sdata['cells'][ysale]
        sc['qty'] += qty
        sc['revenue'] += revenue
        sc['cost'] += cost
        sc['margin'] = sc['revenue'] - sc['cost']
        sdata['total_qty'] += qty
        sdata['total_revenue'] += revenue
        sdata['total_cost'] += cost

    # Постобработка: проценты, тренд, sparkline
    def _finalize(d):
        for y, c in d['cells'].items():
            c['pct'] = float(c['margin'] / c['revenue'] * 100) if c['revenue'] > 0 else 0.0
        d['total_margin'] = d['total_revenue'] - d['total_cost']
        d['total_pct'] = float(d['total_margin'] / d['total_revenue'] * 100) if d['total_revenue'] > 0 else 0.0
        non_empty = [(y, d['cells'][y]['pct']) for y in years if d['cells'][y]['revenue'] > 0]
        if len(non_empty) >= 2:
            d['trend_delta'] = non_empty[-1][1] - non_empty[0][1]
            d['first_year'] = non_empty[0][0]
            d['last_year'] = non_empty[-1][0]
        else:
            d['trend_delta'] = 0.0
            d['first_year'] = d['last_year'] = None
        d['spark_points'] = [(i, d['cells'][y]['pct'] if d['cells'][y]['revenue'] > 0 else None)
                             for i, y in enumerate(years)]

    year_totals = {y: _empty_cell() for y in years}
    plants = []
    for pid, pdata in plants_map.items():
        _finalize(pdata)
        # Сортируем размеры по выручке
        sizes = sorted(pdata['sizes_map'].values(), key=lambda s: s['total_revenue'], reverse=True)
        for s in sizes:
            _finalize(s)
        pdata['sizes'] = sizes
        pdata.pop('sizes_map', None)
        plants.append(pdata)
        # Копим общие итоги по годам
        for y in years:
            c = pdata['cells'][y]
            yt = year_totals[y]
            yt['qty'] += c['qty']
            yt['revenue'] += c['revenue']
            yt['cost'] += c['cost']
            yt['margin'] = yt['revenue'] - yt['cost']

    for y, yt in year_totals.items():
        yt['pct'] = float(yt['margin'] / yt['revenue'] * 100) if yt['revenue'] > 0 else 0.0

    grand = {'qty': Decimal(0), 'revenue': Decimal(0), 'cost': Decimal(0), 'margin': Decimal(0), 'pct': 0.0}
    for yt in year_totals.values():
        grand['qty'] += yt['qty']
        grand['revenue'] += yt['revenue']
        grand['cost'] += yt['cost']
    grand['margin'] = grand['revenue'] - grand['cost']
    grand['pct'] = float(grand['margin'] / grand['revenue'] * 100) if grand['revenue'] > 0 else 0.0

    # Сортируем растения по выручке (от большего к меньшему)
    plants.sort(key=lambda p: p['total_revenue'], reverse=True)

    return {
        'years': years,
        'year_from': year_from,
        'year_to': year_to,
        'plants': plants,
        'year_totals': year_totals,
        'grand_total': grand,
    }

@bp.route('/reports/investor')
@login_required
def reports_investor():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    real_picture = request.args.get('real_picture', '0') == '1'
    use_fixed_balance = not real_picture
    
    # Списки ID для мульти-фильтра
    f_fields = [int(x) for x in request.args.getlist('filter_field')]
    f_plants = [int(x) for x in request.args.getlist('filter_plant')]
    
    # Условия выборки
    conds = [
        Order.status != 'canceled', 
        Order.is_deleted == False, 
        OrderItem.shipped_quantity > 0, 
        Field.investor_id.isnot(None),
        or_(
            func.extract('year', Order.date) < 2025, 
            Order.client_id != Field.investor_id
        )
    ]
    
    if f_start: conds.append(func.date(Order.date) >= f_start)
    if f_end: conds.append(func.date(Order.date) <= f_end)
    if f_fields: conds.append(OrderItem.field_id.in_(f_fields))
    if f_plants: conds.append(OrderItem.plant_id.in_(f_plants))
    
    # Запрос детализации (Главный запрос)
    details_query = db.session.query(OrderItem, Order, Field, Client)\
        .select_from(OrderItem)\
        .join(Order).join(Field).join(Client, Order.client_id == Client.id)\
        .filter(and_(*conds))\
        .order_by(Order.date.desc())
    
    raw_details = details_query.all()
    
    # Кэши
    stock_prices = { (sb.plant_id, sb.size_id, sb.field_id, sb.year): sb.purchase_price for sb in StockBalance.query.all() }
    costs_cache = {} # { year: { batch_year: amount } }
    
    investor_details = []
    investors_summary = {} # { inv_id: {name, share, balance_act, net_result} }
    
    total_period_share = Decimal(0) # Общая сумма доли за период по всем строкам

    for item, order, field, client in raw_details:
        # 1. Основные данные
        qty_det = Decimal(item.shipped_quantity)
        price_det = item.price if item.price is not None else Decimal(0)
        is_return = (field.investor_id == order.client_id)
        
        # 2. Цена закупки (из партии)
        purch_price = stock_prices.get((item.plant_id, item.size_id, item.field_id, item.year), Decimal(0))
        
        # 3. Накопленная себестоимость (База = Год продажи - 1)
        # Мы НЕ смотрим в базу (StockBalance.current_total_cost), мы считаем динамически через сервис
        ship_year = order.date.year
        calc_basis_year = ship_year - 1
        
        if calc_basis_year not in costs_cache:
            costs_cache[calc_basis_year] = calculate_cost_data(calc_basis_year)['accumulated_costs_map']
            
        accum_cost = costs_cache[calc_basis_year].get(item.year, Decimal(0))
        
        # 4. Финальная математика
        full_cost_unit = purch_price + accum_cost
        share_unit = (price_det / 2) - full_cost_unit
        total_share_row = share_unit * qty_det
        
        # 5. Сбор данных в список
        investor_details.append({
            'date': order.date, 
            'order_id': order.id, 
            'plant': item.plant.name, 
            'size': item.size.name,
            'field': field.name, 
            'batch_year': item.year, # Год партии
            'investor': field.investor.name if field.investor else '-',
            'buyer': client.name, 
            'type_code': 2 if is_return else 1,
            'qty': int(qty_det), 
            'price': price_det,
            
            # Новые поля
            'full_cost_unit': full_cost_unit, # Полная себестоимость
            'share_unit': share_unit,         # Доля с 1 шт
            'total_share': total_share_row    # Итого доля строки
        })
        
        # 6. Агрегация для сводки
        total_period_share += total_share_row
        
        if field.investor_id:
            inv_id = field.investor_id
            if inv_id not in investors_summary:
                investors_summary[inv_id] = {'name': field.investor.name, 'share': Decimal(0), 'balance_act': Decimal(0), 'net_result': Decimal(0)}
            investors_summary[inv_id]['share'] += total_share_row

    # 7. Догружаем баланс из Акта сверки (Сальдо)
    for inv_id, data in investors_summary.items():
        # Берем сальдо из акта сверки именно за этот период
        _, balance, _, _ = get_reconciliation_data(inv_id, f_start, f_end, 'grouped', use_fixed_balance=use_fixed_balance)
        data['balance_act'] = balance
        # Итого = Сальдо (Долг клиента) - Доля (Наш долг)
        data['net_result'] = balance - data['share']

    return render_template('finance/reports.html', active_tab='investor', 
                           investor_summary=investors_summary,
                           investor_details=investor_details,
                           total_period_share=total_period_share,
                           filters={'start': f_start, 'end': f_end, 'fields': f_fields, 'plants': f_plants}, 
                           all_fields=Field.query.all(), all_plants=Plant.query.all(),
                           real_picture=real_picture)

@bp.route('/reports/investor/export')
@login_required
def reports_investor_export():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    
    # Получаем параметры (так же как в view)
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    real_picture = request.args.get('real_picture', '0') == '1'
    use_fixed_balance = not real_picture
    f_fields = [int(x) for x in request.args.getlist('filter_field')]
    f_plants = [int(x) for x in request.args.getlist('filter_plant')]
    
    # Базовый запрос
    conds = [
        Order.status != 'canceled', 
        Order.is_deleted == False, 
        OrderItem.shipped_quantity > 0, 
        Field.investor_id.isnot(None),
        or_(func.extract('year', Order.date) < 2025, Order.client_id != Field.investor_id)
    ]
    
    if f_start: conds.append(func.date(Order.date) >= f_start)
    if f_end: conds.append(func.date(Order.date) <= f_end)
    if f_fields: conds.append(OrderItem.field_id.in_(f_fields))
    if f_plants: conds.append(OrderItem.plant_id.in_(f_plants))
    
    raw_details = db.session.query(OrderItem, Order, Field, Client)\
        .select_from(OrderItem)\
        .join(Order).join(Field).join(Client, Order.client_id == Client.id)\
        .filter(and_(*conds))\
        .order_by(Order.date.desc()).all()
        
    stock_prices = { (sb.plant_id, sb.size_id, sb.field_id, sb.year): sb.purchase_price for sb in StockBalance.query.all() }
    costs_cache = {} 

    wb = Workbook()
    ws = wb.active
    ws.title = "Партнер"
    
    # Шапка
    headers = ["Дата", "Партнер", "Покупатель", "Растение", "Размер", "Поле", "Партия", "Цена (Прайс)", "Себест (Полная)", "Доля (Прибыль/шт)", "Кол-во", "Сумма Доли"]
    ws.append(headers)
    for cell in ws[1]: cell.font = Font(bold=True)

    for item, order, field, client in raw_details:
        purch_price = stock_prices.get((item.plant_id, item.size_id, item.field_id, item.year), Decimal(0))
        
        calc_basis_year = order.date.year - 1
        if calc_basis_year not in costs_cache:
            costs_cache[calc_basis_year] = calculate_cost_data(calc_basis_year)['accumulated_costs_map']
        
        accum_cost = costs_cache[calc_basis_year].get(item.year, Decimal(0))
        full_cost_unit = purch_price + accum_cost
        
        price_det = item.price if item.price is not None else Decimal(0)
        share_unit = (price_det / 2) - full_cost_unit
        total_share = share_unit * Decimal(item.shipped_quantity)
        
        ws.append([
            order.date.strftime('%d.%m.%Y'),
            field.investor.name if field.investor else '-',
            client.name,
            item.plant.name,
            item.size.name,
            field.name,
            item.year,
            float(price_det),
            float(full_cost_unit),
            float(share_unit),
            item.shipped_quantity,
            float(total_share)
        ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    
    return send_file(
        buf, 
        download_name=f'investor_report.xlsx', 
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@bp.route('/reports/calculator', methods=['GET', 'POST'])
@login_required
def reports_calculator():
    if current_user.role not in ['admin', 'executive']: return redirect(url_for('main.index'))
    
    calc_results = []
    current_y = msk_now().year
    
    if request.method == 'POST':
        # Получаем списки данных из формы
        p_ids = request.form.getlist('plant_id[]')
        s_ids = request.form.getlist('size_id[]')
        f_ids = request.form.getlist('field_id[]')
        prices = request.form.getlist('price[]')
        years_input = request.form.getlist('year[]')
        
        # Загружаем данные для расчетов
        # Берем прошлый год для себестоимости (т.к. текущий еще не закрыт), либо текущий если надо
        calc_cost_year = current_y - 1 
        cost_data = calculate_cost_data(calc_cost_year)
        accum_costs_map = cost_data['accumulated_costs_map'] # Здесь сидят и Опер. расходы, и Амортизация
        
        # Кэшируем справочники для быстрого доступа
        plants_d = {p.id: p.name for p in Plant.query.all()}
        sizes_d = {s.id: s.name for s in Size.query.all()}
        fields_d = {f.id: f for f in Field.query.all()} 
        
        for i in range(len(p_ids)):
            try:
                # Парсим данные строки
                pid = int(p_ids[i])
                sid = int(s_ids[i])
                fid = int(f_ids[i])
                in_price = Decimal(prices[i]) if prices[i] else Decimal(0)
                
                # Год партии: если не выбран, берем 2017 (начало)
                y_val = years_input[i]
                year = int(y_val) if y_val else 2017
                
                f_obj = fields_d.get(fid)
                is_investor = (f_obj.investor_id is not None)
                
                # 1. Цена закупки (из остатков конкретной партии)
                sb = StockBalance.query.filter_by(plant_id=pid, size_id=sid, field_id=fid, year=year).first()
                purchase_price = sb.purchase_price if sb else Decimal(0)
                
                # 2. Накопленная себестоимость (Опер + Аморт) до года партии
                # Используем accum_costs_map[year], который содержит сумму unit_cost за все годы жизни растения
                accum_cost = accum_costs_map.get(year, Decimal(0))
                
                # Полная себестоимость
                total_cost = purchase_price + accum_cost
                
                # 3. Расчет результата
                if is_investor:
                    # Формула: (Цена продажи / 2) - Полная себестоимость
                    share = (in_price / 2) - total_cost
                    status_text = f"Партнер: {f_obj.investor.name}"
                    formula_text = f"({in_price} / 2) - {total_cost}"
                else:
                    # Собственное: Прибыль = Цена продажи (ни с кем не делим)
                    share = in_price
                    status_text = "Собственность (Без партнера)"
                    formula_text = "100% от цены"

                calc_results.append({
                    'plant': plants_d.get(pid), 
                    'size': sizes_d.get(sid), 
                    'field': f_obj.name, 
                    'year': year, 
                    'status': status_text,
                    'input_price': in_price, 
                    'total_cost': total_cost, 
                    'purchase_price': purchase_price,
                    'accum_cost': accum_cost,
                    'formula': formula_text,
                    'result_price': share,
                    'is_investor': is_investor
                })
            except Exception as e: 
                print(f"Calc error row {i}: {e}")
            
    return render_template('finance/reports.html', active_tab='calculator', calc_results=calc_results, 
                           plants=Plant.query.all(), sizes=Size.query.all(), fields=Field.query.all(), 
                           years_range=range(2017, msk_now().year+1))

@bp.route('/cost/save_to_db', methods=['POST'])
@login_required
def save_cost_to_db():
    if current_user.role != 'admin': 
        return redirect(url_for('main.index'))
    
    # 1. Считаем за прошлый год (чтобы взять полные закрытые данные)
    # Или текущий - 1
    calc_year = msk_now().year - 1 
    
    # 2. Получаем сложную математику из utils.py
    data = calculate_cost_data(calc_year)
    # accum_costs_map хранит {2020: 150руб, 2021: 180руб} (накопленные расходы для партии этого года)
    accum_costs_map = data['accumulated_costs_map'] 
    
    # 3. Обновляем остатки
    stocks = StockBalance.query.filter(StockBalance.quantity > 0).all()
    count = 0
    
    for sb in stocks:
        # Для партии 2020 года берем накопленную сумму расходов для 2020 года
        ac = accum_costs_map.get(sb.year, Decimal(0))
        
        # Полная Себестоимость = Закупка (индивид) + Накопленные (общие)
        sb.current_total_cost = sb.purchase_price + ac
        count += 1
        
    db.session.commit()
    flash(f'Успешно записана себестоимость в БД для {count} позиций (база расчета: {calc_year} г.)')
    log_action("Пересчитал и сохранил себестоимость в БД")
    
    # Возвращаем пользователя на вкладку "Влияние на цену"
    return redirect(url_for('finance.cost_report', tab='impact'))

@bp.route('/reports/projects')
@login_required
def reports_projects():
    if current_user.role not in ['admin', 'executive']: 
        return redirect(url_for('main.index'))
    
    # Фильтр по году создания
    f_year = request.args.get('year', msk_now().year, type=int)
    
    # Берем проекты
    projects_db = Project.query.filter(
        or_(
            func.extract('year', Project.created_at) == f_year,
            Project.status == 'active'
        )
    ).order_by(Project.created_at.desc()).all()
    
    report_data = []
    
    for p in projects_db:
        # ВСЯ МАГИЯ ТЕПЕРЬ ТУТ:
        eco = p.get_economics()
        
        report_data.append({
            'id': p.id,
            'name': p.name,
            'status': p.status,
            'revenue': eco['revenue'],
            'plants_cost': eco['plants_cost'],
            'direct_expenses': eco['direct_expenses'],
            'plan_expenses': sum(b.amount for b in p.budget_items),
            'profit': eco['profit'],
            'margin': eco['margin'],
            'orders_count': len(p.orders)
        })
        
    return render_template('finance/reports.html', active_tab='projects', projects=report_data, selected_year=f_year, years=range(2020, 2030))

@bp.route('/project/<int:project_id>', methods=['GET', 'POST'])
@login_required
def project_detail(project_id):
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))
        
    project = Project.query.get_or_404(project_id)
    
    if request.method == 'POST':
        # 1. Редактирование проекта
        if 'update_project' in request.form:
            project.name = request.form.get('name')
            project.description = request.form.get('description')
            project.status = request.form.get('status')
            db.session.commit()
            flash('Настройки проекта обновлены')

        # 2. Добавление Растений
        elif 'add_item' in request.form:
            plant_id = request.form.get('plant_id')
            size_id = request.form.get('size_id')
            qty = int(request.form.get('quantity') or 0)
            db.session.add(ProjectItem(project_id=project.id, plant_id=plant_id, size_id=size_id, quantity=qty))
            db.session.commit()

        elif 'delete_item' in request.form:
            ProjectItem.query.filter_by(id=request.form.get('item_id')).delete()
            db.session.commit()

        # 3. БЮДЖЕТ (ПЛАНОВЫЕ РАСХОДЫ)
        elif 'add_budget' in request.form:
            name = request.form.get('name')
            amount = float(request.form.get('amount') or 0)
            db.session.add(ProjectBudget(project_id=project.id, name=name, amount=amount))
            db.session.commit()
            flash('Статья бюджета добавлена')

        elif 'delete_budget' in request.form:
            ProjectBudget.query.filter_by(id=request.form.get('budget_id')).delete()
            db.session.commit()

        # 4. ПРИВЯЗКА ЗАКАЗА К ПРОЕКТУ
        elif 'link_order' in request.form:
            oid = request.form.get('order_id')
            order = Order.query.get(oid)
            if order:
                order.project_id = project.id
                db.session.commit()
                flash(f'Заказ #{oid} привязан к проекту')
        
        elif 'unlink_order' in request.form:
            oid = request.form.get('order_id')
            order = Order.query.get(oid)
            if order and order.project_id == project.id:
                order.project_id = None
                db.session.commit()
                flash('Заказ отвязан')

        return redirect(url_for('finance.project_detail', project_id=project.id))

    # Сбор статистики через новый единый метод модели
    eco = project.get_economics()
    
    # Подготовка данных для таблицы бюджета (План/Факт по статьям)
    budget_comparison = []
    total_plan = Decimal(0)
    
    for item in project.budget_items:
        # Считаем сумму расходов, привязанных к ЭТОЙ статье бюджета
        item_fact = db.session.query(func.sum(Expense.amount))\
            .filter(Expense.project_budget_id == item.id).scalar() or Decimal(0)
        
        total_plan += item.amount
        
        budget_comparison.append({
            'id': item.id,
            'name': item.name,
            'plan': item.amount,
            'fact': item_fact,
            'diff': item.amount - item_fact # Положительное - экономия, Отрицательное - перерасход
        })
    
    # Формируем объект статистики из данных get_economics
    stats = {
        'total_plan': total_plan,
        'total_fact': eco['direct_expenses'],
        'unallocated_fact': eco['unallocated_fact'],
        'revenue': eco['revenue'],
        'plants_cost': eco['plants_cost'],
        'profit': eco['profit'],
        'margin': eco['margin'],
        'items_count': sum(i.quantity for i in project.items),
        'cost_per_item': 0
    }
    
    if stats['items_count'] > 0:
        stats['cost_per_item'] = stats['total_fact'] / Decimal(stats['items_count'])
        
    # ИЗМЕНЕНИЕ: Загружаем ВСЕ активные заказы (кроме отмененных и удаленных), 
    # чтобы можно было перепривязать заказ из другого проекта сюда.
    all_orders = Order.query.filter(
        Order.status != 'canceled', 
        Order.is_deleted == False
    ).order_by(Order.date.desc()).all()

    return render_template(
        'finance/project_detail.html', 
        project=project, 
        stats=stats,
        budget_comparison=budget_comparison,
        all_orders=all_orders, # Было free_orders, стало all_orders
        plants=sorted(Plant.query.all(), key=lambda x: x.name),
        sizes=Size.query.all()
    )

@bp.route('/finance/invoices', methods=['GET', 'POST'])
@login_required
def invoices_list():
    # Перенаправляем на объединенную вкладку "Счета и расходы"
    return redirect(url_for('finance.expenses', tab='invoices'))

@bp.route('/finance/invoices/download/<int:inv_id>')
@login_required
def invoice_download(inv_id):
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))
    
    inv = PaymentInvoice.query.get_or_404(inv_id)
    inv_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'invoices')
    try:
        return send_from_directory(inv_dir, inv.filename, as_attachment=True, download_name=inv.original_name)
    except Exception:
        flash("Файл не найден на сервере")
        return redirect(url_for('finance.expenses', tab='invoices'))

@bp.route('/finance/summary', methods=['GET'])
@login_required
def invoices_summary():
    # Доступ разрешен всем авторизованным (включая менеджеров 'user')
    # Фильтры
    f_client = request.args.get('client_id')
    f_start = request.args.get('start_date')
    f_end = request.args.get('end_date')
    
    # Базовый запрос: Все активные заказы
    query = Order.query.filter(Order.is_deleted == False)
    
    # Применяем фильтры
    if f_client:
        query = query.filter(Order.client_id == int(f_client))
    if f_start:
        query = query.filter(func.date(Order.date) >= f_start)
    if f_end:
        query = query.filter(func.date(Order.date) <= f_end)
        
    orders = query.order_by(Order.date.desc()).all()
    
    # Группировка по (client_id, invoice_number)
    # Используем словарь для объединения
    invoices_map = {}
    
    from decimal import Decimal
    
    for o in orders:
        # Если номера счета нет, группируем под ключом "no_invoice" или пропускаем
        # Чтобы отчет был красивым, лучше группировать пустые тоже, но отдельно
        inv_num = o.invoice_number if o.invoice_number else "Без счета"
        
        # Ключ уникальности: Клиент + Номер счета
        # (Разные клиенты могут иметь счета с одинаковым номером "1", "2" и т.д.)
        key = (o.client_id, inv_num)
        
        if key not in invoices_map:
            invoices_map[key] = {
                'client_id': o.client_id,
                'client_name': o.client.name,
                'invoice_number': inv_num,
                'has_invoice': bool(o.invoice_number), # Флаг для ссылки
                'date': o.invoice_date if o.invoice_date else o.date.date(), # Если даты счета нет, берем дату заказа
                'orders_count': 0,
                'total_sum': Decimal(0),
                'paid_sum': Decimal(0),
                'shipped_sum': Decimal(0),
                'debt': Decimal(0),
                'status': 'paid' # По умолчанию paid, если найдем долг - сменим
            }
            
        row = invoices_map[key]
        row['orders_count'] += 1
        row['total_sum'] += o.total_sum
        row['paid_sum'] += o.paid_sum
        
        # Считаем отгруженное (сумма)
        shipped_val = Decimal(0)
        for item in o.items:
            price = item.price if item.price is not None else Decimal(0)
            shipped_val += price * item.shipped_quantity
        row['shipped_sum'] += shipped_val
        
        # Если есть хотя бы одна дата счета в группе, берем её (приоритет у заполненной)
        if o.invoice_date and row['invoice_number'] != "Без счета":
            row['date'] = o.invoice_date

    # Превращаем словарь в список
    report_rows = list(invoices_map.values())
    
    # Досчитываем долги и статусы
    for r in report_rows:
        r['debt'] = r['total_sum'] - r['paid_sum']
        
        if r['debt'] <= 0:
            r['status'] = 'paid' # Оплачен
        elif r['paid_sum'] > 0:
            r['status'] = 'partial' # Частично
        else:
            r['status'] = 'unpaid' # Не оплачен

    # Сортировка (по умолчанию по дате убывания)
    report_rows.sort(key=lambda x: x['date'], reverse=True)
    
    return render_template(
        'finance/invoices_summary.html',
        invoices=report_rows,
        clients=Client.query.order_by(Client.name).all(),
        filters={'client_id': f_client, 'start': f_start, 'end': f_end}
    )

# --- УПРАВЛЕНИЕ ПРОЕКТАМИ ---

@bp.route('/projects', methods=['GET', 'POST'])
@login_required
def projects_list():
    if current_user.role not in ['admin', 'executive']:
        return redirect(url_for('main.index'))

    tab = request.args.get('tab', 'list')

    if request.method == 'POST':
        name = request.form.get('name')
        desc = request.form.get('description')
        if name:
            new_proj = Project(name=name, description=desc, status='active')
            db.session.add(new_proj)
            db.session.commit()
            flash(f'Проект "{name}" создан')
            log_action(f"Создал проект {name}")
        return redirect(url_for('finance.projects_list', tab='list'))

    # Фильтр статуса
    show_closed = request.args.get('show_closed') == '1'

    # Таб "Список"
    projects_data = []
    if tab == 'list':
        query = Project.query
        if not show_closed:
            query = query.filter_by(status='active')
        projects_db = query.order_by(Project.created_at.desc()).all()

        for p in projects_db:
            eco = p.get_economics()
            projects_data.append({
                'id': p.id,
                'name': p.name,
                'description': p.description,
                'status': p.status,
                'total_revenue': eco['revenue'],
                'total_expenses': eco['direct_expenses'],
                'profit': eco['profit']
            })

    # Таб "Отчеты"
    report_data = []
    selected_year = request.args.get('year', msk_now().year, type=int)
    if tab == 'reports':
        projects_db = Project.query.filter(
            or_(
                func.extract('year', Project.created_at) == selected_year,
                Project.status == 'active'
            )
        ).order_by(Project.created_at.desc()).all()

        for p in projects_db:
            eco = p.get_economics()
            report_data.append({
                'id': p.id,
                'name': p.name,
                'status': p.status,
                'revenue': eco['revenue'],
                'plants_cost': eco['plants_cost'],
                'direct_expenses': eco['direct_expenses'],
                'plan_expenses': sum(b.amount for b in p.budget_items),
                'profit': eco['profit'],
                'margin': eco['margin'],
                'orders_count': len(p.orders)
            })

    return render_template('finance/projects_list.html', 
                           active_tab=tab,
                           projects=projects_data,
                           report_data=report_data,
                           show_closed=show_closed,
                           selected_year=selected_year,
                           current_year=msk_now().year)

from app.models import (
    db, Expense, BudgetItem, BudgetPlan, CashflowPlan, Employee, UnitCostOverride, 
    Plant, StockBalance, Order, OrderItem, Field, Payment, Document, DocumentRow, Size,
    Client, PaymentInvoice, Project, ProjectItem, ProjectBudget
)