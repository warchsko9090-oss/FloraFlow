from app.models import db, SQLExample

def seed_knowledge():
    examples = [
        # === 1. ЗАКАЗЫ ===
        {"q": "Сколько сейчас актуальных заказов? Заказы в работе.", "sql": "SELECT status, COUNT(*) as count, SUM(oi.price * oi.quantity) as total_sum FROM \"order\" o JOIN order_item oi ON o.id = oi.order_id WHERE o.status IN ('reserved', 'in_progress') AND o.is_deleted = 0 GROUP BY status;"},
        {"q": "Сколько заказов отгружено?", "sql": "SELECT COUNT(DISTINCT id) FROM \"order\" WHERE status = 'shipped' AND is_deleted = 0;"},
        {"q": "Сколько отгружено частично?", "sql": "SELECT COUNT(DISTINCT o.id) FROM \"order\" o JOIN order_item oi ON o.id = oi.order_id WHERE o.status != 'canceled' AND o.status != 'shipped' AND oi.shipped_quantity > 0 AND oi.shipped_quantity < oi.quantity;"},
        {"q": "Какая сумма в резервах? Деньги в резерве.", "sql": "SELECT SUM((oi.quantity - oi.shipped_quantity) * oi.price) as reserve_sum FROM order_item oi JOIN \"order\" o ON oi.order_id = o.id WHERE o.status IN ('reserved', 'in_progress') AND o.is_deleted = 0;"},
        {"q": "Сумма заказов по клиентам. Кто сколько купил?", "sql": "SELECT c.name, SUM(oi.quantity * oi.price) as total_buy FROM \"order\" o JOIN client c ON o.client_id = c.id JOIN order_item oi ON o.id = oi.order_id WHERE o.status != 'canceled' GROUP BY c.name ORDER BY total_buy DESC;"},
        {"q": "Сколько заказов оплачено? Полная оплата.", "sql": "SELECT COUNT(*) FROM \"order\" o WHERE o.status != 'canceled' AND (SELECT SUM(amount) FROM payment p WHERE p.order_id = o.id) >= (SELECT SUM(quantity * price) FROM order_item oi WHERE oi.order_id = o.id);"},
        {"q": "Когда была последняя отгрузка?", "sql": "SELECT d.date, d.id, d.comment FROM document d WHERE d.doc_type = 'shipment' ORDER BY d.date DESC LIMIT 1;"},

        # === 2. СКЛАД И ОСТАТКИ ===
        {"q": "Сколько всего растений?", "sql": "SELECT SUM(quantity) as total_plants FROM stock_balance WHERE quantity > 0;"},
        {"q": "Сколько сосен? Остатки сосна.", "sql": "SELECT p.name, s.name, f.name, sb.quantity FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%сосн%' AND sb.quantity > 0;"},
        {"q": "Боярышник сливолистный остатки. Есть боярышника?", "sql": "SELECT p.name, s.name, f.name, sb.quantity FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%боярыш%' AND lower(p.name) LIKE '%слив%' AND sb.quantity > 0;"},
        {"q": "Растения размера 60-80.", "sql": "SELECT p.name, s.name, sb.quantity FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id WHERE (s.name LIKE '%60-80%' OR s.name LIKE '%80-100%') AND sb.quantity > 0;"},
        {"q": "Остатки нетов. Нетоварка.", "sql": "SELECT p.name, f.name, sb.quantity FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(s.name) LIKE '%нетов%' AND sb.quantity > 0;"},
        {"q": "Сколько туй на 6 поле? Туя поле 6.", "sql": "SELECT p.name, s.name, sb.quantity FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%туя%' AND f.name LIKE '%6%' AND sb.quantity > 0;"},
        {"q": "Какая цена у туи С3 на 11 поле?", "sql": "SELECT p.name, s.name, f.name, sb.price FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%туя%' AND lower(s.name) LIKE '%c3%' AND f.name LIKE '%11%';"},

        # === 3. ЖУРНАЛ ДОКУМЕНТОВ ===
        {"q": "Когда было последнее поступление гортезии?", "sql": "SELECT d.date, d.id FROM document d JOIN document_row dr ON d.id = dr.document_id JOIN plant p ON dr.plant_id = p.id WHERE d.doc_type = 'income' AND lower(p.name) LIKE '%гортен%' ORDER BY d.date DESC LIMIT 1;"},
        {"q": "Куда переместили спирею с 11 поля?", "sql": "SELECT d.date, p.name, f_from.name as from_field, f_to.name as to_field, dr.quantity FROM document d JOIN document_row dr ON d.id = dr.document_id JOIN plant p ON dr.plant_id = p.id JOIN field f_from ON dr.field_from_id = f_from.id JOIN field f_to ON dr.field_to_id = f_to.id WHERE d.doc_type = 'move' AND lower(p.name) LIKE '%спир%' AND f_from.name LIKE '%11%';"},

        # === 4. ФИНАНСЫ ===
        {"q": "Какая чистая прибыль была в 2024 году?", "sql": "SELECT (SELECT SUM(oi.shipped_quantity * oi.price) FROM order_item oi JOIN \"order\" o ON oi.order_id=o.id WHERE strftime('%Y', o.date)='2024' AND o.status!='canceled') - (SELECT SUM(amount) FROM expense WHERE strftime('%Y', date)='2024') as net_profit;"},
        {"q": "Сколько налички мы потратили в 2024?", "sql": "SELECT SUM(amount) as cash_expenses FROM expense WHERE payment_type = 'cash' AND strftime('%Y', date) = '2024';"},
        {"q": "Сколько безнала потратили в 2025?", "sql": "SELECT SUM(amount) as cashless_expenses FROM expense WHERE payment_type = 'cashless' AND strftime('%Y', date) = '2025';"},
        {"q": "Сколько заплатили инвестору в 2024?", "sql": "SELECT SUM(amount) FROM expense WHERE description LIKE '%инвестор%' AND strftime('%Y', date) = '2024';"},
        {"q": "Какая маржа по туе С3 на 5 поле?", "sql": "SELECT p.name, s.name, f.name, (sb.price - sb.current_total_cost) as margin_rub, ((sb.price - sb.current_total_cost) / sb.price * 100) as margin_percent FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%туя%' AND lower(s.name) LIKE '%c3%' AND f.name LIKE '%5%';"},
        {"q": "Сколько мы должны инвестору всего?", "sql": "SELECT f.name as field, client.name as investor, SUM( (oi.price/2 - (sb.purchase_price + sb.current_total_cost - sb.purchase_price)) * oi.shipped_quantity ) as owed_sum FROM order_item oi JOIN \"order\" o ON oi.order_id=o.id JOIN field f ON oi.field_id=f.id JOIN client ON f.investor_id=client.id JOIN stock_balance sb ON (oi.plant_id=sb.plant_id AND oi.size_id=sb.size_id AND oi.field_id=sb.field_id AND oi.year=sb.year) WHERE o.status != 'canceled' AND f.investor_id IS NOT NULL GROUP BY client.name;"},

        # === 5. БЮДЖЕТ ===
        {"q": "Сколько потратили по статье Растения? Закупка растений.", "sql": "SELECT SUM(amount) FROM expense e JOIN budget_item b ON e.budget_item_id = b.id WHERE lower(b.name) LIKE '%растен%' OR lower(b.code) LIKE '%растен%';"},
        {"q": "Сколько потратили на дороги? Ремонт дороги.", "sql": "SELECT SUM(amount) FROM expense e JOIN budget_item b ON e.budget_item_id = b.id WHERE lower(b.name) LIKE '%дорог%' OR lower(b.name) LIKE '%щебень%';"},
        {"q": "Зарплата менеджера. Сколько получил Алексей?", "sql": "SELECT SUM(amount) FROM expense e JOIN employee emp ON e.employee_id = emp.id WHERE lower(emp.name) LIKE '%алексей%';"},
        
        # === 6. СЕБЕСТОИМОСТЬ ===
        {"q": "Какая себестоимость у можжевельника на 2 поле?", "sql": "SELECT p.name, f.name, sb.year, sb.current_total_cost FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%можж%' AND f.name LIKE '%2%' AND sb.quantity > 0;"},
        {"q": "Итоговая себестоимость склада.", "sql": "SELECT SUM(current_total_cost * quantity) as total_stock_cost FROM stock_balance WHERE quantity > 0;"},

        # === 7. CRM И КАДРЫ ===
        {"q": "Какой средний чек по клиенту Гринпарк?", "sql": "SELECT c.name, AVG(o_sum.total) as avg_check FROM client c JOIN (SELECT order_id, SUM(price*quantity) as total FROM order_item GROUP BY order_id) o_sum ON 1=1 JOIN \"order\" o ON o_sum.order_id = o.id WHERE o.client_id = c.id AND lower(c.name) LIKE '%гринпарк%' AND o.status != 'canceled';"},
        {"q": "Какие топ позиции мы отгружаем?", "sql": "SELECT p.name, s.name, SUM(oi.shipped_quantity) as total_qty, SUM(oi.shipped_quantity * oi.price) as total_money FROM order_item oi JOIN plant p ON oi.plant_id=p.id JOIN size s ON oi.size_id=s.id GROUP BY p.name, s.name ORDER BY total_money DESC LIMIT 10;"},
        {"q": "Сколько всего сотрудников?", "sql": "SELECT COUNT(*) FROM employee WHERE is_active = 1;"},
        {"q": "Как зовут бригадира?", "sql": "SELECT name FROM employee WHERE role = 'brigadier' AND is_active = 1;"},
        {"q": "Какая ставка у бригадира в 2025?", "sql": "SELECT role, rate_type, rate_value FROM salary_rate WHERE role = 'brigadier' AND year = 2025;"},
        {"q": "Сколько часов у бригадира в мае? Часы работы.", "sql": "SELECT e.name, SUM(t.hours_norm + t.hours_norm_over + t.hours_spec + t.hours_spec_over) as total_hours FROM time_log t JOIN employee e ON t.employee_id = e.id WHERE e.role = 'brigadier' AND strftime('%m', t.date) = '05' GROUP BY e.name;"},
        {"q": "Сколько осталось доплатить работникам?", "sql": "TEXT: Посмотрите отчет 'Кадры' -> колонка 'Долг (Остаток)'. В SQL это требует сложного расчета тарифов и ставок."},

        # === 8. СЛОЖНЫЕ КЕЙСЫ (ИСПРАВЛЕНИЯ) ===
        # Поиск полей (точный матч цифры)
        {"q": "Себестоимость туй западных на 4 поле?", "sql": "SELECT p.name, s.name, f.name, sb.current_total_cost FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%туя%' AND lower(p.name) LIKE '%запад%' AND (f.name LIKE '% 4' OR f.name LIKE '4 %' OR f.name LIKE '%Поле 4%') AND sb.quantity > 0;"},
        
        # Размеры на диапазоне полей (BETWEEN не сработает для строк, используем OR или IN)
        {"q": "Какие размеры спирей на полях с 1 по 10?", "sql": "SELECT p.name, s.name, f.name, sb.quantity FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%спире%' AND (f.name IN ('Поле 1', 'Поле 2', 'Поле 3', 'Поле 4', 'Поле 5', 'Поле 6', 'Поле 7', 'Поле 8', 'Поле 9', 'Поле 10') OR f.name LIKE '1 %' OR f.name LIKE '2 %') AND sb.quantity > 0 ORDER BY f.name;"},
        
        # История отгрузок (Важно: таблица order_item, а не stock_balance)
        {"q": "Сколько было отгружено сосен за весь период с 11 поля?", "sql": "SELECT p.name, f.name, SUM(oi.shipped_quantity) as total_shipped FROM order_item oi JOIN plant p ON oi.plant_id=p.id JOIN field f ON oi.field_id=f.id WHERE lower(p.name) LIKE '%сосн%' AND f.name LIKE '%11%' GROUP BY p.name;"},
        
        # === 9. ЦЕНА vs СЕБЕСТОИМОСТЬ (УТОЧНЕНИЕ) ===
        # Вопрос про ЦЕНУ (Прайс)
        {"q": "Какая цена на сосну горную на 11 поле?", "sql": "SELECT p.name, s.name, f.name, sb.price FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%сосн%' AND lower(p.name) LIKE '%горн%' AND f.name LIKE '%11%' AND sb.quantity > 0;"},
        {"q": "Сколько стоит спирея? Прайс.", "sql": "SELECT p.name, s.name, f.name, sb.price FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%спире%' AND sb.quantity > 0;"},
        
        # Вопрос про СЕБЕСТОИМОСТЬ (Себес)
        {"q": "Какая себестоимость сосны горной на 11 поле? Себес.", "sql": "SELECT p.name, s.name, f.name, sb.current_total_cost FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%сосн%' AND lower(p.name) LIKE '%горн%' AND f.name LIKE '%11%' AND sb.quantity > 0;"},
        {"q": "Посчитай себес туй на 5 поле.", "sql": "SELECT p.name, s.name, f.name, sb.current_total_cost FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN size s ON sb.size_id=s.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%туя%' AND f.name LIKE '%5%' AND sb.quantity > 0;"},
    
        # === 10. АГРЕГАЦИЯ (ИСПРАВЛЕНИЕ) ===        {"q": "Сколько штук сосны горной на 11 поле? Всего.", "sql": "SELECT SUM(sb.quantity) as total_qty FROM stock_balance sb JOIN plant p ON sb.plant_id=p.id JOIN field f ON sb.field_id=f.id WHERE lower(p.name) LIKE '%сосн%' AND lower(p.name) LIKE '%горн%' AND f.name LIKE '%11%' AND sb.quantity > 0;"}
    ]

    print("--- ЗАПУСК ОБУЧЕНИЯ (СИДИНГ) ---")
    added = 0
    for ex in examples:
        exists = SQLExample.query.filter(SQLExample.question.ilike(ex['q'])).first()
        if not exists:
            db.session.add(SQLExample(question=ex['q'], sql_query=ex['sql']))
            added += 1
            
    db.session.commit()
    print(f"--- ОБУЧЕНИЕ ЗАВЕРШЕНО: Добавлено {added} новых примеров ---")