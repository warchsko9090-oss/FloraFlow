import os
import re
from sqlalchemy import text, or_
from groq import Groq
from app.models import db, Plant, Field, Size, Client, Employee, SQLExample
from app.utils import msk_now
import traceback

# Настройка API ключа
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

# Стоп-слова (используются только для простого RAG-поиска имен в БД)
STOP_WORDS = {
    'сколько', 'какой', 'какая', 'какие', 'всего', 'остаток', 'остатки', 
    'цена', 'цены', 'стоимость', 'стоит', 'себестоимость', 'закупка', 
    'поле', 'поля', 'склад', 'растение', 'растения', 'размер', 'сорт',
    'купил', 'заказал', 'отгрузил', 'было', 'есть', 'нет', 'менеджер',
    'бригадир', 'рабочий', 'сотрудник', 'фин', 'результат', 'прибыль', 
    'выручка', 'расход', 'бюджет', 'статья', 'год', 'года', 'лет', 'штук', 'шт'
}

# --- 1. ПОЛНАЯ СХЕМА БД (FULL SCHEMA) ---
SCHEMA_ADMIN = f"""
ROLE: SQL Expert (SQLite). CURRENT YEAR: {msk_now().year}

FULL DATABASE SCHEMA:
1. stock_balance (ОСТАТКИ НА СКЛАДЕ/ПОЛЯХ): quantity, current_total_cost (Себестоимость), price (Цена продажи), purchase_price.
   FK: plant_id, size_id, field_id.
2. order_item (СОСТАВ ЗАКАЗОВ): quantity (План/Заказано), shipped_quantity (Отгружено), dug_quantity (Выкопано), price (Цена продажи).
   FK: order_id, plant_id, size_id, field_id.
3. "order" (ЗАКАЗЫ): id, date, status (reserved, in_progress, ready, shipped, canceled, ghost), is_deleted, invoice_number. 
   FK: client_id, project_id. 
   *Note: Only count where is_deleted=0 and status != 'canceled'.*
4. payment (ОПЛАТЫ КЛИЕНТОВ ПО ЗАКАЗАМ): amount, date, comment. FK: order_id.
5. plant: id, name, latin_name.
6. size: id, name.
7. field: id, name, planting_year. FK: investor_id (client_id).
8. client: id, name.
9. expense (РАСХОДЫ): amount, date, payment_type (cash/cashless), description. 
   FK: budget_item_id, employee_id, project_id.
10. budget_item (СТАТЬИ РАСХОДОВ): id, name, code.
11. employee (СОТРУДНИКИ): id, name, role (brigadier, worker, etc), is_active.
12. time_log (ОТРАБОТАННЫЕ ЧАСЫ): date, hours_norm, hours_norm_over, hours_spec, hours_spec_over. FK: employee_id.
13. digging_log (ИСТОРИЯ ВЫКОПКИ): quantity, date, status. FK: order_item_id, plant_id, size_id, field_id, user_id.
14. project (ПРОЕКТЫ): id, name, status.
15. document & document_row (ДОКУМЕНТЫ СКЛАДА): doc_type (income, move, shipment).

CRITICAL RULES:
1. SEARCHING TEXT: Always use `lower(col) LIKE '%val%'`.
2. FIELDS MATCHING: Match `field.name` via string (e.g. `field.name LIKE '%4%'`), never by integer ID.
3. JOINS: Don't forget to join `plant`, `size`, `field`, `client` when querying by names.
4. AGGREGATIONS: Don't `SUM(price)`. Sum money via `SUM(price * quantity)`. 
5. SQLite LIMITATIONS: SQLite doesn't support YEAR() or MONTH() functions, use `strftime('%Y', date) = '2024'`.
"""

def stem_rus(word):
    word = word.lower()
    if len(word) <= 3: return word
    endings = ['ого', 'его', 'ому', 'ему', 'ыми', 'ими', 'ами', 'ями', 
               'ая', 'яя', 'ое', 'ее', 'ые', 'ие', 'ой', 'ей', 
               'ом', 'ем', 'ам', 'ям', 'ах', 'ях', 
               'а', 'я', 'о', 'е', 'ы', 'и', 'у', 'ю', 'ь', 'й']
    for end in endings:
        if word.endswith(end): return word[:-len(end)]
    return word

def get_relevant_context(user_query):
    """Ищет, есть ли упомянутые Растения/Поля в БД (чтобы ИИ не гадал имена)"""
    found_info = []
    raw_words = [w.lower() for w in user_query.split()]
    clean_words = [w for w in raw_words if len(w) > 2 and w not in STOP_WORDS]
    
    roots = []
    for w in clean_words:
        root = stem_rus(w)
        if len(root) >= 3: roots.append(root)
        elif w in ['туя', 'туй']: roots.extend(['туя', 'туй'])

    if not roots: return ""

    try:
        plant_filters = [Plant.name.ilike(f'%{root}%') for root in roots]
        if plant_filters:
            matched = db.session.query(Plant.name).filter(or_(*plant_filters)).limit(5).all()
            if matched: found_info.append(f"Plants found in DB: {', '.join([p[0] for p in matched])}")

        digits = [w for w in raw_words if w.isdigit()]
        if digits:
            for d in digits:
                f_res = db.session.query(Field.name).filter(Field.name.like(f'%{d}%')).limit(3).all()
                if f_res: found_info.append(f"Fields found in DB: {', '.join([f[0] for f in f_res])}")
                
    except Exception as e:
        print(f"Context Error: {e}")
    return "\n".join(found_info)

def get_all_examples():
    """Загружает ВСЕ примеры из базы, так как у Llama 70b огромное контекстное окно"""
    try:
        all_examples = SQLExample.query.all()
        return "\n".join([f"Q: {ex.question}\nSQL: {ex.sql_query}" for ex in all_examples])
    except Exception:
        return ""

def process_query(user_query, role='admin'):
    if not GROQ_API_KEY:
        return "Ошибка конфигурации: Отсутствует API ключ Groq."

    client = Groq(api_key=GROQ_API_KEY)
    model = "llama-3.3-70b-versatile"
    
    context_data = get_relevant_context(user_query)
    all_examples = get_all_examples()
    
    # ---------------------------------------------------------
    # АГЕНТНЫЙ ЦИКЛ: Генерация -> Проверка -> Исправление
    # ---------------------------------------------------------
    MAX_RETRIES = 3
    current_sql = ""
    error_log = ""
    
    for attempt in range(1, MAX_RETRIES + 1):
        prompt = f"""
        {SCHEMA_ADMIN}
        
        VALID SQL EXAMPLES FROM THIS DATABASE:
        {all_examples}
        
        DB CONTEXT (Extracted exact names to use):
        {context_data}
        
        USER REQUEST: "{user_query}"
        """
        
        if error_log:
            prompt += f"""
            
            PREVIOUS ATTEMPT FAILED. 
            You wrote this SQL: {current_sql}
            SQLite returned this error: {error_log}
            
            Please fix the SQL syntax or logic and try again. 
            Remember to use correct table/column names from the schema.
            """
            
        prompt += "\nTASK: Generate a valid SQL query (SQLite) to answer the request. OUTPUT ONLY THE SQL INSIDE ```sql ... ``` TAGS. No text."

        try:
            resp = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model,
                temperature=0.1
            )
            raw_response = resp.choices[0].message.content
            
            match = re.search(r'```sql(.*?)```', raw_response, re.DOTALL | re.IGNORECASE)
            sql_query = match.group(1).strip() if match else raw_response.strip()
            
            # Фильтруем мусор
            sql_query = sql_query.replace('```', '').strip()
            current_sql = sql_query
            print(f"--> [Smart SQL Attempt {attempt}]: {sql_query}")

            # Пробуем выполнить
            result = db.session.execute(text(sql_query))
            rows = result.fetchall()
            keys = result.keys()
            
            # Если дошли сюда — SQL корректный! Выходим из цикла.
            break

        except Exception as e:
            error_log = str(e)
            print(f"--> [SQL Error Attempt {attempt}]: {error_log}")
            db.session.rollback() # Откатываем транзакцию после ошибки
            if attempt == MAX_RETRIES:
                return f"ИИ не смог составить правильный запрос за {MAX_RETRIES} попытки.<br><br><b>Последняя ошибка:</b> {error_log}<br><b>Запрос:</b> <code>{current_sql}</code>"

    # ---------------------------------------------------------
    # ФОРМАТИРОВАНИЕ ОТВЕТА (ЕСЛИ ДАННЫЕ ПОЛУЧЕНЫ)
    # ---------------------------------------------------------
    if not rows:
        return f"Запрос выполнен успешно, но данных не найдено.<br><br><small class='text-muted'>Сгенерированный SQL: <code>{current_sql}</code></small>"
        
    data_str = str(rows[:20]) 
    if len(rows) > 20: data_str += f"... (Total {len(rows)} rows)"
    
    humanize_prompt = f"""
    User Question: "{user_query}"
    Database Result: {data_str}
    Columns: {list(keys)}
    
    Role: You are a reporting assistant.
    Task: Write the final HTML response in Russian based on Result.
    
    STRICT RULES:
    1. **INSERT ACTUAL VALUES**: Do NOT use template syntax like `{{{{ value }}}}` or `[x]`. Use real numbers from 'Database Result'.
    2. **SINGLE NUMBER**: If Result is a single number, return it compactly (e.g., `<b>Всего: 638 шт.</b>`). NO huge headers like `<h3>`.
    3. **TABLES**: If multiple rows, use `<div class='chat-table-wrapper'><table class='table table-sm table-bordered mb-0' style='font-size:12px; white-space:nowrap;'>...</table></div>`. This prevents layout breaking on phones.
    4. **NO MATH**: Do NOT calculate totals manually. Only show what is in 'Database Result'.
    5. **LINK**: Based on the question and context, append a relevant hyperlink to the system page at the very end.
       Format it as: <br><br><a href="/URL" class="btn btn-sm btn-outline-primary">Перейти в отчет</a>
       Available URLs:
       - /orders (Заказы)
       - /stock (Склад)
       - /expenses (Финансы / Расходы)
       - /personnel (Кадры)
       - /digging/report (Журнал выкопки)
       - /budget (Бюджет)
       - /cost (Себестоимость)
    """
    
    try:
        final_resp = client.chat.completions.create(
            messages=[{"role": "user", "content": humanize_prompt}],
            model=model, 
            temperature=0.3
        )
        html_response = final_resp.choices[0].message.content
        html_response += f"<br><br><small class='text-muted' style='font-size:10px; cursor:pointer;' onclick='alert(\"{current_sql.replace('\"', '&quot;')}\")'>Показать SQL</small>"
        return html_response
    except Exception as e:
        return f"Данные найдены, но произошла ошибка генерации ответа: {str(e)}"

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ АДМИНКИ ---
def generate_sql_only(description):
    if not GROQ_API_KEY: return "-- No API Key"
    hints = get_relevant_context(description)
    examples = get_all_examples()
    prompt = f"{SCHEMA_ADMIN}\nCTX:{hints}\nEX:{examples}\nTask: SQL for '{description}'\nReturn ONLY SQL inside ```sql ... ```."
    
    try:
        client = Groq(api_key=GROQ_API_KEY)
        resp = client.chat.completions.create(messages=[{"role": "user", "content": prompt}], model="llama-3.3-70b-versatile", temperature=0.1)
        content = resp.choices[0].message.content
        match = re.search(r'```sql(.*?)```', content, re.DOTALL | re.IGNORECASE)
        return match.group(1).strip() if match else content.strip()
    except Exception as e:
        return f"-- Error: {e}"
