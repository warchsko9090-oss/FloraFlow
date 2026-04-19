import os
import json
from datetime import datetime
from groq import Groq
from app.models import db, Client, Plant, StockBalance, TgTask, User
from sqlalchemy import func
from app.utils import msk_today

client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))

def tool_find_user(username):
    """Инструмент для поиска ID сотрудника системы по его имени/логину из чата"""
    if not username: return json.dumps({"error": "Имя не указано"})
    clean_name = username.replace('@', '').strip().lower()
    
    # СЛОВАРЬ СИНОНИМОВ (Telegram имена -> Системные логины в БД)
    user_aliases = {
        "behruz": "behruz", "бехруз": "behruz", "бехруз одоев": "behruz",
        "алексей": "aleksei", "алексей питомник одоев": "aleksei", "aleksei": "aleksei",
        "kirill_8988": "kirill", "к": "kirill", "кирилл": "kirill", "овчинников": "kirill"
    }
    
    # Ищем сначала по словарю, если нет - берем как есть
    target_username = user_aliases.get(clean_name, clean_name)
    
    user = User.query.filter(func.lower(User.username).like(f"%{target_username}%")).first()
    if user:
        return json.dumps({
            "user_id": user.id, 
            "username": user.username,
            "role": user.role,
            "status": "found"
        })
    return json.dumps({"error": f"Сотрудник '{clean_name}' не найден", "status": "not_found"})

def tool_find_client(client_name):
    """Инструмент для поиска ID клиента по имени"""
    if not client_name: return json.dumps({"error": "Имя не указано"})
    client_obj = Client.query.filter(func.lower(Client.name).like(f"%{client_name.lower()}%")).first()
    if client_obj:
        return json.dumps({"client_id": client_obj.id, "client_name": client_obj.name, "status": "found"})
    return json.dumps({"error": "Клиент не найден", "status": "not_found"})

def tool_check_stock(plant_name):
    """Инструмент для проверки свободных остатков по названию растения"""
    if not plant_name: return json.dumps({"error": "Название растения не указано"})
    plants = Plant.query.filter(func.lower(Plant.name).like(f"%{plant_name.lower()}%")).all()
    if not plants: return json.dumps({"error": f"Растение '{plant_name}' не найдено в базе"})
    
    plant_ids = [p.id for p in plants]
    stocks = StockBalance.query.filter(StockBalance.plant_id.in_(plant_ids), StockBalance.quantity > 0).all()
    
    if not stocks: return json.dumps({"error": "Товар есть в справочнике, но его нет на остатках"})
    
    results = [{"plant_name": s.plant.name, "size_name": s.size.name if s.size else "-", "field_name": s.field.name if s.field else "-", "available_qty": s.quantity} for s in stocks]
    return json.dumps({"status": "success", "stock": results})

def tool_create_dashboard_task(role, action_type, title, details, payload, assignee_id=None, deadline=None):
    """Инструмент для сохранения задачи в Дашборд"""
    try:
        dead_date = datetime.strptime(deadline, '%Y-%m-%d').date() if deadline else None
        
        # ЗАЩИТА: Проверяем, существует ли юзер с таким ID, который выдал ИИ
        if assignee_id:
            user_exists = User.query.get(assignee_id)
            if not user_exists:
                assignee_id = None # ИИ придумал ID, сбрасываем, чтобы не сломать БД
        
        new_task = TgTask(
            raw_text="Сгенерировано ИИ",
            title=title,
            details=details,
            assignee_role=role,
            assignee_id=assignee_id,
            deadline=dead_date,
            action_type=action_type,
            action_payload=json.dumps(payload, ensure_ascii=False) if payload else "{}"
        )
        db.session.add(new_task)
        db.session.commit()
        return json.dumps({"status": "success", "message": "Задача создана"})
    except Exception as e:
        db.session.rollback() # Очищаем сломанную сессию БД
        return json.dumps({"error": str(e)})

tools_schema = [
    {
        "type": "function",
        "function": {
            "name": "find_user",
            "description": "Поиск ID сотрудника системы по его имени, логину или упоминанию (например @Behruz, Алексей).",
            "parameters": {"type": "object", "properties": {"username": {"type": "string"}}, "required": ["username"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_client",
            "description": "Поиск ID клиента (покупателя).",
            "parameters": {"type": "object", "properties": {"client_name": {"type": "string"}}, "required": ["client_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_stock",
            "description": "Проверить остатки растений.",
            "parameters": {"type": "object", "properties": {"plant_name": {"type": "string"}}, "required": ["plant_name"]}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_dashboard_task",
            "description": "Создать карточку задачи в системе.",
            "parameters": {
                "type": "object",
                "properties": {
                    "role": {"type": "string", "enum": ["user", "user2", "admin"], "description": "Роль отдела (user-офис, user2-поле/бригадир)"},
                    "assignee_id": {"type": "integer", "description": "ID конкретного сотрудника (если найден через find_user). Иначе null."},
                    "deadline": {"type": "string", "description": "Дедлайн в формате YYYY-MM-DD. Вычисляй по правилам из инструкции."},
                    "action_type": {
                        "type": "string", 
                        "enum": ["create_order", "digging", "shipment", "income", "payment", "info"], 
                        "description": "Тип задачи: заказ (create_order), копка/поле (digging), отгрузка (shipment), поступление на склад (income), оплата/счет (payment), прочее (info)"
                    },
                    "title": {"type": "string", "description": "Суть (до 5 слов)"},
                    "details": {"type": "string", "description": "Подробное описание задачи"},
                    "payload": {"type": "object", "description": "ОБЯЗАТЕЛЬНО добавь сюда {'order_id': число}, если в тексте упоминается номер заказа. Иначе {}"}
                },
                "required": ["role", "action_type", "title", "details"]
            }
        }
    }
]

available_functions = {
    "find_user": tool_find_user,
    "find_client": tool_find_client,
    "check_stock": tool_check_stock,
    "create_dashboard_task": tool_create_dashboard_task,
}

def process_telegram_message_with_ai(raw_text, sender_name):
    from app.utils import msk_today
    today_date = msk_today()
    today_str = today_date.strftime('%Y-%m-%d')
    current_year = today_date.year
    current_month = today_date.month
    current_day = today_date.day

    system_prompt = f"""
    Ты - умный диспетчер ERP системы 'Зеленый Фонд'. 
    Сегодняшняя дата: {today_str} (Год: {current_year}, Месяц: {current_month}, Число: {current_day}).
    
    Твоя логика работы (СТРОГО СОБЛЮДАЙ ШАГИ):
    
    1. КТО ИСПОЛНИТЕЛЬ: Ищи упоминания сотрудников (через @ или имена, например @Behruz, Алексей). Вызывай 'find_user', чтобы узнать их 'role' и 'user_id'.
    
    2. ТИП ЗАДАЧИ (action_type) и РОЛЬ ОТДЕЛА (role):
       - 'create_order' (role='user'): завести заказ, оформить, добавить в счет клиента.
       - 'shipment' (role='user'): отгрузить товар клиенту (полностью или частично).
       - 'income' (role='user'): поступление товара, изменить остатки на складе.
       - 'payment' (role='user'): привязать оплату, выставить счет, отметить, что деньги пришли.
       - 'digging' (role='user2'): копать, вытаскивать, прикапывать саженцы, прополоть грядки, полевые работы.
       - 'info' (role=на твое усмотрение): любая другая задача или инфо.
       
    3. ДЕДЛАЙН (deadline) В ФОРМАТЕ YYYY-MM-DD:
       Внимательно читай текст и высчитывай дату отталкиваясь от {today_str}.
       ПРАВИЛА:
       - Если указана точная дата "19.04.2026" -> 2026-04-19
       - Если указано "до 19 апреля" -> {current_year}-04-19 (если 19 апреля уже прошло, то {current_year+1}-04-19).
       - Если указано просто число "до 10" или "до десятого":
         Сравни число с текущим днем ({current_day}). 
         Если число БОЛЬШЕ или РАВНО текущему ({current_day}) -> этот же месяц.
         Если число МЕНЬШЕ текущего ({current_day}) -> следующий месяц. 
         Пример: Сегодня 19.04. Если просят "до 25" -> 2026-04-25. Если просят "до 10" -> 2026-05-10.
       - Если дата не указана вообще -> deadline=null.

    4. СОЗДАНИЕ: Вызывай 'create_dashboard_task'. Передай вычисленный deadline, role, action_type и assignee_id.
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Сообщение от руководителя ({sender_name}): {raw_text}"}
    ]

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",  # <--- ИЗМЕНИЛИ МОДЕЛЬ ЗДЕСЬ
            messages=messages,
            tools=tools_schema,
            tool_choice="auto",
            temperature=0.1
        )
        response_message = response.choices[0].message
        tool_calls = response_message.tool_calls
        
        if tool_calls:
            messages.append(response_message)
            for tool_call in tool_calls:
                function_name = tool_call.function.name
                function_to_call = available_functions.get(function_name)
                if function_to_call:
                    # Безопасный парсинг аргументов от ИИ
                    try:
                        function_args = json.loads(tool_call.function.arguments)
                    except:
                        function_args = {}
                        
                    function_response = function_to_call(**function_args)
                    messages.append({"tool_call_id": tool_call.id, "role": "tool", "name": function_name, "content": function_response})
            
            client.chat.completions.create(model="llama-3.3-70b-versatile", messages=messages) # <--- И ИЗМЕНИЛИ ЗДЕСЬ
            
            last_task = TgTask.query.order_by(TgTask.id.desc()).first()
            if last_task and last_task.raw_text == "Сгенерировано ИИ":
                last_task.raw_text = raw_text
                last_task.sender_name = sender_name
                db.session.commit()
            return "Task created"
        else:
            db.session.add(TgTask(raw_text=raw_text, title="Указание из чата", details=response_message.content, assignee_role=None, sender_name=sender_name, action_type="info"))
            db.session.commit()
            return "Info saved"
    except Exception as e:
        print(f"AI Tool Error: {e}")
        db.session.rollback() # ВАЖНО: Сбрасываем все ошибки БД перед созданием ручной задачи
        db.session.add(TgTask(raw_text=raw_text, title="Сообщение из чата (Разобрать вручную)", details=raw_text, assignee_role=None, sender_name=sender_name, action_type="info"))
        db.session.commit()
        return "Fallback saved"