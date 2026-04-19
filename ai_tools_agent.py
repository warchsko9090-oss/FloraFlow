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
    # Убираем @ если есть
    clean_name = username.replace('@', '').strip()
    
    user = User.query.filter(func.lower(User.username).like(f"%{clean_name.lower()}%")).first()
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
                    "role": {"type": "string", "enum": ["user", "brigadier", "admin"], "description": "Роль отдела (user-офис, brigadier-поле)"},
                    "assignee_id": {"type": "integer", "description": "ID конкретного сотрудника (если найден через find_user). Иначе null."},
                    "deadline": {"type": "string", "description": "Дедлайн в формате YYYY-MM-DD. Высчитывается из слов 'сегодня', 'завтра', 'в среду'."},
                    "action_type": {"type": "string", "enum": ["create_order", "digging", "info"], "description": "Тип: заказ, копка или просто инфо"},
                    "title": {"type": "string", "description": "Суть (до 5 слов)"},
                    "details": {"type": "string", "description": "Подробное описание задачи"},
                    "payload": {"type": "object", "description": "Найденные ID клиентов и товаров"}
                },
                "required": ["role", "action_type", "title", "details", "payload"]
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
    today_str = msk_today().strftime('%Y-%m-%d')
    system_prompt = f"""
    Ты - умный диспетчер ERP системы 'Зеленый Фонд'. Сегодня {today_str}.
    
    Твоя логика:
    1. Ищи упоминания сотрудников (через @ или имена). Если есть - вызывай 'find_user'.
    2. Высчитывай дедлайн (deadline) на основе сегодняшней даты ({today_str}), если он указан (например "до конца дня" = {today_str}).
    3. Создавай задачу через 'create_dashboard_task'. Если нашел сотрудника, передай его ID в 'assignee_id'.
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Сообщение от руководителя ({sender_name}): {raw_text}"}
    ]

    try:
        response = client.chat.completions.create(
            model="llama3-70b-8192",
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
                    function_args = json.loads(tool_call.function.arguments)
                    function_response = function_to_call(**function_args)
                    messages.append({"tool_call_id": tool_call.id, "role": "tool", "name": function_name, "content": function_response})
            
            client.chat.completions.create(model="llama3-70b-8192", messages=messages)
            
            last_task = TgTask.query.order_by(TgTask.id.desc()).first()
            if last_task and last_task.raw_text == "Сгенерировано ИИ":
                last_task.raw_text = raw_text
                last_task.sender_name = sender_name
                db.session.commit()
            return "Task created"
        else:
            db.session.add(TgTask(raw_text=raw_text, title="Вопрос из чата", details=response_message.content, assignee_role="user", sender_name=sender_name, action_type="info"))
            db.session.commit()
            return "Info saved"
    except Exception as e:
        print(f"AI Tool Error: {e}")
        db.session.add(TgTask(raw_text=raw_text, title="Сбой ИИ", details=raw_text, assignee_role="user", sender_name=sender_name))
        db.session.commit()
        return "Fallback saved"