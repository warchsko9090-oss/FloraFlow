import os
import json
from groq import Groq
from app.models import db, Client, Plant, Size, StockBalance, TgTask
from sqlalchemy import func

# Инициализация клиента Groq
# Убедись, что переменная окружения GROQ_API_KEY задана на сервере
client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))

def tool_find_client(client_name):
    """Инструмент для поиска ID клиента по имени"""
    if not client_name:
        return json.dumps({"error": "Имя не указано"})
    
    # Ищем частичное совпадение (без учета регистра)
    client_obj = Client.query.filter(func.lower(Client.name).like(f"%{client_name.lower()}%")).first()
    
    if client_obj:
        return json.dumps({
            "client_id": client_obj.id, 
            "client_name": client_obj.name,
            "status": "found"
        })
    return json.dumps({"error": "Клиент не найден", "status": "not_found"})

def tool_check_stock(plant_name):
    """Инструмент для проверки свободных остатков по названию растения"""
    if not plant_name:
        return json.dumps({"error": "Название растения не указано"})
    
    plants = Plant.query.filter(func.lower(Plant.name).like(f"%{plant_name.lower()}%")).all()
    if not plants:
        return json.dumps({"error": f"Растение '{plant_name}' не найдено в базе"})
    
    plant_ids = [p.id for p in plants]
    
    # Считаем свободные остатки (Упрощенно для примера, берем просто факт на складе > 0)
    stocks = StockBalance.query.filter(StockBalance.plant_id.in_(plant_ids), StockBalance.quantity > 0).all()
    
    if not stocks:
        return json.dumps({"error": "Товар найден в базе, но его сейчас нет на остатках (0 шт)"})
    
    results = []
    for s in stocks:
        results.append({
            "plant_name": s.plant.name,
            "size_name": s.size.name if s.size else "Без размера",
            "field_name": s.field.name if s.field else "Без поля",
            "available_qty": s.quantity,
            "price": float(s.price)
        })
    
    return json.dumps({"status": "success", "stock": results})

def tool_create_dashboard_task(role, action_type, title, details, payload):
    """Инструмент для сохранения структурированной задачи в Дашборд ERP"""
    try:
        new_task = TgTask(
            raw_text="Сгенерировано ИИ",
            title=title,
            details=details,
            assignee_role=role,
            action_type=action_type,
            action_payload=json.dumps(payload, ensure_ascii=False) if payload else "{}"
        )
        db.session.add(new_task)
        db.session.commit()
        return json.dumps({"status": "success", "message": "Задача успешно отправлена в Дашборд"})
    except Exception as e:
        return json.dumps({"error": str(e)})

# --- ОПИСАНИЕ ИНСТРУМЕНТОВ ДЛЯ НЕЙРОСЕТИ (JSON SCHEMA) ---
tools_schema = [
    {
        "type": "function",
        "function": {
            "name": "find_client",
            "description": "Найти ID клиента в базе данных ERP по его имени или фамилии (например, 'Овчинников').",
            "parameters": {
                "type": "object",
                "properties": {
                    "client_name": {"type": "string", "description": "Имя или фамилия клиента"}
                },
                "required": ["client_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_stock",
            "description": "Проверить наличие и свободные остатки растения на складе (например, 'Боярышник').",
            "parameters": {
                "type": "object",
                "properties": {
                    "plant_name": {"type": "string", "description": "Название растения для поиска"}
                },
                "required": ["plant_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_dashboard_task",
            "description": "Поставить структурированную задачу сотрудникам в Дашборд ERP.",
            "parameters": {
                "type": "object",
                "properties": {
                    "role": {"type": "string", "enum": ["user", "brigadier"], "description": "Кому задача: user (Менеджер - заказы, счета), brigadier (Бригадир - копка, поле)"},
                    "action_type": {"type": "string", "enum": ["create_order", "digging", "info"], "description": "Тип действия"},
                    "title": {"type": "string", "description": "Краткий заголовок задачи (до 5 слов)"},
                    "details": {"type": "string", "description": "Подробное описание задачи для человека"},
                    "payload": {"type": "object", "description": "JSON объект с найденными ID (client_id, список растений, количества). Если ID не найдены, передать пустой объект {}"}
                },
                "required": ["role", "action_type", "title", "details", "payload"]
            }
        }
    }
]

# Словарик для вызова функций по имени
available_functions = {
    "find_client": tool_find_client,
    "check_stock": tool_check_stock,
    "create_dashboard_task": tool_create_dashboard_task,
}

def process_telegram_message_with_ai(raw_text, sender_name):
    """
    Главная функция обработки сообщения. 
    ИИ может вызывать функции по цепочке, пока не решит задачу.
    """
    system_prompt = f"""
    Ты - диспетчер питомника растений 'Зеленый Фонд'. 
    Твоя задача: читать сообщения от руководителя из Telegram и превращать их в действия внутри ERP системы.
    
    ПРАВИЛА:
    1. Если руководитель просит оформить заказ, ТЫ ОБЯЗАН СНАЧАЛА вызвать инструмент 'find_client', чтобы получить ID клиента.
    2. Если руководитель упоминает растения, вызови 'check_stock', чтобы убедиться, что они есть на складе.
    3. В самом конце ТЫ ОБЯЗАН вызвать инструмент 'create_dashboard_task', чтобы передать задачу менеджерам (role: user) или бригадиру (role: brigadier).
    4. В 'payload' инструмента 'create_dashboard_task' положи найденный client_id и список запрошенных растений с количествами.
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Сообщение от руководителя ({sender_name}): {raw_text}"}
    ]

    # Шаг 1: Отправляем сообщение ИИ и даем ему список инструментов
    try:
        response = client.chat.completions.create(
            model="llama3-70b-8192", # Используем мощную модель, которая понимает инструменты
            messages=messages,
            tools=tools_schema,
            tool_choice="auto",
            temperature=0.1,
            max_tokens=1000
        )
        
        response_message = response.choices[0].message
        
        # Шаг 2: Проверяем, решил ли ИИ использовать инструменты
        tool_calls = response_message.tool_calls
        
        if tool_calls:
            # ИИ решил дернуть функции. Добавляем его ответ в историю
            messages.append(response_message)
            
            # Выполняем все функции, которые запросил ИИ
            for tool_call in tool_calls:
                function_name = tool_call.function.name
                function_to_call = available_functions.get(function_name)
                
                if function_to_call:
                    function_args = json.loads(tool_call.function.arguments)
                    
                    # Вызываем питоновскую функцию
                    function_response = function_to_call(**function_args)
                    
                    # Возвращаем ИИ результат работы функции (например, ID клиента из базы)
                    messages.append({
                        "tool_call_id": tool_call.id,
                        "role": "tool",
                        "name": function_name,
                        "content": function_response,
                    })
            
            # Шаг 3: ИИ получил данные из базы. Просим его сделать финальный вывод
            final_response = client.chat.completions.create(
                model="llama3-70b-8192",
                messages=messages
            )
            
            # Обновляем сырой текст в последней созданной задаче, чтобы сохранить историю
            last_task = TgTask.query.order_by(TgTask.id.desc()).first()
            if last_task and last_task.raw_text == "Сгенерировано ИИ":
                last_task.raw_text = raw_text
                last_task.sender_name = sender_name
                db.session.commit()
                
            return "Задача успешно обработана ИИ и добавлена в Дашборд."
            
        else:
            # ИИ решил не использовать инструменты, а просто ответил текстом
            # Такое бывает, если вопрос абстрактный. Сохраняем просто как инфо-задачу.
            new_task = TgTask(
                raw_text=raw_text,
                title="Вопрос из чата",
                details=response_message.content,
                assignee_role="user",
                action_type="info",
                sender_name=sender_name
            )
            db.session.add(new_task)
            db.session.commit()
            return "Сообщение сохранено как Инфо-задача."

    except Exception as e:
        print(f"AI Tool Error: {e}")
        # Запасной вариант (Fallback)
        fallback_task = TgTask(raw_text=raw_text, title="Сбой ИИ разбора", details=raw_text, assignee_role="user")
        db.session.add(fallback_task)
        db.session.commit()
        return "Произошел сбой, сообщение сохранено как простой текст."