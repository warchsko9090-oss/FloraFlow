from flask import Blueprint, jsonify, request, render_template, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import func, text
from app.models import db, KnowledgeBase, ChatLog, SQLExample
from app.ai_agent import process_query 

bp = Blueprint('chat', __name__)

@bp.route('/chat/feedback', methods=['POST'])
@login_required
def feedback():
    log_id = request.json.get('log_id')
    is_helpful = request.json.get('is_helpful')
    
    log_entry = ChatLog.query.get(log_id)
    if log_entry:
        log_entry.is_helpful = is_helpful
        db.session.commit()
        return jsonify({'status': 'ok'})
    return jsonify({'status': 'error'}), 404

@bp.route('/chat/generate_sql_helper', methods=['POST'])
@login_required
def generate_sql_helper():
    if current_user.role != 'admin': return jsonify({'sql': '-- Access denied'})
    description = request.json.get('description')
    from app.ai_agent import generate_sql_only
    sql = generate_sql_only(description)
    return jsonify({'sql': sql})

@bp.route('/chat/chips')
@login_required
def get_chips():
    questions = KnowledgeBase.query.order_by(func.random()).limit(5).all()
    chips = [q.question for q in questions]
    return jsonify({'chips': chips})

@bp.route('/chat/ask', methods=['POST'])
@login_required
def ask():
    user_msg = request.json.get('message', '').strip()
    if not user_msg: return jsonify({'answer': 'Пустой запрос?'})

    final_answer = ""
    source = "ai"

    # 1. Поиск в статической базе знаний
    kb_match = KnowledgeBase.query.filter(func.lower(KnowledgeBase.question) == user_msg.lower()).first()
    
    if not kb_match:
        all_kb = KnowledgeBase.query.all()
        for item in all_kb:
            tags = [t.strip() for t in item.keywords.split(',')]
            for tag in tags:
                if tag and tag in user_msg.lower():
                    kb_match = item
                    break
            if kb_match: break

    if kb_match:
        final_answer = kb_match.answer
        if kb_match.link:
            final_answer += f'<br><a href="{kb_match.link}" class="btn btn-sm btn-success mt-2">Перейти</a>'
        source = "base"
    else:
        # 2. AI (Динамика)
        try:
            # Если роль executive, представляемся для AI как admin
            ai_role = 'admin' if current_user.role == 'executive' else current_user.role
            final_answer = process_query(user_msg, ai_role)
        except Exception as e:
            final_answer = f"Ошибка системы: {e}"

    # 3. Логирование
    new_log = ChatLog(
        user_id=current_user.id,
        user_message=user_msg,
        ai_response=final_answer
    )
    db.session.add(new_log)
    db.session.commit()

    return jsonify({
        'answer': final_answer, 
        'log_id': new_log.id,
        'source': source
    })

from sqlalchemy import text # Убедись, что это импортировано в начале файла, если нет - добавь

@bp.route('/chat/test_sql_execution', methods=['POST'])
@login_required
def test_sql_execution():
    if current_user.role != 'admin': return jsonify({'error': 'Только админ'})
    
    sql = request.json.get('sql', '').strip()
    
    # Простейшая защита от дурака (чтобы случайно не удалить базу)
    if not sql.lower().startswith('select') and not sql.lower().startswith('with'):
        return jsonify({'error': 'Разрешены только SELECT запросы!'})

    try:
        # Выполняем SQL
        result = db.session.execute(text(sql))
        rows = result.fetchall()
        
        if not rows:
            return jsonify({'success': True, 'html': '<div class="text-muted">Запрос выполнен успешно, но данных не найдено (пусто).</div>'})
            
        # Формируем HTML табличку
        cols = result.keys()
        html = '<table class="table table-bordered table-xs small mb-0"><thead><tr>'
        for c in cols: html += f'<th>{c}</th>'
        html += '</tr></thead><tbody>'
        
        # Показываем первые 5 строк
        for row in rows[:5]:
            html += '<tr>'
            for cell in row: html += f'<td>{cell}</td>'
            html += '</tr>'
        html += '</tbody></table>'
        
        if len(rows) > 5:
            html += f'<div class="text-muted small mt-1">... и еще {len(rows)-5} строк</div>'
            
        return jsonify({'success': True, 'html': html})
        
    except Exception as e:
        return jsonify({'error': f"Ошибка SQL: {e}"})

@bp.route('/chat/manage', methods=['GET', 'POST'])
@login_required
def manage():
    if current_user.role != 'admin': return redirect(url_for('main.index'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            type_add = request.form.get('type_add') # 'text' или 'sql'
            
            if type_add == 'sql':
                db.session.add(SQLExample(
                    question=request.form.get('question'),
                    sql_query=request.form.get('sql_query')
                ))
                flash('AI обучен новому SQL-запросу!')
            else:
                db.session.add(KnowledgeBase(
                    question=request.form.get('question'),
                    keywords=request.form.get('keywords').lower(),
                    answer=request.form.get('answer'),
                    link=request.form.get('link')
                ))
                flash('Статический ответ сохранен!')

            # Отмечаем лог как исправленный
            log_id = request.form.get('log_id_source')
            if log_id:
                log = ChatLog.query.get(log_id)
                if log: log.is_helpful = True 
                
            db.session.commit()
            
        elif action == 'delete':
            KnowledgeBase.query.filter_by(id=request.form.get('id')).delete()
            db.session.commit()
            flash('Удалено')
            
        return redirect(url_for('chat.manage'))
    
    kb_items = KnowledgeBase.query.all()
    bad_logs = ChatLog.query.filter_by(is_helpful=False).order_by(ChatLog.date.desc()).all()
    
    return render_template('chat/chat_manage.html', items=kb_items, bad_logs=bad_logs)