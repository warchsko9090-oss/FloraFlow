from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from app.models import db, User, BlockedIP
from app.utils import log_action, msk_now

bp = Blueprint('auth', __name__)

def get_client_ip():
    """Получает реальный IP пользователя (даже если сайт за прокси Amvera/Nginx)"""
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    return request.remote_addr

@bp.route('/login', methods=['GET', 'POST'])
def login():
    ip = get_client_ip()
    block_record = BlockedIP.query.filter_by(ip_address=ip).first()
    is_blocked = block_record and block_record.failed_attempts >= 3

    # Если IP заблокирован (отбиваем запрос сразу же, не давая нагружать сервер)
    if is_blocked:
        flash('Ваш IP-адрес заблокирован. Обратитесь к администратору.', 'danger')
        return render_template('auth/login.html', is_blocked=True)

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            # Вход успешен -> Сбрасываем счетчик ошибок для этого IP
            if block_record:
                block_record.failed_attempts = 0
                db.session.commit()
                
            login_user(user, remember=True)
            log_action(f"Вход в систему: {user.username} (IP: {ip})")
            
            if user.role == 'executive':
                return redirect(url_for('finance.expenses', tab='invoices'))
            if user.role in['brigadier', 'user2']:
                return redirect(url_for('hr.personnel', tab='timesheet'))
            return redirect(url_for('orders.orders_list'))
            
        else:
            # Ошибка входа -> Увеличиваем счетчик
            if not block_record:
                block_record = BlockedIP(ip_address=ip, failed_attempts=1, locked_at=msk_now())
                db.session.add(block_record)
            else:
                block_record.failed_attempts += 1
                block_record.locked_at = msk_now()
            db.session.commit()

            if block_record.failed_attempts >= 3:
                log_action(f"СИСТЕМА БЕЗОПАСНОСТИ: Заблокирован IP {ip} за подбор пароля")
                flash('Попытки входа исчерпаны. Ваш IP заблокирован в целях безопасности.', 'danger')
                return render_template('auth/login.html', is_blocked=True)
            else:
                attempts_left = 3 - block_record.failed_attempts
                flash(f'Неверный логин или пароль. Осталось попыток: {attempts_left}', 'warning')

    return render_template('auth/login.html', is_blocked=False)

@bp.route('/logout')
@login_required
def logout():
    log_action(f"Выход из системы: {current_user.username}")
    logout_user()
    return redirect(url_for('auth.login'))

@bp.route('/users', methods=['GET', 'POST'])
@login_required
def users_manage():
    if current_user.role not in ['admin', 'user2']: 
        return redirect(url_for('orders.orders_list'))
    
    if request.method == 'POST':
        if current_user.role != 'admin': 
            flash('Только админ может менять пользователей', 'danger') 
            return redirect(url_for('auth.users_manage'))
            
        action = request.form.get('action')
        
        if action == 'add':
            if User.query.filter_by(username=request.form.get('username')).first(): 
                flash('Пользователь с таким логином уже существует', 'danger')
            else:
                u = User(username=request.form.get('username'), role=request.form.get('role'))
                u.set_password(request.form.get('password'))
                db.session.add(u)
                db.session.commit()
                flash('Пользователь создан', 'success')
                log_action(f"Создал пользователя {u.username}")
                
        elif action == 'edit_user':
            u = User.query.get(request.form.get('user_id'))
            if not u:
                flash('Ошибка: Пользователь не найден', 'danger')
            else:
                new_username = request.form.get('username')
                existing = User.query.filter_by(username=new_username).first()
                if existing and existing.id != u.id:
                    flash('Ошибка: Логин уже занят другим пользователем', 'danger')
                else:
                    u.username = new_username
                    u.role = request.form.get('role')
                    db.session.commit()
                    flash('Пользователь обновлен', 'success')
                    log_action(f"Обновил пользователя {u.username}")
                
        elif action == 'delete':
            if int(request.form.get('user_id')) != current_user.id: 
                User.query.filter_by(id=request.form.get('user_id')).delete()
                db.session.commit()
                flash('Пользователь удален', 'success')
                log_action(f"Удалил пользователя ID {request.form.get('user_id')}")
            else:
                flash('Нельзя удалить самого себя', 'warning')
                
        elif action == 'change_password':
            u = User.query.get(request.form.get('user_id'))
            if u: 
                u.set_password(request.form.get('new_password'))
                db.session.commit()
                flash('Пароль изменен', 'success')
                log_action(f"Сменил пароль пользователю {u.username}")
                
        # --- СНЯТИЕ БЛОКИРОВКИ IP ---
        elif action == 'unblock_ip':
            ip = request.form.get('ip_address')
            BlockedIP.query.filter_by(ip_address=ip).delete()
            db.session.commit()
            flash(f'IP {ip} успешно разблокирован', 'success')
            log_action(f"Снял защитную блокировку с IP: {ip}")
                
        return redirect(url_for('auth.users_manage'))
        
    blocked_ips = BlockedIP.query.filter(BlockedIP.failed_attempts >= 3).all()
    return render_template('auth/users.html', users=User.query.all(), blocked_ips=blocked_ips)