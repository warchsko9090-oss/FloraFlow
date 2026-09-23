"""Единый вход Mini App: синяя кнопка → /tg, вкладки по ролям."""
from __future__ import annotations

from flask import Blueprint, jsonify, redirect, render_template, request

from app.models import User
from app.tg_pay import (
    _can_pay_app, _can_sale_role, resolve_user, set_mini_cookie,
    current_telegram_id, log_mini_auth_fail, _auth_fail_hint,
)

bp = Blueprint('tg_hub', __name__, url_prefix='/tg')


def public_hub_url() -> str:
    from app.telegram import default_miniapp_url, miniapp_web_url
    base = (default_miniapp_url() or '').rstrip('/')
    if base.endswith('/tg/pay') or base.endswith('/tg/sale'):
        base = base.rsplit('/tg/', 1)[0] + '/tg'
    elif base.endswith('/tg'):
        pass
    elif base.startswith('https://'):
        # TG_MINIAPP_URL может указывать на /tg/pay — нормализуем.
        if '/tg' not in base:
            base = base + '/tg'
    url = base if base.endswith('/tg') else (base + '/tg' if base else '')
    return miniapp_web_url(url) if url else ''


def _apps_for(user: User | None) -> dict:
    return {
        'pay': bool(user and _can_pay_app(user)),
        'sale': bool(user and _can_sale_role(user)),
    }


def _default_path(apps: dict) -> str:
    start = (request.args.get('startapp') or request.args.get('tab') or '').strip().lower()
    if start in ('sale', 'client', 'выставить') and apps.get('sale'):
        return '/tg/sale'
    if start in ('pay', 'оплата') and apps.get('pay'):
        return '/tg/pay'
    if apps.get('pay') and apps.get('sale'):
        return '/tg/pay'
    if apps.get('sale'):
        return '/tg/sale'
    if apps.get('pay'):
        return '/tg/pay'
    return ''


@bp.route('/')
@bp.route('')
def index():
    """Оболочка с вкладками; при одной роли сразу редирект из JS."""
    return render_template('tg_hub/index.html')


@bp.route('/api/auth', methods=['POST'])
def api_auth():
    user, is_dev, pending = resolve_user()
    if not user:
        if pending:
            return jsonify({
                'error': 'not_linked',
                'telegram_id': pending.get('id'),
                'username': (pending.get('username') or ''),
                'hint': 'Этот Telegram не привязан к пользователю ERP. Войдите логином и паролем один раз.',
                'need_login': True,
            }), 403
        log_mini_auth_fail()
        return jsonify({
            'error': 'unauthorized',
            'hint': _auth_fail_hint(),
            'need_login': True,
        }), 401
    apps = _apps_for(user)
    if not apps['pay'] and not apps['sale']:
        return jsonify({
            'error': 'forbidden',
            'hint': 'Нет доступа к Mini App для роли «' + (user.role or '') + '»',
            'need_login': False,
        }), 403
    session_tg = current_telegram_id()
    resp = jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'dev': is_dev,
        'telegram_id': session_tg,
        'has_telegram': bool(session_tg),
        'apps': apps,
        'default_path': _default_path(apps),
    })
    return set_mini_cookie(resp, user, session_tg)


@bp.route('/api/login', methods=['POST'])
def api_login():
    from app.tg_pay import (
        _bind_telegram_id_force, _telegram_id_from_init_data, _telegram_id_from_mini_cookie,
    )
    from app.models import db
    body = request.get_json(silent=True) if request.is_json else None
    if not isinstance(body, dict):
        body = {}
    username = (body.get('username') or '').strip()
    password = body.get('password') or ''
    if not username or not password:
        return jsonify({'error': 'need_credentials', 'hint': 'Введите логин и пароль ERP'}), 400
    user = User.query.filter(db.func.lower(User.username) == username.lower()).first()
    if not user or not user.check_password(password):
        return jsonify({'error': 'bad_credentials', 'hint': 'Неверный логин или пароль'}), 401
    apps = _apps_for(user)
    if not apps['pay'] and not apps['sale']:
        return jsonify({
            'error': 'forbidden',
            'hint': 'Нет доступа к Mini App для роли «' + (user.role or '') + '»',
        }), 403
    session_tg = _telegram_id_from_init_data() or _telegram_id_from_mini_cookie()
    if session_tg:
        _bind_telegram_id_force(user, session_tg)
    resp = jsonify({
        'id': user.id,
        'username': user.username,
        'role': user.role,
        'dev': False,
        'telegram_id': session_tg,
        'has_telegram': bool(session_tg),
        'bound': bool(session_tg),
        'apps': apps,
        'default_path': _default_path(apps),
    })
    return set_mini_cookie(resp, user, session_tg)


@bp.route('/go/<tab>')
def go_tab(tab: str):
    """Глубокая ссылка с синей кнопки / startapp."""
    tab = (tab or '').strip().lower()
    if tab in ('sale', 'client'):
        return redirect('/tg/sale')
    return redirect('/tg/pay')
