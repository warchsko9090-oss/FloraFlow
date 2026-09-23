import os
import io
import re
import json
import logging
import requests
from urllib.parse import urlsplit

log = logging.getLogger(__name__)

_SESSION = None
_SESSION_PROXY = object()
_PROXY_CACHE = {'url': None, 'ts': 0}

_PROXY_ENV_KEYS = (
    'TG_PROXY', 'TELEGRAM_PROXY',
    'HTTPS_PROXY', 'https_proxy',
    'ALL_PROXY', 'all_proxy',
    'HTTP_PROXY', 'http_proxy',
)
_PROXY_SETTING_KEYS = (
    'tg_proxy', 'telegram_proxy', 'https_proxy', 'http_proxy',
    'socks_proxy', 'proxy',
)


# Telegram Web caches Mini App pages by exact URL. Bump after JS/HTML changes.
MINIAPP_CACHE_V = '20260923c'


def miniapp_web_url(url: str) -> str:
    """web_app URL with cache-buster so Telegram Web does not reuse an old shell."""
    url = (url or '').strip()
    if not url.startswith('https://'):
        return url
    if re.search(r'(?:\?|&)v=', url):
        return url
    return f"{url}{'&' if '?' in url else '?'}v={MINIAPP_CACHE_V}"

def _resolve_chat_id(chat_env_key):
    """Resolve chat ID from environment variable name."""
    return os.environ.get(chat_env_key, "").strip() or None


def _get_bot_token():
    return os.environ.get("TG_BOT_TOKEN", "").strip() or None


def _tg_root():
    """База Bot API. На Amvera api.telegram.org часто недоступен —
    ставят зеркало, например Cloudflare Worker (TG_API_BASE)."""
    raw = (
        os.environ.get('TG_API_BASE')
        or os.environ.get('TELEGRAM_API_BASE')
        or ''
    ).strip()
    if not raw:
        try:
            from flask import has_app_context
            if has_app_context():
                from app.models import AppSetting
                row = AppSetting.query.get('tg_api_base')
                if row and (row.value or '').strip():
                    raw = row.value.strip()
        except Exception:
            pass
    raw = (raw or 'https://api.telegram.org').rstrip('/')
    if not raw.startswith('http'):
        raw = 'https://' + raw
    return raw


def redact_secrets(text):
    """Не светить токен бота и пароль прокси в логах."""
    out = str(text or '')
    token = _get_bot_token()
    if token:
        out = out.replace(token, '***')
    proxy = _PROXY_CACHE.get('url') or ''
    if proxy:
        parts = urlsplit(proxy)
        if parts.password:
            out = out.replace(parts.password, '***')
        if parts.username:
            out = out.replace(parts.username, '***')
    return out


def _normalize_proxy(raw):
    raw = (raw or '').strip()
    if not raw:
        return None
    if raw.startswith('{'):
        try:
            data = json.loads(raw)
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        nested = data.get('url') or data.get('proxy') or data.get('tg_proxy')
        if nested:
            return _normalize_proxy(nested)
        host = data.get('host') or data.get('server') or data.get('ip')
        port = data.get('port')
        if not host or not port:
            return None
        scheme = str(data.get('type') or data.get('scheme') or 'socks5').lower()
        if scheme in ('socks', 'socks5'):
            scheme = 'socks5h'
        user = data.get('user') or data.get('username')
        password = data.get('password') or data.get('pass')
        auth = f'{user}:{password}@' if user else ''
        return f'{scheme}://{auth}{host}:{port}'
    lower = raw.lower()
    if 't.me/proxy' in lower or lower.startswith('tg://proxy'):
        # MTProto нельзя использовать из requests к Bot API.
        return None
    if '://' not in raw:
        raw = 'socks5h://' + raw
    if raw.startswith('socks5://'):
        raw = 'socks5h://' + raw[len('socks5://'):]
    return raw


def _proxy_from_env():
    for key in _PROXY_ENV_KEYS:
        val = (os.environ.get(key) or '').strip()
        if val:
            return val
    return None


def _proxy_from_db():
    try:
        from flask import has_app_context
        if not has_app_context():
            return None
        from app.models import AppSetting
        for key in _PROXY_SETTING_KEYS:
            row = AppSetting.query.get(key)
            if row and (row.value or '').strip():
                return row.value.strip()
        rows = AppSetting.query.all()
        for row in rows:
            key = (row.key or '').lower()
            if 'proxy' not in key:
                continue
            val = (row.value or '').strip()
            if val:
                return val
    except Exception:
        return None
    return None


def _resolve_proxy():
    import time
    now = time.time()
    if now - _PROXY_CACHE['ts'] < 30 and _PROXY_CACHE['ts']:
        return _PROXY_CACHE['url']
    raw = _proxy_from_env() or _proxy_from_db()
    if not raw:
        from flask import has_app_context
        if not has_app_context():
            return None
    url = _normalize_proxy(raw)
    _PROXY_CACHE['url'] = url
    _PROXY_CACHE['ts'] = now
    return url


def describe_proxy():
    """Для логов: схема и хост без логина/пароля."""
    url = _resolve_proxy()
    if not url:
        return 'off'
    parts = urlsplit(url)
    host = parts.hostname or '?'
    port = f':{parts.port}' if parts.port else ''
    return f'{parts.scheme}://{host}{port}'


def _http():
    global _SESSION, _SESSION_PROXY
    proxy = _resolve_proxy()
    if _SESSION is None or _SESSION_PROXY != proxy:
        if proxy and proxy.startswith('socks'):
            try:
                import socks  # noqa: F401  # PySocks
            except ImportError as exc:
                raise RuntimeError('SOCKS proxy needs PySocks (pip install PySocks)') from exc
        _SESSION = requests.Session()
        if proxy:
            _SESSION.proxies.update({'http': proxy, 'https': proxy})
        _SESSION_PROXY = proxy
        log.info('Telegram HTTP session proxy=%s', describe_proxy())
    return _SESSION


CHAT_ROUTES = {
    "hr": ["TG_CHAT_ID_HR", "TG_CHAT_ID"],
    "orders": ["TG_CHAT_ID_ORDERS", "TG_CHAT_ID"],
    # Отчёт по выкопке идёт в ту же группу, что отгрузки (не в производство).
    "digging": ["TG_CHAT_ID_ORDERS", "TG_CHAT_ID"],
    "patents": ["TG_CHAT_ID_PATENTS", "TG_CHAT_ID_HR", "TG_CHAT_ID"],
    # «Расходы Жемчужниково» — чат, по которому ходит монитор из app/expense_chat.py.
    # В этот чат бот тоже иногда пишет (например, подтверждения админа), поэтому
    # маршрут нужен и на исходящую сторону.
    "expenses": ["TG_CHAT_ID_EXPENSES", "TG_CHAT_ID"],
    # Оператор витрины — личные сообщения при запросе КП с /shop.
    "shop": ["TG_CHAT_ID_SHOP"],
}


def _get_chat_id(chat_type):
    # 1) Глобальный override для отладки: если задан TG_TEST_CHAT_ID,
    #    ВСЕ исходящие сообщения уходят в него, минуя CHAT_ROUTES.
    #    Удобно для локального тестирования: можно прогнать любой сценарий
    #    (HR/orders/digging/...) в личный чат, не задевая рабочие группы.
    test_id = _resolve_chat_id("TG_TEST_CHAT_ID")
    if test_id:
        return test_id

    keys = CHAT_ROUTES.get(chat_type, ["TG_CHAT_ID"])
    for key in keys:
        val = _resolve_chat_id(key)
        if val:
            return val
    return None


def _is_test_mode():
    return bool(_resolve_chat_id("TG_TEST_CHAT_ID"))


def _maybe_test_prefix(chat_type):
    """Префикс «🧪 [TEST · <тип>]» в начале сообщения, чтобы при
    переключении в тестовый режим случайно не принять отладочное
    сообщение за рабочее. Включается тем же фактом наличия
    TG_TEST_CHAT_ID — отдельного флага не нужно.
    """
    if not _is_test_mode():
        return ""
    return f"🧪 <b>[TEST · {chat_type}]</b>\n"


def send_message(text, chat_type="hr"):
    """Send a text message to Telegram.
    Returns (True, 'ok') or (False, error_description).
    """
    bot_token = _get_bot_token()
    chat_id = _get_chat_id(chat_type)
    if not bot_token or not chat_id:
        return False, "TG creds not configured"

    payload_text = _maybe_test_prefix(chat_type) + text
    url = f"{_tg_root()}/bot{bot_token}/sendMessage"
    try:
        r = _http().post(url, json={
            'chat_id': chat_id,
            'text': payload_text,
            'parse_mode': 'HTML'
        }, timeout=8)
        if not r.ok:
            return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, "ok"


def send_photo(photo_path, caption="", chat_type="hr"):
    """Send a photo to Telegram.
    Returns (True, 'ok') or (False, error_description).
    """
    bot_token = _get_bot_token()
    chat_id = _get_chat_id(chat_type)
    if not bot_token or not chat_id:
        return False, "TG creds not configured"

    payload_caption = (_maybe_test_prefix(chat_type) + caption) if caption or _is_test_mode() else caption
    url = f"{_tg_root()}/bot{bot_token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as f:
            r = _http().post(url, data={
                'chat_id': chat_id,
                'caption': payload_caption,
                'parse_mode': 'HTML'
            }, files={'photo': f}, timeout=15)
            if not r.ok:
                return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, "ok"

def set_reaction(chat_id, message_id, emoji='✅'):
    """Ставит реакцию бота на конкретное сообщение через Bot API
    `setMessageReaction` (добавлено в Bot API 7.0, февраль 2024).

    `chat_id` — строка или число, реальный id чата из апдейта (без резолва
    через CHAT_ROUTES: реакция всегда ставится туда же, где сообщение).
    `message_id` — int, id сообщения, на которое вешаем эмодзи.

    Возвращает (True, 'ok') или (False, err). Не кидает исключений —
    нам нельзя ронять основной поток ingest-а сообщений.
    """
    bot_token = _get_bot_token()
    if not bot_token or not chat_id or not message_id:
        return False, "TG creds/ids not configured"
    url = f"{_tg_root()}/bot{bot_token}/setMessageReaction"
    body = {
        'chat_id': chat_id,
        'message_id': int(message_id),
        'is_big': False,
    }
    if emoji:
        body['reaction'] = [{'type': 'emoji', 'emoji': emoji}]
    else:
        body['reaction'] = []
    try:
        r = _http().post(url, json=body, timeout=8)
        if not r.ok:
            return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, "ok"


def send_photo_album(photo_paths, chat_type="hr"):
    """
    Отправляет список фотографий как альбом (MediaGroup).
    Telegram принимает максимум 10 фото в одном альбоме, поэтому разбиваем на чанки.
    """
    bot_token = _get_bot_token()
    chat_id = _get_chat_id(chat_type)
    if not bot_token or not chat_id or not photo_paths:
        return False, "TG creds not configured or no photos"

    url = f"{_tg_root()}/bot{bot_token}/sendMediaGroup"
    
    # Telegram разрешает максимум 10 медиафайлов в одной группе
    chunks = [photo_paths[i:i + 10] for i in range(0, len(photo_paths), 10)]
    
    for chunk in chunks:
        media = []
        files = {}
        open_files =[]
        
        try:
            for i, path in enumerate(chunk):
                file_name = f"photo_{i}"
                f = open(path, 'rb')
                open_files.append(f)
                files[file_name] = f
                media.append({
                    'type': 'photo',
                    'media': f'attach://{file_name}'
                })
            
            r = _http().post(url, data={
                'chat_id': chat_id,
                'media': json.dumps(media)
            }, files=files, timeout=20)
            
            for f in open_files:
                f.close()
                
            if not r.ok:
                return False, r.text
        except Exception as exc:
            for f in open_files:
                f.close()
            return False, str(exc)
            
    return True, "ok"


def send_chat_message(chat_id, text, reply_markup=None):
    """Личное сообщение в конкретный chat_id (не через CHAT_ROUTES).

    Если Telegram отклоняет HTML или кнопку Mini App — повторяем без них,
    иначе пользователь видит полное молчание при живом вебхуке.
    """
    bot_token = _get_bot_token()
    if not bot_token or not chat_id:
        return False, "TG creds not configured"
    url = f"{_tg_root()}/bot{bot_token}/sendMessage"
    payloads = []
    base = {'chat_id': chat_id, 'text': text}
    if reply_markup:
        payloads.append({**base, 'parse_mode': 'HTML', 'reply_markup': reply_markup})
        payloads.append({**base, 'reply_markup': reply_markup})
    payloads.append({**base, 'parse_mode': 'HTML'})
    payloads.append(base)
    last_err = 'send failed'
    for payload in payloads:
        try:
            r = _http().post(url, json=payload, timeout=8)
            if r.ok:
                return True, "ok"
            last_err = r.text
        except Exception as exc:
            last_err = str(exc)
    return False, last_err


def send_chat_document(chat_id, path=None, filename=None, caption='', file_bytes=None):
    """Отправляет файл в конкретный чат — на iPhone его удобнее открыть, чем качать из WebView."""
    bot_token = _get_bot_token()
    if not bot_token or not chat_id:
        log.warning('sendDocument skipped: bot_token=%s chat_id=%s', bool(bot_token), chat_id)
        return False, "send_failed"
    url = f"{_tg_root()}/bot{bot_token}/sendDocument"
    name = filename or (os.path.basename(path) if path else 'invoice.pdf')
    try:
        if file_bytes is not None:
            files = {'document': (name, io.BytesIO(file_bytes))}
            r = _http().post(
                url,
                data={'chat_id': chat_id, 'caption': caption or ''},
                files=files,
                timeout=30,
            )
        else:
            if not path:
                return False, "file_missing"
            with open(path, 'rb') as f:
                r = _http().post(
                    url,
                    data={'chat_id': chat_id, 'caption': caption or ''},
                    files={'document': (name, f)},
                    timeout=30,
                )
        if not r.ok:
            log.warning(
                'sendDocument fail chat=%s status=%s body=%s',
                chat_id, r.status_code, (r.text or '')[:500],
            )
            return False, "send_failed"
    except Exception as exc:
        log.exception('sendDocument exception chat=%s', chat_id)
        return False, "send_failed"
    return True, "ok"


def send_document(filename=None, caption='', file_bytes=None, path=None, chat_type='orders'):
    """Файл в групповой чат (orders и т.п.)."""
    chat_id = _get_chat_id(chat_type)
    if not chat_id:
        return False, 'TG chat not configured'
    return send_chat_document(chat_id, path=path, filename=filename, caption=caption, file_bytes=file_bytes)


def download_bot_file(file_id):
    """Скачивает файл из Telegram по file_id. Возвращает (bytes, file_path) или (None, err)."""
    bot_token = _get_bot_token()
    if not bot_token or not file_id:
        return None, "TG creds not configured"
    try:
        r = _http().get(
            f"{_tg_root()}/bot{bot_token}/getFile",
            params={'file_id': file_id},
            timeout=15,
        )
        if not r.ok:
            return None, r.text
        file_path = (r.json().get('result') or {}).get('file_path')
        if not file_path:
            return None, "no file_path"
        fr = _http().get(
            f"{_tg_root()}/file/bot{bot_token}/{file_path}",
            timeout=30,
        )
        if not fr.ok:
            return None, fr.text
        return fr.content, file_path
    except Exception as exc:
        return None, str(exc)


def default_miniapp_url():
    env = (os.environ.get('TG_MINIAPP_URL') or '').strip()
    if env:
        u = env.rstrip('/')
        # Старые env указывали /tg/pay — единый вход теперь /tg
        if u.endswith('/tg/pay') or u.endswith('/tg/sale'):
            u = u.rsplit('/tg/', 1)[0] + '/tg'
        return u
    if os.path.isdir('/data') or os.environ.get('AMVERA'):
        return 'https://floraflowerp-warchesko.amvera.io/tg'
    return ''


def default_webhook_url():
    env = (os.environ.get('TG_WEBHOOK_URL') or '').strip()
    if env:
        return env.rstrip('/')
    mini = default_miniapp_url()
    if mini.startswith('https://'):
        for suffix in ('/tg/pay', '/tg/sale', '/tg'):
            if mini.endswith(suffix):
                return mini[: -len(suffix)] + '/api/telegram/webhook'
        return mini.rstrip('/') + '/api/telegram/webhook'
    return ''


def ensure_webhook(url=None):
    """После рестарта Amvera Telegram должен снова слать апдейты на прод."""
    bot_token = _get_bot_token()
    url = (url or default_webhook_url() or '').rstrip('/')
    if not bot_token or not url.startswith('https://'):
        return False, 'skip'
    try:
        r = _http().post(
            f'{_tg_root()}/bot{bot_token}/setWebhook',
            json={
                'url': url,
                'allowed_updates': [
                    'message', 'edited_message',
                    'channel_post', 'edited_channel_post',
                ],
                'drop_pending_updates': False,
            },
            timeout=10,
        )
        if not r.ok:
            return False, r.text
        data = r.json() if r.content else {}
        if not data.get('ok', True):
            return False, str(data)
    except Exception as exc:
        return False, str(exc)
    return True, url


def delete_webhook(drop_pending=False):
    bot_token = _get_bot_token()
    if not bot_token:
        return False, 'TG_BOT_TOKEN not set'
    try:
        r = _http().post(
            f'{_tg_root()}/bot{bot_token}/deleteWebhook',
            json={'drop_pending_updates': bool(drop_pending)},
            timeout=10,
        )
        data = r.json() if r.content else {}
        if not r.ok or not data.get('ok', True):
            return False, r.text or str(data)
        return True, 'ok'
    except Exception as exc:
        return False, str(exc)


def get_updates(offset=None, timeout=25):
    bot_token = _get_bot_token()
    if not bot_token:
        return []
    params = {
        'timeout': timeout,
        'allowed_updates': [
            'message', 'edited_message',
            'channel_post', 'edited_channel_post',
        ],
    }
    if offset:
        params['offset'] = offset
    r = _http().get(
        f'{_tg_root()}/bot{bot_token}/getUpdates',
        params=params,
        timeout=timeout + 10,
    )
    data = r.json() if r.content else {}
    if not data.get('ok'):
        raise RuntimeError(data.get('description') or r.text)
    return data.get('result') or []


def get_webhook_info():
    bot_token = _get_bot_token()
    if not bot_token:
        return {'ok': False, 'error': 'TG_BOT_TOKEN not set'}
    try:
        r = _http().get(
            f'{_tg_root()}/bot{bot_token}/getWebhookInfo',
            timeout=10,
        )
        return r.json() if r.content else {'ok': False, 'error': r.text}
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


def set_pay_menu_button(url=None, chat_id=None, text='FloraFlow'):
    """Кнопка меню бота → Mini App. chat_id — только для этого пользователя."""
    bot_token = _get_bot_token()
    url = miniapp_web_url((url or default_miniapp_url() or '').rstrip('/'))
    if not bot_token or not url.startswith('https://'):
        return False, 'skip'
    payload = {
        'menu_button': {
            'type': 'web_app',
            'text': text or 'FloraFlow',
            'web_app': {'url': url},
        }
    }
    if chat_id:
        payload['chat_id'] = chat_id
    try:
        r = _http().post(
            f'{_tg_root()}/bot{bot_token}/setChatMenuButton',
            json=payload,
            timeout=8,
        )
        if not r.ok:
            return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, 'ok'
