import os
import requests
import json

def _resolve_chat_id(chat_env_key):
    """Resolve chat ID from environment variable name."""
    return os.environ.get(chat_env_key, "").strip() or None


def _get_bot_token():
    return os.environ.get("TG_BOT_TOKEN", "").strip() or None


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
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        r = requests.post(url, json={
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
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as f:
            r = requests.post(url, data={
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
    url = f"https://api.telegram.org/bot{bot_token}/setMessageReaction"
    try:
        r = requests.post(url, json={
            'chat_id': chat_id,
            'message_id': int(message_id),
            'reaction': [{'type': 'emoji', 'emoji': emoji}],
            'is_big': False,
        }, timeout=8)
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

    url = f"https://api.telegram.org/bot{bot_token}/sendMediaGroup"
    
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
            
            r = requests.post(url, data={
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
    """Личное сообщение в конкретный chat_id (не через CHAT_ROUTES)."""
    bot_token = _get_bot_token()
    if not bot_token or not chat_id:
        return False, "TG creds not configured"
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML',
    }
    if reply_markup:
        payload['reply_markup'] = reply_markup
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        r = requests.post(url, json=payload, timeout=8)
        if not r.ok:
            return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, "ok"


def send_chat_document(chat_id, path, filename=None, caption=''):
    """Отправляет файл в конкретный чат — на iPhone его удобнее открыть, чем качать из WebView."""
    bot_token = _get_bot_token()
    if not bot_token or not chat_id or not path:
        return False, "TG creds not configured"
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    name = filename or os.path.basename(path)
    try:
        with open(path, 'rb') as f:
            r = requests.post(
                url,
                data={'chat_id': chat_id, 'caption': caption or '', 'parse_mode': 'HTML'},
                files={'document': (name, f)},
                timeout=30,
            )
            if not r.ok:
                return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, "ok"


def download_bot_file(file_id):
    """Скачивает файл из Telegram по file_id. Возвращает (bytes, file_path) или (None, err)."""
    bot_token = _get_bot_token()
    if not bot_token or not file_id:
        return None, "TG creds not configured"
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{bot_token}/getFile",
            params={'file_id': file_id},
            timeout=15,
        )
        if not r.ok:
            return None, r.text
        file_path = (r.json().get('result') or {}).get('file_path')
        if not file_path:
            return None, "no file_path"
        fr = requests.get(
            f"https://api.telegram.org/file/bot{bot_token}/{file_path}",
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
        return env.rstrip('/')
    if os.path.isdir('/data') or os.environ.get('AMVERA'):
        return 'https://floraflowerp-warchesko.amvera.io/tg/pay'
    return ''


def set_pay_menu_button(url=None):
    """Кнопка меню бота → Mini App «Счета на оплату»."""
    bot_token = _get_bot_token()
    url = (url or default_miniapp_url() or '').rstrip('/')
    if not bot_token or not url.startswith('https://'):
        return False, 'skip'
    try:
        r = requests.post(
            f'https://api.telegram.org/bot{bot_token}/setChatMenuButton',
            json={
                'menu_button': {
                    'type': 'web_app',
                    'text': 'Счета',
                    'web_app': {'url': url},
                }
            },
            timeout=8,
        )
        if not r.ok:
            return False, r.text
    except Exception as exc:
        return False, str(exc)
    return True, 'ok'
