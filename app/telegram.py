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
    "digging": ["TG_CHAT_ID_HR", "TG_CHAT_ID"],
    "patents": ["TG_CHAT_ID_PATENTS", "TG_CHAT_ID_HR", "TG_CHAT_ID"],
}


def _get_chat_id(chat_type):
    keys = CHAT_ROUTES.get(chat_type, ["TG_CHAT_ID"])
    for key in keys:
        val = _resolve_chat_id(key)
        if val:
            return val
    return None


def send_message(text, chat_type="hr"):
    """Send a text message to Telegram.
    Returns (True, 'ok') or (False, error_description).
    """
    bot_token = _get_bot_token()
    chat_id = _get_chat_id(chat_type)
    if not bot_token or not chat_id:
        return False, "TG creds not configured"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        r = requests.post(url, json={
            'chat_id': chat_id,
            'text': text,
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

    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as f:
            r = requests.post(url, data={
                'chat_id': chat_id,
                'caption': caption,
                'parse_mode': 'HTML'
            }, files={'photo': f}, timeout=15)
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