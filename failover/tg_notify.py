"""Уведомление админу в Telegram."""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request


def send_telegram(bot_token: str, chat_id: str, text: str, *, api_base: str = "https://api.telegram.org") -> None:
    if not bot_token or not chat_id:
        raise RuntimeError("TG_BOT_TOKEN / TG_CHAT_ID not set")
    base = api_base.rstrip("/")
    url = f"{base}/bot{bot_token}/sendMessage"
    body = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text[:3500],
            "disable_web_page_preview": "1",
        }
    ).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"TG HTTP {exc.code}: {exc.read()[:300]}") from exc
    if not data.get("ok"):
        raise RuntimeError(f"TG error: {data}")
