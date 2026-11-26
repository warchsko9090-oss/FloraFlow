"""Amvera kdoor entry: proxy → home, fallback → SHOP_READONLY витрина."""
from app.door_proxy import create_door_app

app = create_door_app()
