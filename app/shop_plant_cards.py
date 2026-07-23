"""Доп. атрибуты растений для витрины /shop (корневая система, стрижка)."""
from __future__ import annotations

from app.models import ShopPlantCard, db

# Подсказки для админки (необязательные значения, поле свободного ввода).
ROOT_SYSTEM_SUGGESTIONS = ('Контейнер', 'Горшок', 'Ком (WRB)', 'ОКС', 'ЗКС')
PRUNING_SUGGESTIONS = ('Шар', 'Ниваки', 'Куб', 'Штамб', 'Стрижка', 'Без стрижки')


def get_plant_card_map() -> dict[int, dict]:
    """{plant_id: {'root_system': str, 'pruning': str}} только с непустыми полями."""
    result: dict[int, dict] = {}
    for card in ShopPlantCard.query.all():
        root = (card.root_system or '').strip()
        pruning = (card.pruning or '').strip()
        if root or pruning:
            result[card.plant_id] = {'root_system': root, 'pruning': pruning}
    return result


def save_plant_card(plant_id: int, root_system: str, pruning: str) -> ShopPlantCard:
    """Создаёт/обновляет карточку растения. Не коммитит сессию."""
    root_system = (root_system or '').strip()[:160]
    pruning = (pruning or '').strip()[:160]

    card = ShopPlantCard.query.filter_by(plant_id=plant_id).first()
    if card is None:
        card = ShopPlantCard(plant_id=plant_id)
        db.session.add(card)
    card.root_system = root_system
    card.pruning = pruning
    return card
