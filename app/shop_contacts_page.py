"""Страница /shop/contacts — реквизиты, карта и текст о питомнике."""

import re

from markupsafe import Markup, escape
from sqlalchemy.exc import ProgrammingError

from app.models import ShopCompanyRequisite, ShopContactsPage, db

DEFAULT_ABOUT_1 = (
    '«Княжество» — архитектурный питомник в Тульской области. '
    'Выращиваем формованные и хвойные растения для ландшафтных проектов и B2B-поставок.'
)
DEFAULT_ABOUT_2 = (
    'Работаем с дизайнерами, застройщиками и оптовыми клиентами: фото с площадки, '
    'самовывоз и доставка, коммерческое предложение — в течение 15 минут.'
)
DEFAULT_LOCATION_ADDRESS = 'Тульская область, уточните адрес площадки у менеджера.'
DEFAULT_LOCATION_COORDINATES = '54.2044, 37.6111'

DEFAULT_REQUISITES = [
    ('Полное наименование', 'ООО «Княжество»'),
    ('ИНН', '0000000000'),
    ('КПП', '000000000'),
    ('ОГРН', '0000000000000'),
    ('Юридический адрес', '301000, Тульская область, …'),
    ('Фактический адрес / питомник', 'Тульская область, …'),
    ('Расчётный счёт', '40702810…'),
    ('Банк', '…'),
    ('БИК', '…'),
    ('Корр. счёт', '…'),
]


def get_contacts_page_settings():
    try:
        page = ShopContactsPage.query.get(1)
    except ProgrammingError as exc:
        # На проде колонок может ещё не быть после деплоя модели.
        # Делаем lazy-миграцию и повторяем запрос.
        db.session.rollback()
        msg = str(getattr(exc, 'orig', exc)).lower()
        if 'shop_contacts_page' in msg and ('location_address' in msg or 'location_coordinates' in msg):
            ensure_contacts_page_columns()
            page = ShopContactsPage.query.get(1)
        else:
            raise
    if not page:
        page = ShopContactsPage(
            id=1,
            location_address=DEFAULT_LOCATION_ADDRESS,
            location_coordinates=DEFAULT_LOCATION_COORDINATES,
        )
        db.session.add(page)
        db.session.commit()
    return page


def get_company_requisites(active_only=True):
    q = ShopCompanyRequisite.query
    if active_only:
        q = q.filter_by(is_active=True)
    return q.order_by(
        ShopCompanyRequisite.sort_order.asc(),
        ShopCompanyRequisite.id.asc(),
    ).all()


def yandex_map_embed_url(page: ShopContactsPage) -> str:
    lng = float(page.map_lng or 37.5)
    lat = float(page.map_lat or 54.2)
    zoom = int(page.map_zoom or 12)
    ll = f'{lng},{lat}'
    pt = f'{lng},{lat},pm2rdm'
    return (
        'https://yandex.ru/map-widget/v1/'
        f'?ll={ll}&z={zoom}&pt={pt}&l=map'
    )


def sanitize_map_embed_html(raw: str) -> str:
    """Разрешаем только iframe (конструктор Яндекс/Google карт)."""
    text = (raw or '').strip()
    if not text:
        return ''
    match = re.search(r'<iframe\b[^>]*>.*?</iframe>', text, re.I | re.S)
    if match:
        return match.group(0)
    if text.lower().startswith('<iframe'):
        return text
    return ''


def map_embed_markup(page: ShopContactsPage):
    custom = sanitize_map_embed_html(page.map_embed_html)
    if custom:
        return Markup(custom)
    src = escape(yandex_map_embed_url(page))
    return Markup(
        f'<iframe src="{src}" width="100%" height="100%" frameborder="0" '
        f'allowfullscreen="true" style="display:block;border:0;"></iframe>'
    )


def seed_contacts_page_defaults(db_session):
    if not ShopContactsPage.query.get(1):
        db_session.add(ShopContactsPage(
            id=1,
            page_heading='Контакты',
            about_heading='О питомнике',
            about_paragraph_1=DEFAULT_ABOUT_1,
            about_paragraph_2=DEFAULT_ABOUT_2,
            map_lat=54.2044,
            map_lng=37.6111,
            map_zoom=13,
            map_pin_label='Княжество — питомник',
            location_address=DEFAULT_LOCATION_ADDRESS,
            location_coordinates=DEFAULT_LOCATION_COORDINATES,
        ))
    if ShopCompanyRequisite.query.count() == 0:
        for i, (label, value) in enumerate(DEFAULT_REQUISITES):
            db_session.add(ShopCompanyRequisite(
                label=label,
                value=value,
                sort_order=(i + 1) * 10,
            ))
    db_session.commit()


def ensure_contacts_page_columns() -> None:
    """Добавляет недостающие колонки в shop_contacts_page без Alembic."""
    from sqlalchemy import inspect, text

    insp = inspect(db.engine)
    if not insp.has_table('shop_contacts_page'):
        return

    existing = {c['name'] for c in insp.get_columns('shop_contacts_page')}
    dialect = db.engine.dialect.name

    for col_name, col_type in (
        ('location_address', 'TEXT'),
        ('location_coordinates', 'VARCHAR(200)'),
    ):
        if col_name in existing:
            continue
        if dialect == 'postgresql':
            db.session.execute(text(
                f"ALTER TABLE shop_contacts_page ADD COLUMN IF NOT EXISTS {col_name} {col_type} DEFAULT ''"
            ))
        else:
            db.session.execute(text(
                f"ALTER TABLE shop_contacts_page ADD COLUMN {col_name} {col_type} DEFAULT ''"
            ))

    db.session.commit()
