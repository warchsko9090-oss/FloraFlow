"""Главная страница knyajestvo.ru — тексты и фото из админки."""

from __future__ import annotations

import os
from types import SimpleNamespace

from app.models import ShopLandingPage, db
from app.photo_storage import (
    LANDING_DESIRE_SLIDES_REL_DIR,
    LANDING_PRICE_TOP_REL_DIR,
    SIDEBAR_REL_DIR,
    list_landing_desire_files,
    list_sidebar_files,
)
from app.shop_images import SHOP_IMG_BG, SHOP_IMG_CARD, shop_image_url

LANDING_REL_DIR = 'data-landing'
LANDING_PRICE_TOP_CARD_COUNT = 10
LANDING_PRICE_BOTTOM_CARD_COUNT = 3


def _replace_legacy_terms(text: str) -> str:
    """Единая замена устаревших формулировок в витрине."""
    value = text or ''
    return (
        value
        .replace('агронома', 'менеджера')
        .replace('Агронома', 'Менеджера')
        .replace('агроном', 'менеджер')
        .replace('Агроном', 'Менеджер')
    )

DEFAULTS = {
    'meta_description': (
        'Премиальный посадочный материал для ландшафтных архитекторов и частных усадеб. '
        'Питомник «Княжество», Тульская область.'
    ),
    'hero_badge': 'Для ландшафтных архитекторов и частных усадеб',
    'hero_title': 'Взрослые растения с безупречным габитусом',
    'hero_subtitle': (
        'Премиальный посадочный материал из питомника «Княжество». '
        'Правильно сформированная корневая система, высокая приживаемость и строгий отбор каждого дерева.'
    ),
    'hero_btn_catalog': 'Получить прайс и наличие',
    'hero_btn_consult': 'Консультация менеджера',
    'interest_eyebrow': 'Качество',
    'interest_title': 'Стандарт качества «Княжества»',
    'interest_subtitle': (
        'На премиальных объектах нет права на ошибку. Материал готов к высадке '
        'и созданию взрослого сада уже в первый сезон.'
    ),
    'card1_title': 'Идеальная крона',
    'card1_text': (
        'Регулярная обрезка и формовка на всех этапах роста. Симметричная крона без проплешин — '
        'готовая архитектурная форма.'
    ),
    'card2_title': 'Подготовленные корни',
    'card2_text': (
        'Многократная перешколка формирует плотный корневой ком — меньше стресса при пересадке '
        'и выше приживаемость.'
    ),
    'card3_title': 'Полная акклиматизация',
    'card3_text': (
        'Материал выращен и адаптирован к нашему климату. Уверенная зимовка без сложного '
        'реанимационного ухода.'
    ),
    'desire_title': 'Материал, за который',
    'desire_title_accent': 'вы ручаетесь перед заказчиком',
    'desire_p1': (
        'Когда вы сдаёте объект, каждое растение должно выглядеть безупречно с первого дня. '
        'Больные листья или кривые стволы разрушают впечатление от самого дорогого проекта.'
    ),
    'desire_p2': (
        'В «Княжестве» мы берём эти риски на себя. Вы получаете предсказуемый, роскошный результат, '
        'который подчеркнёт ваш профессионализм.'
    ),
    'desire_check_1': 'Широкий размерный ряд крупномеров',
    'desire_check_2': 'Возможность лично отобрать материал в питомнике',
    'desire_check_3': 'Профессиональная упаковка и бережная погрузка',
    'desire_check_4': '',
    'desire_check_5': '',
    'desire_check_6': '',
    'form_title': 'Обсудить проект или получить прайс',
    'form_subtitle': (
        'Оставьте контакты — менеджер свяжется с вами, проконсультирует по наличию '
        'и отправит актуальный каталог с ценами.'
    ),
    'form_btn': 'Получить прайс и консультацию',
    'price_top_eyebrow': 'Прайс',
    'price_top_title': 'Ориентиры по стоимости материала',
    'price_top_subtitle': (
        'Примеры позиций из актуального прайса — чтобы сразу понять уровень качества '
        'и диапазон цен на взрослый посадочный материал.'
    ),
    'price_top1_name': 'Берёза повислая',
    'price_top1_spec': 'H 3–4 м · ствол 16–18 см',
    'price_top1_price': 'от 48 000 ₽',
    'price_top1_note': 'Сформированная крона, плотный пересадочный ком, готова к высадке.',
    'price_top2_name': 'Туя западная Brabant',
    'price_top2_spec': 'H 2,5–3 м · контейнер C25',
    'price_top2_price': 'от 12 500 ₽',
    'price_top2_note': 'Плотная колонна, ровный габитус, акклиматизирована в питомнике.',
    'price_top3_name': 'Ginkgo biloba',
    'price_top3_spec': 'H 2–2,5 м · обхват 14–16 см',
    'price_top3_price': 'от 65 000 ₽',
    'price_top3_note': 'Редкая декоративная форма, строгий отбор по симметрии кроны.',
    'price_top_footer': 'Точные цены и наличие — в каталоге и по запросу менеджера.',
    'price_bottom_eyebrow': 'Ценность',
    'price_bottom_title': 'За что вы платите',
    'price_bottom_subtitle': (
        'В стоимость входит не «просто дерево», а готовый архитектурный элемент: '
        'формировка, пересадочная подготовка и контроль качества на каждом этапе.'
    ),
    'price_bottom1_name': 'Клён остролистный',
    'price_bottom1_spec': 'H 4–5 м · крупномер',
    'price_bottom1_price': 'от 120 000 ₽',
    'price_bottom1_note': 'Взрослый габитус для парадных аллей и акцентных групп.',
    'price_bottom2_name': 'Ель обыкновенная',
    'price_bottom2_spec': 'H 3–4 м · плотная крона',
    'price_bottom2_price': 'от 38 000 ₽',
    'price_bottom2_note': 'Уверенная зимовка, материал адаптирован к нашему климату.',
    'price_bottom3_name': 'Сирень обыкновенная',
    'price_bottom3_spec': 'H 2–2,5 м · штамбовая форма',
    'price_bottom3_price': 'от 18 000 ₽',
    'price_bottom3_note': 'Декоративная форма для миксбордеров и придомовых зон.',
    'price_bottom_footer': 'При заказе от 10 древесных позиций — условия для профессионалов и подрядчиков.',
}


PRICE_HOOK_FIELDS = tuple(
    k for k in DEFAULTS if k.startswith('price_')
)
_LANDING_SKIP_MIGRATE = frozenset({'id', 'updated_at'})


def ensure_shop_landing_schema() -> None:
    """Создаёт shop_landing_page и недостающие колонки (shop-контейнер стартует отдельно)."""
    from sqlalchemy import inspect

    insp = inspect(db.engine)
    if not insp.has_table('shop_landing_page'):
        db.create_all()
    else:
        ensure_landing_page_columns()
    seed_landing_page_defaults(db.session)


def ensure_landing_page_columns() -> None:
    """Добавляет новые колонки в shop_landing_page без Alembic."""
    from sqlalchemy import inspect, text

    insp = inspect(db.engine)
    if not insp.has_table('shop_landing_page'):
        return

    existing = {c['name'] for c in insp.get_columns('shop_landing_page')}
    dialect = db.engine.dialect.name
    model_cols = ShopLandingPage.__table__.columns

    for col in model_cols:
        name = col.name
        if name in _LANDING_SKIP_MIGRATE or name in existing:
            continue
        col_type = col.type.compile(dialect=db.engine.dialect)
        if dialect == 'postgresql':
            db.session.execute(text(
                f'ALTER TABLE shop_landing_page ADD COLUMN IF NOT EXISTS {name} {col_type} DEFAULT \'\''
            ))
        else:
            db.session.execute(text(
                f'ALTER TABLE shop_landing_page ADD COLUMN {name} {col_type} DEFAULT \'\''
            ))
    db.session.commit()

    page = db.session.get(ShopLandingPage, 1)
    if not page:
        return
    changed = False
    for key, val in DEFAULTS.items():
        if not (getattr(page, key, None) or '').strip():
            setattr(page, key, val)
            changed = True
    if changed:
        db.session.commit()


def _landing_price_card(page: ShopLandingPage, prefix: str, n: int, *, with_photo: bool = False) -> dict:
    card = {
        'name': (getattr(page, f'{prefix}{n}_name', None) or '').strip(),
        'spec': (getattr(page, f'{prefix}{n}_spec', None) or '').strip(),
        'price': (getattr(page, f'{prefix}{n}_price', None) or '').strip(),
        'note': (getattr(page, f'{prefix}{n}_note', None) or '').strip(),
    }
    if with_photo:
        card['photo'] = landing_image_public_url(
            getattr(page, f'{prefix}{n}_image', None),
            SHOP_IMG_CARD,
        )
    return card


def _landing_price_block(page: ShopLandingPage, which: str) -> dict:
    prefix = 'price_top' if which == 'top' else 'price_bottom'
    card_count = LANDING_PRICE_TOP_CARD_COUNT if which == 'top' else LANDING_PRICE_BOTTOM_CARD_COUNT
    cards = []
    for n in range(1, card_count + 1):
        card = _landing_price_card(page, prefix, n, with_photo=(which == 'top'))
        if any(card.values()):
            cards.append(card)
    title = (getattr(page, f'{prefix}_title', None) or '').strip()
    return {
        'eyebrow': (getattr(page, f'{prefix}_eyebrow', None) or '').strip(),
        'title': title,
        'subtitle': (getattr(page, f'{prefix}_subtitle', None) or '').strip(),
        'footer': (getattr(page, f'{prefix}_footer', None) or '').strip(),
        'cards': cards,
        'show_photos': which == 'top',
        'show': bool(title or cards),
    }


def get_landing_desire_slide_urls(width: int = SHOP_IMG_BG) -> list[str]:
    from flask import current_app

    root = current_app.config['UPLOAD_FOLDER']
    local_files = list_landing_desire_files(root)
    if local_files:
        return [
            shop_image_url(f'{LANDING_DESIRE_SLIDES_REL_DIR}/{name}', width)
            for name in local_files
        ]

    erp = (os.environ.get('ERP_PUBLIC_BASE_URL') or '').strip().rstrip('/')
    if erp:
        import requests

        try:
            r = requests.get(f'{erp}/public/client/landing-desire-slides', timeout=15)
            if r.ok:
                data = r.json()
                return [u for u in (data.get('items') or []) if u]
        except Exception:
            pass
    return []


def landing_desire_slides_payload(width: int = SHOP_IMG_BG) -> dict:
    from app.shop_sidebar import SIDEBAR_SLIDE_INTERVAL_MS

    return {
        'status': 'ok',
        'items': get_landing_desire_slide_urls(width),
        'interval_ms': SIDEBAR_SLIDE_INTERVAL_MS,
    }


def get_landing_page_settings() -> ShopLandingPage:
    from sqlalchemy import inspect

    if inspect(db.engine).has_table('shop_landing_page'):
        ensure_landing_page_columns()
    db.session.expire_all()
    try:
        page = db.session.get(ShopLandingPage, 1)
    except Exception as exc:
        from sqlalchemy.exc import ProgrammingError, OperationalError

        if not isinstance(exc, (ProgrammingError, OperationalError)):
            raise
        db.session.rollback()
        ensure_shop_landing_schema()
        page = db.session.get(ShopLandingPage, 1)
    if not page:
        page = ShopLandingPage(id=1)
        for key, val in DEFAULTS.items():
            setattr(page, key, val)
        db.session.add(page)
        db.session.commit()
        db.session.refresh(page)
    return page


def seed_landing_page_defaults(db_session):
    if ShopLandingPage.query.get(1):
        return
    page = ShopLandingPage(id=1)
    for key, val in DEFAULTS.items():
        setattr(page, key, val)
    db_session.add(page)
    db_session.commit()


def _landing_upload_dir():
    from flask import current_app
    path = os.path.join(current_app.config['UPLOAD_FOLDER'], LANDING_REL_DIR)
    os.makedirs(path, exist_ok=True)
    return path


def landing_image_public_url(stored_path: str | None, width: int = SHOP_IMG_BG) -> str | None:
    if not (stored_path or '').strip():
        return None
    rel = stored_path.replace('\\', '/').lstrip('/')
    return shop_image_url(rel, width)


def _sidebar_fallback(index: int = 0, width: int = SHOP_IMG_BG) -> str | None:
    from flask import current_app
    try:
        files = list_sidebar_files(current_app.config['UPLOAD_FOLDER'])
        if not files:
            return None
        idx = min(max(index, 0), len(files) - 1)
        rel = f'{SIDEBAR_REL_DIR}/{files[idx]}'
        return shop_image_url(rel, width)
    except Exception:
        return None


def resolve_landing_images(page: ShopLandingPage) -> dict:
    hero = landing_image_public_url(page.hero_image, SHOP_IMG_BG)
    if not hero:
        hero = _sidebar_fallback(0, SHOP_IMG_BG)
    hero_mobile = landing_image_public_url(page.hero_image, SHOP_IMG_CARD) or hero

    desire = landing_image_public_url(page.desire_image, SHOP_IMG_BG)
    if not desire:
        desire = _sidebar_fallback(1 if page.hero_image else 0, SHOP_IMG_BG)
    desire_mobile = landing_image_public_url(page.desire_image, SHOP_IMG_CARD) or desire

    return {
        'hero_image': hero,
        'hero_image_mobile': hero_mobile,
        'desire_image': desire,
        'desire_image_mobile': desire_mobile,
    }


def get_landing_context(form_sent: bool = False, form_error: str = '') -> dict:
    from app.shop_contacts import contact_display_label, contact_href, get_shop_contacts_for_site
    from app.shop_sidebar import get_sidebar_slide_urls

    page = get_landing_page_settings()
    landing_data = {}
    for col in ShopLandingPage.__table__.columns:
        val = getattr(page, col.name, None)
        landing_data[col.name] = _replace_legacy_terms(val) if isinstance(val, str) else val
    landing_view = SimpleNamespace(**landing_data)
    sidebar_slides = get_sidebar_slide_urls(SHOP_IMG_BG)
    desire_slides = get_landing_desire_slide_urls(SHOP_IMG_BG)
    ctx = {
        'landing': landing_view,
        'sidebar_slides': sidebar_slides,
        'desire_slides': desire_slides,
        'price_top': _landing_price_block(landing_view, 'top'),
        'price_bottom': _landing_price_block(landing_view, 'bottom'),
        'shop_contacts': get_shop_contacts_for_site(),
        'contact_display_label': contact_display_label,
        'contact_href': contact_href,
        'form_sent': form_sent,
        'form_error': (form_error or '').strip()[:200],
    }
    return ctx
