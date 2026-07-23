import re

from app.models import ShopContact


CONTACT_TYPE_LABELS = dict(ShopContact.CONTACT_TYPES)


def _digits_only(value: str) -> str:
    return re.sub(r'\D', '', value or '')


def contact_type_label(contact_type: str) -> str:
    return CONTACT_TYPE_LABELS.get(contact_type, contact_type or 'Контакт')


def contact_display_label(contact: ShopContact) -> str:
    if contact.label and contact.label.strip():
        return contact.label.strip()
    return contact_type_label(contact.contact_type)


def contact_href(contact: ShopContact) -> str:
    value = (contact.value or '').strip()
    if not value:
        return '#'
    ctype = contact.contact_type
    if ctype == 'phone':
        digits = _digits_only(value)
        return f'tel:+{digits}' if digits else '#'
    if ctype == 'email':
        return f'mailto:{value}'
    if ctype == 'telegram':
        if value.startswith('http'):
            return value
        handle = value.lstrip('@').strip()
        return f'https://t.me/{handle}' if handle else '#'
    if ctype == 'whatsapp':
        digits = _digits_only(value)
        return f'https://wa.me/{digits}' if digits else '#'
    if ctype == 'max':
        if value.startswith('http'):
            return value
        handle = value.lstrip('@').strip()
        return f'https://max.ru/{handle}' if handle else '#'
    if ctype == 'vk':
        if value.startswith('http'):
            return value
        handle = value.lstrip('@').replace('vk.com/', '').strip('/')
        return f'https://vk.com/{handle}' if handle else '#'
    if ctype == 'instagram':
        if value.startswith('http'):
            return value
        handle = value.lstrip('@').replace('instagram.com/', '').strip('/')
        return f'https://instagram.com/{handle}' if handle else '#'
    if ctype == 'website':
        if value.startswith('http'):
            return value
        return f'https://{value}' if value else '#'
    return '#'


def get_shop_contacts(for_site=False, for_kp=False):
    q = ShopContact.query.filter_by(is_active=True)
    if for_site:
        q = q.filter_by(show_on_site=True)
    if for_kp:
        q = q.filter_by(show_in_kp=True)
    return q.order_by(ShopContact.sort_order.asc(), ShopContact.id.asc()).all()


def get_shop_contacts_for_site():
    return get_shop_contacts(for_site=True)


def get_shop_contacts_for_kp():
    return get_shop_contacts(for_kp=True)


def seed_default_shop_contacts(db_session):
    if ShopContact.query.count() > 0:
        return
    db_session.add_all([
        ShopContact(
            contact_type='phone',
            label='Телефон',
            value='+7 (999) 000-00-00',
            sort_order=10,
        ),
        ShopContact(
            contact_type='email',
            label='E-mail',
            value='info@knyazhestvo.ru',
            sort_order=20,
        ),
        ShopContact(
            contact_type='telegram',
            label='Telegram',
            value='@knyazhestvo',
            sort_order=30,
        ),
    ])
    db_session.commit()
