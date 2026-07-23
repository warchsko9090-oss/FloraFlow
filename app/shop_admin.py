import os
import time

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for
from flask_login import login_required, current_user

from app.models import Plant, ShopCompanyRequisite, ShopContact, ShopContactsPage, ShopLandingPage, ShopPlantCard, db
from app.shop_prices import build_admin_price_rows, save_shop_prices_from_form
from app.shop_plant_cards import PRUNING_SUGGESTIONS, ROOT_SYSTEM_SUGGESTIONS, save_plant_card
from app.shop_email import ALL_CATEGORIES, load_email_settings, save_email_settings, send_test_email
from app.shop_contacts_page import get_contacts_page_settings
from app.shop_landing_page import get_landing_page_settings, LANDING_PRICE_TOP_CARD_COUNT
from app.photo_storage import (
    IMAGE_EXTENSIONS,
    LANDING_DESIRE_SLIDES_REL_DIR,
    LANDING_PRICE_TOP_REL_DIR,
    PHOTO_VARIANT_CONTAINER,
    PHOTO_VARIANT_GROUND,
    SIDEBAR_REL_DIR,
    get_container_photo_rel_dir,
    get_legacy_container_photo_rel_dir,
    get_primary_photo_rel_dir,
    list_landing_desire_files,
    list_sidebar_files,
    resolve_photo_source,
)
from app.shop_contacts import contact_type_label
from app.shop_catalog import (
    collect_catalog_pairs_for_admin,
    collect_seedlings_for_admin,
    save_catalog_settings_from_form,
    save_seedling_settings_from_form,
)
from app.utils import log_action

bp = Blueprint('shop_admin', __name__)

ALLOWED_ROLES = ('admin', 'executive', 'shop_manager', 'user')
TABS = ('background', 'photos', 'contacts', 'prices', 'catalog', 'seedlings', 'contact_page', 'landing')


def _deny_if_no_access():
    if current_user.role not in ALLOWED_ROLES:
        flash('Доступ только для администратора или руководителя')
        return redirect(url_for('orders.orders_list'))
    return None


def _sidebar_upload_dir():
    root = current_app.config['UPLOAD_FOLDER']
    path = os.path.join(root, SIDEBAR_REL_DIR)
    os.makedirs(path, exist_ok=True)
    return path


def _sanitize_sidebar_name(name: str) -> str:
    cleaned = os.path.basename((name or '').strip())
    cleaned = cleaned.replace('/', '_').replace('\\', '_').replace('\x00', '')
    cleaned = ''.join(ch for ch in cleaned if ch not in '<>:"|?*' and ord(ch) >= 32)
    return cleaned.rstrip('. ').strip()


@bp.route('/shop-admin', methods=['GET', 'POST'])
@login_required
def shop_admin_index():
    deny = _deny_if_no_access()
    if deny:
        return deny

    tab = (request.args.get('tab') or request.form.get('tab') or 'background').strip()
    if tab not in TABS:
        tab = 'background'

    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()
        if action == 'upload_background':
            return _upload_background(tab)
        if action == 'delete_background':
            return _delete_background(request.form.get('filename', ''), tab)
        if action == 'upload_plant_photos':
            return _upload_plant_photos(tab)
        if action == 'delete_plant_photo':
            return _delete_plant_photo(tab)
        if action == 'save_plant_card':
            return _save_plant_card(tab)
        if action in ('add_contact', 'edit_contact', 'delete_contact'):
            return _handle_contact_action(action, tab)
        if action == 'save_prices':
            return _save_prices(tab)
        if action == 'save_catalog':
            return _save_catalog(tab)
        if action == 'save_seedlings':
            return _save_seedlings(tab)
        if action == 'save_contact_page':
            return _save_contact_page_settings(tab)
        if action == 'save_landing_page':
            return _save_landing_page_settings(tab)
        if action == 'upload_landing_price_top':
            return _upload_landing_price_top(tab)
        if action == 'clear_landing_price_top':
            return _clear_landing_price_top(tab)
        if action == 'upload_landing_desire_slides':
            return _upload_landing_desire_slides(tab)
        if action == 'delete_landing_desire_slide':
            return _delete_landing_desire_slide(tab)
        if action == 'save_shop_email_settings':
            return _save_shop_email_settings(tab)
        if action == 'test_shop_email_settings':
            return _test_shop_email_settings(tab)
        if action in ('add_requisite', 'edit_requisite', 'delete_requisite'):
            return _handle_requisite_action(action, tab)

    return _render_admin(tab)


def _upload_background(tab):
    upload_dir = _sidebar_upload_dir()
    files = request.files.getlist('photos')
    if not files:
        flash('Выберите файлы для загрузки')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    saved = 0
    for file in files:
        if not file or not file.filename:
            continue
        original = file.filename.strip()
        base, ext = os.path.splitext(original)
        ext = ext.lower()
        if ext not in IMAGE_EXTENSIONS:
            flash(f'Пропущен {original}: неподдерживаемый формат')
            continue
        display_base = _sanitize_sidebar_name(base) or f'slide_{int(time.time() * 1000)}'
        unique_name = f'{display_base}{ext}'
        suffix = 2
        while os.path.exists(os.path.join(upload_dir, unique_name)):
            unique_name = f'{display_base}_{suffix}{ext}'
            suffix += 1
        file.save(os.path.join(upload_dir, unique_name))
        saved += 1

    log_action(f'Загрузил {saved} фото фона витрины (data-sidebar)')
    flash(f'Загружено фото фона: {saved}')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _delete_background(filename, tab):
    if not filename or os.path.basename(filename) != filename:
        flash('Некорректное имя файла')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))
    path = os.path.join(_sidebar_upload_dir(), filename)
    if os.path.isfile(path):
        os.remove(path)
        log_action(f'Удалил фото фона витрины: {filename}')
        flash('Фото удалено')
    else:
        flash('Файл не найден')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _upload_plant_photos(tab):
    from app.directory import _sanitize_photo_basename

    plant_id = request.form.get('plant_id', type=int)
    if not plant_id:
        flash('Не выбрано растение')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    photo_variant = request.form.get('photo_variant') or PHOTO_VARIANT_GROUND
    if photo_variant not in (PHOTO_VARIANT_GROUND, PHOTO_VARIANT_CONTAINER):
        photo_variant = PHOTO_VARIANT_GROUND

    plant = Plant.query.get_or_404(plant_id)
    root = current_app.config['UPLOAD_FOLDER']
    rel_dir = (
        get_container_photo_rel_dir(plant.name)
        if photo_variant == PHOTO_VARIANT_CONTAINER
        else get_primary_photo_rel_dir(plant.name)
    )
    upload_dir = os.path.join(root, *rel_dir.split('/'))
    os.makedirs(upload_dir, exist_ok=True)

    files = request.files.getlist('photos')
    if not files:
        flash('Файлы не выбраны')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    saved = 0
    for file in files:
        if not file or not file.filename:
            continue
        original_name = file.filename.strip()
        base_name, ext = os.path.splitext(original_name)
        ext = ext.lower()
        if ext not in IMAGE_EXTENSIONS:
            continue
        display_base = _sanitize_photo_basename(base_name) or f'photo_{int(time.time() * 1000)}'
        unique_name = f'{display_base}{ext}'
        suffix = 2
        while os.path.exists(os.path.join(upload_dir, unique_name)):
            unique_name = f'{display_base}_{suffix}{ext}'
            suffix += 1
        file.save(os.path.join(upload_dir, unique_name))
        saved += 1

    label = 'контейнерной площадки' if photo_variant == PHOTO_VARIANT_CONTAINER else 'грунта'
    log_action(f'Загрузил {saved} фото ({label}) для витрины: {plant.name}')
    flash(f'Загружено фото ({label}): {saved}')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _delete_plant_photo(tab):
    from app.photo_storage import get_legacy_photo_rel_dir

    plant_id = request.form.get('plant_id', type=int)
    filename = request.form.get('filename', '')
    photo_variant = request.form.get('photo_variant') or PHOTO_VARIANT_GROUND
    if photo_variant not in (PHOTO_VARIANT_GROUND, PHOTO_VARIANT_CONTAINER):
        photo_variant = PHOTO_VARIANT_GROUND
    if not plant_id or not filename or os.path.basename(filename) != filename:
        flash('Некорректные параметры')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    plant = Plant.query.get_or_404(plant_id)
    root = current_app.config['UPLOAD_FOLDER']
    if photo_variant == PHOTO_VARIANT_CONTAINER:
        source_rel_dir, _ = resolve_photo_source(root, plant.id, plant.name, variant=PHOTO_VARIANT_CONTAINER)
        primary_rel_dir = get_container_photo_rel_dir(plant.name)
        legacy_rel_dir = get_legacy_container_photo_rel_dir(plant.id, plant.name)
    else:
        source_rel_dir, _ = resolve_photo_source(root, plant.id, plant.name, variant=PHOTO_VARIANT_GROUND)
        primary_rel_dir = get_primary_photo_rel_dir(plant.name)
        legacy_rel_dir = get_legacy_photo_rel_dir(plant.id, plant.name)

    for rel_dir in (source_rel_dir, primary_rel_dir, legacy_rel_dir):
        file_path = os.path.join(root, *f'{rel_dir}/{filename}'.split('/'))
        if os.path.exists(file_path):
            os.remove(file_path)
            log_action(f'Удалил фото {filename} у растения {plant.name} (витрина)')
            flash('Фото удалено')
            return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    flash('Файл не найден')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _save_plant_card(tab):
    plant_id = request.form.get('plant_id', type=int)
    if not plant_id:
        flash('Не выбрано растение')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    plant = Plant.query.get_or_404(plant_id)
    save_plant_card(
        plant_id,
        request.form.get('root_system', ''),
        request.form.get('pruning', ''),
    )
    db.session.commit()
    log_action(f'Обновил характеристики витрины: {plant.name}')
    flash(f'Характеристики сохранены: {plant.name}')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _handle_contact_action(action, tab):
    if action == 'add_contact':
        contact_type = (request.form.get('contact_type') or 'phone').strip()
        value = (request.form.get('value') or '').strip()
        if not value:
            flash('Укажите значение контакта')
            return redirect(url_for('shop_admin.shop_admin_index', tab=tab))
        contact = ShopContact(
            contact_type=contact_type,
            label=(request.form.get('label') or '').strip(),
            value=value,
            sort_order=request.form.get('sort_order', type=int) or 0,
            show_on_site=bool(request.form.get('show_on_site')),
            show_in_kp=bool(request.form.get('show_in_kp')),
        )
        db.session.add(contact)
        db.session.commit()
        log_action(f'Добавил контакт витрины: {contact_type} {value}')
        flash('Контакт добавлен')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    contact_id = request.form.get('contact_id', type=int)
    contact = ShopContact.query.get(contact_id) if contact_id else None
    if not contact:
        flash('Контакт не найден')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    if action == 'delete_contact':
        db.session.delete(contact)
        db.session.commit()
        log_action(f'Удалил контакт витрины #{contact_id}')
        flash('Контакт удалён')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    if action == 'edit_contact':
        value = (request.form.get('value') or '').strip()
        if not value:
            flash('Значение контакта не может быть пустым')
            return redirect(url_for('shop_admin.shop_admin_index', tab=tab))
        contact.contact_type = (request.form.get('contact_type') or contact.contact_type).strip()
        contact.label = (request.form.get('label') or '').strip()
        contact.value = value
        contact.sort_order = request.form.get('sort_order', type=int) or 0
        contact.show_on_site = bool(request.form.get('show_on_site'))
        contact.show_in_kp = bool(request.form.get('show_in_kp'))
        contact.is_active = bool(request.form.get('is_active'))
        db.session.commit()
        log_action(f'Обновил контакт витрины #{contact.id}')
        flash('Контакт сохранён')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _save_contact_page_settings(tab):
    from app.shop_contacts_page import sanitize_map_embed_html

    page = get_contacts_page_settings()
    page.page_heading = (request.form.get('page_heading') or 'Контакты').strip()
    page.about_heading = (request.form.get('about_heading') or 'О питомнике').strip()
    page.about_paragraph_1 = (request.form.get('about_paragraph_1') or '').strip()
    page.about_paragraph_2 = (request.form.get('about_paragraph_2') or '').strip()
    page.location_address = (request.form.get('location_address') or '').strip()
    page.location_coordinates = (request.form.get('location_coordinates') or '').strip()
    page.map_pin_label = (request.form.get('map_pin_label') or '').strip()
    page.map_embed_html = sanitize_map_embed_html(request.form.get('map_embed_html') or '')
    try:
        page.map_lat = float((request.form.get('map_lat') or '54.2').replace(',', '.'))
        page.map_lng = float((request.form.get('map_lng') or '37.6').replace(',', '.'))
        page.map_zoom = int(request.form.get('map_zoom') or 12)
    except (TypeError, ValueError):
        flash('Проверьте координаты и масштаб карты')
        return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))
    page.map_zoom = max(5, min(18, page.map_zoom))
    db.session.commit()
    log_action('Обновил страницу контактов витрины')
    flash('Страница контактов сохранена')
    return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))


def _landing_price_top_upload_dir():
    path = os.path.join(current_app.config['UPLOAD_FOLDER'], LANDING_PRICE_TOP_REL_DIR.replace('/', os.sep))
    os.makedirs(path, exist_ok=True)
    return path


def _landing_desire_slides_upload_dir():
    path = os.path.join(current_app.config['UPLOAD_FOLDER'], LANDING_DESIRE_SLIDES_REL_DIR.replace('/', os.sep))
    os.makedirs(path, exist_ok=True)
    return path


def _landing_admin_photo_url(stored_path: str | None) -> str | None:
    rel = (stored_path or '').strip().replace('\\', '/').lstrip('/')
    if not rel:
        return None
    return url_for('main.serve_uploaded_file', filename=rel)


def _landing_price_top_card_numbers():
    return [str(n) for n in range(1, LANDING_PRICE_TOP_CARD_COUNT + 1)]


def _save_landing_page_settings(tab):
    page = get_landing_page_settings()
    text_fields = [
        'meta_description', 'hero_badge', 'hero_title', 'hero_subtitle',
        'hero_btn_catalog', 'hero_btn_consult',
        'interest_eyebrow', 'interest_title', 'interest_subtitle',
        'card1_title', 'card1_text', 'card2_title', 'card2_text',
        'card3_title', 'card3_text',
        'desire_title', 'desire_title_accent',
        'desire_check_1', 'desire_check_2', 'desire_check_3',
        'desire_check_4', 'desire_check_5', 'desire_check_6',
        'form_title', 'form_subtitle', 'form_btn',
        'price_top_eyebrow', 'price_top_title', 'price_top_subtitle',
        'price_top_footer',
        'price_bottom_eyebrow', 'price_bottom_title', 'price_bottom_subtitle',
        'price_bottom_footer',
    ]
    for n in range(1, LANDING_PRICE_TOP_CARD_COUNT + 1):
        text_fields.extend([
            f'price_top{n}_name', f'price_top{n}_spec',
            f'price_top{n}_price', f'price_top{n}_note',
        ])
    for n in range(1, 4):
        text_fields.extend([
            f'price_bottom{n}_name', f'price_bottom{n}_spec',
            f'price_bottom{n}_price', f'price_bottom{n}_note',
        ])
    for field in text_fields:
        setattr(page, field, (request.form.get(field) or '').strip())
    db.session.commit()
    db.session.expire_all()
    log_action('Обновил главную страницу витрины')
    flash('Главная страница сохранена')
    return redirect(url_for('shop_admin.shop_admin_index', tab='landing'))


def _upload_landing_price_top(tab):
    card = (request.form.get('card') or '').strip()
    if card not in _landing_price_top_card_numbers():
        flash('Некорректная карточка')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    file = request.files.get('photo')
    if not file or not file.filename:
        flash('Выберите изображение')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    original = file.filename.strip()
    base, ext = os.path.splitext(original)
    ext = ext.lower()
    if ext not in IMAGE_EXTENSIONS:
        flash(f'Неподдерживаемый формат: {original}')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    field_name = f'price_top{card}_image'
    upload_dir = _landing_price_top_upload_dir()
    unique_name = f'top{card}{ext}'
    file.save(os.path.join(upload_dir, unique_name))

    page = get_landing_page_settings()
    old = (getattr(page, field_name) or '').strip()
    if old:
        old_name = os.path.basename(old.replace('/', os.sep))
        if old_name != unique_name:
            old_path = os.path.join(upload_dir, old_name)
            if os.path.isfile(old_path):
                try:
                    os.remove(old_path)
                except OSError:
                    pass

    setattr(page, field_name, f'{LANDING_PRICE_TOP_REL_DIR}/{unique_name}')
    db.session.commit()
    log_action(f'Загрузил фото примера {card} ценового блока (верх)')
    flash(f'Фото примера {card} обновлено')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _clear_landing_price_top(tab):
    card = (request.form.get('card') or '').strip()
    if card not in _landing_price_top_card_numbers():
        flash('Некорректная карточка')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    field_name = f'price_top{card}_image'
    page = get_landing_page_settings()
    old = (getattr(page, field_name) or '').strip()
    if old:
        try:
            full = os.path.join(current_app.config['UPLOAD_FOLDER'], old.replace('/', os.sep))
            if os.path.isfile(full):
                os.remove(full)
        except OSError:
            pass
    setattr(page, field_name, '')
    db.session.commit()
    flash(f'Фото примера {card} удалено')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _upload_landing_desire_slides(tab):
    upload_dir = _landing_desire_slides_upload_dir()
    files = request.files.getlist('photos')
    if not files:
        flash('Выберите файлы для загрузки')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))

    saved = 0
    for file in files:
        if not file or not file.filename:
            continue
        original = file.filename.strip()
        base, ext = os.path.splitext(original)
        ext = ext.lower()
        if ext not in IMAGE_EXTENSIONS:
            flash(f'Пропущен {original}: неподдерживаемый формат')
            continue
        display_base = _sanitize_sidebar_name(base) or f'desire_{int(time.time() * 1000)}'
        unique_name = f'{display_base}{ext}'
        suffix = 2
        while os.path.exists(os.path.join(upload_dir, unique_name)):
            unique_name = f'{display_base}_{suffix}{ext}'
            suffix += 1
        file.save(os.path.join(upload_dir, unique_name))
        saved += 1

    log_action(f'Загрузил {saved} слайдов блока «Материал…»')
    flash(f'Загружено слайдов: {saved}')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _delete_landing_desire_slide(tab):
    filename = (request.form.get('filename') or '').strip()
    if not filename or os.path.basename(filename) != filename:
        flash('Некорректное имя файла')
        return redirect(url_for('shop_admin.shop_admin_index', tab=tab))
    path = os.path.join(_landing_desire_slides_upload_dir(), filename)
    if os.path.isfile(path):
        os.remove(path)
        log_action(f'Удалил слайд блока «Материал…»: {filename}')
        flash('Слайд удалён')
    else:
        flash('Файл не найден')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _handle_requisite_action(action, tab):
    if action == 'add_requisite':
        label = (request.form.get('label') or '').strip()
        value = (request.form.get('value') or '').strip()
        if not label or not value:
            flash('Укажите название и значение реквизита')
            return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))
        db.session.add(ShopCompanyRequisite(
            label=label,
            value=value,
            sort_order=request.form.get('sort_order', type=int) or 0,
        ))
        db.session.commit()
        log_action(f'Добавил реквизит: {label}')
        flash('Реквизит добавлен')
        return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))

    req_id = request.form.get('requisite_id', type=int)
    req = ShopCompanyRequisite.query.get(req_id) if req_id else None
    if not req:
        flash('Реквизит не найден')
        return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))

    if action == 'delete_requisite':
        db.session.delete(req)
        db.session.commit()
        log_action(f'Удалил реквизит #{req_id}')
        flash('Реквизит удалён')
        return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))

    if action == 'edit_requisite':
        label = (request.form.get('label') or '').strip()
        value = (request.form.get('value') or '').strip()
        if not label or not value:
            flash('Название и значение не могут быть пустыми')
            return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))
        req.label = label
        req.value = value
        req.sort_order = request.form.get('sort_order', type=int) or 0
        req.is_active = bool(request.form.get('is_active'))
        db.session.commit()
        log_action(f'Обновил реквизит #{req.id}')
        flash('Реквизит сохранён')
        return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))

    return redirect(url_for('shop_admin.shop_admin_index', tab='contact_page'))


def _save_catalog(tab):
    changed = save_catalog_settings_from_form(request.form)
    db.session.commit()
    log_action(f'Обновил настройки каталога витрины ({changed} изм.)')
    flash(f'Настройки каталога сохранены ({changed} изменений)')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _save_shop_email_settings(tab):
    recipients = []
    idx = 0
    while True:
        email_key = f'email_recipient_email_{idx}'
        if email_key not in request.form:
            break
        email = (request.form.get(email_key) or '').strip()
        name = (request.form.get(f'email_recipient_name_{idx}') or '').strip()
        enabled = bool(request.form.get(f'email_recipient_enabled_{idx}'))
        categories = request.form.getlist(f'email_recipient_categories_{idx}')
        if email:
            recipients.append({
                'email': email,
                'name': name,
                'enabled': enabled,
                'categories': categories,
            })
        idx += 1

    cfg = {
        'enabled': bool(request.form.get('email_enabled')),
        'smtp': {
            'host': (request.form.get('email_smtp_host') or '').strip(),
            'port': request.form.get('email_smtp_port', type=int) or 587,
            'tls': bool(request.form.get('email_smtp_tls')),
            'username': (request.form.get('email_smtp_username') or '').strip(),
            'password': (request.form.get('email_smtp_password') or '').strip(),
            'from_email': (request.form.get('email_from_email') or '').strip(),
            'from_name': (request.form.get('email_from_name') or '').strip(),
        },
        'recipients': recipients,
    }
    save_email_settings(cfg)
    db.session.commit()
    log_action('Обновил почтовые уведомления витрины')
    flash('Почтовые уведомления сохранены')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _test_shop_email_settings(tab):
    ok, msg = send_test_email()
    if ok:
        flash('Тестовое письмо отправлено')
    else:
        flash(f'Ошибка тестовой отправки: {msg}')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _save_seedlings(tab):
    changed = save_seedling_settings_from_form(request.form)
    db.session.commit()
    log_action(f'Обновил настройки саженцев на витрине ({changed} изм.)')
    flash(f'Настройки саженцев сохранены ({changed} изменений)')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _save_prices(tab):
    from app.public_client_api import get_aggregated_catalog

    base_catalog = get_aggregated_catalog(apply_shop_prices=False)
    changed, rejected = save_shop_prices_from_form(request.form, base_catalog)
    db.session.commit()
    log_action(f'Обновил цены витрины ({changed} поз.)')
    if rejected:
        flash(
            f'Цены на сайте сохранены ({changed} изменений). '
            f'{rejected} поз. пропущено: розница не может быть ниже оптовой.',
            'warning',
        )
    else:
        flash(f'Цены на сайте сохранены ({changed} изменений)')
    return redirect(url_for('shop_admin.shop_admin_index', tab=tab))


def _build_catalog_admin_rows(admin_pairs):
    """Группирует строки каталога по растению для вкладки «Каталог»."""
    by_plant = {}
    for row in admin_pairs:
        pid = row['plant_id']
        if pid not in by_plant:
            by_plant[pid] = {
                'plant_id': pid,
                'plant_name': row['plant_name'],
                'is_hot': row['is_hot'],
                'display_order': row['display_order'],
                'is_hidden': row['is_hidden'],
                'variants': [],
            }
        by_plant[pid]['variants'].append(row)
        if row['is_hot']:
            by_plant[pid]['is_hot'] = True
        if row['is_hidden']:
            by_plant[pid]['is_hidden'] = True

    groups = list(by_plant.values())
    groups.sort(key=lambda g: (
        0 if g['is_hot'] else 1,
        int(g['display_order'] or 0),
        g['plant_name'].lower(),
    ))
    return groups


def _render_admin(tab):
    root = current_app.config['UPLOAD_FOLDER']
    sidebar_files = list_sidebar_files(root)
    sidebar_items = [
        {
            'name': name,
            'url': url_for('main.serve_uploaded_file', filename=f'{SIDEBAR_REL_DIR}/{name}'),
        }
        for name in sidebar_files
    ]

    from app.public_client_api import get_aggregated_catalog

    base_catalog = get_aggregated_catalog(apply_shop_prices=False)
    catalog_plant_ids = {item['plant_id'] for item in base_catalog}
    admin_pairs = collect_catalog_pairs_for_admin()
    seedling_rows = collect_seedlings_for_admin()
    catalog_plant_ids |= {row['plant_id'] for row in admin_pairs}
    catalog_plant_ids |= {row['plant_id'] for row in seedling_rows}
    plants = Plant.query.filter(Plant.id.in_(catalog_plant_ids)).order_by(Plant.name).all() if catalog_plant_ids else []

    try:
        card_rows = {c.plant_id: c for c in ShopPlantCard.query.all()}
    except Exception:
        db.session.rollback()
        card_rows = {}
    plant_cards = []
    for plant in plants:
        ground_rel, ground_files = resolve_photo_source(root, plant.id, plant.name, variant=PHOTO_VARIANT_GROUND)
        container_rel, container_files = resolve_photo_source(root, plant.id, plant.name, variant=PHOTO_VARIANT_CONTAINER)
        info = card_rows.get(plant.id)
        plant_cards.append({
            'id': plant.id,
            'name': plant.name,
            'rel_dir': ground_rel,
            'root_system': (info.root_system or '') if info else '',
            'pruning': (info.pruning or '') if info else '',
            'is_hot': bool(info.is_hot) if info else False,
            'display_order': int(info.display_order or 0) if info else 0,
            'is_hidden': bool(info.is_hidden) if info else False,
            'photos': [
                {
                    'name': fn,
                    'url': url_for('main.serve_uploaded_file', filename=f'{ground_rel}/{fn}'),
                }
                for fn in ground_files
            ],
            'container_rel_dir': container_rel,
            'container_photos': [
                {
                    'name': fn,
                    'url': url_for('main.serve_uploaded_file', filename=f'{container_rel}/{fn}'),
                }
                for fn in container_files
            ],
        })

    contacts = ShopContact.query.order_by(
        ShopContact.sort_order.asc(),
        ShopContact.id.asc(),
    ).all()

    price_items = [
        {
            'plant_id': r['plant_id'],
            'size_id': r['size_id'],
            'plant_name': r['plant_name'],
            'size_name': r['size_name'],
            'price': r['base_price'],
        }
        for r in admin_pairs
        if (r['free_qty'] > 0 or r['show_on_request']) and not r['is_hidden']
    ]
    price_items.extend([
        {
            'plant_id': r['plant_id'],
            'size_id': r['size_id'],
            'plant_name': r['plant_name'],
            'size_name': 'Саженец',
            'price': r['base_price'],
        }
        for r in seedling_rows
        if not r.get('seedling_hidden')
        and (r['seedling_visible'] or r['seedling_on_request'] or r['free_qty'] > 0)
    ])
    price_rows = build_admin_price_rows(price_items)
    catalog_rows = _build_catalog_admin_rows(admin_pairs)

    contacts_page = get_contacts_page_settings()
    company_requisites = ShopCompanyRequisite.query.order_by(
        ShopCompanyRequisite.sort_order.asc(),
        ShopCompanyRequisite.id.asc(),
    ).all()

    landing_page = get_landing_page_settings()
    landing_desire_slide_items = [
        {
            'name': name,
            'url': url_for('main.serve_uploaded_file', filename=f'{LANDING_DESIRE_SLIDES_REL_DIR}/{name}'),
        }
        for name in list_landing_desire_files(root)
    ]
    landing_price_top_photos = {
        str(n): _landing_admin_photo_url(getattr(landing_page, f'price_top{n}_image', None))
        for n in range(1, LANDING_PRICE_TOP_CARD_COUNT + 1)
    }
    email_settings = load_email_settings()

    return render_template(
        'shop_admin/index.html',
        tab=tab,
        sidebar_items=sidebar_items,
        plant_cards=plant_cards,
        contacts=contacts,
        price_rows=price_rows,
        catalog_rows=catalog_rows,
        seedling_rows=seedling_rows,
        contacts_page=contacts_page,
        company_requisites=company_requisites,
        landing_page=landing_page,
        landing_desire_slide_items=landing_desire_slide_items,
        landing_price_top_photos=landing_price_top_photos,
        email_settings=email_settings,
        email_categories=ALL_CATEGORIES,
        contact_types=ShopContact.CONTACT_TYPES,
        contact_type_label=contact_type_label,
        image_extensions=', '.join(IMAGE_EXTENSIONS),
        root_system_suggestions=ROOT_SYSTEM_SUGGESTIONS,
        pruning_suggestions=PRUNING_SUGGESTIONS,
    )
