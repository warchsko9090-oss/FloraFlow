import os
import re
import time

import requests


IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")

PHOTO_VARIANT_GROUND = 'ground'
PHOTO_VARIANT_CONTAINER = 'container'
CONTAINER_PHOTO_SUFFIX = '_container'

_ERP_PHOTO_MAP_GROUND: dict[str, str] | None = None
_ERP_PHOTO_MAP_CONTAINER: dict[str, str] | None = None
_ERP_PHOTO_MAP_AT = 0.0
_ERP_PHOTO_MAP_TTL = 300


def erp_public_base_url() -> str:
    return (os.environ.get('ERP_PUBLIC_BASE_URL') or '').strip().rstrip('/')


def _load_erp_photo_maps() -> tuple[dict[str, str], dict[str, str]]:
    """Кэш plant_id -> путь фото с ERP (грунт и контейнер отдельно)."""
    global _ERP_PHOTO_MAP_GROUND, _ERP_PHOTO_MAP_CONTAINER, _ERP_PHOTO_MAP_AT
    erp = erp_public_base_url()
    if not erp:
        return {}, {}

    now = time.time()
    if (
        _ERP_PHOTO_MAP_GROUND is not None
        and _ERP_PHOTO_MAP_CONTAINER is not None
        and now - _ERP_PHOTO_MAP_AT < _ERP_PHOTO_MAP_TTL
    ):
        return _ERP_PHOTO_MAP_GROUND, _ERP_PHOTO_MAP_CONTAINER

    try:
        r = requests.get(f'{erp}/public/client/photo-map', timeout=30)
        r.raise_for_status()
        payload = r.json() or {}
        _ERP_PHOTO_MAP_GROUND = {
            str(k): v for k, v in (payload.get('photos') or {}).items() if v
        }
        _ERP_PHOTO_MAP_CONTAINER = {
            str(k): v for k, v in (payload.get('container_photos') or {}).items() if v
        }
    except Exception:
        _ERP_PHOTO_MAP_GROUND = {}
        _ERP_PHOTO_MAP_CONTAINER = {}
    _ERP_PHOTO_MAP_AT = now
    return _ERP_PHOTO_MAP_GROUND, _ERP_PHOTO_MAP_CONTAINER


def get_erp_photo_map() -> dict[str, str]:
    ground, _container = _load_erp_photo_maps()
    return ground


def get_erp_photo_rel(plant_id: int, variant: str = PHOTO_VARIANT_GROUND) -> str | None:
    ground, container = _load_erp_photo_maps()
    if variant == PHOTO_VARIANT_CONTAINER:
        return container.get(str(plant_id))
    return ground.get(str(plant_id))


def get_safe_plant_folder_name(plant_name: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", plant_name or "").strip().replace(" ", "_")
    return cleaned or "unnamed_plant"


def get_legacy_plant_folder_name(plant_id: int, plant_name: str) -> str:
    return f"plant_{plant_id}_{get_safe_plant_folder_name(plant_name)}"


def get_primary_photo_rel_dir(plant_name: str) -> str:
    return f"photo/{get_safe_plant_folder_name(plant_name)}"


def get_legacy_photo_rel_dir(plant_id: int, plant_name: str) -> str:
    return f"photo/{get_legacy_plant_folder_name(plant_id, plant_name)}"


def get_container_photo_rel_dir(plant_name: str) -> str:
    return f"photo/{get_safe_plant_folder_name(plant_name)}{CONTAINER_PHOTO_SUFFIX}"


def get_legacy_container_photo_rel_dir(plant_id: int, plant_name: str) -> str:
    return f"photo/{get_legacy_plant_folder_name(plant_id, plant_name)}{CONTAINER_PHOTO_SUFFIX}"


def _to_abs(root: str, rel_path: str) -> str:
    return os.path.join(root, *rel_path.split("/"))


SIDEBAR_REL_DIR = "data-sidebar"
LANDING_DESIRE_SLIDES_REL_DIR = "data-landing/desire-slides"
LANDING_PRICE_TOP_REL_DIR = "data-landing/price-top"


def list_photo_files(root: str, rel_dir: str):
    abs_dir = _to_abs(root, rel_dir)
    if not os.path.isdir(abs_dir):
        return []
    files = []
    for name in sorted(os.listdir(abs_dir)):
        if name.lower().endswith(IMAGE_EXTENSIONS):
            files.append(name)
    return files


def list_sidebar_files(root: str):
    return list_photo_files(root, SIDEBAR_REL_DIR)


def list_landing_desire_files(root: str):
    return list_photo_files(root, LANDING_DESIRE_SLIDES_REL_DIR)


def resolve_photo_source(root: str, plant_id: int, plant_name: str, variant: str = PHOTO_VARIANT_GROUND):
    """Фото для витрины. variant: ground | container (без подмешивания друг друга)."""
    if variant == PHOTO_VARIANT_CONTAINER:
        primary_rel = get_container_photo_rel_dir(plant_name)
        legacy_rel = get_legacy_container_photo_rel_dir(plant_id, plant_name)
        primary_files = list_photo_files(root, primary_rel)
        if primary_files:
            return primary_rel, primary_files
        legacy_files = list_photo_files(root, legacy_rel)
        if legacy_files:
            return legacy_rel, legacy_files
        erp_rel = get_erp_photo_rel(plant_id, PHOTO_VARIANT_CONTAINER)
        if erp_rel:
            folder, _, name = erp_rel.rpartition('/')
            if folder and name:
                return folder, [name]
        return primary_rel, []

    primary_rel = get_primary_photo_rel_dir(plant_name)
    legacy_rel = get_legacy_photo_rel_dir(plant_id, plant_name)

    primary_files = list_photo_files(root, primary_rel)
    if primary_files:
        return primary_rel, primary_files

    legacy_files = list_photo_files(root, legacy_rel)
    if legacy_files:
        return legacy_rel, legacy_files

    erp_rel = get_erp_photo_rel(plant_id, PHOTO_VARIANT_GROUND)
    if erp_rel:
        folder, _, name = erp_rel.rpartition('/')
        if folder and name:
            return folder, [name]

    return primary_rel, []
