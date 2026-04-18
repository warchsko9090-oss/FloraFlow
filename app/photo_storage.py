import os
import re


IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")


def get_safe_plant_folder_name(plant_name: str) -> str:
    cleaned = re.sub(r"[^\w\s-]", "", plant_name or "").strip().replace(" ", "_")
    return cleaned or "unnamed_plant"


def get_legacy_plant_folder_name(plant_id: int, plant_name: str) -> str:
    return f"plant_{plant_id}_{get_safe_plant_folder_name(plant_name)}"


def get_primary_photo_rel_dir(plant_name: str) -> str:
    return f"photo/{get_safe_plant_folder_name(plant_name)}"


def get_legacy_photo_rel_dir(plant_id: int, plant_name: str) -> str:
    return f"photo/{get_legacy_plant_folder_name(plant_id, plant_name)}"


def _to_abs(root: str, rel_path: str) -> str:
    return os.path.join(root, *rel_path.split("/"))


def list_photo_files(root: str, rel_dir: str):
    abs_dir = _to_abs(root, rel_dir)
    if not os.path.isdir(abs_dir):
        return []
    files = []
    for name in sorted(os.listdir(abs_dir)):
        if name.lower().endswith(IMAGE_EXTENSIONS):
            files.append(name)
    return files


def resolve_photo_source(root: str, plant_id: int, plant_name: str):
    primary_rel = get_primary_photo_rel_dir(plant_name)
    legacy_rel = get_legacy_photo_rel_dir(plant_id, plant_name)

    primary_files = list_photo_files(root, primary_rel)
    if primary_files:
        return primary_rel, primary_files

    legacy_files = list_photo_files(root, legacy_rel)
    if legacy_files:
        return legacy_rel, legacy_files

    return primary_rel, []
