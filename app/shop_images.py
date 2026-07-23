"""On-the-fly resized images for the public shop (disk cache)."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from flask import abort, current_app, send_file, url_for

ALLOWED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

SHOP_IMG_THUMB = 120
SHOP_IMG_CARD = 480
SHOP_IMG_PREVIEW = 960
SHOP_IMG_BG = 1280


def _erp_public_base_url() -> str:
    return (os.environ.get('ERP_PUBLIC_BASE_URL') or '').strip().rstrip('/')


def shop_image_url(rel_filename: str | None, width: int | None = None) -> str | None:
    if not rel_filename:
        return None
    rel = rel_filename.replace('\\', '/').lstrip('/')
    erp = _erp_public_base_url()
    if erp:
        if width:
            return f'{erp}/public/client/image/{int(width)}/{rel}'
        return f'{erp}/uploads/{rel}'
    if width:
        return url_for(
            "public_client_api.serve_resized_image",
            width=int(width),
            filename=rel_filename,
        )
    return url_for("main.serve_uploaded_file", filename=rel_filename)


def _safe_upload_path(rel_filename: str) -> Path | None:
    rel = (rel_filename or "").replace("\\", "/").lstrip("/")
    if not rel or ".." in rel.split("/"):
        return None
    root = Path(current_app.config["UPLOAD_FOLDER"]).resolve()
    full = (root / rel).resolve()
    if not str(full).startswith(str(root)) or not full.is_file():
        return None
    if full.suffix.lower() not in ALLOWED_IMAGE_EXT:
        return None
    return full


def _cache_path(source: Path, rel: str, width: int) -> Path:
    root = Path(current_app.config["UPLOAD_FOLDER"])
    digest = hashlib.md5(rel.encode("utf-8")).hexdigest()
    return root / ".cache" / "resize" / str(width) / f"{digest}.jpg"


def build_resized_image_response(rel_filename: str, width: int):
    width = min(max(int(width), 64), 1920)
    source = _safe_upload_path(rel_filename)
    if source is None:
        abort(404)

    rel = rel_filename.replace("\\", "/").lstrip("/")
    cache_file = _cache_path(source, rel, width)
    try:
        if cache_file.is_file() and cache_file.stat().st_mtime >= source.stat().st_mtime:
            return send_file(
                cache_file,
                mimetype="image/jpeg",
                max_age=60 * 60 * 24 * 30,
                conditional=True,
            )
    except OSError:
        pass

    try:
        from PIL import Image
    except ImportError:
        return send_file(source, conditional=True)

    try:
        with Image.open(source) as im:
            im = im.convert("RGB")
            if im.width > width:
                height = max(1, int(im.height * width / im.width))
                im = im.resize((width, height), Image.Resampling.LANCZOS)
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            im.save(cache_file, "JPEG", quality=82, optimize=True)
        return send_file(
            cache_file,
            mimetype="image/jpeg",
            max_age=60 * 60 * 24 * 30,
            conditional=True,
        )
    except Exception:
        return send_file(source, conditional=True)
