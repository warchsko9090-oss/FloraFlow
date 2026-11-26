"""Приём еженедельного снимка витрины на Amvera kdoor → /data.

POST /internal/failover-sync
  Authorization: Bearer <FAILOVER_SYNC_TOKEN>
  Body: application/gzip tar (failover.db + media + MANIFEST.json)

GET /internal/failover-sync/status
  Authorization: Bearer <FAILOVER_SYNC_TOKEN>
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request

bp = Blueprint("failover_sync_api", __name__)

MANIFEST_NAME = "MANIFEST.json"
DB_NAME = "failover.db"


def _token_ok() -> bool:
    expected = (os.environ.get("FAILOVER_SYNC_TOKEN") or "").strip()
    if not expected:
        return False
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        got = auth[7:].strip()
    else:
        got = (request.headers.get("X-Failover-Sync-Token") or "").strip()
    if not got:
        return False
    return hashlib.sha256(got.encode()).digest() == hashlib.sha256(expected.encode()).digest()


def _data_root() -> Path:
    raw = (os.environ.get("UPLOAD_FOLDER") or current_app.config.get("UPLOAD_FOLDER") or "/data").strip()
    return Path(raw)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _verify_staging(staging: Path, manifest: dict) -> list[str]:
    errors: list[str] = []
    db_path = staging / DB_NAME
    if not db_path.is_file():
        errors.append("missing failover.db")
        return errors

    expected_db = (manifest.get("db") or {}).get("sha256")
    if expected_db:
        got = _sha256_file(db_path)
        if got != expected_db:
            errors.append(f"db sha256 mismatch: got {got[:12]}… want {expected_db[:12]}…")

    # SQLite integrity
    try:
        import sqlite3

        con = sqlite3.connect(str(db_path))
        try:
            row = con.execute("PRAGMA integrity_check").fetchone()
            if not row or str(row[0]).lower() != "ok":
                errors.append(f"sqlite integrity_check: {row}")
        finally:
            con.close()
    except Exception as exc:
        errors.append(f"sqlite open failed: {exc}")

    files = manifest.get("files") or []
    for item in files:
        rel = str(item.get("path") or "").replace("\\", "/").lstrip("/")
        if not rel or rel == DB_NAME or rel == MANIFEST_NAME:
            continue
        target = staging / rel
        if not target.is_file():
            errors.append(f"missing file: {rel}")
            continue
        want = item.get("sha256")
        if want and _sha256_file(target) != want:
            errors.append(f"hash mismatch: {rel}")
        want_size = item.get("size")
        if want_size is not None and target.stat().st_size != int(want_size):
            errors.append(f"size mismatch: {rel}")

    counts = manifest.get("counts") or {}
    min_media = int(counts.get("media_files") or 0)
    if min_media > 0:
        actual = sum(1 for p in staging.rglob("*") if p.is_file() and p.name not in (DB_NAME, MANIFEST_NAME))
        if actual < min_media:
            errors.append(f"media count {actual} < manifest {min_media}")

    return errors


def _apply_staging(staging: Path, data_root: Path) -> None:
    data_root.mkdir(parents=True, exist_ok=True)
    # failover.db атомарно
    db_src = staging / DB_NAME
    db_dst = data_root / DB_NAME
    tmp_db = data_root / f".{DB_NAME}.partial"
    if tmp_db.exists():
        tmp_db.unlink()
    shutil.copy2(db_src, tmp_db)
    os.replace(tmp_db, db_dst)

    # медиа: копируем каталоги/файлы из staging (кроме db/manifest)
    for path in staging.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(staging).as_posix()
        if rel in (DB_NAME, MANIFEST_NAME):
            continue
        dest = data_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)

    man_src = staging / MANIFEST_NAME
    if man_src.is_file():
        shutil.copy2(man_src, data_root / MANIFEST_NAME)
        stamp = data_root / "SNAPSHOT.txt"
        stamp.write_text(
            f"applied_at={datetime.now(timezone.utc).isoformat()}\n"
            f"source_manifest={MANIFEST_NAME}\n",
            encoding="utf-8",
        )


@bp.route("/internal/failover-sync/status", methods=["GET"])
def failover_sync_status():
    if not _token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    root = _data_root()
    man_path = root / MANIFEST_NAME
    db_path = root / DB_NAME
    payload = {
        "ok": True,
        "data_root": str(root),
        "db_exists": db_path.is_file(),
        "db_size": db_path.stat().st_size if db_path.is_file() else 0,
        "manifest_exists": man_path.is_file(),
    }
    if man_path.is_file():
        try:
            payload["manifest"] = json.loads(man_path.read_text(encoding="utf-8"))
        except Exception as exc:
            payload["manifest_error"] = str(exc)
    snap = root / "SNAPSHOT.txt"
    if snap.is_file():
        payload["snapshot_txt"] = snap.read_text(encoding="utf-8")[:500]
    return jsonify(payload)


@bp.route("/internal/failover-sync", methods=["POST"])
def failover_sync_upload():
    if not _token_ok():
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if not (os.environ.get("FAILOVER_SYNC_TOKEN") or "").strip():
        return jsonify({"ok": False, "error": "FAILOVER_SYNC_TOKEN not configured"}), 503

    raw = request.get_data(cache=False, as_text=False)
    if not raw or len(raw) < 64:
        return jsonify({"ok": False, "error": "empty body"}), 400
    max_mb = int(os.environ.get("FAILOVER_SYNC_MAX_MB") or "1500")
    if len(raw) > max_mb * 1024 * 1024:
        return jsonify({"ok": False, "error": f"payload too large (>{max_mb} MB)"}), 413

    data_root = _data_root()
    with tempfile.TemporaryDirectory(prefix="failover-sync-") as tmp:
        staging = Path(tmp) / "staging"
        staging.mkdir()
        archive = Path(tmp) / "snap.tar.gz"
        archive.write_bytes(raw)
        try:
            with tarfile.open(archive, "r:gz") as tar:
                # безопасная распаковка
                for member in tar.getmembers():
                    name = member.name.replace("\\", "/")
                    if name.startswith("/") or ".." in name.split("/"):
                        return jsonify({"ok": False, "error": f"unsafe path in archive: {name}"}), 400
                tar.extractall(staging)
        except tarfile.TarError as exc:
            return jsonify({"ok": False, "error": f"bad tar.gz: {exc}"}), 400

        # если архив с одной корневой папкой — спуститься
        kids = [p for p in staging.iterdir()]
        if len(kids) == 1 and kids[0].is_dir() and not (staging / DB_NAME).exists():
            staging = kids[0]

        man_path = staging / MANIFEST_NAME
        if not man_path.is_file():
            return jsonify({"ok": False, "error": "MANIFEST.json missing in archive"}), 400
        try:
            manifest = json.loads(man_path.read_text(encoding="utf-8"))
        except Exception as exc:
            return jsonify({"ok": False, "error": f"bad MANIFEST.json: {exc}"}), 400

        errors = _verify_staging(staging, manifest)
        if errors:
            return jsonify({"ok": False, "error": "integrity_failed", "details": errors[:40]}), 422

        try:
            _apply_staging(staging, data_root)
        except Exception as exc:
            current_app.logger.exception("failover sync apply failed")
            return jsonify({"ok": False, "error": f"apply failed: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "applied": True,
            "data_root": str(data_root),
            "created_at": (manifest.get("created_at") if isinstance(manifest, dict) else None),
            "counts": (manifest.get("counts") if isinstance(manifest, dict) else None),
        }
    )
