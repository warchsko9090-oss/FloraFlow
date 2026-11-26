#!/usr/bin/env python3
"""Снимок БД + медиа для Amvera readonly-витрины (пока дом жив).

Пишет в каталог (по умолчанию amvera-shop-door/data-snapshot/):
  failover.db          — SQLite-копия схемы/данных приложения
  MANIFEST.json        — sha256, размеры, счётчики (цены/фото)
  uploads/...          — фото и слайды (зеркало UPLOAD_FOLDER)

Опции:
  --pack               — ещё и snapshot.tar.gz
  --verify PATH        — только проверить уже собранный снимок

На Amvera Data:
  /data/failover.db
  /data/<те же относительные пути медиа>

Env на Amvera:
  DATABASE_URL=sqlite:////data/failover.db
  UPLOAD_FOLDER=/data
  SHOP_READONLY=1
  FAILOVER_SYNC_TOKEN=...

Запуск из корня репо (нужен доступ к прод-БД как у shop):

  python scripts/export-shop-failover-snapshot.py
  python scripts/export-shop-failover-snapshot.py --pack --out D:\\backup\\failover-snap
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "amvera-shop-door" / "data-snapshot"
MANIFEST_NAME = "MANIFEST.json"
DB_NAME = "failover.db"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _copy_tree(src: Path, dst: Path) -> int:
    if not src.is_dir():
        return 0
    count = 0
    for path in src.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(src)
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        count += 1
    return count


def _dump_sqlalchemy_to_sqlite(src_url: str, sqlite_path: Path) -> None:
    from sqlalchemy import create_engine, MetaData, select

    if sqlite_path.exists():
        sqlite_path.unlink()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    src = create_engine(src_url)
    dst_url = "sqlite:///" + str(sqlite_path.resolve()).replace("\\", "/")
    dst = create_engine(dst_url)

    meta = MetaData()
    meta.reflect(bind=src)
    meta.create_all(bind=dst)

    src_conn = src.connect()
    dst_conn = dst.connect()
    try:
        for table in meta.sorted_tables:
            rows = src_conn.execute(select(table)).mappings().all()
            if not rows:
                continue
            payload = [dict(r) for r in rows]
            dst_conn.execute(table.insert(), payload)
        dst_conn.commit()
    finally:
        src_conn.close()
        dst_conn.close()
        src.dispose()
        dst.dispose()


def _sqlite_counts(db_path: Path) -> dict:
    con = sqlite3.connect(str(db_path))
    try:
        check = con.execute("PRAGMA integrity_check").fetchone()
        if not check or str(check[0]).lower() != "ok":
            raise RuntimeError(f"integrity_check failed: {check}")
        tables = {
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        out: dict = {"integrity": "ok", "tables": sorted(tables)}

        def count(name: str) -> int | None:
            if name not in tables:
                return None
            return int(con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0])

        # Типичные таблицы витрины / цен (имена могут отличаться — пишем что есть)
        for name in (
            "plant",
            "plants",
            "stock",
            "stock_item",
            "size",
            "sizes",
            "shop_landing_page",
            "shop_plant_card",
            "shop_variant",
            "price",
            "prices",
        ):
            c = count(name)
            if c is not None:
                out[name] = c
        return out
    finally:
        con.close()


def _build_manifest(out: Path, extra: dict | None = None) -> dict:
    files = []
    media_files = 0
    for path in sorted(out.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(out).as_posix()
        if rel == MANIFEST_NAME:
            continue
        item = {
            "path": rel,
            "size": path.stat().st_size,
            "sha256": _sha256_file(path),
        }
        files.append(item)
        if rel != DB_NAME:
            media_files += 1

    db_path = out / DB_NAME
    db_meta = {
        "sha256": _sha256_file(db_path) if db_path.is_file() else None,
        "size": db_path.stat().st_size if db_path.is_file() else 0,
    }
    counts = {"media_files": media_files, "files_total": len(files)}
    if db_path.is_file():
        counts.update(_sqlite_counts(db_path))

    manifest = {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db": db_meta,
        "counts": counts,
        "files": files,
    }
    if extra:
        manifest.update(extra)
    (out / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return manifest


def verify_snapshot(out: Path) -> list[str]:
    errors: list[str] = []
    man_path = out / MANIFEST_NAME
    db_path = out / DB_NAME
    if not man_path.is_file():
        return ["MANIFEST.json missing"]
    if not db_path.is_file():
        return ["failover.db missing"]
    try:
        manifest = json.loads(man_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"bad MANIFEST.json: {exc}"]

    want_db = (manifest.get("db") or {}).get("sha256")
    if want_db and _sha256_file(db_path) != want_db:
        errors.append("db sha256 mismatch")
    try:
        _sqlite_counts(db_path)
    except Exception as exc:
        errors.append(str(exc))

    for item in manifest.get("files") or []:
        rel = str(item.get("path") or "")
        if not rel or rel == MANIFEST_NAME:
            continue
        path = out / rel
        if not path.is_file():
            errors.append(f"missing {rel}")
            continue
        if item.get("sha256") and _sha256_file(path) != item["sha256"]:
            errors.append(f"hash mismatch {rel}")
        if item.get("size") is not None and path.stat().st_size != int(item["size"]):
            errors.append(f"size mismatch {rel}")
    return errors


def _pack_tar_gz(out: Path, archive: Path) -> Path:
    if archive.exists():
        archive.unlink()
    with tarfile.open(archive, "w:gz") as tar:
        for path in out.rglob("*"):
            if not path.is_file():
                continue
            if path.name.endswith(".tar.gz"):
                continue
            tar.add(path, arcname=path.relative_to(out).as_posix())
    return archive


def main() -> int:
    parser = argparse.ArgumentParser(description="Export failover shop snapshot")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output directory")
    parser.add_argument("--pack", action="store_true", help="Also write snapshot.tar.gz")
    parser.add_argument("--verify", type=Path, default=None, help="Verify existing snapshot dir and exit")
    args = parser.parse_args()

    if args.verify:
        errs = verify_snapshot(args.verify)
        if errs:
            print("VERIFY FAIL:")
            for e in errs:
                print(" ", e)
            return 1
        print("VERIFY OK")
        return 0

    out: Path = args.out
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("SECRET_KEY", os.environ.get("SECRET_KEY") or "export-snapshot-temp-key")
    os.environ.setdefault("DISABLE_SCHEDULER", "1")

    from app import create_app

    app = create_app()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    db_path = out / DB_NAME

    with app.app_context():
        src_url = app.config["SQLALCHEMY_DATABASE_URI"]
        upload_folder = Path(app.config["UPLOAD_FOLDER"])
        print(f"Source DB: {src_url.split('@')[-1] if '@' in src_url else src_url}")
        print(f"Upload folder: {upload_folder}")
        print(f"Output: {out}")

        print("Dumping tables → SQLite …")
        _dump_sqlalchemy_to_sqlite(src_url, db_path)
        print(f"  wrote {db_path} ({db_path.stat().st_size // 1024} KB)")

        total_files = 0
        if upload_folder.is_dir():
            for child in upload_folder.iterdir():
                name = child.name
                if name.endswith(".db") or name.endswith(".sqlite") or name == DB_NAME:
                    continue
                if child.is_dir():
                    n = _copy_tree(child, out / name)
                    print(f"  media {name}/ → {n} files")
                    total_files += n
                elif child.is_file() and child.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                    shutil.copy2(child, out / name)
                    total_files += 1
        else:
            print(f"WARN: upload folder missing: {upload_folder}")

        code_rev = None
        try:
            import subprocess

            code_rev = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=str(ROOT), text=True, stderr=subprocess.DEVNULL
            ).strip()
        except Exception:
            pass

        (out / "SNAPSHOT.txt").write_text(
            f"failover snapshot\n"
            f"created={datetime.now(timezone.utc).isoformat()}\n"
            f"db={DB_NAME}\n"
            f"media_files≈{total_files}\n"
            f"git_head={code_rev or ''}\n",
            encoding="utf-8",
        )

        print("Building MANIFEST + integrity …")
        manifest = _build_manifest(out, extra={"git_head": code_rev, "media_copied": total_files})
        print(f"  integrity={manifest['counts'].get('integrity')} media={manifest['counts'].get('media_files')}")

        errs = verify_snapshot(out)
        if errs:
            print("VERIFY FAIL after export:")
            for e in errs:
                print(" ", e)
            return 1
        print("VERIFY OK")

        if args.pack:
            archive = out.parent / f"{out.name}.tar.gz"
            if out.name == "data-snapshot":
                archive = out.parent / "snapshot.tar.gz"
            _pack_tar_gz(out, archive)
            print(f"  packed {archive} ({archive.stat().st_size // 1024} KB)")

    print("Done.")
    print("Upload via: scripts/sync-failover-weekly.ps1  (or POST /internal/failover-sync)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
