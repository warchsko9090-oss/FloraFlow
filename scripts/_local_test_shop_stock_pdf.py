import os
import traceback
from pathlib import Path

env_path = Path.home() / ".floraflow" / "backup-pg.env"
for line in env_path.read_text(encoding="utf-8").splitlines():
    if line.startswith("FLORAFLOW_BACKUP_DATABASE_URL="):
        os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip()
        break
os.environ["DISABLE_SCHEDULER"] = "1"
os.environ["FLASK_ENV"] = "development"

shop_root = Path(__file__).resolve().parents[1] / "knyajestvo-code"
os.chdir(shop_root)
import sys

sys.path.insert(0, str(shop_root))

from shop_site.factory import create_app
from app.utils import _pdf_static_dir

app = create_app()
with app.app_context():
    print("root_path", app.root_path)
    print("static_folder", app.static_folder)
    print("pdf_static", _pdf_static_dir())
    print("dejavu", (_pdf_static_dir() / "DejaVuSans.ttf").is_file())
    try:
        with app.test_client() as c:
            rv = c.get("/shop/stock.pdf")
            print(
                "status",
                rv.status_code,
                "ctype",
                rv.headers.get("Content-Type"),
                "len",
                len(rv.data or b""),
            )
            if rv.status_code != 200:
                print((rv.data or b"")[:2000])
            else:
                out = Path(__file__).resolve().parent / "_local_shop_stock.pdf"
                out.write_bytes(rv.data)
                print("wrote", out)
    except Exception:
        traceback.print_exc()
