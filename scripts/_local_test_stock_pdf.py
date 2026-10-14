import os
import traceback
from pathlib import Path

env_path = Path.home() / ".floraflow" / "backup-pg.env"
for line in env_path.read_text(encoding="utf-8").splitlines():
    if line.startswith("FLORAFLOW_BACKUP_DATABASE_URL="):
        os.environ["DATABASE_URL"] = line.split("=", 1)[1].strip()
        break
os.environ["DISABLE_SCHEDULER"] = "1"

from app import create_app

app = create_app()
with app.app_context():
    try:
        from app.public_client_api import get_public_product_stock_report

        end, groups = get_public_product_stock_report()
        print("groups", len(groups), "end", end)
        for g in groups:
            if g.get("is_section"):
                print(" section", g.get("section_key"), g.get("name"))
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
                print((rv.data or b"")[:3000])
            else:
                out = Path("scripts/_local_stock.pdf")
                out.write_bytes(rv.data)
                print("wrote", out)
    except Exception:
        traceback.print_exc()
