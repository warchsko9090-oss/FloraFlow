import sqlite3, os
path = os.path.abspath('nursery.db')
print('DB path:', path)
conn = sqlite3.connect(path)
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
print('tables:')
print('\n'.join(' - '+r[0] for r in cur.fetchall()))
for t in ['order','plant','client','field','stock_balance','document','order_item','payment']:
    try:
        cur.execute(f"SELECT count(*) FROM {t}")
        print(f"{t}: {cur.fetchone()[0]}")
    except Exception:
        pass
conn.close()
