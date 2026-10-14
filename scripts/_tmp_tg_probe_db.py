import psycopg2

url = (
    "postgresql://warchesko:Z960827zz@"
    "floraflow1-warchesko.tcp-mia0.amvera.tech:5432/FloraFlow?sslmode=require"
)
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute(
    "SELECT key, left(coalesce(value,''), 80) FROM app_setting "
    "WHERE lower(key) LIKE %s OR lower(key) LIKE %s OR lower(key) LIKE %s "
    "OR lower(key) LIKE %s ORDER BY 1",
    ("%tg%", "%bot%", "%token%", "%proxy%"),
)
print("settings:")
for row in cur.fetchall():
    print(" ", row)
cur.execute('SELECT username, role FROM "user" ORDER BY id')
print("users:")
for row in cur.fetchall():
    print(" ", row)
conn.close()
