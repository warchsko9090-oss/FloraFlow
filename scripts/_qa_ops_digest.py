from pathlib import Path
import psycopg

env = Path.home() / '.floraflow' / 'backup-pg.env'
url = next(
    l.split('=', 1)[1].strip()
    for l in env.read_text(encoding='utf-8').splitlines()
    if l.startswith('FLORAFLOW_BACKUP_DATABASE_URL=')
)
conn = psycopg.connect(url)
cur = conn.cursor()
cur.execute(
    """
    select table_name from information_schema.tables
    where table_schema='public'
      and (table_name ilike '%digest%' or table_name ilike '%ops%' or table_name ilike '%ai_ops%')
    order by 1
    """
)
print('tables', [r[0] for r in cur.fetchall()])

for t in [
    'ops_digest', 'ai_ops_digest', 'ops_day_digest', 'sales_chat_digest',
    'ai_ops_run', 'ai_ops_log',
]:
    cur.execute(
        "select exists (select 1 from information_schema.tables where table_name=%s)",
        (t,),
    )
    if cur.fetchone()[0]:
        cur.execute(f"select column_name from information_schema.columns where table_name=%s order by 1", (t,))
        print(t, 'cols', [r[0] for r in cur.fetchall()])

# try common patterns
cur.execute(
    """
    select table_name, column_name
    from information_schema.columns
    where table_schema='public'
      and (column_name ilike '%summary%' or column_name ilike '%digest%')
      and table_name ilike '%ops%'
    order by 1,2
    """
)
print('ops summary cols', cur.fetchall())

cur.execute(
    """
    select table_name, column_name
    from information_schema.columns
    where table_schema='public' and column_name = 'summary_text'
    """
)
print('summary_text tables', cur.fetchall())
conn.close()
