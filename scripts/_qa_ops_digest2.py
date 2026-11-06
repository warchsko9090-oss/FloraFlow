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
    '''select digest_date, message_count, left(summary_text, 200), sent_at,
              length(summary_text), length(coalesce(actions_json,''))
       from ops_digest_log
       where digest_date >= current_date - 7
       order by digest_date desc'''
)
print('=== recent digests ===')
for r in cur.fetchall():
    print(r)
    print('---')

cur.execute(
    '''select channel, count(*), sum(length(coalesce(raw_text,'')))
       from ops_chat_message
       where (tg_date::date = current_date or created_at::date = current_date
              or (tg_date at time zone 'UTC' + interval '3 hours')::date = current_date)
       group by 1 order by 1'''
)
print('=== today msgs by channel (loose) ===')
for r in cur.fetchall():
    print(r)

cur.execute(
    '''select column_name from information_schema.columns
       where table_name='ops_chat_message' order by ordinal_position'''
)
print('ops_chat_message cols', [r[0] for r in cur.fetchall()])

cur.execute(
    '''select count(*), sum(length(coalesce(text, raw_text, '')))
       from ops_chat_message
       where digest_date = current_date or true'''
)
# get actual date filter from code
cur.execute(
    '''select min(created_at), max(created_at), count(*) from ops_chat_message'''
)
print('ops_chat range', cur.fetchone())

cur.execute(
    '''select date(created_at + interval '3 hours') d, count(*),
              sum(length(coalesce(
                nullif(text,''), nullif(raw_text,''), coalesce(transcript,''), ''
              )))
       from ops_chat_message
       where created_at >= now() - interval '5 days'
       group by 1 order by 1 desc'''
)
print('try created_at days')
try:
    print(cur.fetchall())
except Exception as e:
    print('err', e)
    conn.rollback()
    cur.execute(
        '''select date(created_at + interval '3 hours') d, count(*)
           from ops_chat_message
           where created_at >= now() - interval '5 days'
           group by 1 order by 1 desc'''
    )
    print(cur.fetchall())

conn.close()
