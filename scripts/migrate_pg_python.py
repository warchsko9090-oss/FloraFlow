#!/usr/bin/env python3
"""Migrate FloraFlow Postgres: old Amvera cluster -> new (logical data only).

Uses SQLAlchemy reflection + row copy — no pg_dump required.

Usage:
  python scripts/migrate_pg_python.py dump   --url OLD_URL --file backups/floraflow.sql
  python scripts/migrate_pg_python.py copy   --old OLD_URL --new NEW_URL
  python scripts/migrate_pg_python.py verify --old OLD_URL --new NEW_URL

Prefer `copy` in one step during maintenance window (apps paused).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote, urlunparse, parse_qs, urlencode

try:
    from sqlalchemy import create_engine, MetaData, Table, select, text, inspect
    from sqlalchemy.schema import CreateTable, CreateIndex, CreateSequence
    from sqlalchemy.exc import ProgrammingError
except ImportError:
    print('Install project deps first: pip install -r requirements.txt', file=sys.stderr)
    sys.exit(1)


VERIFY_TABLES = (
    'action_log',
    'order',
    'order_item',
    'tg_task',
    'expense',
    'client',
    'document',
    'document_row',
)


def normalize_url(url: str) -> str:
    url = (url or '').strip()
    if url.startswith('postgres://'):
        url = 'postgresql://' + url[len('postgres://'):]
    if not url.startswith('postgresql'):
        raise SystemExit('URL must start with postgresql://')
    # psycopg2 driver
    if url.startswith('postgresql://'):
        url = 'postgresql+psycopg2://' + url[len('postgresql://'):]
    return url


def engine_from(url: str):
    return create_engine(normalize_url(url), pool_pre_ping=True)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def list_user_tables(conn) -> list[str]:
    rows = conn.execute(text("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """))
    return [r[0] for r in rows]


def table_count(conn, table: str) -> int:
    return int(conn.execute(text(f'SELECT COUNT(*) FROM {quote_ident(table)}')).scalar() or 0)


def reset_sequences(conn, tables: list[str]) -> None:
    for table in tables:
        rows = conn.execute(text("""
            SELECT a.attname AS col,
                   pg_get_serial_sequence(
                       format('%I.%I', 'public', :table), a.attname
                   ) AS seq
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = :table
              AND a.attnum > 0
              AND NOT a.attisdropped
        """), {'table': table})
        for col, seq in rows:
            if not seq:
                continue
            conn.execute(text(
                f'SELECT setval(:seq, '
                f'COALESCE((SELECT MAX({quote_ident(col)}) FROM {quote_ident(table)}), 1), '
                f'true)'
            ), {'seq': seq})


def cmd_verify(old_url: str, new_url: str) -> int:
    old_e = engine_from(old_url)
    new_e = engine_from(new_url)
    ok = True
    print(f'{"table":<22} {"old":>12} {"new":>12} ok?')
    with old_e.connect() as oc, new_e.connect() as nc:
        for t in VERIFY_TABLES:
            try:
                o = table_count(oc, t)
            except Exception as e:
                o = f'ERR:{e.__class__.__name__}'
                ok = False
            try:
                n = table_count(nc, t)
            except Exception as e:
                n = f'ERR:{e.__class__.__name__}'
                ok = False
            match = o == n and isinstance(o, int)
            if not match:
                ok = False
            print(f'{t:<22} {str(o):>12} {str(n):>12} {"YES" if match else "NO"}')
    if ok:
        print('VERIFY OK — можно переключать DATABASE_URL')
        return 0
    print('VERIFY FAILED — URL не переключать')
    return 1


def cmd_copy(old_url: str, new_url: str) -> int:
    """Reflect schema from old, recreate on new, copy all rows."""
    old_e = engine_from(old_url)
    new_e = engine_from(new_url)

    meta = MetaData()
    print('Reflecting schema from OLD…')
    meta.reflect(bind=old_e, schema='public')
    tables = list(meta.sorted_tables)
    if not tables:
        print('No tables found on old DB', file=sys.stderr)
        return 1

    print(f'Found {len(tables)} tables')

    with new_e.begin() as conn:
        # Drop existing public tables on new (fresh cluster may have empty FloraFlow)
        existing = list_user_tables(conn)
        if existing:
            print(f'Dropping {len(existing)} existing tables on NEW…')
            conn.execute(text('DROP SCHEMA public CASCADE'))
            conn.execute(text('CREATE SCHEMA public'))
            conn.execute(text('GRANT ALL ON SCHEMA public TO public'))

        print('Creating tables on NEW…')
        for table in tables:
            # strip schema for CreateTable on public
            ddl = str(CreateTable(table).compile(dialect=new_e.dialect))
            conn.execute(text(ddl))

        # Indexes not always in CreateTable for all dialects — create remaining
        for table in tables:
            for idx in table.indexes:
                try:
                    conn.execute(CreateIndex(idx))
                except Exception:
                    pass

    print('Copying data…')
    with old_e.connect() as src, new_e.begin() as dst:
        # disable FK checks via session_replication_role
        dst.execute(text("SET session_replication_role = 'replica'"))
        for table in tables:
            name = table.name
            rows = src.execute(select(table)).mappings().all()
            if not rows:
                print(f'  {name}: 0')
                continue
            # insert in chunks
            chunk = 500
            data = [dict(r) for r in rows]
            for i in range(0, len(data), chunk):
                dst.execute(table.insert(), data[i:i + chunk])
            print(f'  {name}: {len(data)}')
        print('Resetting sequences…')
        reset_sequences(dst, [t.name for t in tables])
        dst.execute(text("SET session_replication_role = 'origin'"))

    print('Copy done. Running verify…')
    return cmd_verify(old_url, new_url)


def cmd_dump_sql(url: str, path: Path) -> int:
    """Plain SQL dump (INSERT) for backup artifact."""
    eng = engine_from(url)
    meta = MetaData()
    meta.reflect(bind=eng, schema='public')
    path.parent.mkdir(parents=True, exist_ok=True)
    with eng.connect() as conn, path.open('w', encoding='utf-8') as f:
        f.write('-- FloraFlow logical dump\n')
        f.write('SET session_replication_role = replica;\n')
        for table in meta.sorted_tables:
            f.write(f'\n-- TABLE {table.name}\n')
            f.write(str(CreateTable(table).compile(dialect=eng.dialect)) + ';\n')
            rows = conn.execute(select(table)).mappings().all()
            cols = [c.name for c in table.columns]
            col_list = ', '.join(quote_ident(c) for c in cols)
            for r in rows:
                vals = []
                for c in cols:
                    v = r[c]
                    if v is None:
                        vals.append('NULL')
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    elif isinstance(v, bool):
                        vals.append('TRUE' if v else 'FALSE')
                    else:
                        s = str(v).replace("'", "''")
                        vals.append("'" + s + "'")
                f.write(
                    f'INSERT INTO {quote_ident(table.name)} ({col_list}) '
                    f'VALUES ({", ".join(vals)});\n'
                )
        f.write('\nSET session_replication_role = origin;\n')
    print(f'Wrote {path} ({path.stat().st_size} bytes)')
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description='FloraFlow PG migration without pg_dump')
    sub = p.add_subparsers(dest='cmd', required=True)

    c = sub.add_parser('copy', help='Schema+data old -> new (maintenance window)')
    c.add_argument('--old', required=True)
    c.add_argument('--new', required=True)

    v = sub.add_parser('verify', help='Compare row counts')
    v.add_argument('--old', required=True)
    v.add_argument('--new', required=True)

    d = sub.add_parser('dump', help='Write SQL dump file from old')
    d.add_argument('--url', required=True)
    d.add_argument('--file', default='backups/floraflow.sql')

    args = p.parse_args(argv)
    if args.cmd == 'copy':
        return cmd_copy(args.old, args.new)
    if args.cmd == 'verify':
        return cmd_verify(args.old, args.new)
    if args.cmd == 'dump':
        return cmd_dump_sql(args.url, Path(args.file))
    return 1


if __name__ == '__main__':
    sys.exit(main())
