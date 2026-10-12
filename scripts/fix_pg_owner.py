#!/usr/bin/env python3
"""Передать владение schema public пользователю приложения (Amvera).

Нужен внешний POSTGRES-URL и логин с правами владельца/суперпользователя
(обычно postgres после «Активировать Superuser Access»).

Usage:
  python scripts/fix_pg_owner.py --url "postgresql://postgres:PASS@HOST:5432/FloraFlow?sslmode=require" --owner warchesko
"""
from __future__ import annotations

import argparse
import sys

try:
    import psycopg
except ImportError:
    print('pip install "psycopg[binary]"', file=sys.stderr)
    sys.exit(1)


SQL = """
GRANT USAGE, CREATE ON SCHEMA public TO {owner};
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {owner};
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {owner};
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO {owner};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {owner};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {owner};
-- Сначала таблицы: SERIAL/IDENTITY sequences переходят вместе с ними.
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.relname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f')
  LOOP
    EXECUTE format('ALTER TABLE public.%I OWNER TO {owner}', r.name);
  END LOOP;

  FOR r IN
    SELECT c.relname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')
  LOOP
    EXECUTE format('ALTER VIEW public.%I OWNER TO {owner}', r.name);
  END LOOP;

  -- Только «свободные» sequences (не привязанные к колонке таблицы).
  FOR r IN
    SELECT c.relname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind = 'S'
      AND NOT EXISTS (
        SELECT 1
        FROM pg_depend d
        WHERE d.objid = c.oid
          AND d.deptype = 'a'
      )
  LOOP
    EXECUTE format('ALTER SEQUENCE public.%I OWNER TO {owner}', r.name);
  END LOOP;
END $$;
"""


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--url', required=True, help='postgresql://... с правами postgres/superuser')
    p.add_argument('--owner', default='warchesko', help='роль приложения')
    args = p.parse_args()
    owner = args.owner.strip()
    if not owner.isidentifier():
        raise SystemExit('bad --owner')
    url = args.url.strip()
    if url.startswith('postgres://'):
        url = 'postgresql://' + url[len('postgres://'):]
    sql = SQL.format(owner=owner)
    with psycopg.connect(url) as conn:
        conn.execute(sql)
        conn.commit()
        rows = conn.execute(
            """
            SELECT tablename, tableowner
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
            """
        ).fetchall()
    print(f'OK — owner set to {owner}. Tables:')
    for name, tbl_owner in rows:
        mark = '✓' if tbl_owner == owner else f'! {tbl_owner}'
        print(f'  {name}: {mark}')


if __name__ == '__main__':
    main()
