"""Set tg_ping_once=1 and optionally poll tg_ping_last."""
import json
import sys
import time

import psycopg2

URL = (
    "postgresql://warchesko:Z960827zz@"
    "floraflow1-warchesko.tcp-mia0.amvera.tech:5432/FloraFlow?sslmode=require"
)


def upsert(cur, key, value):
    cur.execute("SELECT 1 FROM app_setting WHERE key = %s", (key,))
    if cur.fetchone():
        cur.execute("UPDATE app_setting SET value = %s WHERE key = %s", (value, key))
    else:
        cur.execute(
            "INSERT INTO app_setting (key, value) VALUES (%s, %s)",
            (key, value),
        )


def main():
    mode = (sys.argv[1] if len(sys.argv) > 1 else "set").strip()
    conn = psycopg2.connect(URL)
    conn.autocommit = True
    cur = conn.cursor()
    if mode == "set":
        upsert(cur, "tg_ping_once", "1")
        print("SET tg_ping_once=1")
    elif mode == "get":
        cur.execute(
            "SELECT key, value FROM app_setting WHERE key IN ('tg_ping_once', 'tg_ping_last')"
        )
        for key, value in cur.fetchall():
            print("===", key, "===")
            print(value)
            if key == "tg_ping_last" and value:
                try:
                    data = json.loads(value)
                    for r in data.get("results") or []:
                        print(
                            " ",
                            "OK" if r.get("ok") else "FAIL",
                            r.get("chat_type"),
                            r.get("chat_id"),
                            r.get("detail"),
                        )
                except Exception as exc:
                    print("parse err", exc)
    elif mode == "wait":
        upsert(cur, "tg_ping_once", "1")
        print("SET tg_ping_once=1; waiting...")
        deadline = time.time() + 600
        last_at = None
        cur.execute("SELECT value FROM app_setting WHERE key = 'tg_ping_last'")
        row = cur.fetchone()
        if row and row[0]:
            try:
                last_at = json.loads(row[0]).get("at")
            except Exception:
                last_at = None
        while time.time() < deadline:
            time.sleep(8)
            cur.execute(
                "SELECT value FROM app_setting WHERE key IN ('tg_ping_once', 'tg_ping_last')"
            )
            rows = dict(cur.fetchall())
            once = (rows.get("tg_ping_once") or "").strip()
            raw = rows.get("tg_ping_last") or ""
            at = None
            if raw:
                try:
                    data = json.loads(raw)
                    at = data.get("at")
                except Exception:
                    data = None
            else:
                data = None
            print(f"poll once={once!r} at={at}")
            if once in ("0", "") and data and at and at != last_at:
                print("DONE")
                for r in data.get("results") or []:
                    print(
                        " ",
                        "OK" if r.get("ok") else "FAIL",
                        r.get("label"),
                        r.get("chat_id"),
                        r.get("detail"),
                    )
                break
        else:
            print("TIMEOUT")
            sys.exit(1)
    else:
        print("usage: set|get|wait")
        sys.exit(2)
    conn.close()


if __name__ == "__main__":
    main()
