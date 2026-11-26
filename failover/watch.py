#!/usr/bin/env python3
"""Сторож failover для knyajestvo.ru.

Логика:
  1) Прямой health домашнего IP (Host: domain) — истина о доме.
  2) Публичный A @ — куда сейчас смотрит DNS.
  3) Дом down + DNS ещё на дом → wake Amvera, A→failover IP, TG.
  4) Дом up + DNS на failover → A→дом, sleep Amvera, TG.

Запуск:
  python failover/watch.py
  python failover/watch.py --dry-run
  python failover/watch.py --set-ttl 300

Секреты — env или файл failover/.env (не коммитить).
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from amvera_scale import AmveraError, sleep as amvera_sleep, wake as amvera_wake  # noqa: E402
from regru_dns import RegRuError, get_a_records, set_apex_a, set_zone_ttl  # noqa: E402
from tg_notify import send_telegram  # noqa: E402


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


def env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)) or default)
    except ValueError:
        return default


def resolve_a(name: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(name, 443, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    out: list[str] = []
    for info in infos:
        ip = info[4][0]
        if ":" not in ip and ip not in out:
            out.append(ip)
    return out


def http_health(
    *,
    host: str,
    ip: str,
    path: str = "/healthz",
    timeout: float = 8.0,
    sni_host: str | None = None,
) -> tuple[bool, str]:
    """HTTPS GET на конкретный IP с Host/SNI = host."""
    sni = sni_host or host
    url_path = path if path.startswith("/") else f"/{path}"
    context = ssl.create_default_context()
    try:
        with socket.create_connection((ip, 443), timeout=timeout) as sock:
            with context.wrap_socket(sock, server_hostname=sni) as ssock:
                req = (
                    f"GET {url_path} HTTP/1.1\r\n"
                    f"Host: {host}\r\n"
                    f"User-Agent: floraflow-failover/1.0\r\n"
                    f"Connection: close\r\n\r\n"
                ).encode("ascii")
                ssock.sendall(req)
                chunks = []
                while True:
                    buf = ssock.recv(4096)
                    if not buf:
                        break
                    chunks.append(buf)
                    if len(b"".join(chunks)) > 20000:
                        break
        raw = b"".join(chunks).decode("latin-1", errors="replace")
        status_line = raw.split("\r\n", 1)[0]
        ok = status_line.startswith("HTTP/1.") and (" 200 " in status_line or status_line.endswith(" 200"))
        # also accept 200 without trailing space variants
        if not ok and " 200" in status_line:
            ok = True
        return ok, status_line or "empty response"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def public_http_ok(url: str, timeout: float = 10.0) -> tuple[bool, str]:
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "floraflow-failover/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = getattr(resp, "status", None) or resp.getcode()
            return int(code) == 200, f"HTTP {code}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def main() -> int:
    parser = argparse.ArgumentParser(description="FloraFlow DNS/Amvera failover watcher")
    parser.add_argument("--dry-run", action="store_true", help="Только проверки, без DNS/Amvera/TG side-effects")
    parser.add_argument("--set-ttl", type=int, default=0, help="Выставить зональный TTL и выйти")
    parser.add_argument("--force-failover", action="store_true", help="Принудительно уйти на failover")
    parser.add_argument("--force-home", action="store_true", help="Принудительно вернуть на дом")
    args = parser.parse_args()

    _load_dotenv(ROOT / ".env")

    domain = env("FAILOVER_DOMAIN", "knyajestvo.ru")
    home_ip = env("HOME_IP", "95.31.41.191")
    failover_ip = env("FAILOVER_IP")
    health_path = env("HOME_HEALTH_PATH", "/healthz")
    fail_threshold = env_int("FAIL_THRESHOLD", 2)
    ok_threshold = env_int("OK_THRESHOLD", 2)
    state_path = Path(env("FAILOVER_STATE_FILE", str(ROOT / "state.json")))

    regru_user = env("REGRU_USERNAME")
    regru_pass = env("REGRU_PASSWORD")
    amvera_url = env("AMVERA_MCP_URL", "https://openmcp.mia0.amvera.ru/mcp")
    amvera_token = env("AMVERA_TOKEN")
    amvera_project = env("AMVERA_PROJECT_ID")
    tg_token = env("TG_BOT_TOKEN")
    tg_chat = env("TG_CHAT_ID")
    tg_api = env("TG_API_BASE", "https://api.telegram.org")

    dry = args.dry_run or env("DRY_RUN", "").lower() in ("1", "true", "yes")

    if args.set_ttl:
        if not regru_user or not regru_pass:
            print("REGRU_USERNAME/PASSWORD required", file=sys.stderr)
            return 2
        if dry:
            print(f"[dry-run] set TTL {args.set_ttl} for {domain}")
            return 0
        set_zone_ttl(regru_user, regru_pass, domain, args.set_ttl)
        print(f"TTL set to {args.set_ttl}")
        return 0

    if not failover_ip:
        print("FAILOVER_IP is required (Amvera public A for stub)", file=sys.stderr)
        return 2

    # --- probes ---
    home_ok, home_detail = http_health(host=domain, ip=home_ip, path=health_path)
    # fallback: / if /healthz missing on old shop
    if not home_ok and health_path != "/":
        home_ok2, home_detail2 = http_health(host=domain, ip=home_ip, path="/")
        if home_ok2:
            home_ok, home_detail = home_ok2, f"{home_detail} → / ok ({home_detail2})"

    public_ips = resolve_a(domain)
    dns_on_home = home_ip in public_ips
    dns_on_failover = failover_ip in public_ips
    public_ok, public_detail = public_http_ok(f"https://{domain}/")

    print(
        json.dumps(
            {
                "domain": domain,
                "home_ok": home_ok,
                "home_detail": home_detail,
                "public_ips": public_ips,
                "dns_on_home": dns_on_home,
                "dns_on_failover": dns_on_failover,
                "public_ok": public_ok,
                "public_detail": public_detail,
                "dry_run": dry,
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    # state for hysteresis
    state = {"fail_streak": 0, "ok_streak": 0, "mode": "unknown"}
    if state_path.is_file():
        try:
            state.update(json.loads(state_path.read_text(encoding="utf-8")))
        except Exception:
            pass

    if home_ok:
        state["ok_streak"] = int(state.get("ok_streak") or 0) + 1
        state["fail_streak"] = 0
    else:
        state["fail_streak"] = int(state.get("fail_streak") or 0) + 1
        state["ok_streak"] = 0

    want_failover = args.force_failover or (
        not home_ok and state["fail_streak"] >= fail_threshold and not args.force_home
    )
    want_home = args.force_home or (
        home_ok and state["ok_streak"] >= ok_threshold and not args.force_failover
    )

    actions: list[str] = []

    def notify(text: str) -> None:
        actions.append(f"tg:{text[:80]}")
        if dry or not tg_token or not tg_chat:
            print(f"[tg skipped] {text}")
            return
        try:
            send_telegram(tg_token, tg_chat, text, api_base=tg_api)
        except Exception as exc:
            print(f"TG failed: {exc}", file=sys.stderr)

    def dns_flip(to_ip: str, old_ip: str | None) -> None:
        actions.append(f"dns:{to_ip}")
        if dry:
            print(f"[dry-run] DNS A @ → {to_ip} (old={old_ip})")
            return
        if not regru_user or not regru_pass:
            raise RegRuError("REGRU_USERNAME/PASSWORD required for DNS flip")
        # if old unknown, try from API
        cur_old = old_ip
        if not cur_old:
            try:
                for rr in get_a_records(regru_user, regru_pass, domain):
                    if rr["subdomain"] in ("@", domain, ""):
                        cur_old = rr["ip"]
                        break
            except Exception as exc:
                print(f"get_a_records warn: {exc}")
        set_apex_a(regru_user, regru_pass, domain, to_ip, old_ip=cur_old)

    def amvera_set(replicas: int) -> None:
        actions.append(f"amvera:{replicas}")
        if dry:
            print(f"[dry-run] Amvera replicas → {replicas}")
            return
        if not amvera_token or not amvera_project:
            print("Amvera token/project missing — skip scale", file=sys.stderr)
            return
        if replicas <= 0:
            amvera_sleep(amvera_url, amvera_token, amvera_project)
        else:
            amvera_wake(amvera_url, amvera_token, amvera_project)

    try:
        if want_failover and not dns_on_failover:
            print("→ failover")
            amvera_set(1)
            # дать контейнеру чуть подняться до DNS
            if not dry:
                time.sleep(env_int("WAKE_GRACE_SEC", 15))
            dns_flip(failover_ip, home_ip if dns_on_home else None)
            state["mode"] = "failover"
            notify(
                f"⚠️ knyajestvo: дом недоступен ({home_detail}).\n"
                f"Поднял Amvera-заглушку, DNS A → {failover_ip}.\n"
                f"Клиенты увидят витрину без остатков. Подними дом, когда сможешь."
            )
        elif want_home and dns_on_failover:
            print("→ failback home")
            dns_flip(home_ip, failover_ip)
            if not dry:
                time.sleep(env_int("FAILBACK_GRACE_SEC", 20))
            amvera_set(0)
            state["mode"] = "home"
            notify(
                f"✅ knyajestvo: дом снова отвечает.\n"
                f"DNS A → {home_ip}, Amvera stub усыпляю."
            )
        elif want_failover and dns_on_failover:
            print("already on failover")
            state["mode"] = "failover"
        elif want_home and dns_on_home:
            print("already on home")
            state["mode"] = "home"
        else:
            print(
                f"waiting hysteresis fail={state['fail_streak']}/{fail_threshold} "
                f"ok={state['ok_streak']}/{ok_threshold}"
            )
    except (RegRuError, AmveraError) as exc:
        print(f"ACTION ERROR: {exc}", file=sys.stderr)
        notify(f"❌ knyajestvo failover error: {exc}")
        if not dry:
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        return 1

    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    state["last_actions"] = actions
    if not dry:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        print(f"[dry-run] state would be: {state}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
