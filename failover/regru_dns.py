"""Reg.ru API2: смена A-записи без дублей (remove all → add one)."""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


API = "https://api.reg.ru/api/regru2"


class RegRuError(RuntimeError):
    pass


def _call(username: str, password: str, method: str, **fields: Any) -> dict:
    form: dict[str, str] = {
        "username": username,
        "password": password,
        "output_format": "json",
    }
    for key, value in fields.items():
        if isinstance(value, (dict, list)):
            form["input_format"] = "json"
            form[key] = json.dumps(value, ensure_ascii=False)
        else:
            form[key] = str(value)

    body = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        f"{API}/{method}",
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RegRuError(f"HTTP {exc.code}: {raw[:500]}") from exc
    except urllib.error.URLError as exc:
        raise RegRuError(str(exc)) from exc

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RegRuError(f"bad JSON: {raw[:300]}") from exc
    if parsed.get("result") != "success":
        raise RegRuError(parsed.get("error_text") or parsed.get("error_code") or raw[:400])
    return parsed


def _norm_sub(sub: str | None) -> str:
    s = (sub or "@").strip()
    if s in ("", "@"):
        return "@"
    return s


def get_a_records(username: str, password: str, domain: str) -> list[dict[str, str]]:
    parsed = _call(
        username,
        password,
        "zone/get_resource_records",
        domains=[{"dname": domain}],
    )
    cleaned: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for dom in (parsed.get("answer") or {}).get("domains") or []:
        for rr in dom.get("rrs") or []:
            if str(rr.get("rectype") or "").upper() != "A":
                continue
            sub = _norm_sub(str(rr.get("subdomain") or "@"))
            ip = str(rr.get("content") or rr.get("data") or rr.get("ipaddr") or "")
            key = (sub, ip)
            if not ip or key in seen:
                continue
            seen.add(key)
            cleaned.append({"subdomain": sub, "ip": ip})
    return cleaned


def get_resource_records(username: str, password: str, domain: str) -> list[dict[str, str]]:
    parsed = _call(
        username,
        password,
        "zone/get_resource_records",
        domains=[{"dname": domain}],
    )
    out: list[dict[str, str]] = []
    for dom in (parsed.get("answer") or {}).get("domains") or []:
        for rr in dom.get("rrs") or []:
            sub = _norm_sub(str(rr.get("subdomain") or "@"))
            rtype = str(rr.get("rectype") or "").upper()
            content = str(rr.get("content") or rr.get("data") or rr.get("ipaddr") or "")
            if rtype and content:
                out.append({"subdomain": sub, "type": rtype, "content": content})
    return out


def _remove_a_all(username: str, password: str, domain: str, subdomain: str) -> None:
    """Удалить ВСЕ A для имени. content не передаём — Reg.ru снимает все совпадения."""
    sub = _norm_sub(subdomain)
    # Рабочий формат client API: domains[{dname}] + subdomain + record_type
    # (domain_name= часто принимается add_alias, но remove_record без domains молча не чистит).
    attempts: list[dict[str, Any]] = [
        {
            "domains": [{"dname": domain}],
            "subdomain": sub,
            "record_type": "A",
        },
        {
            "dname": domain,
            "subdomain": sub,
            "record_type": "A",
        },
        {
            "domain_name": domain,
            "subdomain": sub,
            "record_type": "A",
        },
    ]
    # Для apex иногда API ждёт пустой subdomain вместо @
    if sub == "@":
        for base in (
            {"domains": [{"dname": domain}], "record_type": "A"},
            {"dname": domain, "record_type": "A"},
        ):
            attempts.append({**base, "subdomain": ""})
            attempts.append({**base, "subdomain": "@"})

    last_err: Exception | None = None
    for fields in attempts:
        try:
            _call(username, password, "zone/remove_record", **fields)
            return
        except RegRuError as exc:
            last_err = exc
            # «нечего удалять» — ок
            msg = str(exc).lower()
            if any(x in msg for x in ("not found", "не найден", "no such", "empty", "nothing")):
                return
            continue
    # Если все попытки упали — проверим, не пусто ли уже
    left = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]
    if not left:
        return
    raise RegRuError(f"remove_record A {sub} failed, still have {left}: {last_err}")


def _remove_a_ip(username: str, password: str, domain: str, subdomain: str, ip: str) -> None:
    sub = _norm_sub(subdomain)
    for fields in (
        {"domains": [{"dname": domain}], "subdomain": sub, "record_type": "A", "content": ip},
        {"dname": domain, "subdomain": sub, "record_type": "A", "content": ip},
        {"domain_name": domain, "subdomain": sub, "record_type": "A", "content": ip},
    ):
        try:
            _call(username, password, "zone/remove_record", **fields)
            return
        except RegRuError:
            continue


def set_a_record(
    username: str,
    password: str,
    domain: str,
    subdomain: str,
    new_ip: str,
    *,
    old_ip: str | None = None,
) -> dict:
    """Поставить ровно одну A subdomain = new_ip (снять все старые A этого имени)."""
    del old_ip  # больше не полагаемся на один old_ip — чистим все
    sub = _norm_sub(subdomain)
    current = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]
    only_new = len(current) == 1 and current[0]["ip"] == new_ip
    if only_new:
        return {"result": "success", "skipped": True, "subdomain": sub, "ip": new_ip}

    # 1) точечно снять чужие IP
    for rr in current:
        if rr["ip"] != new_ip:
            _remove_a_ip(username, password, domain, sub, rr["ip"])

    # 2) если дублей new_ip несколько или чужие остались — снять ВСЕ A имени
    after = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]
    wrong = [r for r in after if r["ip"] != new_ip]
    dup_new = sum(1 for r in after if r["ip"] == new_ip)
    if wrong or dup_new > 1:
        _remove_a_all(username, password, domain, sub)
        after = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]

    # 3) добавить, если нет ровно одной нужной
    if not any(r["ip"] == new_ip for r in after):
        result = _call(
            username,
            password,
            "zone/add_alias",
            domains=[{"dname": domain}],
            subdomain=sub,
            ipaddr=new_ip,
        )
    else:
        result = {"result": "success", "kept": True}

    # 4) финальная зачистка чужих (если add оставил старые)
    final = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]
    for rr in final:
        if rr["ip"] != new_ip:
            _remove_a_ip(username, password, domain, sub, rr["ip"])
    final = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]
    ips = sorted({r["ip"] for r in final})
    if ips != [new_ip]:
        # последний шанс: wipe + add через domain_name (как desktop-скрипт)
        _remove_a_all(username, password, domain, sub)
        _call(
            username,
            password,
            "zone/add_alias",
            domain_name=domain,
            subdomain=sub,
            ipaddr=new_ip,
        )
        final = [r for r in get_a_records(username, password, domain) if r["subdomain"] == sub]
        ips = sorted({r["ip"] for r in final})
        if ips != [new_ip]:
            raise RegRuError(f"A {sub} still {ips}, want [{new_ip}]")
    return result if isinstance(result, dict) else {"result": "success"}


def set_apex_a(
    username: str,
    password: str,
    domain: str,
    new_ip: str,
    *,
    old_ip: str | None = None,
    sync_www: bool = True,
) -> dict:
    """Ровно один A @ = new_ip; www — A на тот же IP (CNAME www снимаем, чтобы не плодить хаос)."""
    del old_ip
    result = set_a_record(username, password, domain, "@", new_ip)

    if not sync_www:
        return result

    # Если www CNAME — удалить CNAME, поставить A (иначе при двух A @ клиент ловит оба).
    try:
        for rr in get_resource_records(username, password, domain):
            if rr.get("subdomain") != "www":
                continue
            if rr.get("type") == "CNAME":
                for fields in (
                    {
                        "domains": [{"dname": domain}],
                        "subdomain": "www",
                        "record_type": "CNAME",
                        "content": rr["content"],
                    },
                    {
                        "dname": domain,
                        "subdomain": "www",
                        "record_type": "CNAME",
                        "content": rr["content"],
                    },
                ):
                    try:
                        _call(username, password, "zone/remove_record", **fields)
                        break
                    except RegRuError:
                        continue
    except RegRuError:
        pass

    set_a_record(username, password, domain, "www", new_ip)
    return result


def set_zone_ttl(username: str, password: str, domain: str, ttl: int = 300) -> dict:
    return _call(
        username,
        password,
        "zone/update_soa",
        domains=[{"dname": domain}],
        ttl=int(ttl),
    )
