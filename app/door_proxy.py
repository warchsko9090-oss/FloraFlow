"""WSGI-дверь: прокси на дом; при недоступности — локальный Flask (SHOP_READONLY)."""
from __future__ import annotations

import json
import os

import httpx


HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "content-encoding",
}


def _env_true(name: str, default: str = "0") -> bool:
    return (os.environ.get(name) or default).strip().lower() in ("1", "true", "yes")


class DoorProxyMiddleware:
    """Проксирует на UPSTREAM_ORIGIN; при ошибке отдаёт wrapped Flask app."""

    def __init__(self, app):
        self.app = app
        self.upstream = (os.environ.get("UPSTREAM_ORIGIN") or "http://95.31.41.191").rstrip("/")
        self.upstream_host = (os.environ.get("UPSTREAM_HOST") or "knyajestvo.ru").strip()
        self.connect_timeout = float(os.environ.get("UPSTREAM_CONNECT_TIMEOUT") or "3")
        self.verify_tls = _env_true("UPSTREAM_VERIFY_TLS", "0")
        self.force_local = _env_true("FAILOVER_STUB", "0") or _env_true("DOOR_FORCE_LOCAL", "0")
        self.tg_origin = (os.environ.get("TG_API_ORIGIN") or "https://api.telegram.org").rstrip("/")
        rewrite_from = [
            x.strip()
            for x in (os.environ.get("REWRITE_FROM") or "ff-m1-warchesko.mia0.amvera.tech").split(",")
            if x.strip()
        ]
        self.rewrite_from = rewrite_from
        self.rewrite_to = (os.environ.get("REWRITE_TO") or "ffdoor-warchesko.msk1.amvera.tech").strip()
        self._client = httpx.Client(
            timeout=httpx.Timeout(60.0, connect=self.connect_timeout),
            verify=self.verify_tls,
            follow_redirects=False,
        )
        self._tg = httpx.Client(timeout=httpx.Timeout(120.0, connect=15.0), follow_redirects=False)
        self._last_mode = "door"

    def __call__(self, environ, start_response):
        path = environ.get("PATH_INFO") or "/"
        method = (environ.get("REQUEST_METHOD") or "GET").upper()

        if path == "/healthz" or path.startswith("/healthz/"):
            return self._healthz(environ, start_response)

        if path == "/tg-api" or path.startswith("/tg-api/"):
            return self._tg_relay(environ, start_response, path)

        # Снимок с дома всегда на локальный kdoor (не проксировать на дом)
        if path == "/internal/failover-sync" or path.startswith("/internal/failover-sync/"):
            self._last_mode = "door"
            return self.app(environ, start_response)

        if self.force_local:
            self._last_mode = "readonly"
            return self.app(environ, start_response)

        try:
            return self._proxy(environ, start_response, path, method)
        except Exception:
            self._last_mode = "readonly"
            return self.app(environ, start_response)

    def _read_body(self, environ) -> bytes:
        try:
            length = int(environ.get("CONTENT_LENGTH") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return b""
        return environ["wsgi.input"].read(length)

    def _req_headers(self, environ) -> dict[str, str]:
        out: dict[str, str] = {}
        for key, value in environ.items():
            if not key.startswith("HTTP_"):
                continue
            name = key[5:].replace("_", "-").title()
            if name.lower() in HOP_BY_HOP or name.lower().startswith("x-amvera"):
                continue
            out[name] = value
        ctype = environ.get("CONTENT_TYPE")
        if ctype:
            out["Content-Type"] = ctype
        out["Host"] = self.upstream_host
        out["X-Forwarded-Proto"] = "https"
        out["X-Forwarded-Host"] = self.upstream_host
        return out

    def _proxy(self, environ, start_response, path: str, method: str):
        qs = environ.get("QUERY_STRING") or ""
        target = f"{self.upstream}{path}"
        if qs:
            target = f"{target}?{qs}"
        body = self._read_body(environ)
        upstream = self._client.request(method, target, headers=self._req_headers(environ), content=body or None)
        resp_body = upstream.content
        headers = []
        ct = (upstream.headers.get("content-type") or "").lower()
        text_ct = (
            "text/html",
            "text/css",
            "application/javascript",
            "text/javascript",
            "application/json",
            "text/plain",
            "application/xml",
            "text/xml",
            "image/svg+xml",
        )
        if self.rewrite_from and self.rewrite_to and ct.startswith(text_ct):
            for old in self.rewrite_from:
                resp_body = resp_body.replace(old.encode(), self.rewrite_to.encode())
        for key, value in upstream.headers.multi_items():
            lk = key.lower()
            if lk in HOP_BY_HOP:
                continue
            if self.rewrite_from and self.rewrite_to and lk in ("location", "link", "content-security-policy"):
                for old in self.rewrite_from:
                    value = value.replace(old, self.rewrite_to)
            headers.append((key, value))
        self._last_mode = "door"
        start_response(f"{upstream.status_code} {upstream.reason_phrase}", headers)
        return [resp_body]

    def _tg_relay(self, environ, start_response, path: str):
        suffix = path[len("/tg-api") :].lstrip("/")
        target = f"{self.tg_origin}/{suffix}" if suffix else f"{self.tg_origin}/"
        qs = environ.get("QUERY_STRING") or ""
        if qs:
            target = f"{target}?{qs}"
        method = (environ.get("REQUEST_METHOD") or "GET").upper()
        headers: dict[str, str] = {}
        for key, value in environ.items():
            if not key.startswith("HTTP_"):
                continue
            name = key[5:].replace("_", "-").title()
            lk = name.lower()
            if lk in HOP_BY_HOP or lk.startswith("x-amvera") or lk.startswith("x-forwarded"):
                continue
            headers[name] = value
        ctype = environ.get("CONTENT_TYPE")
        if ctype:
            headers["Content-Type"] = ctype
        try:
            upstream = self._tg.request(
                method, target, headers=headers, content=self._read_body(environ) or None
            )
        except Exception as exc:
            msg = f"tg-api unavailable: {exc}".encode()
            start_response("502 Bad Gateway", [("Content-Type", "text/plain; charset=utf-8")])
            return [msg]
        out_headers = [(k, v) for k, v in upstream.headers.multi_items() if k.lower() not in HOP_BY_HOP]
        start_response(f"{upstream.status_code} {upstream.reason_phrase}", out_headers)
        return [upstream.content]

    def _healthz(self, environ, start_response):
        payload = {
            "ok": True,
            "mode": "readonly" if self.force_local else self._last_mode,
            "upstream": self.upstream,
            "host": self.upstream_host,
            "readonly": _env_true("SHOP_READONLY", "1"),
            "force_local": self.force_local,
        }
        # лёгкая проверка upstream (не блокируем надолго)
        if not self.force_local:
            try:
                r = self._client.get(f"{self.upstream}/", headers={"Host": self.upstream_host})
                payload["upstream_ok"] = r.status_code < 500
            except Exception as exc:
                payload["upstream_ok"] = False
                payload["upstream_error"] = str(exc)[:200]
                payload["mode"] = "readonly"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8")])
        return [body]


def create_door_app():
    """Flask app + door proxy. Для Amvera kdoor."""
    # Локальный fallback всегда readonly-витрина
    os.environ.setdefault("SHOP_READONLY", "1")
    os.environ.setdefault("SHOP_ONLY", "1")
    os.environ.setdefault("DISABLE_SCHEDULER", "1")
    os.environ.setdefault("DISABLE_AI_SEED", "1")
    os.environ.setdefault("TG_USE_POLLING", "0")
    os.environ.setdefault("BEHIND_PROXY", "1")
    # create_app требует SECRET_KEY; для публичной readonly-витрины достаточно стабильного дефолта
    if not (os.environ.get("SECRET_KEY") or "").strip():
        import hashlib

        seed = (os.environ.get("AMVERA") or "kdoor") + (os.environ.get("UPSTREAM_HOST") or "knyajestvo.ru")
        os.environ["SECRET_KEY"] = hashlib.sha256(f"floraflow-door-{seed}".encode()).hexdigest()

    from app import create_app

    flask_app = create_app()
    return DoorProxyMiddleware(flask_app)
