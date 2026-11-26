"""Управление репликами Amvera через MCP (scaleProject)."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
import uuid
from typing import Any


class AmveraError(RuntimeError):
    pass


def _rpc(mcp_url: str, token: str, method: str, params: dict[str, Any] | None = None) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": method,
        "params": params or {},
    }
    req = urllib.request.Request(
        mcp_url.rstrip("/"),
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise AmveraError(f"HTTP {exc.code}: {raw[:600]}") from exc
    except urllib.error.URLError as exc:
        raise AmveraError(str(exc)) from exc

    # Streamable HTTP may return SSE lines: data: {...}
    text = raw.strip()
    if text.startswith("event:") or "data:" in text:
        chunks = []
        for line in text.splitlines():
            if line.startswith("data:"):
                chunks.append(line[5:].strip())
        text = chunks[-1] if chunks else text

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AmveraError(f"bad MCP response: {raw[:400]}") from exc

    if "error" in parsed and parsed["error"]:
        raise AmveraError(str(parsed["error"]))
    return parsed.get("result", parsed)


def scale_project(mcp_url: str, token: str, project_id: str, replicas: int) -> Any:
    """replicas=0 — сон, 1 — запуск."""
    # MCP tools/call
    return _rpc(
        mcp_url,
        token,
        "tools/call",
        {
            "name": "scaleProject",
            "arguments": {
                "projectId": project_id,
                "replicas": int(replicas),
            },
        },
    )


def wake(mcp_url: str, token: str, project_id: str) -> Any:
    return scale_project(mcp_url, token, project_id, 1)


def sleep(mcp_url: str, token: str, project_id: str) -> Any:
    return scale_project(mcp_url, token, project_id, 0)
