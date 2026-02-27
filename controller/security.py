import hmac
import ipaddress
import os
import re
from threading import Lock
from typing import Dict, Optional, Set, Tuple

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


AUTH_TOKEN = os.getenv("AUTH_TOKEN", "").strip()
TRUST_X_FORWARDED_FOR = os.getenv("TRUST_X_FORWARDED_FOR", "0").strip() in (
    "1",
    "true",
    "TRUE",
    "yes",
    "YES",
)
TRUSTED_PROXY_IPS: Set[str] = set(
    item.strip()
    for item in os.getenv("TRUSTED_PROXY_IPS", "127.0.0.1,::1").split(",")
    if item.strip()
)
API_RATE_LIMIT_ENABLED = os.getenv("API_RATE_LIMIT_ENABLED", "0").strip() in (
    "1",
    "true",
    "TRUE",
    "yes",
    "YES",
)
API_RATE_LIMIT_WINDOW_SECONDS = int(_get_int_env("API_RATE_LIMIT_WINDOW_SECONDS", 60))
if API_RATE_LIMIT_WINDOW_SECONDS < 1:
    API_RATE_LIMIT_WINDOW_SECONDS = 1
API_RATE_LIMIT_MAX_REQUESTS = int(_get_int_env("API_RATE_LIMIT_MAX_REQUESTS", 120))
if API_RATE_LIMIT_MAX_REQUESTS < 1:
    API_RATE_LIMIT_MAX_REQUESTS = 1
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMIT_STATE: Dict[str, Tuple[int, int]] = {}
_RATE_LIMIT_LAST_CLEANUP_AT = 0
RATE_LIMIT_STATIC_SEGMENTS: Set[str] = {
    "create",
    "set_speed",
    "set_status",
    "assign_node",
    "unassign_node",
    "stats",
    "sync",
    "tasks",
    "next",
    "report",
    "health",
    "admin",
    "db",
    "integrity",
    "verify_export",
    "export",
    "migrate",
    "security",
    "overview",
    "node_access",
    "status",
    "audit",
}


def verify_admin_authorization(authorization: Optional[str]) -> Optional[JSONResponse]:
    if not AUTH_TOKEN:
        return None
    expected = "Bearer {0}".format(AUTH_TOKEN)
    if not hmac.compare_digest(str(authorization or ""), expected):
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})
    return None


def is_auth_exempt_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    if normalized in ("/health", "/openapi.json", "/docs", "/redoc", "/favicon.ico", "/robots.txt"):
        return True
    if normalized.startswith("/docs/") or normalized.startswith("/redoc/"):
        return True
    # 订阅链接需给客户端直接拉取，保持匿名可访问。
    if normalized.startswith("/sub/"):
        return True
    return False


def build_rate_limit_path_key(path: str) -> str:
    normalized = str(path or "").strip()
    if not normalized:
        normalized = "/"
    if normalized == "/":
        return normalized

    segments = [seg for seg in normalized.split("/") if seg]
    if not segments:
        return "/"

    if segments[0] not in ("users", "nodes"):
        return "/" + "/".join(segments)

    normalized_segments = [segments[0]]
    for index, seg in enumerate(segments[1:], start=1):
        value = str(seg or "").strip()
        if value in RATE_LIMIT_STATIC_SEGMENTS:
            normalized_segments.append(value)
            continue
        if index == 1:
            normalized_segments.append("*")
            continue
        if re.match(r"^[0-9]+$", value):
            normalized_segments.append("*")
            continue
        normalized_segments.append("*")
    return "/" + "/".join(normalized_segments)


def get_rate_limit_identity(request: Request) -> str:
    request_ip = get_request_ip(request)
    if not request_ip:
        request_ip = "unknown"
    path_key = build_rate_limit_path_key(str(request.url.path or "/"))
    return "{0}:{1}".format(request_ip, path_key)


def is_rate_limit_target_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    if normalized.startswith("/admin/"):
        return True
    if normalized == "/users/create":
        return True
    if normalized.startswith("/users/") and (
        normalized.endswith("/set_speed")
        or normalized.endswith("/set_status")
        or normalized.endswith("/assign_node")
        or normalized.endswith("/unassign_node")
    ):
        return True
    if normalized.startswith("/nodes/") and (
        normalized.endswith("/create")
        or normalized.endswith("/tasks/create")
    ):
        return True
    if normalized.startswith("/nodes/") and normalized.count("/") == 2:
        return True
    return False


def check_and_consume_rate_limit(identity: str, now_ts: int) -> Tuple[bool, int]:
    global _RATE_LIMIT_LAST_CLEANUP_AT
    with _RATE_LIMIT_LOCK:
        window_start, count = _RATE_LIMIT_STATE.get(identity, (0, 0))
        if window_start <= 0 or now_ts - window_start >= API_RATE_LIMIT_WINDOW_SECONDS:
            window_start, count = now_ts, 0
        count += 1
        _RATE_LIMIT_STATE[identity] = (window_start, count)

        if now_ts - _RATE_LIMIT_LAST_CLEANUP_AT >= max(60, API_RATE_LIMIT_WINDOW_SECONDS):
            expired_keys = []
            for key, item in _RATE_LIMIT_STATE.items():
                if now_ts - int(item[0]) >= API_RATE_LIMIT_WINDOW_SECONDS:
                    expired_keys.append(key)
            for key in expired_keys:
                _RATE_LIMIT_STATE.pop(key, None)
            _RATE_LIMIT_LAST_CLEANUP_AT = now_ts

    if count > API_RATE_LIMIT_MAX_REQUESTS:
        retry_after = API_RATE_LIMIT_WINDOW_SECONDS - (now_ts - window_start)
        if retry_after < 1:
            retry_after = 1
        return True, retry_after
    return False, 0


def validate_agent_ip(agent_ip: Optional[str]) -> Optional[str]:
    if agent_ip is None:
        return None
    value = str(agent_ip).strip()
    if value == "":
        return ""
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="agent_ip must be a valid IPv4/IPv6 address") from exc
    return value


def get_request_ip(request: Request) -> str:
    # 默认仅信任直连源 IP。仅在开启 TRUST_X_FORWARDED_FOR 且请求来自可信代理时才解析 XFF。
    direct_ip = ""
    if request.client and request.client.host:
        direct_ip = str(request.client.host).strip()
    direct_ip_normalized = direct_ip
    try:
        if direct_ip:
            direct_ip_normalized = str(ipaddress.ip_address(direct_ip))
    except ValueError:
        direct_ip_normalized = direct_ip

    if (
        TRUST_X_FORWARDED_FOR
        and direct_ip
        and (
            direct_ip in TRUSTED_PROXY_IPS
            or direct_ip_normalized in TRUSTED_PROXY_IPS
        )
    ):
        xff = request.headers.get("x-forwarded-for", "").strip()
        if xff:
            first = xff.split(",")[0].strip()
            if first:
                return first
    if direct_ip:
        return direct_ip
    return ""


def verify_node_agent_ip(request: Request, node_code: str, expected_agent_ip: Optional[str]) -> None:
    expected = str(expected_agent_ip or "").strip()
    if not expected:
        return
    try:
        expected_normalized = str(ipaddress.ip_address(expected))
    except ValueError:
        raise HTTPException(status_code=500, detail="node agent_ip config invalid")

    request_ip_raw = get_request_ip(request)
    if not request_ip_raw:
        raise HTTPException(status_code=403, detail="node source ip unavailable")
    try:
        request_normalized = str(ipaddress.ip_address(request_ip_raw))
    except ValueError:
        raise HTTPException(status_code=403, detail="node source ip invalid")

    if request_normalized != expected_normalized:
        raise HTTPException(
            status_code=403,
            detail="node source ip not allowed for {0}".format(node_code),
        )
