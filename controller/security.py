import hmac
import heapq
import ipaddress
import os
import re
from threading import Lock
from typing import Dict, Optional, Set, Tuple

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from controller.settings import ADMIN_API_WHITELIST_ITEMS, API_DOCS_ENABLED


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_csv_set(raw: str, *, lower: bool = False) -> Set[str]:
    values: Set[str] = set()
    for item in str(raw or "").split(","):
        value = str(item or "").strip()
        if not value:
            continue
        if lower:
            value = value.lower()
        values.add(value)
    return values


AUTH_TOKEN = os.getenv("AUTH_TOKEN", "").strip()
ADMIN_AUTH_TOKEN = os.getenv("ADMIN_AUTH_TOKEN", "").strip()
NODE_AUTH_TOKEN = os.getenv("NODE_AUTH_TOKEN", "").strip()
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
API_RATE_LIMIT_TRUSTED_LOOPBACK_ACTORS = _parse_csv_set(
    os.getenv("API_RATE_LIMIT_TRUSTED_LOOPBACK_ACTORS", "sb-bot,sb-admin"),
    lower=True,
)
RATE_LIMIT_STATE_MAX_KEYS = int(_get_int_env("RATE_LIMIT_STATE_MAX_KEYS", 20000))
if RATE_LIMIT_STATE_MAX_KEYS < 100:
    RATE_LIMIT_STATE_MAX_KEYS = 100
UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = int(_get_int_env("UNAUTHORIZED_AUDIT_SAMPLE_SECONDS", 30))
if UNAUTHORIZED_AUDIT_SAMPLE_SECONDS < 0:
    UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = 0
UNAUTHORIZED_AUDIT_STATE_MAX_KEYS = int(_get_int_env("UNAUTHORIZED_AUDIT_STATE_MAX_KEYS", 20000))
if UNAUTHORIZED_AUDIT_STATE_MAX_KEYS < 100:
    UNAUTHORIZED_AUDIT_STATE_MAX_KEYS = 100
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMIT_STATE: Dict[str, Tuple[int, int]] = {}
_RATE_LIMIT_LAST_CLEANUP_AT = 0
_UNAUTH_AUDIT_LOCK = Lock()
_UNAUTH_AUDIT_STATE: Dict[str, Tuple[int, int]] = {}
_UNAUTH_AUDIT_LAST_CLEANUP_AT = 0
RATE_LIMIT_STATIC_SEGMENTS: Set[str] = {
    "create",
    "set_speed",
    "set_limit_mode",
    "set_status",
    "set_profile",
    "renew",
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
_NODE_AGENT_PATH_PATTERNS = (
    re.compile(r"^/nodes/[^/]+/sync$"),
    re.compile(r"^/nodes/[^/]+/tasks/next$"),
    re.compile(r"^/nodes/[^/]+/tasks/[0-9]+/report$"),
    re.compile(r"^/nodes/[^/]+/report_reality$"),
)
_ADMIN_API_WHITELIST_NETWORKS = []
_ADMIN_API_WHITELIST_INVALID_ITEMS = []
for _raw_item in ADMIN_API_WHITELIST_ITEMS:
    _value = str(_raw_item or "").strip()
    if not _value:
        continue
    try:
        _ADMIN_API_WHITELIST_NETWORKS.append(ipaddress.ip_network(_value, strict=False))
    except ValueError:
        _ADMIN_API_WHITELIST_INVALID_ITEMS.append(_value)


def _trim_oldest_state_items(
    state: Dict[str, Tuple[int, int]],
    max_keys: int,
) -> None:
    if max_keys < 1:
        max_keys = 1
    overflow = int(len(state) - max_keys)
    if overflow <= 0:
        return
    oldest_items = heapq.nsmallest(
        overflow,
        state.items(),
        key=lambda item: int((item[1] or (0, 0))[0] or 0),
    )
    for key, _ in oldest_items:
        state.pop(key, None)


def _split_auth_tokens(raw: str) -> list:
    tokens = []
    for item in str(raw or "").split(","):
        token = str(item or "").strip()
        if token:
            tokens.append(token)
    return tokens


def get_admin_auth_tokens() -> list:
    effective = str(ADMIN_AUTH_TOKEN or "").strip()
    if not effective:
        effective = str(AUTH_TOKEN or "").strip()
    if not effective:
        effective = str(NODE_AUTH_TOKEN or "").strip()
    return _split_auth_tokens(effective)


def get_node_auth_tokens() -> list:
    effective = str(NODE_AUTH_TOKEN or "").strip()
    if not effective:
        effective = str(AUTH_TOKEN or "").strip()
    if not effective:
        effective = str(ADMIN_AUTH_TOKEN or "").strip()
    return _split_auth_tokens(effective)


def get_admin_auth_token_source() -> str:
    if _split_auth_tokens(ADMIN_AUTH_TOKEN):
        return "admin_auth_token"
    if _split_auth_tokens(AUTH_TOKEN):
        return "auth_token"
    if _split_auth_tokens(NODE_AUTH_TOKEN):
        return "node_auth_token_fallback"
    return "none"


def get_node_auth_token_source() -> str:
    if _split_auth_tokens(NODE_AUTH_TOKEN):
        return "node_auth_token"
    if _split_auth_tokens(AUTH_TOKEN):
        return "auth_token"
    if _split_auth_tokens(ADMIN_AUTH_TOKEN):
        return "admin_auth_token_fallback"
    return "none"


def is_auth_token_split_active() -> bool:
    admin_tokens = get_admin_auth_tokens()
    node_tokens = get_node_auth_tokens()
    if not admin_tokens or not node_tokens:
        return False
    return set(admin_tokens) != set(node_tokens)


def get_auth_tokens() -> list:
    # Backward-compatible alias for existing call sites/tests.
    return get_admin_auth_tokens()


def verify_authorization_with_tokens(
    authorization: Optional[str],
    tokens: list,
) -> Optional[JSONResponse]:
    if not tokens:
        return None
    auth_value = str(authorization or "")
    for token in tokens:
        expected = "Bearer {0}".format(token)
        if hmac.compare_digest(auth_value, expected):
            return None
    return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})


def verify_admin_authorization(authorization: Optional[str]) -> Optional[JSONResponse]:
    return verify_authorization_with_tokens(authorization, get_admin_auth_tokens())


def verify_node_authorization(authorization: Optional[str]) -> Optional[JSONResponse]:
    return verify_authorization_with_tokens(authorization, get_node_auth_tokens())


def has_any_admin_auth_token() -> bool:
    return bool(get_admin_auth_tokens())


def has_any_node_auth_token() -> bool:
    return bool(get_node_auth_tokens())


def is_node_agent_auth_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    for pattern in _NODE_AGENT_PATH_PATTERNS:
        if pattern.match(normalized):
            return True
    return False


def is_auth_exempt_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    if normalized in ("/health", "/favicon.ico", "/robots.txt"):
        return True
    if API_DOCS_ENABLED and normalized in ("/openapi.json", "/docs", "/redoc"):
        return True
    if API_DOCS_ENABLED and (normalized.startswith("/docs/") or normalized.startswith("/redoc/")):
        return True
    # 订阅链接需给客户端直接拉取，保持匿名可访问。
    if normalized.startswith("/sub/"):
        return True
    return False


def is_admin_api_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    if is_auth_exempt_path(normalized):
        return False
    return not is_node_agent_auth_path(normalized)


def is_admin_api_whitelist_enabled() -> bool:
    return bool(_ADMIN_API_WHITELIST_NETWORKS)


def get_admin_api_whitelist_invalid_items() -> list:
    return list(_ADMIN_API_WHITELIST_INVALID_ITEMS)


def is_request_allowed_by_admin_api_whitelist(request: Request) -> bool:
    if not is_admin_api_whitelist_enabled():
        return True

    request_ip_raw = get_request_ip(request)
    if not request_ip_raw:
        return False
    try:
        request_ip_obj = ipaddress.ip_address(str(request_ip_raw))
    except ValueError:
        return False

    # 运维脚本/bot 的本机调用默认放行，避免误锁本机管理能力。
    if request_ip_obj.is_loopback:
        return True

    for network in _ADMIN_API_WHITELIST_NETWORKS:
        if request_ip_obj in network:
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


def get_rate_limit_auth_bucket(path: str, authorization: Optional[str]) -> str:
    normalized_path = str(path or "").strip() or "/"
    if is_node_agent_auth_path(normalized_path):
        if has_any_node_auth_token():
            return "auth" if verify_node_authorization(authorization) is None else "anon"
        return "open"
    if is_admin_api_path(normalized_path):
        if has_any_admin_auth_token():
            return "auth" if verify_admin_authorization(authorization) is None else "anon"
        return "open"
    return "open"


def get_rate_limit_identity(request: Request, auth_bucket: str = "anon") -> str:
    request_ip = get_request_ip(request)
    if not request_ip:
        request_ip = "unknown"
    path_key = build_rate_limit_path_key(str(request.url.path or "/"))
    bucket_value = str(auth_bucket or "").strip() or "anon"
    return "{0}:{1}:{2}".format(request_ip, path_key, bucket_value)


def should_bypass_rate_limit_for_request(
    request: Request,
    auth_bucket: str = "anon",
) -> bool:
    if str(auth_bucket or "").strip() != "auth":
        return False
    if not API_RATE_LIMIT_TRUSTED_LOOPBACK_ACTORS:
        return False
    actor = str(request.headers.get("X-Actor", "") or "").strip().lower()
    if not actor or actor not in API_RATE_LIMIT_TRUSTED_LOOPBACK_ACTORS:
        return False
    request_ip_raw = get_request_ip(request)
    if not request_ip_raw:
        return False
    try:
        request_ip = ipaddress.ip_address(str(request_ip_raw))
    except ValueError:
        return False
    return bool(request_ip.is_loopback)


def is_rate_limit_target_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    if normalized.startswith("/admin/"):
        return True
    if normalized in ("/nodes", "/users"):
        return True
    if normalized == "/users/create":
        return True
    if normalized.startswith("/users/") and (
        normalized.endswith("/set_speed")
        or normalized.endswith("/set_limit_mode")
        or normalized.endswith("/set_status")
        or normalized.endswith("/set_profile")
        or normalized.endswith("/renew")
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
        if len(_RATE_LIMIT_STATE) > RATE_LIMIT_STATE_MAX_KEYS:
            _trim_oldest_state_items(_RATE_LIMIT_STATE, RATE_LIMIT_STATE_MAX_KEYS)

    if count > API_RATE_LIMIT_MAX_REQUESTS:
        retry_after = API_RATE_LIMIT_WINDOW_SECONDS - (now_ts - window_start)
        if retry_after < 1:
            retry_after = 1
        return True, retry_after
    return False, 0


def build_unauthorized_audit_key(source_ip: str, path: str, method: str) -> str:
    return "{0}:{1}:{2}".format(
        str(source_ip or "").strip() or "-",
        str(path or "").strip() or "/",
        str(method or "").strip().upper() or "GET",
    )


def should_write_unauthorized_audit(key: str, now_ts: int) -> Tuple[bool, int]:
    global _UNAUTH_AUDIT_LAST_CLEANUP_AT
    sample_seconds = int(UNAUTHORIZED_AUDIT_SAMPLE_SECONDS)
    if sample_seconds <= 0:
        return True, 0

    with _UNAUTH_AUDIT_LOCK:
        last_ts, dropped = _UNAUTH_AUDIT_STATE.get(key, (0, 0))
        if last_ts <= 0 or now_ts - int(last_ts) >= sample_seconds:
            dropped_count = int(dropped or 0)
            _UNAUTH_AUDIT_STATE[key] = (int(now_ts), 0)
            if now_ts - _UNAUTH_AUDIT_LAST_CLEANUP_AT >= max(60, sample_seconds * 2):
                expire_before = int(now_ts) - sample_seconds * 10
                expired_keys = []
                for state_key, value in _UNAUTH_AUDIT_STATE.items():
                    item_ts = int(value[0] or 0)
                    if item_ts > 0 and item_ts < expire_before:
                        expired_keys.append(state_key)
                for expired_key in expired_keys:
                    _UNAUTH_AUDIT_STATE.pop(expired_key, None)
                _UNAUTH_AUDIT_LAST_CLEANUP_AT = int(now_ts)
            if len(_UNAUTH_AUDIT_STATE) > UNAUTHORIZED_AUDIT_STATE_MAX_KEYS:
                _trim_oldest_state_items(
                    _UNAUTH_AUDIT_STATE, UNAUTHORIZED_AUDIT_STATE_MAX_KEYS
                )
            return True, dropped_count

        _UNAUTH_AUDIT_STATE[key] = (int(last_ts), int(dropped or 0) + 1)
        if len(_UNAUTH_AUDIT_STATE) > UNAUTHORIZED_AUDIT_STATE_MAX_KEYS:
            _trim_oldest_state_items(_UNAUTH_AUDIT_STATE, UNAUTHORIZED_AUDIT_STATE_MAX_KEYS)
        return False, int(dropped or 0) + 1


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
