import base64
import hashlib
import hmac
import ipaddress
import json
import os
import sqlite3
import tarfile
import time
import uuid
from pathlib import Path
import shutil
import tempfile
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "app.db"
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "").strip()


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


TRUST_X_FORWARDED_FOR = os.getenv("TRUST_X_FORWARDED_FOR", "0").strip() in (
    "1",
    "true",
    "TRUE",
    "yes",
    "YES",
)
TRUSTED_PROXY_IPS = set(
    item.strip()
    for item in os.getenv("TRUSTED_PROXY_IPS", "127.0.0.1,::1").split(",")
    if item.strip()
)
NODE_TASK_RUNNING_TIMEOUT_SECONDS = int(
    _get_int_env("NODE_TASK_RUNNING_TIMEOUT", 120)
)
if NODE_TASK_RUNNING_TIMEOUT_SECONDS < 30:
    NODE_TASK_RUNNING_TIMEOUT_SECONDS = 30
NODE_TASK_RETENTION_SECONDS = int(_get_int_env("NODE_TASK_RETENTION_SECONDS", 7 * 86400))
if NODE_TASK_RETENTION_SECONDS < 3600:
    NODE_TASK_RETENTION_SECONDS = 3600
SUB_LINK_SIGN_KEY = os.getenv("SUB_LINK_SIGN_KEY", "").strip()
SUB_LINK_REQUIRE_SIGNATURE = os.getenv("SUB_LINK_REQUIRE_SIGNATURE", "0").strip() in (
    "1",
    "true",
    "TRUE",
    "yes",
    "YES",
)
SUB_LINK_DEFAULT_TTL_SECONDS = int(_get_int_env("SUB_LINK_DEFAULT_TTL_SECONDS", 7 * 86400))
if SUB_LINK_DEFAULT_TTL_SECONDS < 60:
    SUB_LINK_DEFAULT_TTL_SECONDS = 60
if SUB_LINK_DEFAULT_TTL_SECONDS > 30 * 86400:
    SUB_LINK_DEFAULT_TTL_SECONDS = 30 * 86400
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

app = FastAPI()


def verify_admin_authorization(authorization: Optional[str]) -> Optional[JSONResponse]:
    if not AUTH_TOKEN:
        return None
    expected = "Bearer {0}".format(AUTH_TOKEN)
    if not hmac.compare_digest(str(authorization or ""), expected):
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})
    return None


def is_auth_exempt_path(path: str) -> bool:
    normalized = str(path or "").strip() or "/"
    if normalized in ("/health", "/openapi.json", "/docs", "/redoc"):
        return True
    if normalized.startswith("/docs/") or normalized.startswith("/redoc/"):
        return True
    # 订阅链接需给客户端直接拉取，保持匿名可访问。
    if normalized.startswith("/sub/"):
        return True
    return False


def get_rate_limit_identity(request: Request) -> str:
    request_ip = ""
    if request.client and request.client.host:
        request_ip = str(request.client.host).strip()
    if not request_ip:
        request_ip = "unknown"
    path = str(request.url.path or "/")
    return "{0}:{1}".format(request_ip, path)


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


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if not AUTH_TOKEN:
        return await call_next(request)
    if is_auth_exempt_path(request.url.path):
        return await call_next(request)

    authorization = request.headers.get("Authorization")
    expected = "Bearer {0}".format(AUTH_TOKEN)
    if not hmac.compare_digest(str(authorization or ""), expected):
        try:
            with get_connection() as conn:
                write_audit_log(
                    conn,
                    action="auth.unauthorized",
                    resource_type="http",
                    resource_id=str(request.url.path or "/"),
                    detail={"method": request.method},
                    actor=get_request_actor(request),
                    source_ip=get_source_ip_for_audit(request),
                )
                conn.commit()
        except Exception:
            pass
        return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})
    return await call_next(request)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if not API_RATE_LIMIT_ENABLED:
        return await call_next(request)
    if not is_rate_limit_target_path(request.url.path):
        return await call_next(request)

    now_ts = int(time.time())
    identity = get_rate_limit_identity(request)
    limited, retry_after = check_and_consume_rate_limit(identity, now_ts)
    if limited:
        return JSONResponse(
            status_code=429,
            content={"ok": False, "error": "rate_limited", "retry_after": retry_after},
            headers={"Retry-After": str(retry_after)},
        )
    return await call_next(request)


class CreateUserRequest(BaseModel):
    display_name: str = Field(min_length=1)
    tuic_port: int = Field(ge=1, le=65535)
    speed_mbps: int = Field(gt=0)
    valid_days: int = Field(gt=0)
    note: str = ""


class CreateNodeRequest(BaseModel):
    node_code: str = Field(min_length=1)
    region: str = ""
    host: str = Field(min_length=1)
    agent_ip: Optional[str] = None
    reality_server_name: Optional[str] = None
    tuic_server_name: Optional[str] = None
    tuic_listen_port: Optional[int] = Field(default=None, ge=1, le=65535)
    tuic_port_start: int = Field(ge=1, le=65535)
    tuic_port_end: int = Field(ge=1, le=65535)
    enabled: int = 1
    supports_reality: Optional[int] = None
    supports_tuic: Optional[int] = None
    monitor_enabled: Optional[int] = None
    note: str = ""


class AssignNodeRequest(BaseModel):
    node_code: str = Field(min_length=1)


class SetUserSpeedRequest(BaseModel):
    speed_mbps: int = Field(ge=1, le=10000)


class SetUserStatusRequest(BaseModel):
    status: str = Field(min_length=1)


class CreateNodeTaskRequest(BaseModel):
    task_type: str = Field(min_length=1)
    payload: Optional[Dict[str, Any]] = None
    max_attempts: Optional[int] = Field(default=None, ge=1, le=3)


class ReportNodeTaskRequest(BaseModel):
    status: str = Field(min_length=1)
    result: str = ""


class UpdateNodeRequest(BaseModel):
    region: Optional[str] = None
    host: Optional[str] = None
    agent_ip: Optional[str] = None
    reality_server_name: Optional[str] = None
    tuic_server_name: Optional[str] = None
    tuic_listen_port: Optional[int] = Field(default=None, ge=1, le=65535)
    reality_private_key: Optional[str] = None
    reality_public_key: Optional[str] = None
    reality_short_id: Optional[str] = None
    tuic_port_start: Optional[int] = Field(default=None, ge=1, le=65535)
    tuic_port_end: Optional[int] = Field(default=None, ge=1, le=65535)
    enabled: Optional[int] = None
    supports_reality: Optional[int] = None
    supports_tuic: Optional[int] = None
    monitor_enabled: Optional[int] = None
    note: Optional[str] = None


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        # WAL + NORMAL 同步可显著降低读写阻塞，适合本项目这种高读低写场景。
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA wal_autocheckpoint = 1000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_code TEXT UNIQUE,
                display_name TEXT,
                status TEXT,
                created_at INTEGER,
                expire_at INTEGER,
                grace_days INTEGER,
                speed_mbps INTEGER,
                limit_mode TEXT,
                mark INTEGER UNIQUE,
                vless_uuid TEXT,
                tuic_secret TEXT,
                tuic_port INTEGER UNIQUE,
                note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes(
                node_code TEXT PRIMARY KEY,
                region TEXT,
                host TEXT,
                agent_ip TEXT,
                reality_server_name TEXT,
                tuic_server_name TEXT,
                tuic_listen_port INTEGER,
                monitor_enabled INTEGER,
                last_seen_at INTEGER,
                reality_private_key TEXT,
                reality_public_key TEXT,
                reality_short_id TEXT,
                tuic_port_start INTEGER,
                tuic_port_end INTEGER,
                enabled INTEGER,
                supports_reality INTEGER,
                supports_tuic INTEGER,
                note TEXT
            )
            """
        )
        node_columns = conn.execute("PRAGMA table_info(nodes)").fetchall()
        node_column_names = set(row["name"] for row in node_columns)
        if "reality_server_name" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_server_name TEXT")
        if "agent_ip" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN agent_ip TEXT")
        if "tuic_server_name" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN tuic_server_name TEXT")
        if "tuic_listen_port" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN tuic_listen_port INTEGER")
        if "monitor_enabled" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN monitor_enabled INTEGER")
        if "last_seen_at" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN last_seen_at INTEGER")
        if "reality_private_key" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_private_key TEXT")
        if "reality_public_key" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_public_key TEXT")
        if "reality_short_id" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_short_id TEXT")
        if "supports_reality" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN supports_reality INTEGER")
        if "supports_tuic" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN supports_tuic INTEGER")
        conn.execute("UPDATE nodes SET supports_reality = 1 WHERE supports_reality IS NULL")
        conn.execute("UPDATE nodes SET supports_tuic = 1 WHERE supports_tuic IS NULL")
        conn.execute("UPDATE nodes SET monitor_enabled = 0 WHERE monitor_enabled IS NULL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_nodes(
                user_code TEXT,
                node_code TEXT,
                tuic_port INTEGER,
                created_at INTEGER,
                PRIMARY KEY (user_code, node_code),
                FOREIGN KEY (user_code) REFERENCES users(user_code),
                FOREIGN KEY (node_code) REFERENCES nodes(node_code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS node_tasks(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_code TEXT NOT NULL,
                task_type TEXT NOT NULL,
                payload_json TEXT,
                status TEXT NOT NULL,
                attempts INTEGER,
                max_attempts INTEGER,
                created_at INTEGER,
                updated_at INTEGER,
                result_text TEXT,
                FOREIGN KEY (node_code) REFERENCES nodes(node_code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT,
                action TEXT NOT NULL,
                resource_type TEXT,
                resource_id TEXT,
                detail TEXT,
                source_ip TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        node_task_columns = conn.execute("PRAGMA table_info(node_tasks)").fetchall()
        node_task_column_names = set(row["name"] for row in node_task_columns)
        if "attempts" not in node_task_column_names:
            conn.execute("ALTER TABLE node_tasks ADD COLUMN attempts INTEGER")
        if "max_attempts" not in node_task_column_names:
            conn.execute("ALTER TABLE node_tasks ADD COLUMN max_attempts INTEGER")
        conn.execute("UPDATE node_tasks SET attempts = 0 WHERE attempts IS NULL")
        conn.execute("UPDATE node_tasks SET max_attempts = 1 WHERE max_attempts IS NULL")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_node_tasks_node_status_id
            ON node_tasks(node_code, status, id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_nodes_node_code
            ON user_nodes(node_code)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
            ON audit_logs(created_at DESC)
            """
        )
        conn.commit()


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


def get_request_actor(request: Optional[Request]) -> str:
    if request is None:
        return ""
    actor = str(request.headers.get("X-Actor", "") or "").strip()
    if not actor:
        return ""
    if len(actor) > 120:
        actor = actor[:120]
    return actor


def get_source_ip_for_audit(request: Optional[Request]) -> str:
    if request is None:
        return ""
    return get_request_ip(request)


def normalize_audit_detail(detail: Any, max_length: int = 1200) -> str:
    if isinstance(detail, str):
        text = detail.strip()
    else:
        try:
            text = json.dumps(detail, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            text = str(detail)
    if len(text) > max_length:
        text = text[:max_length] + "...(truncated)"
    return text


def write_audit_log(
    conn: sqlite3.Connection,
    action: str,
    resource_type: str = "",
    resource_id: str = "",
    detail: Any = "",
    actor: str = "",
    source_ip: str = "",
    created_at: int = 0,
) -> None:
    action_text = str(action or "").strip()
    if not action_text:
        return
    ts = int(created_at or time.time())
    conn.execute(
        """
        INSERT INTO audit_logs(actor, action, resource_type, resource_id, detail, source_ip, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(actor or "").strip(),
            action_text,
            str(resource_type or "").strip(),
            str(resource_id or "").strip(),
            normalize_audit_detail(detail),
            str(source_ip or "").strip(),
            ts,
        ),
    )


ALLOWED_NODE_TASK_TYPES = {
    "restart_singbox",
    "status_singbox",
    "status_agent",
    "logs_singbox",
    "logs_agent",
    "update_sync",
    "config_set",
}


def parse_task_payload(payload_json: Optional[str]) -> Dict[str, Any]:
    raw = str(payload_json or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def build_task_row_dict(row: sqlite3.Row) -> Dict[str, Any]:
    attempts = int(row["attempts"] or 0)
    max_attempts = int(row["max_attempts"] or 1)
    if max_attempts < 1:
        max_attempts = 1
    return {
        "id": int(row["id"]),
        "node_code": str(row["node_code"]),
        "task_type": str(row["task_type"]),
        "payload": parse_task_payload(row["payload_json"]),
        "status": str(row["status"]),
        "attempts": attempts,
        "max_attempts": max_attempts,
        "created_at": int(row["created_at"] or 0),
        "updated_at": int(row["updated_at"] or 0),
        "result_text": str(row["result_text"] or ""),
    }


def append_task_result(existing: str, extra_line: str) -> str:
    base = str(existing or "").strip()
    line = str(extra_line or "").strip()
    if not line:
        return base
    if not base:
        return line
    return "{0}\n{1}".format(base, line)


def run_node_task_housekeeping(
    conn: sqlite3.Connection,
    now_ts: int,
    node_code: Optional[str] = None,
) -> Dict[str, int]:
    timeout_before = now_ts - NODE_TASK_RUNNING_TIMEOUT_SECONDS
    params: List[Union[str, int]] = [timeout_before]
    node_filter_sql = ""
    if node_code:
        node_filter_sql = " AND node_code = ?"
        params.append(node_code)

    stale_rows = conn.execute(
        """
        SELECT id, attempts, max_attempts, result_text
        FROM node_tasks
        WHERE status = 'running' AND updated_at > 0 AND updated_at <= ?
        {0}
        """.format(node_filter_sql),
        tuple(params),
    ).fetchall()

    retried_count = 0
    timeout_count = 0
    for row in stale_rows:
        task_id = int(row["id"])
        attempts = int(row["attempts"] or 0)
        max_attempts = int(row["max_attempts"] or 1)
        if max_attempts < 1:
            max_attempts = 1
        stale_note = "[controller] task timed out after {0}s".format(
            NODE_TASK_RUNNING_TIMEOUT_SECONDS
        )
        next_status = "timeout"
        if attempts < max_attempts:
            next_status = "pending"

        next_result = append_task_result(str(row["result_text"] or ""), stale_note)
        conn.execute(
            """
            UPDATE node_tasks
            SET status = ?, updated_at = ?, result_text = ?
            WHERE id = ?
            """,
            (next_status, now_ts, next_result, task_id),
        )
        if next_status == "pending":
            retried_count += 1
        else:
            timeout_count += 1

    exhausted_params: List[Union[str, int]] = []
    exhausted_filter_sql = ""
    if node_code:
        exhausted_filter_sql = " AND node_code = ?"
        exhausted_params.append(node_code)
    exhausted_rows = conn.execute(
        """
        SELECT id, result_text
        FROM node_tasks
        WHERE status = 'pending' AND attempts >= max_attempts
        {0}
        """.format(exhausted_filter_sql),
        tuple(exhausted_params),
    ).fetchall()
    exhausted_count = 0
    for row in exhausted_rows:
        exhausted_note = "[controller] retries exhausted"
        next_result = append_task_result(str(row["result_text"] or ""), exhausted_note)
        conn.execute(
            """
            UPDATE node_tasks
            SET status = 'failed', updated_at = ?, result_text = ?
            WHERE id = ?
            """,
            (now_ts, next_result, int(row["id"])),
        )
        exhausted_count += 1

    retention_before = now_ts - NODE_TASK_RETENTION_SECONDS
    delete_params: List[Union[str, int]] = [retention_before]
    delete_node_filter_sql = ""
    if node_code:
        delete_node_filter_sql = " AND node_code = ?"
        delete_params.append(node_code)
    delete_cursor = conn.execute(
        """
        DELETE FROM node_tasks
        WHERE status IN ('success', 'failed', 'timeout')
          AND updated_at > 0
          AND updated_at <= ?
          {0}
        """.format(delete_node_filter_sql),
        tuple(delete_params),
    )
    deleted_count = int(delete_cursor.rowcount or 0)
    if (
        retried_count > 0
        or timeout_count > 0
        or exhausted_count > 0
        or deleted_count > 0
    ):
        conn.commit()
    return {
        "retried_count": retried_count,
        "timeout_count": timeout_count,
        "exhausted_count": exhausted_count,
        "deleted_count": deleted_count,
    }


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post(
    "/admin/backup",
    summary="Create controller backup",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
    response_model=None,
)
def create_backup(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    created_at = int(time.time())
    data_dir = BASE_DIR / "data"
    backup_dir = Path("/var/backups/sb-controller")
    backup_name = time.strftime("backup-%Y%m%d-%H%M%S", time.localtime(created_at)) + ".tar.gz"
    backup_path = backup_dir / backup_name

    if not data_dir.exists():
        raise HTTPException(status_code=500, detail="Data directory not found")

    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(backup_path, "w:gz") as archive:
            archive.add(data_dir, arcname="data")
        size_bytes = int(backup_path.stat().st_size)
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Backup failed: {0}".format(exc)) from exc

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.backup.create",
            resource_type="backup",
            resource_id=backup_name,
            detail={"path": str(backup_path), "size_bytes": size_bytes},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=created_at,
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(backup_path),
        "size_bytes": size_bytes,
        "created_at": created_at,
    }


@app.post(
    "/admin/migrate/export",
    summary="Create migration export package",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
    response_model=None,
)
def create_migrate_export(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    created_at = int(time.time())
    migrate_dir = Path("/var/backups/sb-migrate")
    backup_name = time.strftime("sb-migrate-%Y%m%d-%H%M%S", time.localtime(created_at)) + ".tar.gz"
    backup_path = migrate_dir / backup_name

    stage_dir = Path(tempfile.mkdtemp(prefix="sb-migrate-stage-"))
    try:
        project_stage = stage_dir / "sb-bot-panel"
        project_stage.mkdir(parents=True, exist_ok=True)

        data_dir = BASE_DIR / "data"
        env_file = BASE_DIR / ".env"
        scripts_dir = BASE_DIR / "scripts"
        if data_dir.exists():
            shutil.copytree(data_dir, project_stage / "data", dirs_exist_ok=True)
        if env_file.exists():
            shutil.copy2(env_file, project_stage / ".env")
        if scripts_dir.exists():
            shutil.copytree(scripts_dir, project_stage / "scripts", dirs_exist_ok=True)

        systemd_stage = stage_dir / "systemd"
        systemd_stage.mkdir(parents=True, exist_ok=True)
        for service_name in ("sb-controller.service", "sb-bot.service"):
            service_path = Path("/etc/systemd/system") / service_name
            if service_path.exists():
                shutil.copy2(service_path, systemd_stage / service_name)

        migrate_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(backup_path, "w:gz") as archive:
            archive.add(project_stage, arcname="sb-bot-panel")
            if any(systemd_stage.iterdir()):
                archive.add(systemd_stage, arcname="systemd")
        size_bytes = int(backup_path.stat().st_size)
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Migrate export failed: {0}".format(exc)) from exc
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.migrate.export",
            resource_type="migrate",
            resource_id=backup_name,
            detail={"path": str(backup_path), "size_bytes": size_bytes},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=created_at,
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(backup_path),
        "size_bytes": size_bytes,
        "created_at": created_at,
    }


@app.get(
    "/admin/node_access/status",
    summary="Node access control status",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
    response_model=None,
)
def get_node_access_status(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[int, str, List]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT node_code, agent_ip, enabled
            FROM nodes
            ORDER BY node_code
            """
        ).fetchall()

    locked_nodes = []
    unlocked_nodes = []
    for row in rows:
        node_code = str(row["node_code"])
        agent_ip = str(row["agent_ip"] or "").strip()
        item = {
            "node_code": node_code,
            "agent_ip": agent_ip if agent_ip else "",
            "enabled": int(row["enabled"] or 0),
        }
        if agent_ip:
            locked_nodes.append(item)
        else:
            unlocked_nodes.append(item)

    return {
        "total_nodes": len(rows),
        "locked_nodes": len(locked_nodes),
        "unlocked_nodes": len(unlocked_nodes),
        "locked_items": locked_nodes,
        "unlocked_items": unlocked_nodes,
        "hint": "建议每个节点设置 agent_ip，并在防火墙中只放行节点IP到 controller 端口。",
    }


@app.get(
    "/admin/audit",
    summary="List audit logs",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
    response_model=None,
)
def list_admin_audit_logs(
    limit: int = 50,
    action: str = "",
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[List[Dict[str, Union[int, str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200
    action_value = str(action or "").strip()
    with get_connection() as conn:
        if action_value:
            rows = conn.execute(
                """
                SELECT id, actor, action, resource_type, resource_id, detail, source_ip, created_at
                FROM audit_logs
                WHERE action = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (action_value, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, actor, action, resource_type, resource_id, detail, source_ip, created_at
                FROM audit_logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]


@app.post("/users/create")
def create_user(payload: CreateUserRequest, request: Request) -> Dict[str, Union[int, str]]:
    now = int(time.time())

    with get_connection() as conn:
        row = conn.execute("SELECT COALESCE(MAX(mark), 1000) + 1 AS next_mark FROM users").fetchone()
        mark = int(row["next_mark"])
        user_code = f"u{mark}"
        vless_uuid = str(uuid.uuid4())
        tuic_secret = str(uuid.uuid4())
        expire_at = now + payload.valid_days * 86400

        try:
            conn.execute(
                """
                INSERT INTO users(
                    user_code,
                    display_name,
                    status,
                    created_at,
                    expire_at,
                    grace_days,
                    speed_mbps,
                    limit_mode,
                    mark,
                    vless_uuid,
                    tuic_secret,
                    tuic_port,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_code,
                    payload.display_name,
                    "active",
                    now,
                    expire_at,
                    3,
                    payload.speed_mbps,
                    "tc",
                    mark,
                    vless_uuid,
                    tuic_secret,
                    payload.tuic_port,
                    payload.note,
                ),
            )
            write_audit_log(
                conn,
                action="user.create",
                resource_type="user",
                resource_id=user_code,
                detail={
                    "display_name": payload.display_name,
                    "speed_mbps": payload.speed_mbps,
                    "valid_days": payload.valid_days,
                },
                actor=get_request_actor(request),
                source_ip=get_source_ip_for_audit(request),
                created_at=now,
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="Conflict: duplicate unique field") from exc

    return {
        "user_code": user_code,
        "mark": mark,
        "vless_uuid": vless_uuid,
        "tuic_secret": tuic_secret,
        "tuic_port": payload.tuic_port,
        "speed_mbps": payload.speed_mbps,
        "expire_at": expire_at,
    }


@app.post("/users/{user_code}/set_speed")
def set_user_speed(
    user_code: str, payload: SetUserSpeedRequest, request: Request
) -> Dict[str, Union[bool, int, str]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        conn.execute(
            "UPDATE users SET speed_mbps = ? WHERE user_code = ?",
            (payload.speed_mbps, user_code),
        )
        write_audit_log(
            conn,
            action="user.set_speed",
            resource_type="user",
            resource_id=user_code,
            detail={"speed_mbps": payload.speed_mbps},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "speed_mbps": payload.speed_mbps}


@app.post("/users/{user_code}/set_status")
def set_user_status(
    user_code: str, payload: SetUserStatusRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    status_value = str(payload.status or "").strip().lower()
    if status_value not in ("active", "disabled"):
        raise HTTPException(status_code=400, detail="status must be active or disabled")

    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        conn.execute(
            "UPDATE users SET status = ? WHERE user_code = ?",
            (status_value, user_code),
        )
        write_audit_log(
            conn,
            action="user.set_status",
            resource_type="user",
            resource_id=user_code,
            detail={"status": status_value},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "status": status_value}


@app.delete("/users/{user_code}")
def delete_user(user_code: str, request: Request) -> Dict[str, Union[bool, str]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        binding_row = conn.execute(
            "SELECT 1 FROM user_nodes WHERE user_code = ? LIMIT 1",
            (user_code,),
        ).fetchone()
        if binding_row is not None:
            raise HTTPException(status_code=400, detail="该用户仍有节点绑定，请先解绑后再删除")

        conn.execute("DELETE FROM users WHERE user_code = ?", (user_code,))
        write_audit_log(
            conn,
            action="user.delete",
            resource_type="user",
            resource_id=user_code,
            detail={"ok": True},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code}


@app.post("/nodes/create")
def create_node(payload: CreateNodeRequest, request: Request) -> Dict[str, Union[int, str, None]]:
    if payload.tuic_port_start > payload.tuic_port_end:
        raise HTTPException(status_code=400, detail="Invalid port range: start must be <= end")
    if payload.enabled not in (0, 1):
        raise HTTPException(status_code=400, detail="enabled must be 0 or 1")
    supports_reality = 1 if payload.supports_reality is None else payload.supports_reality
    supports_tuic = 1 if payload.supports_tuic is None else payload.supports_tuic
    if supports_reality not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_reality must be 0 or 1")
    if supports_tuic not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_tuic must be 0 or 1")
    monitor_enabled = 0 if payload.monitor_enabled is None else payload.monitor_enabled
    if monitor_enabled not in (0, 1):
        raise HTTPException(status_code=400, detail="monitor_enabled must be 0 or 1")
    agent_ip = validate_agent_ip(payload.agent_ip)

    with get_connection() as conn:
        try:
            conn.execute(
                """
                INSERT INTO nodes(
                    node_code,
                    region,
                    host,
                    agent_ip,
                    reality_server_name,
                    tuic_server_name,
                    tuic_listen_port,
                    monitor_enabled,
                    tuic_port_start,
                    tuic_port_end,
                    enabled,
                    supports_reality,
                    supports_tuic,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.node_code,
                    payload.region,
                    payload.host,
                    agent_ip,
                    payload.reality_server_name,
                    payload.tuic_server_name,
                    payload.tuic_listen_port,
                    monitor_enabled,
                    payload.tuic_port_start,
                    payload.tuic_port_end,
                    payload.enabled,
                    supports_reality,
                    supports_tuic,
                    payload.note,
                ),
            )
            write_audit_log(
                conn,
                action="node.create",
                resource_type="node",
                resource_id=payload.node_code,
                detail={
                    "region": payload.region,
                    "host": payload.host,
                    "enabled": payload.enabled,
                    "supports_reality": supports_reality,
                    "supports_tuic": supports_tuic,
                },
                actor=get_request_actor(request),
                source_ip=get_source_ip_for_audit(request),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="node_code already exists") from exc

    return {
        "node_code": payload.node_code,
        "region": payload.region,
        "host": payload.host,
        "agent_ip": agent_ip,
        "reality_server_name": payload.reality_server_name,
        "tuic_server_name": payload.tuic_server_name,
        "tuic_listen_port": payload.tuic_listen_port,
        "monitor_enabled": monitor_enabled,
        "tuic_port_start": payload.tuic_port_start,
        "tuic_port_end": payload.tuic_port_end,
        "enabled": payload.enabled,
        "supports_reality": supports_reality,
        "supports_tuic": supports_tuic,
        "note": payload.note,
    }


@app.get("/nodes")
def list_nodes() -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            ORDER BY node_code
            """
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/nodes/{node_code}")
def get_node(node_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Node not found")
    return dict(row)


@app.get("/nodes/{node_code}/stats")
def get_node_stats(node_code: str) -> Dict[str, Union[int, str]]:
    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        count_row = conn.execute(
            "SELECT COUNT(*) AS bound_users FROM user_nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
    return {"node_code": node_code, "bound_users": int(count_row["bound_users"])}


@app.post("/nodes/{node_code}/tasks/create", response_model=None)
def create_node_task(
    node_code: str,
    payload: CreateNodeTaskRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    task_type = str(payload.task_type or "").strip()
    if task_type not in ALLOWED_NODE_TASK_TYPES:
        raise HTTPException(status_code=400, detail="unsupported task_type")
    payload_obj = payload.payload if isinstance(payload.payload, dict) else {}
    payload_json = json.dumps(payload_obj, ensure_ascii=False)
    max_attempts = int(payload.max_attempts or 1)
    if max_attempts < 1:
        max_attempts = 1
    if max_attempts > 3:
        max_attempts = 3
    now_ts = int(time.time())

    with get_connection() as conn:
        run_node_task_housekeeping(conn, now_ts)
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        cursor = conn.execute(
            """
            INSERT INTO node_tasks(
                node_code,
                task_type,
                payload_json,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            )
            VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, '')
            """,
            (node_code, task_type, payload_json, max_attempts, now_ts, now_ts),
        )
        task_id = int(cursor.lastrowid or 0)
        write_audit_log(
            conn,
            action="node.task.create",
            resource_type="node_task",
            resource_id=str(task_id),
            detail={
                "node_code": node_code,
                "task_type": task_type,
                "max_attempts": max_attempts,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

        created_row = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE id = ?
            """,
            (task_id,),
        ).fetchone()
    if created_row is None:
        raise HTTPException(status_code=500, detail="create task failed")
    return build_task_row_dict(created_row)


@app.get("/nodes/{node_code}/tasks", response_model=None)
def list_node_tasks(
    node_code: str,
    limit: int = 20,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[List[Dict[str, Any]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    with get_connection() as conn:
        run_node_task_housekeeping(conn, int(time.time()), node_code=node_code)
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        rows = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE node_code = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (node_code, limit),
        ).fetchall()
    return [build_task_row_dict(row) for row in rows]


@app.post("/nodes/{node_code}/tasks/next", response_model=None)
def get_next_node_task(
    node_code: str,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    now_ts = int(time.time())
    with get_connection() as conn:
        run_node_task_housekeeping(conn, now_ts, node_code=node_code)
        node_row = conn.execute(
            "SELECT node_code, agent_ip FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])

        row = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE node_code = ? AND status = 'pending' AND attempts < max_attempts
            ORDER BY id ASC
            LIMIT 1
            """,
            (node_code,),
        ).fetchone()
        if row is None:
            return {"ok": True, "task": None}

        cursor = conn.execute(
            """
            UPDATE node_tasks
            SET status = 'running', attempts = attempts + 1, updated_at = ?
            WHERE id = ? AND status = 'pending' AND attempts < max_attempts
            """,
            (now_ts, int(row["id"])),
        )
        if int(cursor.rowcount or 0) <= 0:
            return {"ok": True, "task": None}
        conn.commit()

        running_row = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE id = ?
            """,
            (int(row["id"]),),
        ).fetchone()

    if running_row is None:
        return {"ok": True, "task": None}
    return {"ok": True, "task": build_task_row_dict(running_row)}


@app.post("/nodes/{node_code}/tasks/{task_id}/report", response_model=None)
def report_node_task(
    node_code: str,
    task_id: int,
    payload: ReportNodeTaskRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    status_value = str(payload.status or "").strip().lower()
    if status_value not in ("running", "success", "failed"):
        raise HTTPException(status_code=400, detail="status must be running/success/failed")
    result_text = str(payload.result or "")
    if len(result_text) > 12000:
        result_text = result_text[:12000]
    now_ts = int(time.time())

    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code, agent_ip FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])

        task_row = conn.execute(
            """
            SELECT id, attempts, max_attempts, result_text
            FROM node_tasks
            WHERE id = ? AND node_code = ?
            """,
            (task_id, node_code),
        ).fetchone()
        if task_row is None:
            raise HTTPException(status_code=404, detail="Task not found")

        attempts = int(task_row["attempts"] or 0)
        max_attempts = int(task_row["max_attempts"] or 1)
        if max_attempts < 1:
            max_attempts = 1
        next_status = status_value
        next_result = result_text
        if status_value == "failed" and attempts < max_attempts:
            next_status = "pending"
            retry_note = "[controller] auto retry scheduled ({0}/{1})".format(
                attempts,
                max_attempts,
            )
            next_result = append_task_result(result_text, retry_note)

        conn.execute(
            """
            UPDATE node_tasks
            SET status = ?, result_text = ?, updated_at = ?
            WHERE id = ? AND node_code = ?
            """,
            (next_status, next_result, now_ts, task_id, node_code),
        )
        write_audit_log(
            conn,
            action="node.task.report",
            resource_type="node_task",
            resource_id=str(task_id),
            detail={"node_code": node_code, "status": next_status},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

    return {"ok": True, "node_code": node_code, "task_id": task_id, "status": next_status}


# Used by node-side agent polling periodically to sync node and bound-user config.
# If nodes.agent_ip is set, this endpoint enforces source-IP matching for extra safety.
@app.get("/nodes/{node_code}/sync")
def get_node_sync(node_code: str, request: Request) -> Dict[str, Union[Dict, List, int]]:
    generated_at = int(time.time())
    with get_connection() as conn:
        node_row = conn.execute(
            """
            SELECT
                node_code,
                enabled,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                supports_reality,
                supports_tuic,
                tuic_port_start,
                tuic_port_end,
                reality_public_key,
                reality_short_id
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])
        conn.execute(
            "UPDATE nodes SET last_seen_at = ? WHERE node_code = ?",
            (generated_at, node_code),
        )

        user_rows = conn.execute(
            """
            SELECT
                u.user_code,
                u.display_name,
                u.status,
                u.expire_at,
                u.speed_mbps,
                u.vless_uuid,
                u.tuic_secret,
                un.tuic_port,
                un.created_at AS bound_at
            FROM user_nodes un
            JOIN users u ON u.user_code = un.user_code
            WHERE un.node_code = ?
            ORDER BY u.user_code ASC
            """,
            (node_code,),
        ).fetchall()

    node_data = dict(node_row)
    node_data["last_seen_at"] = generated_at
    return {
        "node": node_data,
        "users": [dict(row) for row in user_rows],
        "generated_at": generated_at,
    }


@app.patch("/nodes/{node_code}")
def update_node(
    node_code: str, payload: UpdateNodeRequest, request: Request
) -> Dict[str, Union[int, str, None]]:
    update_data = payload.model_dump(exclude_unset=True)
    if "agent_ip" in update_data:
        update_data["agent_ip"] = validate_agent_ip(update_data.get("agent_ip"))
    if "tuic_port_start" in update_data and update_data["tuic_port_start"] is None:
        raise HTTPException(status_code=400, detail="tuic_port_start must be an integer in 1-65535")
    if "tuic_port_end" in update_data and update_data["tuic_port_end"] is None:
        raise HTTPException(status_code=400, detail="tuic_port_end must be an integer in 1-65535")
    if "enabled" in update_data and update_data["enabled"] not in (0, 1):
        raise HTTPException(status_code=400, detail="enabled must be 0 or 1")
    if "reality_private_key" in update_data:
        private_key = update_data["reality_private_key"]
        if private_key is None or str(private_key).strip() == "":
            raise HTTPException(status_code=400, detail="reality_private_key must be a non-empty string")
    if "reality_public_key" in update_data:
        public_key = update_data["reality_public_key"]
        if public_key is None or str(public_key).strip() == "":
            raise HTTPException(status_code=400, detail="reality_public_key must be a non-empty string")
    if "reality_short_id" in update_data:
        short_id = update_data["reality_short_id"]
        if short_id is None:
            raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)")
        short_id_str = str(short_id)
        if not short_id_str.isalnum() and short_id_str != "":
            raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)")
        if len(short_id_str) > 8:
            raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)")
        if short_id_str != "":
            try:
                int(short_id_str, 16)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)") from exc
    if "supports_reality" in update_data and update_data["supports_reality"] is None:
        raise HTTPException(status_code=400, detail="supports_reality must be 0 or 1")
    if "supports_tuic" in update_data and update_data["supports_tuic"] is None:
        raise HTTPException(status_code=400, detail="supports_tuic must be 0 or 1")
    if "supports_reality" in update_data and update_data["supports_reality"] not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_reality must be 0 or 1")
    if "supports_tuic" in update_data and update_data["supports_tuic"] not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_tuic must be 0 or 1")
    if "monitor_enabled" in update_data and update_data["monitor_enabled"] is None:
        raise HTTPException(status_code=400, detail="monitor_enabled must be 0 or 1")
    if "monitor_enabled" in update_data and update_data["monitor_enabled"] not in (0, 1):
        raise HTTPException(status_code=400, detail="monitor_enabled must be 0 or 1")

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="Node not found")

        port_start = int(update_data.get("tuic_port_start", existing["tuic_port_start"]))
        port_end = int(update_data.get("tuic_port_end", existing["tuic_port_end"]))
        if port_start > port_end:
            raise HTTPException(status_code=400, detail="Invalid port range: start must be <= end")

        if update_data:
            allowed_fields = {
                "region",
                "host",
                "agent_ip",
                "reality_server_name",
                "tuic_server_name",
                "tuic_listen_port",
                "monitor_enabled",
                "reality_private_key",
                "reality_public_key",
                "reality_short_id",
                "tuic_port_start",
                "tuic_port_end",
                "enabled",
                "supports_reality",
                "supports_tuic",
                "note",
            }
            assignments = []
            values = []
            for key, value in update_data.items():
                if key in allowed_fields:
                    assignments.append("{0} = ?".format(key))
                    values.append(value)
            if assignments:
                values.append(node_code)
                conn.execute(
                    "UPDATE nodes SET {0} WHERE node_code = ?".format(", ".join(assignments)),
                    tuple(values),
                )
                audit_update_data = dict(update_data)
                if "reality_private_key" in audit_update_data:
                    audit_update_data["reality_private_key"] = "***"
                write_audit_log(
                    conn,
                    action="node.update",
                    resource_type="node",
                    resource_id=node_code,
                    detail=audit_update_data,
                    actor=get_request_actor(request),
                    source_ip=get_source_ip_for_audit(request),
                )
                conn.commit()

        updated = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()

    if updated is None:
        raise HTTPException(status_code=404, detail="Node not found")
    return dict(updated)


@app.delete("/nodes/{node_code}")
def delete_node(node_code: str, request: Request) -> Dict[str, bool]:
    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        bound_row = conn.execute(
            "SELECT 1 FROM user_nodes WHERE node_code = ? LIMIT 1",
            (node_code,),
        ).fetchone()
        if bound_row is not None:
            raise HTTPException(status_code=400, detail="该节点仍有用户绑定，请先解绑后再删除")

        conn.execute("DELETE FROM nodes WHERE node_code = ?", (node_code,))
        write_audit_log(
            conn,
            action="node.delete",
            resource_type="node",
            resource_id=node_code,
            detail={"ok": True},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True}


# Strategy B: each node owns an independent TUIC port pool.
# Port allocation is recorded in user_nodes, and assignment picks
# the smallest free port in [tuic_port_start, tuic_port_end].
def _pick_smallest_free_port(
    conn: sqlite3.Connection, node_code: str, port_start: int, port_end: int
) -> Optional[int]:
    used_rows = conn.execute(
        "SELECT tuic_port FROM user_nodes WHERE node_code = ? ORDER BY tuic_port",
        (node_code,),
    ).fetchall()
    used_ports = set(int(row["tuic_port"]) for row in used_rows)
    for port in range(port_start, port_end + 1):
        if port not in used_ports:
            return port
    return None


@app.post("/users/{user_code}/assign_node")
def assign_node(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[int, str]]:
    now = int(time.time())
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_row = conn.execute(
            """
            SELECT node_code, tuic_port_start, tuic_port_end, enabled
            FROM nodes
            WHERE node_code = ?
            """,
            (payload.node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        if int(node_row["enabled"]) != 1:
            raise HTTPException(status_code=400, detail="Node is disabled")

        existing = conn.execute(
            """
            SELECT tuic_port
            FROM user_nodes
            WHERE user_code = ? AND node_code = ?
            """,
            (user_code, payload.node_code),
        ).fetchone()
        if existing is not None:
            raise HTTPException(status_code=409, detail="User already assigned to this node")

        tuic_port = _pick_smallest_free_port(
            conn,
            payload.node_code,
            int(node_row["tuic_port_start"]),
            int(node_row["tuic_port_end"]),
        )
        if tuic_port is None:
            raise HTTPException(status_code=409, detail="No available TUIC port in node pool")

        conn.execute(
            """
            INSERT INTO user_nodes(user_code, node_code, tuic_port, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (user_code, payload.node_code, tuic_port, now),
        )
        write_audit_log(
            conn,
            action="user.assign_node",
            resource_type="user_node",
            resource_id="{0}:{1}".format(user_code, payload.node_code),
            detail={"tuic_port": tuic_port},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now,
        )
        conn.commit()

    return {"user_code": user_code, "node_code": payload.node_code, "tuic_port": tuic_port}


@app.post("/users/{user_code}/unassign_node")
def unassign_node(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (payload.node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        binding_row = conn.execute(
            """
            SELECT 1
            FROM user_nodes
            WHERE user_code = ? AND node_code = ?
            """,
            (user_code, payload.node_code),
        ).fetchone()
        if binding_row is None:
            raise HTTPException(status_code=404, detail="User-node binding not found")

        conn.execute(
            "DELETE FROM user_nodes WHERE user_code = ? AND node_code = ?",
            (user_code, payload.node_code),
        )
        write_audit_log(
            conn,
            action="user.unassign_node",
            resource_type="user_node",
            resource_id="{0}:{1}".format(user_code, payload.node_code),
            detail={"ok": True},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "node_code": payload.node_code}


@app.get("/users/{user_code}/nodes")
def list_user_nodes(user_code: str) -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        rows = conn.execute(
            """
            SELECT
                un.node_code,
                un.tuic_port,
                un.created_at,
                n.host,
                n.region,
                n.reality_server_name,
                n.enabled
            FROM user_nodes un
            JOIN nodes n ON n.node_code = un.node_code
            WHERE un.user_code = ?
            ORDER BY un.node_code
            """,
            (user_code,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/users/{user_code}")
def get_user(user_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_code = ?", (user_code,)).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return dict(row)


@app.get("/users")
def list_users() -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                user_code,
                display_name,
                status,
                expire_at,
                speed_mbps
            FROM users
            ORDER BY id ASC
            LIMIT 500
            """
        ).fetchall()
    return [dict(row) for row in rows]


def ensure_user_exists(user_code: str) -> None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")


def build_sub_signature(user_code: str, expire_at: int) -> str:
    if not SUB_LINK_SIGN_KEY:
        return ""
    message = "{0}:{1}".format(user_code, int(expire_at))
    return hmac.new(
        SUB_LINK_SIGN_KEY.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_sub_access(user_code: str, exp: str = "", sig: str = "") -> None:
    ensure_user_exists(user_code)
    if not SUB_LINK_SIGN_KEY:
        return

    exp_raw = str(exp or "").strip()
    sig_raw = str(sig or "").strip()
    if not exp_raw or not sig_raw:
        if SUB_LINK_REQUIRE_SIGNATURE:
            raise HTTPException(status_code=403, detail="subscription signature required")
        return

    try:
        expire_at = int(exp_raw)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="invalid subscription signature") from exc
    now_ts = int(time.time())
    if expire_at <= now_ts:
        raise HTTPException(status_code=403, detail="subscription signature expired")

    expected = build_sub_signature(user_code, expire_at)
    if not expected or not hmac.compare_digest(sig_raw, expected):
        raise HTTPException(status_code=403, detail="invalid subscription signature")


def _build_subscription_links_text(user_code: str) -> str:
    with get_connection() as conn:
        user_row = conn.execute(
            """
            SELECT user_code, vless_uuid, tuic_secret
            FROM users
            WHERE user_code = ?
            """,
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_rows = conn.execute(
            """
            SELECT
                un.node_code,
                un.tuic_port,
                n.host,
                n.reality_server_name,
                n.tuic_server_name,
                n.tuic_listen_port,
                n.reality_public_key,
                n.reality_short_id,
                n.supports_reality,
                n.supports_tuic
            FROM user_nodes un
            JOIN nodes n ON n.node_code = un.node_code
            WHERE un.user_code = ? AND n.enabled = 1
            ORDER BY un.node_code ASC
            """,
            (user_code,),
        ).fetchall()

    lines: List[str] = []
    generated_links = 0

    for row in node_rows:
        node_code = str(row["node_code"])
        host = str(row["host"])
        reality_sni_raw = row["reality_server_name"] or ""
        tuic_sni_raw = row["tuic_server_name"] or ""
        supports_reality = 1 if row["supports_reality"] is None else int(row["supports_reality"])
        supports_tuic = 1 if row["supports_tuic"] is None else int(row["supports_tuic"])

        if supports_reality == 1:
            pbk_raw = row["reality_public_key"] or ""
            sid_raw = row["reality_short_id"] or ""
            if reality_sni_raw and pbk_raw and sid_raw:
                sni = quote(str(reality_sni_raw), safe="")
                pbk = quote(str(pbk_raw), safe="")
                sid = quote(str(sid_raw), safe="")
                name = quote("{0}-R".format(node_code), safe="")
                lines.append(
                    "vless://{0}@{1}:443?encryption=none&type=tcp&security=reality&sni={2}"
                    "&fp=chrome&pbk={3}&sid={4}#{5}".format(
                        user_row["vless_uuid"],
                        host,
                        sni,
                        pbk,
                        sid,
                        name,
                    )
                )
                generated_links += 1
            else:
                lines.append("# {0}: REALITY params missing, skipped vless link".format(node_code))

        # B 模式：优先使用 user_nodes.tuic_port（端口池分配），可支持按用户端口能力（例如限速策略）。
        # A 模式：当 user_nodes.tuic_port 缺失时，回退 nodes.tuic_listen_port（默认 8443）。
        if supports_tuic == 1:
            try:
                assigned_port = int(row["tuic_port"] or 0)
            except (TypeError, ValueError):
                assigned_port = 0
            if assigned_port >= 1 and assigned_port <= 65535:
                tuic_port = assigned_port
            else:
                try:
                    tuic_port = int(row["tuic_listen_port"] or 8443)
                except (TypeError, ValueError):
                    tuic_port = 8443
            name = quote("{0}-T".format(node_code), safe="")
            tuic_link = "tuic://{0}:{0}@{1}:{2}?alpn=h3".format(
                user_row["tuic_secret"], host, tuic_port
            )
            tuic_sni_final = str(tuic_sni_raw or host)
            tuic_link = "{0}&sni={1}".format(
                tuic_link, quote(str(tuic_sni_final), safe="")
            )
            lines.append("{0}#{1}".format(tuic_link, name))
            generated_links += 1

    if generated_links == 0:
        return "# no available links"

    return "\n".join(lines)


@app.get("/sub/links/{user_code}", response_class=PlainTextResponse)
def get_sub_links(user_code: str, exp: str = "", sig: str = "") -> PlainTextResponse:
    verify_sub_access(user_code, exp=exp, sig=sig)
    text = _build_subscription_links_text(user_code)
    return PlainTextResponse(content=text)


@app.get("/sub/base64/{user_code}", response_class=PlainTextResponse)
def get_sub_base64(user_code: str, exp: str = "", sig: str = "") -> PlainTextResponse:
    verify_sub_access(user_code, exp=exp, sig=sig)
    text = _build_subscription_links_text(user_code)
    encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
    return PlainTextResponse(content=encoded)


@app.get(
    "/admin/sub/sign/{user_code}",
    summary="Generate signed subscription URLs",
    description="AUTH_TOKEN 为空时不校验；非空时需要 Bearer。可用于 bot 生成带签名订阅链接。",
    response_model=None,
)
def get_signed_sub_urls(
    user_code: str,
    request: Request,
    ttl_seconds: int = 0,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    ensure_user_exists(user_code)

    ttl = int(ttl_seconds or 0)
    if ttl <= 0:
        ttl = SUB_LINK_DEFAULT_TTL_SECONDS
    if ttl > 30 * 86400:
        ttl = 30 * 86400
    expire_at = int(time.time()) + ttl

    base_links = str(request.base_url).rstrip("/")
    links_path = "/sub/links/{0}".format(user_code)
    base64_path = "/sub/base64/{0}".format(user_code)
    query_string = ""
    if SUB_LINK_SIGN_KEY:
        sig = build_sub_signature(user_code, expire_at)
        query_string = "?exp={0}&sig={1}".format(expire_at, sig)

    links_url = "{0}{1}{2}".format(base_links, links_path, query_string)
    base64_url = "{0}{1}{2}".format(base_links, base64_path, query_string)
    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.sub.sign",
            resource_type="user",
            resource_id=user_code,
            detail={"ttl_seconds": ttl, "signed": bool(SUB_LINK_SIGN_KEY)},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()
    return {
        "ok": True,
        "user_code": user_code,
        "signed": bool(SUB_LINK_SIGN_KEY),
        "expire_at": expire_at,
        "links_url": links_url,
        "base64_url": base64_url,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, reload=False)
