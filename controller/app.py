import json
import os
import sqlite3
import tarfile
import time
import uuid
from pathlib import Path
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from controller.audit import get_request_actor, get_source_ip_for_audit, write_audit_log
from controller.db import BASE_DIR, get_connection, init_db
from controller.node_tasks import (
    ALLOWED_NODE_TASK_TYPES,
    append_task_result,
    build_task_row_dict,
    run_node_task_housekeeping,
)
from controller.schemas import (
    AssignNodeRequest,
    CreateNodeRequest,
    CreateNodeTaskRequest,
    CreateUserRequest,
    ReportNodeTaskRequest,
    SetUserSpeedRequest,
    SetUserStatusRequest,
    UpdateNodeRequest,
)
from controller.subscription import (
    build_signed_subscription_urls,
    build_subscription_base64_text,
    build_subscription_links_text,
    verify_sub_access,
)
from controller.security import (
    API_RATE_LIMIT_ENABLED,
    API_RATE_LIMIT_MAX_REQUESTS,
    API_RATE_LIMIT_WINDOW_SECONDS,
    AUTH_TOKEN,
    TRUSTED_PROXY_IPS,
    TRUST_X_FORWARDED_FOR,
    check_and_consume_rate_limit,
    get_rate_limit_identity,
    is_auth_exempt_path,
    is_rate_limit_target_path,
    validate_agent_ip,
    verify_admin_authorization,
    verify_node_agent_ip,
)


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


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

app = FastAPI()
misc_router = APIRouter(tags=["misc"])
admin_router = APIRouter(tags=["admin"])
users_router = APIRouter(tags=["users"])
nodes_router = APIRouter(tags=["nodes"])
sub_router = APIRouter(tags=["sub"])


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if not AUTH_TOKEN:
        return await call_next(request)
    if is_auth_exempt_path(request.url.path):
        return await call_next(request)

    authorization = request.headers.get("Authorization")
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
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

@app.on_event("startup")
def on_startup() -> None:
    init_db()


@misc_router.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@admin_router.post(
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


@admin_router.post(
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


@admin_router.get(
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


@admin_router.get(
    "/admin/security/status",
    summary="Security configuration status",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
    response_model=None,
)
def get_admin_security_status(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[bool, int, str, List[str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    warnings: List[str] = []
    if not AUTH_TOKEN:
        warnings.append("AUTH_TOKEN 未设置：管理接口未启用鉴权")
    if not SUB_LINK_SIGN_KEY:
        warnings.append("SUB_LINK_SIGN_KEY 未设置：订阅签名功能不可用")
    if SUB_LINK_SIGN_KEY and not SUB_LINK_REQUIRE_SIGNATURE:
        warnings.append("已设置 SUB_LINK_SIGN_KEY，但未强制签名（兼容模式）")
    if TRUST_X_FORWARDED_FOR and not TRUSTED_PROXY_IPS:
        warnings.append("已启用 XFF 信任，但 TRUSTED_PROXY_IPS 为空")
    if not API_RATE_LIMIT_ENABLED:
        warnings.append("轻量限流未启用")

    return {
        "auth_enabled": bool(AUTH_TOKEN),
        "trust_x_forwarded_for": TRUST_X_FORWARDED_FOR,
        "trusted_proxy_ips": sorted(TRUSTED_PROXY_IPS),
        "sub_link_sign_enabled": bool(SUB_LINK_SIGN_KEY),
        "sub_link_require_signature": SUB_LINK_REQUIRE_SIGNATURE,
        "sub_link_default_ttl_seconds": SUB_LINK_DEFAULT_TTL_SECONDS,
        "api_rate_limit_enabled": API_RATE_LIMIT_ENABLED,
        "api_rate_limit_window_seconds": API_RATE_LIMIT_WINDOW_SECONDS,
        "api_rate_limit_max_requests": API_RATE_LIMIT_MAX_REQUESTS,
        "warnings": warnings,
    }


@admin_router.get(
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


@users_router.post("/users/create")
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


@users_router.post("/users/{user_code}/set_speed")
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


@users_router.post("/users/{user_code}/set_status")
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


@users_router.delete("/users/{user_code}")
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


@nodes_router.post("/nodes/create")
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


@nodes_router.get("/nodes")
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


@nodes_router.get("/nodes/{node_code}")
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


@nodes_router.get("/nodes/{node_code}/stats")
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


@nodes_router.post("/nodes/{node_code}/tasks/create", response_model=None)
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
        run_node_task_housekeeping(
            conn,
            now_ts,
            running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
            retention_seconds=NODE_TASK_RETENTION_SECONDS,
        )
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


@nodes_router.get("/nodes/{node_code}/tasks", response_model=None)
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
        run_node_task_housekeeping(
            conn,
            int(time.time()),
            running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
            retention_seconds=NODE_TASK_RETENTION_SECONDS,
            node_code=node_code,
        )
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


@nodes_router.post("/nodes/{node_code}/tasks/next", response_model=None)
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
        run_node_task_housekeeping(
            conn,
            now_ts,
            running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
            retention_seconds=NODE_TASK_RETENTION_SECONDS,
            node_code=node_code,
        )
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


@nodes_router.post("/nodes/{node_code}/tasks/{task_id}/report", response_model=None)
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
@nodes_router.get("/nodes/{node_code}/sync")
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


@nodes_router.patch("/nodes/{node_code}")
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


@nodes_router.delete("/nodes/{node_code}")
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


@users_router.post("/users/{user_code}/assign_node")
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


@users_router.post("/users/{user_code}/unassign_node")
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


@users_router.get("/users/{user_code}/nodes")
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


@users_router.get("/users/{user_code}")
def get_user(user_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_code = ?", (user_code,)).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return dict(row)


@users_router.get("/users")
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


@sub_router.get("/sub/links/{user_code}", response_class=PlainTextResponse)
def get_sub_links(user_code: str, exp: str = "", sig: str = "") -> PlainTextResponse:
    verify_sub_access(
        user_code,
        sign_key=SUB_LINK_SIGN_KEY,
        require_signature=SUB_LINK_REQUIRE_SIGNATURE,
        exp=exp,
        sig=sig,
    )
    text = build_subscription_links_text(user_code)
    return PlainTextResponse(content=text)


@sub_router.get("/sub/base64/{user_code}", response_class=PlainTextResponse)
def get_sub_base64(user_code: str, exp: str = "", sig: str = "") -> PlainTextResponse:
    verify_sub_access(
        user_code,
        sign_key=SUB_LINK_SIGN_KEY,
        require_signature=SUB_LINK_REQUIRE_SIGNATURE,
        exp=exp,
        sig=sig,
    )
    return PlainTextResponse(content=build_subscription_base64_text(user_code))


@admin_router.get(
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
    signed_data = build_signed_subscription_urls(
        user_code=user_code,
        base_url=str(request.base_url).rstrip("/"),
        ttl_seconds=int(ttl_seconds or 0),
        default_ttl_seconds=SUB_LINK_DEFAULT_TTL_SECONDS,
        sign_key=SUB_LINK_SIGN_KEY,
    )
    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.sub.sign",
            resource_type="user",
            resource_id=user_code,
            detail={"ttl_seconds": int(signed_data["ttl_seconds"]), "signed": bool(SUB_LINK_SIGN_KEY)},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()
    return {
        "ok": True,
        "user_code": str(signed_data["user_code"]),
        "signed": bool(signed_data["signed"]),
        "expire_at": int(signed_data["expire_at"]),
        "links_url": str(signed_data["links_url"]),
        "base64_url": str(signed_data["base64_url"]),
    }


app.include_router(misc_router)
app.include_router(admin_router)
app.include_router(users_router)
app.include_router(nodes_router)
app.include_router(sub_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, reload=False)
