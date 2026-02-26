import os
import tarfile
import time
from pathlib import Path
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from controller.audit import get_request_actor, get_source_ip_for_audit, write_audit_log
from controller.db import BASE_DIR, get_connection, init_db
from controller.node_runtime_service import (
    create_node_task_service,
    get_next_node_task_service,
    get_node_sync_service,
    list_node_tasks_service,
    report_node_task_service,
)
from controller.nodes_service import (
    create_node_service,
    delete_node_service,
    get_node_service,
    get_node_stats_service,
    list_nodes_service,
    update_node_service,
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
from controller.users_service import (
    assign_node_service,
    create_user_service,
    delete_user_service,
    get_user_service,
    list_user_nodes_service,
    list_users_service,
    set_user_speed_service,
    set_user_status_service,
    unassign_node_service,
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
    verify_admin_authorization,
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
    return create_user_service(payload, request)


@users_router.post("/users/{user_code}/set_speed")
def set_user_speed(
    user_code: str, payload: SetUserSpeedRequest, request: Request
) -> Dict[str, Union[bool, int, str]]:
    return set_user_speed_service(user_code, payload, request)


@users_router.post("/users/{user_code}/set_status")
def set_user_status(
    user_code: str, payload: SetUserStatusRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    return set_user_status_service(user_code, payload, request)


@users_router.delete("/users/{user_code}")
def delete_user(user_code: str, request: Request) -> Dict[str, Union[bool, str]]:
    return delete_user_service(user_code, request)


@nodes_router.post("/nodes/create")
def create_node(payload: CreateNodeRequest, request: Request) -> Dict[str, Union[int, str, None]]:
    return create_node_service(payload, request)


@nodes_router.get("/nodes")
def list_nodes() -> List[Dict[str, Union[int, str, None]]]:
    return list_nodes_service()


@nodes_router.get("/nodes/{node_code}")
def get_node(node_code: str) -> Dict[str, Union[int, str, None]]:
    return get_node_service(node_code)


@nodes_router.get("/nodes/{node_code}/stats")
def get_node_stats(node_code: str) -> Dict[str, Union[int, str]]:
    return get_node_stats_service(node_code)


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
    return create_node_task_service(
        node_code=node_code,
        payload=payload,
        request=request,
        running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
        retention_seconds=NODE_TASK_RETENTION_SECONDS,
    )


@nodes_router.get("/nodes/{node_code}/tasks", response_model=None)
def list_node_tasks(
    node_code: str,
    limit: int = 20,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[List[Dict[str, Any]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return list_node_tasks_service(
        node_code=node_code,
        limit=limit,
        running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
        retention_seconds=NODE_TASK_RETENTION_SECONDS,
    )


@nodes_router.post("/nodes/{node_code}/tasks/next", response_model=None)
def get_next_node_task(
    node_code: str,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return get_next_node_task_service(
        node_code=node_code,
        request=request,
        running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
        retention_seconds=NODE_TASK_RETENTION_SECONDS,
    )


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
    return report_node_task_service(
        node_code=node_code,
        task_id=task_id,
        payload=payload,
        request=request,
    )


# Used by node-side agent polling periodically to sync node and bound-user config.
# If nodes.agent_ip is set, this endpoint enforces source-IP matching for extra safety.
@nodes_router.get("/nodes/{node_code}/sync")
def get_node_sync(node_code: str, request: Request) -> Dict[str, Union[Dict, List, int]]:
    return get_node_sync_service(node_code, request)


@nodes_router.patch("/nodes/{node_code}")
def update_node(
    node_code: str, payload: UpdateNodeRequest, request: Request
) -> Dict[str, Union[int, str, None]]:
    return update_node_service(node_code, payload, request)


@nodes_router.delete("/nodes/{node_code}")
def delete_node(node_code: str, request: Request) -> Dict[str, bool]:
    return delete_node_service(node_code, request)


@users_router.post("/users/{user_code}/assign_node")
def assign_node(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[int, str]]:
    return assign_node_service(user_code, payload, request)


@users_router.post("/users/{user_code}/unassign_node")
def unassign_node(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    return unassign_node_service(user_code, payload, request)


@users_router.get("/users/{user_code}/nodes")
def list_user_nodes(user_code: str) -> List[Dict[str, Union[int, str, None]]]:
    return list_user_nodes_service(user_code)


@users_router.get("/users/{user_code}")
def get_user(user_code: str) -> Dict[str, Union[int, str, None]]:
    return get_user_service(user_code)


@users_router.get("/users")
def list_users() -> List[Dict[str, Union[int, str, None]]]:
    return list_users_service()


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
