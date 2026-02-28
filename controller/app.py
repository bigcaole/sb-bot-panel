import time
import logging
from threading import Lock

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from controller.audit import (
    cleanup_old_audit_logs,
    get_request_actor,
    get_source_ip_for_audit,
    write_audit_log,
)
from controller.db import get_connection, init_db
from controller.routers_admin import (
    cleanup_expired_ip_blocks_once,
    run_security_auto_block_once,
    router as admin_router,
)
from controller.routers_misc import router as misc_router
from controller.routers_nodes import router as nodes_router
from controller.routers_sub import router as sub_router
from controller.routers_users import router as users_router
from controller.settings import (
    API_DOCS_ENABLED,
    AUDIT_LOG_CLEANUP_BATCH_SIZE,
    AUDIT_LOG_CLEANUP_INTERVAL_SECONDS,
    AUDIT_LOG_RETENTION_DAYS,
    SECURITY_AUTO_BLOCK_ENABLED,
    SECURITY_AUTO_BLOCK_INTERVAL_SECONDS,
    SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS,
)
from controller.security import (
    API_RATE_LIMIT_ENABLED,
    AUTH_TOKEN,
    build_unauthorized_audit_key,
    check_and_consume_rate_limit,
    get_rate_limit_identity,
    has_any_admin_auth_token,
    has_any_node_auth_token,
    is_admin_api_path,
    is_admin_api_whitelist_enabled,
    is_auth_exempt_path,
    is_node_agent_auth_path,
    is_request_allowed_by_admin_api_whitelist,
    is_rate_limit_target_path,
    should_write_unauthorized_audit,
    verify_admin_authorization,
    verify_node_authorization,
)


logger = logging.getLogger(__name__)

if API_DOCS_ENABLED:
    app = FastAPI()
else:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
_SECURITY_BLOCK_CLEANUP_LAST_AT = 0
_AUDIT_LOG_CLEANUP_LAST_AT = 0
_SECURITY_AUTO_BLOCK_LAST_AT = 0
_SECURITY_BLOCK_CLEANUP_LOCK = Lock()
_AUDIT_LOG_CLEANUP_LOCK = Lock()
_SECURITY_AUTO_BLOCK_LOCK = Lock()


def _maybe_run_periodic_task(
    now_ts: int,
    interval_seconds: int,
    last_at: int,
    task_lock: Lock,
    task_runner,
    task_name: str = "periodic_task",
) -> int:
    if int(interval_seconds) <= 0:
        return int(last_at)
    if now_ts - int(last_at) < int(interval_seconds):
        return int(last_at)
    if not task_lock.acquire(blocking=False):
        return int(last_at)
    try:
        if now_ts - int(last_at) < int(interval_seconds):
            return int(last_at)
        try:
            task_runner()
        except Exception as exc:
            logger.warning("periodic task failed: name=%s error=%s", task_name, exc)
        return int(now_ts)
    finally:
        task_lock.release()


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    global _SECURITY_BLOCK_CLEANUP_LAST_AT
    global _AUDIT_LOG_CLEANUP_LAST_AT
    global _SECURITY_AUTO_BLOCK_LAST_AT
    now_ts = int(time.time())
    _SECURITY_BLOCK_CLEANUP_LAST_AT = _maybe_run_periodic_task(
        now_ts=now_ts,
        interval_seconds=SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS,
        last_at=_SECURITY_BLOCK_CLEANUP_LAST_AT,
        task_lock=_SECURITY_BLOCK_CLEANUP_LOCK,
        task_runner=lambda: cleanup_expired_ip_blocks_once(now_ts=now_ts),
        task_name="security_block_cleanup",
    )
    _AUDIT_LOG_CLEANUP_LAST_AT = _maybe_run_periodic_task(
        now_ts=now_ts,
        interval_seconds=AUDIT_LOG_CLEANUP_INTERVAL_SECONDS,
        last_at=_AUDIT_LOG_CLEANUP_LAST_AT,
        task_lock=_AUDIT_LOG_CLEANUP_LOCK,
        task_runner=lambda: _run_audit_log_cleanup_once(now_ts=now_ts),
        task_name="audit_log_cleanup",
    )
    if SECURITY_AUTO_BLOCK_ENABLED:
        _SECURITY_AUTO_BLOCK_LAST_AT = _maybe_run_periodic_task(
            now_ts=now_ts,
            interval_seconds=SECURITY_AUTO_BLOCK_INTERVAL_SECONDS,
            last_at=_SECURITY_AUTO_BLOCK_LAST_AT,
            task_lock=_SECURITY_AUTO_BLOCK_LOCK,
            task_runner=lambda: _run_security_auto_block_once(now_ts=now_ts),
            task_name="security_auto_block",
        )

    request_path = str(request.url.path or "/")
    if is_auth_exempt_path(request_path):
        return await call_next(request)
    if is_admin_api_path(request_path) and is_admin_api_whitelist_enabled():
        if not is_request_allowed_by_admin_api_whitelist(request):
            try:
                source_ip = get_source_ip_for_audit(request)
                now_ts = int(time.time())
                audit_key = build_unauthorized_audit_key(
                    source_ip=source_ip,
                    path=request_path,
                    method=str(request.method or "GET"),
                )
                should_log, dropped_count = should_write_unauthorized_audit(
                    "admin-whitelist:{0}".format(audit_key), now_ts
                )
                with get_connection() as conn:
                    if should_log:
                        detail = {"method": request.method}
                        if dropped_count > 0:
                            detail["sampled_dropped"] = int(dropped_count)
                        write_audit_log(
                            conn,
                            action="auth.source_not_allowed",
                            resource_type="http",
                            resource_id=request_path,
                            detail=detail,
                            actor=get_request_actor(request),
                            source_ip=source_ip,
                        )
                        conn.commit()
            except Exception:
                pass
            return JSONResponse(
                status_code=403,
                content={"ok": False, "error": "source_not_allowed"},
            )
    if API_RATE_LIMIT_ENABLED and is_rate_limit_target_path(request.url.path):
        identity = get_rate_limit_identity(request)
        limited, retry_after = check_and_consume_rate_limit(identity, now_ts)
        if limited:
            return JSONResponse(
                status_code=429,
                content={"ok": False, "error": "rate_limited", "retry_after": retry_after},
                headers={"Retry-After": str(retry_after)},
            )

    authorization = request.headers.get("Authorization")
    if is_node_agent_auth_path(request_path):
        auth_error = verify_node_authorization(authorization)
        if (not has_any_node_auth_token()) and auth_error is None:
            return await call_next(request)
    else:
        auth_error = verify_admin_authorization(authorization)
        if (not has_any_admin_auth_token()) and auth_error is None:
            return await call_next(request)
    if auth_error is not None:
        try:
            source_ip = get_source_ip_for_audit(request)
            now_ts = int(time.time())
            audit_key = build_unauthorized_audit_key(
                source_ip=source_ip,
                path=str(request.url.path or "/"),
                method=str(request.method or "GET"),
            )
            should_log, dropped_count = should_write_unauthorized_audit(audit_key, now_ts)
            with get_connection() as conn:
                if should_log:
                    detail = {"method": request.method}
                    if dropped_count > 0:
                        detail["sampled_dropped"] = int(dropped_count)
                    write_audit_log(
                        conn,
                        action="auth.unauthorized",
                        resource_type="http",
                        resource_id=str(request.url.path or "/"),
                        detail=detail,
                        actor=get_request_actor(request),
                        source_ip=source_ip,
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
    # AUTH_TOKEN 开启时，受保护路径的限流已在 auth_middleware 中处理，
    # 这里跳过以避免重复计数。
    if (has_any_admin_auth_token() or has_any_node_auth_token()) and not is_auth_exempt_path(request.url.path):
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


def _run_audit_log_cleanup_once(now_ts: int) -> None:
    with get_connection() as conn:
        cleanup_old_audit_logs(
            conn,
            now_ts=now_ts,
            retention_days=AUDIT_LOG_RETENTION_DAYS,
            batch_size=AUDIT_LOG_CLEANUP_BATCH_SIZE,
        )
        conn.commit()


def _run_security_auto_block_once(now_ts: int) -> None:
    with get_connection() as conn:
        run_security_auto_block_once(conn, now_ts=now_ts)
        conn.commit()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


app.include_router(misc_router)
app.include_router(admin_router)
app.include_router(users_router)
app.include_router(nodes_router)
app.include_router(sub_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, reload=False)
