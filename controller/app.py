import time

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
    is_auth_exempt_path,
    is_rate_limit_target_path,
    should_write_unauthorized_audit,
    verify_admin_authorization,
)


app = FastAPI()
_SECURITY_BLOCK_CLEANUP_LAST_AT = 0
_AUDIT_LOG_CLEANUP_LAST_AT = 0
_SECURITY_AUTO_BLOCK_LAST_AT = 0


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    global _SECURITY_BLOCK_CLEANUP_LAST_AT
    global _AUDIT_LOG_CLEANUP_LAST_AT
    global _SECURITY_AUTO_BLOCK_LAST_AT
    now_ts = int(time.time())
    if now_ts - int(_SECURITY_BLOCK_CLEANUP_LAST_AT) >= SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS:
        try:
            cleanup_expired_ip_blocks_once(now_ts=now_ts)
        except Exception:
            pass
        _SECURITY_BLOCK_CLEANUP_LAST_AT = now_ts
    if now_ts - int(_AUDIT_LOG_CLEANUP_LAST_AT) >= AUDIT_LOG_CLEANUP_INTERVAL_SECONDS:
        try:
            with get_connection() as conn:
                cleanup_old_audit_logs(
                    conn,
                    now_ts=now_ts,
                    retention_days=AUDIT_LOG_RETENTION_DAYS,
                    batch_size=AUDIT_LOG_CLEANUP_BATCH_SIZE,
                )
                conn.commit()
        except Exception:
            pass
        _AUDIT_LOG_CLEANUP_LAST_AT = now_ts
    if (
        SECURITY_AUTO_BLOCK_ENABLED
        and now_ts - int(_SECURITY_AUTO_BLOCK_LAST_AT) >= SECURITY_AUTO_BLOCK_INTERVAL_SECONDS
    ):
        try:
            with get_connection() as conn:
                run_security_auto_block_once(conn, now_ts=now_ts)
                conn.commit()
        except Exception:
            pass
        _SECURITY_AUTO_BLOCK_LAST_AT = now_ts

    if not AUTH_TOKEN:
        return await call_next(request)
    if is_auth_exempt_path(request.url.path):
        return await call_next(request)

    authorization = request.headers.get("Authorization")
    auth_error = verify_admin_authorization(authorization)
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


app.include_router(misc_router)
app.include_router(admin_router)
app.include_router(users_router)
app.include_router(nodes_router)
app.include_router(sub_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, reload=False)
