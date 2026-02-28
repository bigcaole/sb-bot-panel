import ipaddress
import re
import sqlite3
import subprocess
import shutil
import tarfile
import tempfile
import time
from threading import Lock
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from controller.audit import (
    cleanup_old_audit_logs,
    get_request_actor,
    get_source_ip_for_audit,
    write_audit_log,
)
from controller.db import BASE_DIR, get_connection
from controller.db_migration import (
    compare_snapshot_with_live,
    export_db_snapshot,
    get_db_integrity_status,
    load_export_payload,
    validate_export_payload,
)
from controller.node_runtime_service import create_node_task_service, get_node_sync_service
from controller.node_tasks import sanitize_task_payload_for_display
from controller.redaction import mask_sensitive_text
from controller.schemas import (
    AuditEventRequest,
    BlockIpRequest,
    CreateNodeTaskRequest,
    UnblockIpRequest,
    VerifyDbExportRequest,
)
from controller.security import (
    ADMIN_AUTH_TOKEN as SECURITY_ADMIN_AUTH_TOKEN,
    AUTH_TOKEN as SECURITY_AUTH_TOKEN,
    NODE_AUTH_TOKEN as SECURITY_NODE_AUTH_TOKEN,
    API_RATE_LIMIT_ENABLED,
    API_RATE_LIMIT_MAX_REQUESTS,
    API_RATE_LIMIT_WINDOW_SECONDS,
    TRUSTED_PROXY_IPS,
    TRUST_X_FORWARDED_FOR,
    UNAUTHORIZED_AUDIT_SAMPLE_SECONDS,
    get_admin_auth_tokens,
    get_admin_auth_token_source,
    get_node_auth_token_source,
    get_node_auth_tokens,
    is_auth_token_split_active,
    verify_admin_authorization,
)
from controller.settings import (
    API_DOCS_ENABLED,
    AGENT_DEFAULT_POLL_INTERVAL,
    ADMIN_OVERVIEW_CACHE_TTL_SECONDS,
    ADMIN_API_WHITELIST_ITEMS,
    ADMIN_API_WHITELIST_SOURCE,
    ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS,
    AUDIT_LOG_CLEANUP_BATCH_SIZE,
    AUDIT_LOG_CLEANUP_INTERVAL_SECONDS,
    AUDIT_LOG_RETENTION_DAYS,
    BACKUP_RETENTION_COUNT,
    CONTROLLER_PORT_WHITELIST_ITEMS,
    CONTROLLER_PUBLIC_URL,
    PANEL_BASE_URL,
    CONTROLLER_PORT,
    MIGRATE_RETENTION_COUNT,
    NODE_TASK_MAX_PENDING_PER_NODE,
    NODE_TASK_RETENTION_SECONDS,
    NODE_TASK_RUNNING_TIMEOUT_SECONDS,
    NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS,
    SECURITY_AUTO_BLOCK_DURATION_SECONDS,
    SECURITY_AUTO_BLOCK_ENABLED,
    SECURITY_AUTO_BLOCK_INTERVAL_SECONDS,
    SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL,
    SECURITY_AUTO_BLOCK_THRESHOLD,
    SECURITY_AUTO_BLOCK_WINDOW_SECONDS,
    SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS,
    SECURITY_BLOCK_PROTECTED_IPS_ITEMS,
    SECURITY_EVENTS_EXCLUDE_LOCAL,
    SUB_LINK_DEFAULT_TTL_SECONDS,
    SUB_LINK_REQUIRE_SIGNATURE,
    SUB_LINK_SIGN_KEY,
)
from controller.subscription import build_signed_subscription_urls


router = APIRouter(tags=["admin"])
# Compatibility alias for existing tests/tools that import controller.routers_admin.AUTH_TOKEN.
AUTH_TOKEN = SECURITY_AUTH_TOKEN
ADMIN_AUTH_TOKEN = SECURITY_ADMIN_AUTH_TOKEN
NODE_AUTH_TOKEN = SECURITY_NODE_AUTH_TOKEN
ADMIN_AI_CONTEXT_EXPORT_SCRIPT = BASE_DIR / "scripts" / "admin" / "ai_context_export.sh"
ADMIN_AI_CONTEXT_EXPORT_TIMEOUT_SECONDS = 40
ADMIN_OPS_SNAPSHOT_SCRIPT = BASE_DIR / "scripts" / "admin" / "ops_snapshot.sh"
ADMIN_OPS_SNAPSHOT_TIMEOUT_SECONDS = 40
WEAK_AUTH_TOKEN_EXAMPLES = {
    "devtoken123",
    "change_me_to_a_random_token",
    "changeme",
    "change_me",
    "token",
    "password",
    "admin",
    "123456",
}
ADMIN_AUTH_DESCRIPTION = (
    "管理接口鉴权：优先 ADMIN_AUTH_TOKEN；兼容回退 AUTH_TOKEN（最后回退 NODE_AUTH_TOKEN）。"
    "配置了 token 时需要请求头 Authorization: Bearer <token>。"
)
_ADMIN_OVERVIEW_CACHE_TTL_SECONDS = int(ADMIN_OVERVIEW_CACHE_TTL_SECONDS)
_ADMIN_OVERVIEW_CACHE_EXPIRE_AT = 0
_ADMIN_OVERVIEW_CACHE_PAYLOAD: Optional[Dict[str, Union[int, Dict, List]]] = None
_ADMIN_OVERVIEW_CACHE_LOCK = Lock()
_SECURITY_STATUS_CACHE_TTL_SECONDS = int(ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS)
_SECURITY_STATUS_CACHE_EXPIRE_AT = 0
_SECURITY_STATUS_CACHE_PAYLOAD: Optional[Dict[str, Union[bool, int, List[str]]]] = None
_SECURITY_STATUS_CACHE_LOCK = Lock()


def invalidate_admin_snapshots_cache() -> None:
    global _ADMIN_OVERVIEW_CACHE_EXPIRE_AT
    global _ADMIN_OVERVIEW_CACHE_PAYLOAD
    global _SECURITY_STATUS_CACHE_EXPIRE_AT
    global _SECURITY_STATUS_CACHE_PAYLOAD
    _ADMIN_OVERVIEW_CACHE_EXPIRE_AT = 0
    _ADMIN_OVERVIEW_CACHE_PAYLOAD = None
    _SECURITY_STATUS_CACHE_EXPIRE_AT = 0
    _SECURITY_STATUS_CACHE_PAYLOAD = None


def _normalize_controller_url(raw_url: str) -> str:
    value = str(raw_url or "").strip().rstrip("/")
    if not value:
        return ""
    if not re.match(r"^https?://", value):
        value = "http://{0}".format(value)
    return value.rstrip("/")


def _normalize_public_base_url(raw_url: str) -> str:
    value = str(raw_url or "").strip().rstrip("/")
    if not value:
        return ""
    if not re.match(r"^https?://", value):
        value = "https://{0}".format(value)
    return value.rstrip("/")


def _collect_auth_token_risks(tokens: List[str]) -> List[Dict[str, Union[int, List[str]]]]:
    risks: List[Dict[str, Union[int, List[str]]]] = []
    for index, raw_token in enumerate(tokens, start=1):
        token = str(raw_token or "").strip()
        if not token:
            continue
        issues: List[str] = []
        if len(token) < 24:
            issues.append("too_short")
        if token.lower() in WEAK_AUTH_TOKEN_EXAMPLES:
            issues.append("default_like")
        if token.isdigit():
            issues.append("numeric_only")
        if len(set(token)) < 6:
            issues.append("low_character_variety")
        if not any(ch.isalpha() for ch in token) or not any(ch.isdigit() for ch in token):
            issues.append("missing_alpha_or_digit")
        if issues:
            risks.append({"index": index, "issues": issues})
    return risks


def _mask_sensitive_audit_detail(detail_text: str) -> str:
    return mask_sensitive_text(detail_text)


def _has_control_characters(text: str) -> bool:
    return any((ord(ch) < 32 or ord(ch) == 127) for ch in str(text or ""))


def _enqueue_task_for_nodes(
    request: Request,
    task_type: str,
    task_payload: Dict[str, Union[int, str]],
    max_attempts: int,
    include_disabled_flag: bool,
    force_new_flag: bool,
    audit_action: str,
) -> Dict[str, Union[bool, int, str, List[Dict[str, str]]]]:
    with get_connection() as conn:
        if include_disabled_flag:
            node_rows = conn.execute(
                "SELECT node_code, enabled FROM nodes ORDER BY node_code ASC LIMIT 500"
            ).fetchall()
        else:
            node_rows = conn.execute(
                "SELECT node_code, enabled FROM nodes WHERE enabled = 1 ORDER BY node_code ASC LIMIT 500"
            ).fetchall()

    selected = len(node_rows)
    created = 0
    deduplicated = 0
    failed = 0
    failures: List[Dict[str, str]] = []

    payload = CreateNodeTaskRequest(
        task_type=task_type,
        payload=task_payload,
        max_attempts=max_attempts,
        force_new=force_new_flag,
    )
    for row in node_rows:
        node_code = str(row["node_code"] or "").strip()
        if not node_code:
            continue
        try:
            task_data = create_node_task_service(
                node_code=node_code,
                payload=payload,
                request=request,
                running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
                retention_seconds=NODE_TASK_RETENTION_SECONDS,
                max_pending_per_node=NODE_TASK_MAX_PENDING_PER_NODE,
            )
            if bool(task_data.get("deduplicated")):
                deduplicated += 1
            else:
                created += 1
        except HTTPException as exc:
            failed += 1
            failures.append(
                {
                    "node_code": node_code,
                    "error": str(exc.detail),
                }
            )

    with get_connection() as conn:
        write_audit_log(
            conn,
            action=audit_action,
            resource_type="security",
            resource_id="nodes",
            detail={
                "selected": selected,
                "created": created,
                "deduplicated": deduplicated,
                "failed": failed,
                "task_type": task_type,
                "include_disabled": include_disabled_flag,
                "force_new": force_new_flag,
                "payload_keys": sorted(list(task_payload.keys())),
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=int(time.time()),
        )
        conn.commit()

    return {
        "ok": True,
        "selected": selected,
        "created": created,
        "deduplicated": deduplicated,
        "failed": failed,
        "task_type": task_type,
        "include_disabled": include_disabled_flag,
        "force_new": force_new_flag,
        "payload": sanitize_task_payload_for_display(task_payload),
        "failures": failures[:20],
    }


def _enqueue_config_set_for_nodes(
    request: Request,
    config_payload: Dict[str, Union[int, str]],
    include_disabled_flag: bool,
    force_new_flag: bool,
    audit_action: str,
) -> Dict[str, Union[bool, int, str, List[Dict[str, str]]]]:
    return _enqueue_task_for_nodes(
        request=request,
        task_type="config_set",
        task_payload=config_payload,
        max_attempts=1,
        include_disabled_flag=include_disabled_flag,
        force_new_flag=force_new_flag,
        audit_action=audit_action,
    )


@router.post(
    "/admin/auth/sync_node_tokens",
    summary="Enqueue config_set(auth_token) for nodes",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "使用当前节点鉴权主 token（优先 NODE_AUTH_TOKEN，第一个非空项）为节点下发 config_set(auth_token) 任务。"
    ),
    response_model=None,
)
def sync_node_auth_tokens(
    request: Request,
    include_disabled: int = 0,
    force_new: int = 0,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, List[Dict[str, str]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    tokens = get_node_auth_tokens()
    if not tokens:
        raise HTTPException(status_code=400, detail="NODE_AUTH_TOKEN 未配置")
    primary_token = str(tokens[0]).strip()
    if not primary_token:
        raise HTTPException(status_code=400, detail="NODE_AUTH_TOKEN 主 token 为空")

    include_disabled_flag = int(include_disabled) == 1
    force_new_flag = int(force_new) == 1
    return _enqueue_config_set_for_nodes(
        request=request,
        config_payload={"auth_token": primary_token},
        include_disabled_flag=include_disabled_flag,
        force_new_flag=force_new_flag,
        audit_action="admin.auth.sync_node_tokens",
    )


@router.post(
    "/admin/nodes/sync_agent_defaults",
    summary="Enqueue default agent config sync for nodes",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "默认下发 auth_token(节点鉴权主 token) + poll_interval，并在配置了 CONTROLLER_PUBLIC_URL 时额外下发 controller_url。"
    ),
    response_model=None,
)
def sync_node_agent_defaults(
    request: Request,
    include_disabled: int = 0,
    force_new: int = 0,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, List[Dict[str, str]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    tokens = get_node_auth_tokens()
    if not tokens:
        raise HTTPException(status_code=400, detail="NODE_AUTH_TOKEN 未配置")
    primary_token = str(tokens[0]).strip()
    if not primary_token:
        raise HTTPException(status_code=400, detail="NODE_AUTH_TOKEN 主 token 为空")

    config_payload: Dict[str, Union[int, str]] = {
        "auth_token": primary_token,
        "poll_interval": int(AGENT_DEFAULT_POLL_INTERVAL),
    }
    normalized_public_url = _normalize_controller_url(CONTROLLER_PUBLIC_URL)
    if normalized_public_url:
        config_payload["controller_url"] = normalized_public_url

    include_disabled_flag = int(include_disabled) == 1
    force_new_flag = int(force_new) == 1
    return _enqueue_config_set_for_nodes(
        request=request,
        config_payload=config_payload,
        include_disabled_flag=include_disabled_flag,
        force_new_flag=force_new_flag,
        audit_action="admin.nodes.sync_agent_defaults",
    )


@router.post(
    "/admin/nodes/sync_time",
    summary="Enqueue node time-sync task",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "下发 sync_time 任务，节点会把系统时间校准到管理服务器当前时间。"
    ),
    response_model=None,
)
def sync_node_time(
    request: Request,
    include_disabled: int = 0,
    force_new: int = 0,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, List[Dict[str, str]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    include_disabled_flag = int(include_disabled) == 1
    force_new_flag = int(force_new) == 1
    now_ts = int(time.time())
    task_payload: Dict[str, Union[int, str]] = {
        "server_unix": now_ts,
    }
    result = _enqueue_task_for_nodes(
        request=request,
        task_type="sync_time",
        task_payload=task_payload,
        max_attempts=1,
        include_disabled_flag=include_disabled_flag,
        force_new_flag=force_new_flag,
        audit_action="admin.nodes.sync_time",
    )
    result["server_unix"] = now_ts
    return result


@router.get(
    "/admin/nodes/{node_code}/sync_preview",
    summary="Preview node sync payload (admin)",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "用于管理端排查节点下发内容，不受 nodes.agent_ip 限制。"
    ),
    response_model=None,
)
def get_admin_node_sync_preview(
    node_code: str,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[Dict, List, int, None]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    result = get_node_sync_service(
        node_code=node_code,
        request=request,
        enforce_agent_ip=False,
        touch_last_seen=False,
    )
    now_ts = int(time.time())
    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.nodes.sync_preview",
            resource_type="node",
            resource_id=node_code,
            detail={"users": len(result.get("users", []))},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()
    return result


def build_unauthorized_events_snapshot(
    now_ts: int,
    window_seconds: int,
    top_limit: int,
    include_local: Optional[bool] = None,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Union[int, List[Dict[str, Union[int, str]]]]]:
    if window_seconds < 60:
        window_seconds = 60
    if window_seconds > 7 * 86400:
        window_seconds = 7 * 86400
    if top_limit < 1:
        top_limit = 1
    if top_limit > 20:
        top_limit = 20

    include_local_effective = (
        bool(include_local) if include_local is not None else (not SECURITY_EVENTS_EXCLUDE_LOCAL)
    )
    since_ts = int(now_ts - window_seconds)
    where_sql = "action = 'auth.unauthorized' AND created_at >= ?"
    if not include_local_effective:
        where_sql += (
            " AND source_ip NOT IN ('127.0.0.1', '::1', 'localhost', 'testclient', '::ffff:127.0.0.1')"
        )
    if conn is None:
        with get_connection() as local_conn:
            return build_unauthorized_events_snapshot(
                now_ts=now_ts,
                window_seconds=window_seconds,
                top_limit=top_limit,
                include_local=include_local,
                conn=local_conn,
            )

    unauthorized_count = int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM audit_logs WHERE {0}".format(where_sql),
            (since_ts,),
        ).fetchone()["c"]
        or 0
    )
    unauthorized_top_rows = conn.execute(
        """
        SELECT source_ip, COUNT(*) AS c
        FROM audit_logs
        WHERE {0}
          AND source_ip IS NOT NULL
          AND source_ip != ''
        GROUP BY source_ip
        ORDER BY c DESC, source_ip ASC
        LIMIT ?
        """.format(where_sql),
        (since_ts, top_limit),
    ).fetchall()

    top_items: List[Dict[str, Union[int, str]]] = []
    for row in unauthorized_top_rows:
        top_items.append(
            {
                "source_ip": str(row["source_ip"] or ""),
                "count": int(row["c"] or 0),
            }
        )
    return {
        "window_seconds": int(window_seconds),
        "include_local": bool(include_local_effective),
        "since": int(since_ts),
        "unauthorized": int(unauthorized_count),
        "top_unauthorized_ips": top_items,
    }


def build_security_status_payload(
    conn: Optional[sqlite3.Connection] = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Union[bool, int, List[str]]]:
    admin_auth_tokens = get_admin_auth_tokens()
    node_auth_tokens = get_node_auth_tokens()
    admin_auth_source = get_admin_auth_token_source()
    node_auth_source = get_node_auth_token_source()
    auth_split_active = is_auth_token_split_active()
    auth_tokens = list(admin_auth_tokens)
    for item in node_auth_tokens:
        value = str(item or "").strip()
        if value and value not in auth_tokens:
            auth_tokens.append(value)
    weak_token_risks = _collect_auth_token_risks(auth_tokens)
    if now_ts is None:
        now_ts = int(time.time())
    else:
        now_ts = int(now_ts)
    protected_invalid_items = get_invalid_ip_or_cidr_items(SECURITY_BLOCK_PROTECTED_IPS_ITEMS)
    protected_effective_count = max(
        0, int(len(SECURITY_BLOCK_PROTECTED_IPS_ITEMS) - len(protected_invalid_items))
    )
    admin_api_whitelist_invalid_items = get_invalid_ip_or_cidr_items(ADMIN_API_WHITELIST_ITEMS)
    admin_api_whitelist_effective_count = max(
        0, int(len(ADMIN_API_WHITELIST_ITEMS) - len(admin_api_whitelist_invalid_items))
    )
    if conn is None:
        with get_connection() as local_conn:
            return build_security_status_payload(conn=local_conn, now_ts=now_ts)
    active_block_count = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM security_ip_blocks
            WHERE expire_at = 0 OR expire_at > ?
            """,
            (now_ts,),
        ).fetchone()["c"]
        or 0
    )
    warnings: List[str] = []
    if not admin_auth_tokens:
        warnings.append("ADMIN_AUTH_TOKEN 未设置：管理接口未启用鉴权")
    if not node_auth_tokens:
        warnings.append("NODE_AUTH_TOKEN 未设置：节点接口未启用鉴权")
    if admin_auth_source == "auth_token":
        warnings.append("管理鉴权仍在使用 AUTH_TOKEN 兼容模式（建议显式设置 ADMIN_AUTH_TOKEN）")
    if node_auth_source == "auth_token":
        warnings.append("节点鉴权仍在使用 AUTH_TOKEN 兼容模式（建议显式设置 NODE_AUTH_TOKEN）")
    if (
        admin_auth_tokens
        and node_auth_tokens
        and set(admin_auth_tokens) == set(node_auth_tokens)
    ):
        warnings.append("ADMIN_AUTH_TOKEN 与 NODE_AUTH_TOKEN 当前等值（建议拆分不同 token）")
    if len(admin_auth_tokens) > 1:
        warnings.append("ADMIN_AUTH_TOKEN 处于多 token 过渡模式（建议迁移完成后移除旧 token）")
    if len(node_auth_tokens) > 1:
        warnings.append("NODE_AUTH_TOKEN 处于多 token 过渡模式（建议迁移完成后移除旧 token）")
    if weak_token_risks:
        warnings.append("鉴权 token 强度偏弱（建议至少 24 位随机串，含字母与数字）")
    if not SUB_LINK_SIGN_KEY:
        warnings.append("SUB_LINK_SIGN_KEY 未设置：订阅签名功能不可用")
    if SUB_LINK_SIGN_KEY and not SUB_LINK_REQUIRE_SIGNATURE:
        warnings.append("已设置 SUB_LINK_SIGN_KEY，但未强制签名（兼容模式）")
    if TRUST_X_FORWARDED_FOR and not TRUSTED_PROXY_IPS:
        warnings.append("已启用 XFF 信任，但 TRUSTED_PROXY_IPS 为空")
    if not API_RATE_LIMIT_ENABLED:
        warnings.append("轻量限流未启用")
    if ADMIN_API_WHITELIST_SOURCE == "controller_port_whitelist_fallback":
        warnings.append(
            "ADMIN_API_WHITELIST 未显式设置：当前使用 CONTROLLER_PORT_WHITELIST 回退（建议显式配置）"
        )
    if not ADMIN_API_WHITELIST_ITEMS:
        warnings.append("ADMIN_API_WHITELIST 未设置：管理接口未启用应用层来源限制")
    if admin_api_whitelist_invalid_items:
        warnings.append("ADMIN_API_WHITELIST 含无效项（已忽略），请修正格式")
    if API_DOCS_ENABLED:
        warnings.append("API 文档入口已启用（建议仅排障临时开启）")
    if not SECURITY_EVENTS_EXCLUDE_LOCAL:
        warnings.append("安全事件统计包含本机来源（可能放大测试噪声）")
    if UNAUTHORIZED_AUDIT_SAMPLE_SECONDS <= 0:
        warnings.append("未授权审计采样已关闭（高扫描场景下 audit_logs 增长会更快）")
    if AUDIT_LOG_RETENTION_DAYS < 7:
        warnings.append("审计日志保留天数过短（小于 7 天）")
    if SECURITY_AUTO_BLOCK_ENABLED:
        warnings.append("自动封禁已启用（请确认阈值与白名单策略，避免误封）")
    if SECURITY_AUTO_BLOCK_ENABLED and (not CONTROLLER_PORT_WHITELIST_ITEMS) and (
        not SECURITY_BLOCK_PROTECTED_IPS_ITEMS
    ):
        warnings.append("自动封禁已启用，但未配置 SECURITY_BLOCK_PROTECTED_IPS（建议至少加入运维来源）")
    if protected_invalid_items:
        warnings.append("SECURITY_BLOCK_PROTECTED_IPS 含无效项（已忽略），请修正格式")

    return {
        "auth_enabled": bool(admin_auth_tokens),
        "auth_token_count": len(auth_tokens),
        "admin_auth_enabled": bool(admin_auth_tokens),
        "admin_auth_token_count": len(admin_auth_tokens),
        "admin_auth_source": admin_auth_source,
        "node_auth_enabled": bool(node_auth_tokens),
        "node_auth_token_count": len(node_auth_tokens),
        "node_auth_source": node_auth_source,
        "auth_token_split_active": bool(auth_split_active),
        "weak_auth_token_count": len(weak_token_risks),
        "weak_auth_token_risks": weak_token_risks,
        "controller_port_whitelist": CONTROLLER_PORT_WHITELIST_ITEMS,
        "controller_port_whitelist_count": len(CONTROLLER_PORT_WHITELIST_ITEMS),
        "admin_api_whitelist": ADMIN_API_WHITELIST_ITEMS,
        "admin_api_whitelist_source": str(ADMIN_API_WHITELIST_SOURCE),
        "admin_api_whitelist_count": len(ADMIN_API_WHITELIST_ITEMS),
        "admin_api_whitelist_effective_count": int(admin_api_whitelist_effective_count),
        "admin_api_whitelist_invalid": admin_api_whitelist_invalid_items,
        "admin_api_whitelist_invalid_count": len(admin_api_whitelist_invalid_items),
        "admin_api_whitelist_enabled": bool(admin_api_whitelist_effective_count > 0),
        "trust_x_forwarded_for": TRUST_X_FORWARDED_FOR,
        "trusted_proxy_ips": sorted(TRUSTED_PROXY_IPS),
        "sub_link_sign_enabled": bool(SUB_LINK_SIGN_KEY),
        "sub_link_require_signature": SUB_LINK_REQUIRE_SIGNATURE,
        "sub_link_default_ttl_seconds": SUB_LINK_DEFAULT_TTL_SECONDS,
        "api_rate_limit_enabled": API_RATE_LIMIT_ENABLED,
        "api_rate_limit_window_seconds": API_RATE_LIMIT_WINDOW_SECONDS,
        "api_rate_limit_max_requests": API_RATE_LIMIT_MAX_REQUESTS,
        "admin_overview_cache_ttl_seconds": int(_ADMIN_OVERVIEW_CACHE_TTL_SECONDS),
        "admin_security_status_cache_ttl_seconds": int(_SECURITY_STATUS_CACHE_TTL_SECONDS),
        "api_docs_enabled": bool(API_DOCS_ENABLED),
        "unauthorized_audit_sample_seconds": UNAUTHORIZED_AUDIT_SAMPLE_SECONDS,
        "unauthorized_audit_sampling_enabled": bool(UNAUTHORIZED_AUDIT_SAMPLE_SECONDS > 0),
        "audit_log_retention_days": AUDIT_LOG_RETENTION_DAYS,
        "audit_log_cleanup_interval_seconds": AUDIT_LOG_CLEANUP_INTERVAL_SECONDS,
        "audit_log_cleanup_batch_size": AUDIT_LOG_CLEANUP_BATCH_SIZE,
        "security_block_cleanup_interval_seconds": SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS,
        "security_block_protected_ips": SECURITY_BLOCK_PROTECTED_IPS_ITEMS,
        "security_block_protected_ips_count": len(SECURITY_BLOCK_PROTECTED_IPS_ITEMS),
        "security_block_protected_ips_effective_count": int(protected_effective_count),
        "security_block_protected_ips_invalid": protected_invalid_items,
        "security_block_protected_ips_invalid_count": len(protected_invalid_items),
        "security_auto_block_enabled": bool(SECURITY_AUTO_BLOCK_ENABLED),
        "security_auto_block_interval_seconds": int(SECURITY_AUTO_BLOCK_INTERVAL_SECONDS),
        "security_auto_block_window_seconds": int(SECURITY_AUTO_BLOCK_WINDOW_SECONDS),
        "security_auto_block_threshold": int(SECURITY_AUTO_BLOCK_THRESHOLD),
        "security_auto_block_duration_seconds": int(SECURITY_AUTO_BLOCK_DURATION_SECONDS),
        "security_auto_block_max_per_interval": int(SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL),
        "blocked_ip_count": active_block_count,
        "security_events_exclude_local": bool(SECURITY_EVENTS_EXCLUDE_LOCAL),
        "node_task_max_pending_per_node": NODE_TASK_MAX_PENDING_PER_NODE,
        "warnings": warnings,
    }


def get_security_status_payload_cached(now_ts: int) -> Dict[str, Union[bool, int, List[str]]]:
    global _SECURITY_STATUS_CACHE_EXPIRE_AT
    global _SECURITY_STATUS_CACHE_PAYLOAD
    cache_ttl = int(_SECURITY_STATUS_CACHE_TTL_SECONDS)
    if cache_ttl <= 0:
        return build_security_status_payload(now_ts=now_ts)

    cached_payload = _SECURITY_STATUS_CACHE_PAYLOAD
    if cached_payload is not None and now_ts < int(_SECURITY_STATUS_CACHE_EXPIRE_AT):
        return cached_payload

    with _SECURITY_STATUS_CACHE_LOCK:
        cached_payload = _SECURITY_STATUS_CACHE_PAYLOAD
        if cached_payload is not None and now_ts < int(_SECURITY_STATUS_CACHE_EXPIRE_AT):
            return cached_payload
        fresh_payload = build_security_status_payload(now_ts=now_ts)
        _SECURITY_STATUS_CACHE_PAYLOAD = fresh_payload
        _SECURITY_STATUS_CACHE_EXPIRE_AT = int(now_ts + cache_ttl)
        return fresh_payload


def get_invalid_ip_or_cidr_items(items: List[str]) -> List[str]:
    invalid_items: List[str] = []
    seen = set()
    for raw in items:
        value = str(raw or "").strip()
        if not value:
            continue
        try:
            ipaddress.ip_address(value)
            continue
        except ValueError:
            pass
        try:
            ipaddress.ip_network(value, strict=False)
            continue
        except ValueError:
            if value not in seen:
                invalid_items.append(value)
                seen.add(value)
    return invalid_items


def normalize_source_ip(value: str) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        raise HTTPException(status_code=400, detail="source_ip is required")
    try:
        return str(ipaddress.ip_address(raw_value))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="source_ip must be valid IPv4/IPv6") from exc


def build_protected_source_rules(
    conn, request_source_ip: str
) -> Tuple[Set[str], List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]]]:
    protected_ips: Set[str] = {
        "127.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
    }
    protected_networks: List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]] = []

    def _add_protected_value(raw_value: str) -> None:
        value = str(raw_value or "").strip()
        if not value:
            return
        try:
            protected_ips.add(str(ipaddress.ip_address(value)))
            return
        except ValueError:
            pass
        try:
            network = ipaddress.ip_network(value, strict=False)
            if int(network.num_addresses) == 1:
                protected_ips.add(str(network.network_address))
            else:
                protected_networks.append(network)
        except ValueError:
            return

    _add_protected_value(request_source_ip)

    rows = conn.execute(
        """
        SELECT agent_ip
        FROM nodes
        WHERE enabled = 1
          AND agent_ip IS NOT NULL
          AND TRIM(agent_ip) != ''
        """
    ).fetchall()
    for row in rows:
        _add_protected_value(str(row["agent_ip"] or ""))

    for item in CONTROLLER_PORT_WHITELIST_ITEMS:
        _add_protected_value(str(item or ""))
    for item in SECURITY_BLOCK_PROTECTED_IPS_ITEMS:
        _add_protected_value(str(item or ""))

    return protected_ips, protected_networks


def is_source_ip_protected(
    source_ip: str,
    protected_ips: Set[str],
    protected_networks: List[Union[ipaddress.IPv4Network, ipaddress.IPv6Network]],
) -> bool:
    normalized = str(source_ip or "").strip()
    if not normalized:
        return False
    try:
        ip_obj = ipaddress.ip_address(normalized)
    except ValueError:
        return normalized in protected_ips
    if str(ip_obj) in protected_ips:
        return True
    for network in protected_networks:
        if ip_obj in network:
            return True
    return False


def _run_local_export_script(
    script_path: Path, output_path: Path, timeout_seconds: int
) -> Tuple[bool, str]:
    if not script_path.exists():
        return False, "script not found: {0}".format(script_path)
    if not script_path.is_file():
        return False, "script path is not a file: {0}".format(script_path)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return False, "create output directory failed: {0}".format(exc)
    try:
        proc = subprocess.run(  # nosec B603
            ["bash", str(script_path), "--output", str(output_path)],
            capture_output=True,
            text=True,
            timeout=max(5, int(timeout_seconds)),
            check=False,
        )
    except FileNotFoundError:
        return False, "bash not found"
    except subprocess.TimeoutExpired:
        return False, "export command timeout"
    if int(proc.returncode or 0) != 0:
        stderr_text = str(proc.stderr or "").strip()
        stdout_text = str(proc.stdout or "").strip()
        detail = stderr_text if stderr_text else stdout_text
        return False, detail or "export command failed"
    if not output_path.exists():
        return False, "export output file not found"
    return True, ""


def export_admin_ai_context_snapshot(output_path: Path) -> Tuple[bool, str]:
    return _run_local_export_script(
        script_path=ADMIN_AI_CONTEXT_EXPORT_SCRIPT,
        output_path=output_path,
        timeout_seconds=ADMIN_AI_CONTEXT_EXPORT_TIMEOUT_SECONDS,
    )


def export_admin_ops_snapshot(output_path: Path) -> Tuple[bool, str]:
    return _run_local_export_script(
        script_path=ADMIN_OPS_SNAPSHOT_SCRIPT,
        output_path=output_path,
        timeout_seconds=ADMIN_OPS_SNAPSHOT_TIMEOUT_SECONDS,
    )


def run_ufw_command(args: List[str], timeout_seconds: int = 20) -> Tuple[int, str, str]:
    try:
        proc = subprocess.run(  # nosec B603
            args,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        return int(proc.returncode or 0), str(proc.stdout or ""), str(proc.stderr or "")
    except FileNotFoundError:
        return 127, "", "ufw not found"
    except subprocess.TimeoutExpired as exc:
        stdout_text = str(exc.stdout or "")
        stderr_text = str(exc.stderr or "")
        return 124, stdout_text, stderr_text or "ufw command timeout"


def is_ufw_rule_exists_message(text: str) -> bool:
    merged = str(text or "").lower()
    return ("skip" in merged and "existing" in merged) or ("already" in merged and "rule" in merged)


def is_ufw_invalid_syntax_message(text: str) -> bool:
    merged = str(text or "").lower()
    return "invalid syntax" in merged


def build_ufw_deny_arg_sets(source_ip: str) -> List[List[str]]:
    port_text = str(CONTROLLER_PORT)
    return [
        ["ufw", "deny", "proto", "tcp", "from", source_ip, "to", "any", "port", port_text],
        ["ufw", "deny", "from", source_ip, "to", "any", "port", port_text, "proto", "tcp"],
    ]


def build_ufw_delete_arg_sets(source_ip: str) -> List[List[str]]:
    port_text = str(CONTROLLER_PORT)
    return [
        [
            "ufw",
            "--force",
            "delete",
            "deny",
            "proto",
            "tcp",
            "from",
            source_ip,
            "to",
            "any",
            "port",
            port_text,
        ],
        [
            "ufw",
            "--force",
            "delete",
            "deny",
            "from",
            source_ip,
            "to",
            "any",
            "port",
            port_text,
            "proto",
            "tcp",
        ],
    ]


def has_ufw_ip_block(source_ip: str) -> bool:
    code, stdout, stderr = run_ufw_command(["ufw", "status", "numbered"])
    if code == 127:
        raise HTTPException(status_code=503, detail="ufw is not available on controller host")
    if code != 0:
        # 状态读取失败时回退到现有新增逻辑，避免误判中断业务。
        return False
    source_ip_lower = str(source_ip or "").strip().lower()
    if not source_ip_lower:
        return False
    port_marker = "{0}/tcp".format(int(CONTROLLER_PORT))
    for raw_line in str(stdout or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        line_lower = line.lower()
        if "deny" not in line_lower:
            continue
        if source_ip_lower not in line_lower:
            continue
        if port_marker in line_lower:
            return True
        if "anywhere" in line_lower:
            return True
    return False


def apply_ufw_ip_block(source_ip: str) -> Dict[str, Union[bool, str]]:
    if has_ufw_ip_block(source_ip):
        return {"ok": True, "result": "existing", "already_blocked": True}
    last_error = ""
    for args in build_ufw_deny_arg_sets(source_ip):
        code, stdout, stderr = run_ufw_command(args)
        merged = "{0}\n{1}".format(stdout, stderr).strip()
        if code == 0:
            return {"ok": True, "result": (stdout or stderr or "ok").strip(), "already_blocked": False}
        if code == 127:
            raise HTTPException(status_code=503, detail="ufw is not available on controller host")
        if is_ufw_rule_exists_message(merged):
            return {"ok": True, "result": (stdout or stderr or "existing").strip(), "already_blocked": True}
        last_error = (stderr or stdout or "unknown error").strip()
        if not is_ufw_invalid_syntax_message(merged):
            break
    raise HTTPException(
        status_code=500,
        detail="ufw deny failed: {0}".format(last_error or "unknown error"),
    )


def find_ufw_block_rule_numbers(source_ip: str) -> List[int]:
    code, stdout, stderr = run_ufw_command(["ufw", "status", "numbered"])
    if code == 127:
        raise HTTPException(status_code=503, detail="ufw is not available on controller host")
    if code != 0:
        # 状态读取失败时返回空列表，调用方会走兼容删除逻辑。
        _ = stderr
        return []
    source_ip_lower = str(source_ip or "").strip().lower()
    if not source_ip_lower:
        return []
    port_marker = "{0}/tcp".format(int(CONTROLLER_PORT))
    result: List[int] = []
    for raw_line in str(stdout or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        line_lower = line.lower()
        if "deny" not in line_lower:
            continue
        if source_ip_lower not in line_lower:
            continue
        if port_marker not in line_lower:
            continue
        matched = re.match(r"^\[\s*([0-9]+)\]", line)
        if not matched:
            continue
        try:
            result.append(int(matched.group(1)))
        except (TypeError, ValueError):
            continue
    result.sort(reverse=True)
    return result


def remove_ufw_ip_block(source_ip: str) -> Dict[str, Union[int, str]]:
    rule_numbers = find_ufw_block_rule_numbers(source_ip)
    if rule_numbers:
        removed_by_number = 0
        last_output = ""
        for rule_no in rule_numbers:
            code, stdout, stderr = run_ufw_command(
                ["ufw", "--force", "delete", str(int(rule_no))]
            )
            merged = "{0}\n{1}".format(stdout, stderr).strip()
            last_output = merged
            if code == 127:
                raise HTTPException(status_code=503, detail="ufw is not available on controller host")
            if code == 0:
                removed_by_number += 1
                continue
            merged_lower = merged.lower()
            if "non-existent" in merged_lower or "not found" in merged_lower:
                continue
            raise HTTPException(
                status_code=500,
                detail="ufw delete failed: {0}".format((stderr or stdout or "unknown error").strip()),
            )
        return {"removed": int(removed_by_number), "result": last_output}

    removed = 0
    last_output = ""
    for _ in range(128):
        should_continue = False
        handled = False
        for args in build_ufw_delete_arg_sets(source_ip):
            code, stdout, stderr = run_ufw_command(args)
            merged = "{0}\n{1}".format(stdout, stderr).strip()
            merged_lower = merged.lower()
            last_output = merged
            if code == 0:
                removed += 1
                should_continue = True
                handled = True
                break
            if code == 127:
                raise HTTPException(status_code=503, detail="ufw is not available on controller host")
            if "non-existent" in merged_lower or "not found" in merged_lower:
                return {"removed": int(removed), "result": last_output}
            if is_ufw_invalid_syntax_message(merged):
                continue
            raise HTTPException(
                status_code=500,
                detail="ufw delete failed: {0}".format((stderr or stdout or "unknown error").strip()),
            )
        if should_continue:
            continue
        if handled:
            continue
        break
    return {"removed": int(removed), "result": last_output}


def get_protected_source_ips(conn, request_source_ip: str) -> List[str]:
    protected_ips, _ = build_protected_source_rules(conn, request_source_ip=request_source_ip)
    return sorted(protected_ips)


def cleanup_expired_ip_blocks(conn, now_ts: int) -> Dict[str, Union[int, List[str]]]:
    expired_rows = conn.execute(
        """
        SELECT source_ip
        FROM security_ip_blocks
        WHERE expire_at > 0 AND expire_at <= ?
        ORDER BY expire_at ASC, source_ip ASC
        LIMIT 200
        """,
        (int(now_ts),),
    ).fetchall()
    released = 0
    failed_items: List[str] = []
    for row in expired_rows:
        source_ip = str(row["source_ip"] or "").strip()
        if not source_ip:
            continue
        try:
            remove_ufw_ip_block(source_ip)
            conn.execute("DELETE FROM security_ip_blocks WHERE source_ip = ?", (source_ip,))
            released += 1
        except HTTPException:
            failed_items.append(source_ip)
    return {"released": int(released), "failed": failed_items}


def cleanup_expired_ip_blocks_once(now_ts: int) -> Dict[str, Union[int, List[str]]]:
    with get_connection() as conn:
        result = cleanup_expired_ip_blocks(conn, now_ts=int(now_ts))
        conn.commit()
    if int(result.get("released", 0) or 0) > 0:
        invalidate_admin_snapshots_cache()
    return result


def run_security_maintenance_cleanup(conn, now_ts: int) -> Dict[str, Union[int, List[str]]]:
    total_released_blocks = 0
    failed_block_items = set()
    block_cleanup_rounds = 0
    for _ in range(20):
        block_cleanup_rounds += 1
        block_report = cleanup_expired_ip_blocks(conn, now_ts=int(now_ts))
        released = int(block_report.get("released", 0) or 0)
        total_released_blocks += released
        failed = block_report.get("failed", [])
        if isinstance(failed, list):
            for item in failed[:200]:
                value = str(item or "").strip()
                if value:
                    failed_block_items.add(value)
        if released <= 0:
            break

    total_removed_audit_logs = 0
    audit_cleanup_rounds = 0
    for _ in range(20):
        audit_cleanup_rounds += 1
        removed = int(
            cleanup_old_audit_logs(
                conn,
                now_ts=int(now_ts),
                retention_days=AUDIT_LOG_RETENTION_DAYS,
                batch_size=AUDIT_LOG_CLEANUP_BATCH_SIZE,
            )
            or 0
        )
        if removed <= 0:
            break
        total_removed_audit_logs += removed
        if removed < AUDIT_LOG_CLEANUP_BATCH_SIZE:
            break

    active_block_count = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM security_ip_blocks
            WHERE expire_at = 0 OR expire_at > ?
            """,
            (int(now_ts),),
        ).fetchone()["c"]
        or 0
    )
    return {
        "cleaned_expired_blocks": int(total_released_blocks),
        "cleanup_failed_blocks": sorted(failed_block_items),
        "cleaned_audit_logs": int(total_removed_audit_logs),
        "active_blocked_ips": int(active_block_count),
        "audit_retention_days": int(AUDIT_LOG_RETENTION_DAYS),
        "audit_cleanup_batch_size": int(AUDIT_LOG_CLEANUP_BATCH_SIZE),
        "block_cleanup_rounds": int(block_cleanup_rounds),
        "audit_cleanup_rounds": int(audit_cleanup_rounds),
    }


def run_security_auto_block_once(conn, now_ts: int) -> Dict[str, Union[int, List[str], bool]]:
    if not SECURITY_AUTO_BLOCK_ENABLED:
        return {
            "enabled": False,
            "blocked_count": 0,
            "blocked_items": [],
            "failed_items": [],
            "skipped_items": [],
            "window_seconds": int(SECURITY_AUTO_BLOCK_WINDOW_SECONDS),
            "threshold": int(SECURITY_AUTO_BLOCK_THRESHOLD),
            "duration_seconds": int(SECURITY_AUTO_BLOCK_DURATION_SECONDS),
            "max_per_interval": int(SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL),
        }

    snapshot = build_unauthorized_events_snapshot(
        now_ts=int(now_ts),
        window_seconds=int(SECURITY_AUTO_BLOCK_WINDOW_SECONDS),
        top_limit=max(20, int(SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL) * 20),
        include_local=False,
        conn=conn,
    )
    top_rows = snapshot.get("top_unauthorized_ips", [])
    if not isinstance(top_rows, list):
        top_rows = []

    active_rows = conn.execute(
        """
        SELECT source_ip
        FROM security_ip_blocks
        WHERE expire_at = 0 OR expire_at > ?
        """,
        (int(now_ts),),
    ).fetchall()
    active_ip_set = set()
    for row in active_rows:
        value = str(row["source_ip"] or "").strip()
        if value:
            active_ip_set.add(value)

    protected_ip_set, protected_networks = build_protected_source_rules(conn, request_source_ip="")
    blocked_items: List[str] = []
    failed_items: List[str] = []
    skipped_items: List[str] = []
    max_blocks = int(SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL)

    for item in top_rows:
        if len(blocked_items) >= max_blocks:
            break
        if not isinstance(item, dict):
            continue
        source_ip = str(item.get("source_ip", "")).strip()
        try:
            hit_count = int(item.get("count", 0) or 0)
        except (TypeError, ValueError):
            hit_count = 0
        if not source_ip:
            continue
        if hit_count < int(SECURITY_AUTO_BLOCK_THRESHOLD):
            continue
        try:
            ip_obj = ipaddress.ip_address(source_ip)
        except ValueError:
            skipped_items.append("{0}:invalid".format(source_ip))
            continue
        if not ip_obj.is_global:
            skipped_items.append("{0}:non_global".format(source_ip))
            continue
        if is_source_ip_protected(source_ip, protected_ip_set, protected_networks):
            skipped_items.append("{0}:protected".format(source_ip))
            continue
        if source_ip in active_ip_set:
            skipped_items.append("{0}:already_blocked".format(source_ip))
            continue

        duration_seconds = int(SECURITY_AUTO_BLOCK_DURATION_SECONDS)
        expire_at = 0 if duration_seconds == 0 else int(now_ts + duration_seconds)
        try:
            apply_ufw_ip_block(source_ip)
            conn.execute(
                """
                INSERT INTO security_ip_blocks(source_ip, created_at, expire_at, reason, operator)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(source_ip) DO UPDATE SET
                    created_at=excluded.created_at,
                    expire_at=excluded.expire_at,
                    reason=excluded.reason,
                    operator=excluded.operator
                """,
                (
                    source_ip,
                    int(now_ts),
                    int(expire_at),
                    "auto-security-events",
                    "system:auto-block",
                ),
            )
            write_audit_log(
                conn,
                action="admin.security.auto_block",
                resource_type="ip",
                resource_id=source_ip,
                detail={
                    "count": int(hit_count),
                    "window_seconds": int(SECURITY_AUTO_BLOCK_WINDOW_SECONDS),
                    "threshold": int(SECURITY_AUTO_BLOCK_THRESHOLD),
                    "duration_seconds": int(duration_seconds),
                    "expire_at": int(expire_at),
                    "controller_port": int(CONTROLLER_PORT),
                },
                actor="system:auto-block",
                source_ip="",
                created_at=int(now_ts),
            )
            blocked_items.append(source_ip)
            active_ip_set.add(source_ip)
        except HTTPException:
            failed_items.append(source_ip)

    result = {
        "enabled": True,
        "blocked_count": int(len(blocked_items)),
        "blocked_items": blocked_items,
        "failed_items": failed_items,
        "skipped_items": skipped_items[:50],
        "window_seconds": int(SECURITY_AUTO_BLOCK_WINDOW_SECONDS),
        "threshold": int(SECURITY_AUTO_BLOCK_THRESHOLD),
        "duration_seconds": int(SECURITY_AUTO_BLOCK_DURATION_SECONDS),
        "max_per_interval": int(max_blocks),
    }
    if int(result.get("blocked_count", 0) or 0) > 0:
        invalidate_admin_snapshots_cache()
    return result


def build_admin_overview_payload(now_ts: int) -> Dict[str, Union[int, Dict, List]]:
    with get_connection() as conn:
        users_row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_count,
              SUM(CASE WHEN status = 'disabled' THEN 1 ELSE 0 END) AS disabled_count
            FROM users
            """
        ).fetchone()
        users_total = int(users_row["total"] or 0)
        users_active = int(users_row["active_count"] or 0)
        users_disabled = int(users_row["disabled_count"] or 0)

        nodes_row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled_count
            FROM nodes
            """
        ).fetchone()
        nodes_total = int(nodes_row["total"] or 0)
        nodes_enabled = int(nodes_row["enabled_count"] or 0)

        bindings_total = int(
            conn.execute("SELECT COUNT(*) AS c FROM user_nodes").fetchone()["c"] or 0
        )
        monitor_rows = conn.execute(
            """
            SELECT node_code, last_seen_at
            FROM nodes
            WHERE monitor_enabled = 1
            ORDER BY node_code
            """
        ).fetchall()
        task_rows = conn.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM node_tasks
            GROUP BY status
            """
        ).fetchall()
        pending_by_node_rows = conn.execute(
            """
            SELECT node_code, COUNT(*) AS c
            FROM node_tasks
            WHERE status = 'pending' AND attempts < max_attempts
            GROUP BY node_code
            ORDER BY c DESC, node_code ASC
            LIMIT 10
            """
        ).fetchall()
        unauthorized_24h_snapshot = build_unauthorized_events_snapshot(
            now_ts=now_ts,
            window_seconds=86400,
            top_limit=5,
            conn=conn,
        )
        unauthorized_1h_snapshot = build_unauthorized_events_snapshot(
            now_ts=now_ts,
            window_seconds=3600,
            top_limit=3,
            conn=conn,
        )
        node_task_idempotency_24h = get_node_task_idempotency_snapshot(
            now_ts=now_ts,
            window_seconds=86400,
            top_limit=5,
            conn=conn,
        )

    monitor_enabled_count = len(monitor_rows)
    monitor_online = 0
    monitor_offline = 0
    monitor_never_seen = 0
    offline_items: List[Dict[str, Union[str, int]]] = []
    for row in monitor_rows:
        node_code = str(row["node_code"] or "")
        try:
            last_seen_at = int(row["last_seen_at"] or 0)
        except (TypeError, ValueError):
            last_seen_at = 0
        is_online = (
            last_seen_at > 0
            and (now_ts - last_seen_at) <= NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS
        )
        if is_online:
            monitor_online += 1
            continue
        monitor_offline += 1
        if last_seen_at <= 0:
            monitor_never_seen += 1
        offline_items.append({"node_code": node_code, "last_seen_at": last_seen_at})

    task_counts: Dict[str, int] = {
        "pending": 0,
        "running": 0,
        "failed": 0,
        "timeout": 0,
        "success": 0,
    }
    for row in task_rows:
        status_value = str(row["status"] or "").strip().lower()
        if status_value in task_counts:
            task_counts[status_value] = int(row["c"] or 0)

    pending_by_node: List[Dict[str, Union[str, int]]] = []
    for row in pending_by_node_rows:
        pending_by_node.append(
            {"node_code": str(row["node_code"] or ""), "pending": int(row["c"] or 0)}
        )
    queue_cap_per_node = int(NODE_TASK_MAX_PENDING_PER_NODE)
    near_cap_threshold = max(1, int((queue_cap_per_node * 8 + 9) / 10))
    near_cap_nodes: List[Dict[str, Union[str, int]]] = []
    for item in pending_by_node:
        try:
            pending_count = int(item.get("pending", 0) or 0)
        except (TypeError, ValueError):
            pending_count = 0
        if pending_count >= near_cap_threshold:
            near_cap_nodes.append(item)

    return {
        "generated_at": now_ts,
        "totals": {
            "users": users_total,
            "active_users": users_active,
            "disabled_users": users_disabled,
            "nodes": nodes_total,
            "enabled_nodes": nodes_enabled,
            "bindings": bindings_total,
        },
        "monitor": {
            "threshold_seconds": NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS,
            "enabled_nodes": monitor_enabled_count,
            "online_nodes": monitor_online,
            "offline_nodes": monitor_offline,
            "never_seen_nodes": monitor_never_seen,
            "offline_items": offline_items,
        },
        "tasks": {
            "pending": int(task_counts.get("pending", 0)),
            "running": int(task_counts.get("running", 0)),
            "failed": int(task_counts.get("failed", 0)),
            "timeout": int(task_counts.get("timeout", 0)),
            "success": int(task_counts.get("success", 0)),
            "queue_cap_per_node": queue_cap_per_node,
            "near_cap_threshold": near_cap_threshold,
            "near_cap_nodes": near_cap_nodes,
            "pending_by_node": pending_by_node,
            "idempotency_24h": node_task_idempotency_24h,
        },
        "security": build_security_status_payload(conn=conn, now_ts=now_ts),
        "security_events": {
            "unauthorized_1h": int(unauthorized_1h_snapshot["unauthorized"]),
            "unauthorized_24h": int(unauthorized_24h_snapshot["unauthorized"]),
            "top_unauthorized_ips": unauthorized_24h_snapshot["top_unauthorized_ips"],
        },
    }


def get_admin_overview_payload_cached(now_ts: int) -> Dict[str, Union[int, Dict, List]]:
    global _ADMIN_OVERVIEW_CACHE_EXPIRE_AT
    global _ADMIN_OVERVIEW_CACHE_PAYLOAD
    cache_ttl = int(_ADMIN_OVERVIEW_CACHE_TTL_SECONDS)
    if cache_ttl <= 0:
        return build_admin_overview_payload(now_ts=now_ts)

    cached_payload = _ADMIN_OVERVIEW_CACHE_PAYLOAD
    if cached_payload is not None and now_ts < int(_ADMIN_OVERVIEW_CACHE_EXPIRE_AT):
        return cached_payload

    with _ADMIN_OVERVIEW_CACHE_LOCK:
        cached_payload = _ADMIN_OVERVIEW_CACHE_PAYLOAD
        if cached_payload is not None and now_ts < int(_ADMIN_OVERVIEW_CACHE_EXPIRE_AT):
            return cached_payload
        fresh_payload = build_admin_overview_payload(now_ts=now_ts)
        _ADMIN_OVERVIEW_CACHE_PAYLOAD = fresh_payload
        _ADMIN_OVERVIEW_CACHE_EXPIRE_AT = int(now_ts + cache_ttl)
        return fresh_payload


def get_node_task_idempotency_snapshot(
    now_ts: int,
    window_seconds: int = 86400,
    top_limit: int = 10,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Union[int, float, List[Dict[str, Union[str, int, float]]]]]:
    if window_seconds < 60:
        window_seconds = 60
    if window_seconds > 30 * 86400:
        window_seconds = 30 * 86400
    if top_limit < 1:
        top_limit = 1
    if top_limit > 20:
        top_limit = 20
    since_ts = int(now_ts - window_seconds)

    if conn is None:
        with get_connection() as local_conn:
            return get_node_task_idempotency_snapshot(
                now_ts=now_ts,
                window_seconds=window_seconds,
                top_limit=top_limit,
                conn=local_conn,
            )

    created_count = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM audit_logs
            WHERE action = 'node.task.create' AND created_at >= ?
            """,
            (since_ts,),
        ).fetchone()["c"]
        or 0
    )
    deduplicated_count = int(
        conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM audit_logs
            WHERE action = 'node.task.deduplicated' AND created_at >= ?
            """,
            (since_ts,),
        ).fetchone()["c"]
        or 0
    )
    per_node_rows = conn.execute(
        """
        SELECT
          COALESCE(NULLIF(TRIM(json_extract(detail, '$.node_code')), ''), 'unknown') AS node_code,
          SUM(CASE WHEN action = 'node.task.create' THEN 1 ELSE 0 END) AS created,
          SUM(CASE WHEN action = 'node.task.deduplicated' THEN 1 ELSE 0 END) AS deduplicated,
          COUNT(*) AS incoming_total
        FROM audit_logs
        WHERE action IN ('node.task.create', 'node.task.deduplicated')
          AND created_at >= ?
        GROUP BY node_code
        ORDER BY incoming_total DESC, node_code ASC
        LIMIT ?
        """,
        (since_ts, top_limit),
    ).fetchall()

    by_node_items: List[Dict[str, Union[str, int, float]]] = []
    for row in per_node_rows:
        node_code = str(row["node_code"] or "").strip() or "unknown"
        create_count = int(row["created"] or 0)
        dedup_count = int(row["deduplicated"] or 0)
        total_incoming = int(row["incoming_total"] or 0)
        dedup_ratio = float((dedup_count / total_incoming) if total_incoming > 0 else 0.0)
        by_node_items.append(
            {
                "node_code": node_code,
                "created": create_count,
                "deduplicated": dedup_count,
                "incoming_total": total_incoming,
                "dedup_ratio": round(dedup_ratio, 4),
            }
        )

    incoming_total = int(created_count + deduplicated_count)
    dedup_ratio_total = float((deduplicated_count / incoming_total) if incoming_total > 0 else 0.0)
    return {
        "window_seconds": int(window_seconds),
        "since": int(since_ts),
        "incoming_total": incoming_total,
        "created": int(created_count),
        "deduplicated": int(deduplicated_count),
        "dedup_ratio": round(dedup_ratio_total, 4),
        "top_nodes": by_node_items,
    }


@router.get(
    "/admin/node_tasks/idempotency",
    summary="Node task idempotency snapshot",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "统计任务创建与去重命中情况，用于观察任务下发幂等性。"
    ),
    response_model=None,
)
def get_admin_node_task_idempotency(
    window_seconds: int = 86400,
    top: int = 10,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[int, float, List[Dict[str, Union[str, int, float]]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return get_node_task_idempotency_snapshot(
        now_ts=int(time.time()),
        window_seconds=int(window_seconds or 0),
        top_limit=int(top or 0),
    )


def cleanup_archives_by_count(
    directory: Path, name_prefix: str, keep_count: int
) -> int:
    if keep_count < 1:
        keep_count = 1
    if not directory.exists():
        return 0
    files = []
    for item in directory.iterdir():
        if item.is_file() and item.name.startswith(name_prefix) and item.name.endswith(".tar.gz"):
            files.append(item)
    def _mtime(file_path: Path) -> float:
        try:
            return float(file_path.stat().st_mtime)
        except OSError:
            return 0.0

    files.sort(key=_mtime, reverse=True)
    removed_count = 0
    for old_file in files[keep_count:]:
        try:
            old_file.unlink()
            removed_count += 1
        except OSError:
            continue
    return removed_count


@router.post(
    "/admin/emergency/disable_users",
    summary="Emergency stop: disable all active users",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "默认 dry_run=1 仅预览影响范围；dry_run=0 才会实际禁用全部 active 用户。"
    ),
    response_model=None,
)
def emergency_disable_active_users(
    request: Request,
    dry_run: int = 1,
    reason: str = "",
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, List[str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    dry_run_value = int(dry_run)
    if dry_run_value not in (0, 1):
        raise HTTPException(status_code=400, detail="dry_run must be 0 or 1")
    dry_run_flag = bool(dry_run_value == 1)
    reason_text = str(reason or "").strip()
    now_ts = int(time.time())

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT u.user_code, COUNT(un.node_code) AS binding_count
            FROM users u
            LEFT JOIN user_nodes un ON un.user_code = u.user_code
            WHERE u.status = 'active'
            GROUP BY u.user_code
            ORDER BY u.id ASC
            LIMIT 20000
            """
        ).fetchall()
        active_user_codes = [
            str(row["user_code"] or "").strip()
            for row in rows
            if str(row["user_code"] or "").strip()
        ]
        active_user_count = len(active_user_codes)
        affected_bindings = 0
        for row in rows:
            try:
                affected_bindings += int(row["binding_count"] or 0)
            except (TypeError, ValueError):
                continue

        changed_count = 0
        if not dry_run_flag and active_user_count > 0:
            update_result = conn.execute(
                "UPDATE users SET status = 'disabled' WHERE status = 'active'"
            )
            changed_count = int(update_result.rowcount or 0)

        audit_action = "admin.emergency.disable_users.preview" if dry_run_flag else "admin.emergency.disable_users.apply"
        write_audit_log(
            conn,
            action=audit_action,
            resource_type="emergency",
            resource_id="users",
            detail={
                "dry_run": dry_run_flag,
                "active_user_count": int(active_user_count),
                "changed_user_count": int(changed_count),
                "affected_bindings": int(affected_bindings),
                "reason": reason_text,
                "sample_user_codes": active_user_codes[:20],
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

    return {
        "ok": True,
        "dry_run": dry_run_flag,
        "active_user_count": int(active_user_count),
        "changed_user_count": int(changed_count),
        "affected_bindings": int(affected_bindings),
        "sample_user_codes": active_user_codes[:20],
        "sample_truncated": bool(active_user_count > 20),
        "reason": reason_text,
        "created_at": now_ts,
    }


@router.post(
    "/admin/backup",
    summary="Create controller backup",
    description=ADMIN_AUTH_DESCRIPTION,
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
        cleaned_files = cleanup_archives_by_count(
            backup_dir, "backup-", BACKUP_RETENTION_COUNT
        )
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Backup failed: {0}".format(exc)) from exc

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.backup.create",
            resource_type="backup",
            resource_id=backup_name,
            detail={
                "path": str(backup_path),
                "size_bytes": size_bytes,
                "cleaned_files": cleaned_files,
                "keep_count": BACKUP_RETENTION_COUNT,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=created_at,
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(backup_path),
        "size_bytes": size_bytes,
        "cleaned_files": cleaned_files,
        "keep_count": BACKUP_RETENTION_COUNT,
        "created_at": created_at,
    }


@router.post(
    "/admin/diagnostics/ai_context_export",
    summary="Export AI diagnostic context package",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def export_ai_context_package(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    created_at = int(time.time())
    output_name = "sb-admin-ai-context-manual-{0}.md".format(
        time.strftime("%Y%m%d-%H%M%S", time.localtime(created_at))
    )
    output_path = Path("/tmp") / output_name
    export_ok, export_error = export_admin_ai_context_snapshot(output_path)
    if not export_ok:
        raise HTTPException(
            status_code=500,
            detail="ai context export failed: {0}".format(export_error or "unknown"),
        )

    try:
        size_bytes = int(output_path.stat().st_size)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail="read ai context file failed: {0}".format(exc),
        ) from exc

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.diagnostics.ai_context_export",
            resource_type="diagnostics",
            resource_id=output_name,
            detail={
                "path": str(output_path),
                "size_bytes": size_bytes,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=created_at,
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(output_path),
        "size_bytes": size_bytes,
        "created_at": created_at,
    }


@router.post(
    "/admin/diagnostics/ops_snapshot",
    summary="Export admin operations snapshot",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def export_ops_snapshot_package(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    created_at = int(time.time())
    output_name = "sb-admin-ops-snapshot-manual-{0}.txt".format(
        time.strftime("%Y%m%d-%H%M%S", time.localtime(created_at))
    )
    output_path = Path("/tmp") / output_name
    export_ok, export_error = export_admin_ops_snapshot(output_path)
    if not export_ok:
        raise HTTPException(
            status_code=500,
            detail="ops snapshot export failed: {0}".format(export_error or "unknown"),
        )

    try:
        size_bytes = int(output_path.stat().st_size)
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail="read ops snapshot file failed: {0}".format(exc),
        ) from exc

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.diagnostics.ops_snapshot",
            resource_type="diagnostics",
            resource_id=output_name,
            detail={
                "path": str(output_path),
                "size_bytes": size_bytes,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=created_at,
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(output_path),
        "size_bytes": size_bytes,
        "created_at": created_at,
    }


@router.post(
    "/admin/db/export",
    summary="Create logical DB export snapshot",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "用于后续跨数据库迁移前的一致性校验。"
    ),
    response_model=None,
)
def create_db_export(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, Dict]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    export_dir = Path("/var/backups/sb-controller")
    fallback_dir = BASE_DIR / "data" / "exports"
    try:
        result = export_db_snapshot(export_dir=export_dir, keep_count=BACKUP_RETENTION_COUNT)
    except OSError:
        try:
            result = export_db_snapshot(export_dir=fallback_dir, keep_count=BACKUP_RETENTION_COUNT)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=500, detail="DB export failed: {0}".format(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=500, detail="DB export failed: {0}".format(exc)) from exc

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.db.export",
            resource_type="db_export",
            resource_id=str(Path(str(result.get("path", ""))).name),
            detail={
                "path": str(result.get("path", "")),
                "size_bytes": int(result.get("size_bytes", 0) or 0),
                "schema_version": int(result.get("schema_version", 0) or 0),
                "cleaned_files": int(result.get("cleaned_files", 0) or 0),
                "keep_count": int(result.get("keep_count", 1) or 1),
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=int(result.get("created_at", int(time.time())) or int(time.time())),
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(result.get("path", "")),
        "size_bytes": int(result.get("size_bytes", 0) or 0),
        "created_at": int(result.get("created_at", 0) or 0),
        "schema_version": int(result.get("schema_version", 0) or 0),
        "table_summaries": result.get("table_summaries", {}),
        "snapshot_sha256": str(result.get("snapshot_sha256", "")),
        "cleaned_files": int(result.get("cleaned_files", 0) or 0),
        "keep_count": int(result.get("keep_count", 1) or 1),
    }


@router.post(
    "/admin/db/verify_export",
    summary="Verify logical DB export snapshot",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "可校验快照格式与校验和，并可选与当前 SQLite 数据做一致性比对。"
    ),
    response_model=None,
)
def verify_db_export(
    payload: VerifyDbExportRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, List, Dict]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    export_path = Path(str(payload.path).strip())
    if not export_path.exists():
        raise HTTPException(status_code=404, detail="export file not found")
    try:
        export_payload = load_export_payload(export_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    validation = validate_export_payload(export_payload)
    compare_live = bool(payload.compare_live)
    live_match = False
    mismatches: List = []
    live_tables: Dict = {}

    if compare_live and bool(validation.get("snapshot_valid")):
        compare_result = compare_snapshot_with_live(export_payload)
        live_match = bool(compare_result.get("live_match"))
        mismatches = compare_result.get("mismatches", [])
        live_tables = compare_result.get("live_tables", {})
        ignored_tables = compare_result.get("ignored_tables", [])
    else:
        ignored_tables = []

    ok = bool(validation.get("snapshot_valid")) and (not compare_live or live_match)
    created_at = int(export_payload.get("created_at", int(time.time())) or int(time.time()))

    with get_connection() as conn:
        write_audit_log(
            conn,
            action="admin.db.verify_export",
            resource_type="db_export",
            resource_id=export_path.name,
            detail={
                "path": str(export_path),
                "snapshot_valid": bool(validation.get("snapshot_valid")),
                "compare_live": compare_live,
                "live_match": live_match if compare_live else None,
                "mismatch_count": len(mismatches) if isinstance(mismatches, list) else 0,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=int(time.time()),
        )
        conn.commit()

    return {
        "ok": ok,
        "path": str(export_path),
        "created_at": created_at,
        "snapshot_valid": bool(validation.get("snapshot_valid")),
        "format_ok": bool(validation.get("format_ok")),
        "table_results": validation.get("table_results", {}),
        "errors": validation.get("errors", []),
        "compare_live": compare_live,
        "live_match": live_match if compare_live else None,
        "mismatches": mismatches if compare_live else [],
        "live_tables": live_tables if compare_live else {},
        "ignored_tables": ignored_tables if compare_live else [],
    }


@router.get(
    "/admin/db/integrity",
    summary="Check SQLite integrity status",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "可用于迁移前后快速确认 DB 完整性。"
    ),
    response_model=None,
)
def get_db_integrity(
    include_checksums: int = 0,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str, Dict]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return get_db_integrity_status(include_checksums=bool(include_checksums))


@router.post(
    "/admin/migrate/export",
    summary="Create migration export package",
    description=ADMIN_AUTH_DESCRIPTION,
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
        cleaned_files = cleanup_archives_by_count(
            migrate_dir, "sb-migrate-", MIGRATE_RETENTION_COUNT
        )
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
            detail={
                "path": str(backup_path),
                "size_bytes": size_bytes,
                "cleaned_files": cleaned_files,
                "keep_count": MIGRATE_RETENTION_COUNT,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=created_at,
        )
        conn.commit()

    return {
        "ok": True,
        "path": str(backup_path),
        "size_bytes": size_bytes,
        "cleaned_files": cleaned_files,
        "keep_count": MIGRATE_RETENTION_COUNT,
        "created_at": created_at,
    }


@router.get(
    "/admin/traffic/ranking",
    summary="Estimated traffic ranking (non-metered)",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "按用户限速配置与绑定节点数量给出估算排行（非真实流量计费数据）。"
    ),
    response_model=None,
)
def get_admin_traffic_ranking(
    limit: int = 20,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[int, str, List[Dict[str, Union[int, str]]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    limit_value = int(limit or 0)
    if limit_value < 1:
        limit_value = 1
    if limit_value > 100:
        limit_value = 100

    now_ts = int(time.time())
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                u.user_code,
                u.display_name,
                u.status,
                u.expire_at,
                u.speed_mbps,
                u.limit_mode,
                COUNT(un.node_code) AS bindings
            FROM users u
            LEFT JOIN user_nodes un ON un.user_code = u.user_code
            GROUP BY u.user_code
            ORDER BY u.id ASC
            LIMIT 2000
            """
        ).fetchall()

    items: List[Dict[str, Union[int, str]]] = []
    active_total = 0
    ranked_total = 0
    for row in rows:
        status_value = str(row["status"] or "").strip().lower()
        try:
            expire_at = int(row["expire_at"] or 0)
        except (TypeError, ValueError):
            expire_at = 0
        if status_value == "active":
            active_total += 1
        if status_value != "active":
            continue
        if expire_at > 0 and expire_at <= now_ts:
            continue

        try:
            bindings = int(row["bindings"] or 0)
        except (TypeError, ValueError):
            bindings = 0
        if bindings <= 0:
            continue

        try:
            speed_mbps = int(row["speed_mbps"] or 0)
        except (TypeError, ValueError):
            speed_mbps = 0
        limit_mode = str(row["limit_mode"] or "tc").strip().lower() or "tc"
        if limit_mode != "tc":
            speed_mbps = 0
        estimated_mbps = int(max(0, speed_mbps) * max(0, bindings))
        ranked_total += 1
        items.append(
            {
                "user_code": str(row["user_code"] or ""),
                "display_name": str(row["display_name"] or ""),
                "limit_mode": limit_mode,
                "speed_mbps": int(max(0, speed_mbps)),
                "bindings": int(max(0, bindings)),
                "estimated_mbps": estimated_mbps,
            }
        )

    items.sort(
        key=lambda x: (
            -int(x.get("estimated_mbps", 0) or 0),
            -int(x.get("speed_mbps", 0) or 0),
            str(x.get("user_code", "")),
        )
    )
    top_items = items[:limit_value]
    for idx, item in enumerate(top_items, start=1):
        item["rank"] = int(idx)

    return {
        "ok": True,
        "generated_at": now_ts,
        "limit": int(limit_value),
        "ranked_user_count": int(ranked_total),
        "active_user_count": int(active_total),
        "items": top_items,
        "note": "estimated_by_speed_x_bindings",
        "warning": "non-metered-estimate",
    }


@router.get(
    "/admin/node_access/status",
    summary="Node access control status",
    description=ADMIN_AUTH_DESCRIPTION,
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
    locked_enabled_nodes = []
    unlocked_enabled_nodes = []
    unlocked_disabled_nodes = []
    whitelist_networks = []
    whitelist_invalid = []
    for item in CONTROLLER_PORT_WHITELIST_ITEMS:
        try:
            whitelist_networks.append(ipaddress.ip_network(str(item), strict=False))
        except ValueError:
            whitelist_invalid.append(str(item))

    whitelist_missing_nodes = []
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
            if int(row["enabled"] or 0) == 1:
                locked_enabled_nodes.append(item)
                try:
                    agent_ip_obj = ipaddress.ip_address(agent_ip)
                    covered = any(agent_ip_obj in network for network in whitelist_networks)
                    if not covered:
                        whitelist_missing_nodes.append(
                            {"node_code": node_code, "agent_ip": agent_ip}
                        )
                except ValueError:
                    whitelist_missing_nodes.append(
                        {"node_code": node_code, "agent_ip": agent_ip}
                    )
        else:
            unlocked_nodes.append(item)
            if int(row["enabled"] or 0) == 1:
                unlocked_enabled_nodes.append(item)
            else:
                unlocked_disabled_nodes.append(item)

    enabled_total_nodes = 0
    for row in rows:
        if int(row["enabled"] or 0) == 1:
            enabled_total_nodes += 1

    return {
        "total_nodes": len(rows),
        "enabled_nodes": enabled_total_nodes,
        "locked_nodes": len(locked_nodes),
        "unlocked_nodes": len(unlocked_nodes),
        "locked_enabled_nodes": len(locked_enabled_nodes),
        "unlocked_enabled_nodes": len(unlocked_enabled_nodes),
        "unlocked_disabled_nodes": len(unlocked_disabled_nodes),
        "locked_items": locked_nodes,
        "unlocked_items": unlocked_nodes,
        "locked_enabled_items": locked_enabled_nodes,
        "unlocked_enabled_items": unlocked_enabled_nodes,
        "unlocked_disabled_items": unlocked_disabled_nodes,
        "controller_port_whitelist": CONTROLLER_PORT_WHITELIST_ITEMS,
        "whitelist_invalid_items": whitelist_invalid,
        "whitelist_missing_nodes": whitelist_missing_nodes,
        "whitelist_missing_count": len(whitelist_missing_nodes),
        "hint": "建议每个节点设置 agent_ip，并在防火墙中只放行节点IP到 controller 端口。",
    }


@router.get(
    "/admin/overview",
    summary="Admin overview status",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "返回控制面概览（用户/节点/任务队列/节点心跳/安全配置）。"
    ),
    response_model=None,
)
def get_admin_overview(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[int, Dict, List]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return get_admin_overview_payload_cached(now_ts=int(time.time()))


@router.get(
    "/admin/security/events",
    summary="Security event statistics",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "支持按时间窗口统计未授权访问。"
    ),
    response_model=None,
)
def get_admin_security_events(
    window_seconds: int = 3600,
    top: int = 5,
    include_local: Optional[int] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[int, List[Dict[str, Union[int, str]]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    include_local_flag: Optional[bool] = None
    if include_local is not None:
        if int(include_local) not in (0, 1):
            raise HTTPException(status_code=400, detail="include_local must be 0 or 1")
        include_local_flag = bool(int(include_local))
    return build_unauthorized_events_snapshot(
        now_ts=int(time.time()),
        window_seconds=int(window_seconds or 0),
        top_limit=int(top or 0),
        include_local=include_local_flag,
    )


@router.get(
    "/admin/security/status",
    summary="Security configuration status",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def get_admin_security_status(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[bool, int, str, List[str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return get_security_status_payload_cached(now_ts=int(time.time()))


@router.post(
    "/admin/security/block_ip",
    summary="Block source IP on controller port",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def block_security_source_ip(
    payload: BlockIpRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    source_ip = normalize_source_ip(payload.source_ip)
    duration_seconds = int(payload.duration_seconds or 0)
    if duration_seconds < 0:
        duration_seconds = 0
    now_ts = int(time.time())
    expire_at = 0 if duration_seconds == 0 else int(now_ts + duration_seconds)
    reason = str(payload.reason or "").strip()
    request_ip = get_source_ip_for_audit(request)

    with get_connection() as conn:
        cleanup_expired_ip_blocks(conn, now_ts=now_ts)
        protected_ips, protected_networks = build_protected_source_rules(
            conn, request_source_ip=request_ip
        )
        if is_source_ip_protected(source_ip, protected_ips, protected_networks):
            raise HTTPException(status_code=400, detail="该IP受保护，拒绝封禁")
        block_result = apply_ufw_ip_block(source_ip)
        already_blocked = bool(block_result.get("already_blocked", False))
        conn.execute(
            """
            INSERT INTO security_ip_blocks(source_ip, created_at, expire_at, reason, operator)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(source_ip) DO UPDATE SET
                created_at=excluded.created_at,
                expire_at=excluded.expire_at,
                reason=excluded.reason,
                operator=excluded.operator
            """,
            (
                source_ip,
                now_ts,
                expire_at,
                reason,
                get_request_actor(request),
            ),
        )
        write_audit_log(
            conn,
            action="admin.security.block_ip",
            resource_type="ip",
            resource_id=source_ip,
            detail={
                "duration_seconds": duration_seconds,
                "expire_at": int(expire_at),
                "controller_port": int(CONTROLLER_PORT),
                "already_blocked": bool(already_blocked),
            },
            actor=get_request_actor(request),
            source_ip=request_ip,
        )
        conn.commit()
    invalidate_admin_snapshots_cache()
    return {
        "ok": True,
        "source_ip": source_ip,
        "duration_seconds": duration_seconds,
        "expire_at": int(expire_at),
        "controller_port": int(CONTROLLER_PORT),
        "already_blocked": bool(already_blocked),
    }


@router.post(
    "/admin/security/unblock_ip",
    summary="Unblock source IP on controller port",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def unblock_security_source_ip(
    payload: UnblockIpRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    source_ip = normalize_source_ip(payload.source_ip)
    reason = str(payload.reason or "").strip()
    request_ip = get_source_ip_for_audit(request)
    with get_connection() as conn:
        remove_result = remove_ufw_ip_block(source_ip)
        conn.execute("DELETE FROM security_ip_blocks WHERE source_ip = ?", (source_ip,))
        write_audit_log(
            conn,
            action="admin.security.unblock_ip",
            resource_type="ip",
            resource_id=source_ip,
            detail={
                "reason": reason,
                "removed_rules": int(remove_result.get("removed", 0) or 0),
                "controller_port": int(CONTROLLER_PORT),
            },
            actor=get_request_actor(request),
            source_ip=request_ip,
        )
        conn.commit()
    invalidate_admin_snapshots_cache()
    return {
        "ok": True,
        "source_ip": source_ip,
        "removed_rules": int(remove_result.get("removed", 0) or 0),
    }


@router.post(
    "/admin/security/maintenance_cleanup",
    summary="Run manual security maintenance cleanup",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "执行过期封禁清理与审计日志保留清理。"
    ),
    response_model=None,
)
def run_admin_security_maintenance_cleanup(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, List[str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    now_ts = int(time.time())
    with get_connection() as conn:
        cleanup_result = run_security_maintenance_cleanup(conn, now_ts=now_ts)
        write_audit_log(
            conn,
            action="admin.security.maintenance_cleanup",
            resource_type="security",
            resource_id="manual",
            detail={
                "cleaned_expired_blocks": int(cleanup_result.get("cleaned_expired_blocks", 0) or 0),
                "cleaned_audit_logs": int(cleanup_result.get("cleaned_audit_logs", 0) or 0),
                "cleanup_failed_blocks": cleanup_result.get("cleanup_failed_blocks", []),
                "active_blocked_ips": int(cleanup_result.get("active_blocked_ips", 0) or 0),
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()
    invalidate_admin_snapshots_cache()

    return {
        "ok": True,
        "cleaned_expired_blocks": int(cleanup_result.get("cleaned_expired_blocks", 0) or 0),
        "cleanup_failed_blocks": cleanup_result.get("cleanup_failed_blocks", []),
        "cleaned_audit_logs": int(cleanup_result.get("cleaned_audit_logs", 0) or 0),
        "active_blocked_ips": int(cleanup_result.get("active_blocked_ips", 0) or 0),
        "audit_retention_days": int(cleanup_result.get("audit_retention_days", 0) or 0),
        "audit_cleanup_batch_size": int(cleanup_result.get("audit_cleanup_batch_size", 0) or 0),
        "block_cleanup_rounds": int(cleanup_result.get("block_cleanup_rounds", 0) or 0),
        "audit_cleanup_rounds": int(cleanup_result.get("audit_cleanup_rounds", 0) or 0),
        "created_at": int(now_ts),
    }


@router.post(
    "/admin/security/auto_block/run",
    summary="Run security auto block check once",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "按安全事件阈值执行一次自动封禁检查。"
    ),
    response_model=None,
)
def run_admin_security_auto_block_once(
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, List[str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    now_ts = int(time.time())
    with get_connection() as conn:
        result = run_security_auto_block_once(conn, now_ts=now_ts)
        write_audit_log(
            conn,
            action="admin.security.auto_block_run",
            resource_type="security",
            resource_id="manual",
            detail={
                "enabled": bool(result.get("enabled")),
                "blocked_count": int(result.get("blocked_count", 0) or 0),
                "failed_count": len(result.get("failed_items", []))
                if isinstance(result.get("failed_items"), list)
                else 0,
                "window_seconds": int(result.get("window_seconds", 0) or 0),
                "threshold": int(result.get("threshold", 0) or 0),
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()
    invalidate_admin_snapshots_cache()

    return {
        "ok": True,
        "enabled": bool(result.get("enabled")),
        "blocked_count": int(result.get("blocked_count", 0) or 0),
        "blocked_items": result.get("blocked_items", []),
        "failed_items": result.get("failed_items", []),
        "skipped_items": result.get("skipped_items", []),
        "window_seconds": int(result.get("window_seconds", 0) or 0),
        "threshold": int(result.get("threshold", 0) or 0),
        "duration_seconds": int(result.get("duration_seconds", 0) or 0),
        "max_per_interval": int(result.get("max_per_interval", 0) or 0),
        "created_at": int(now_ts),
    }


@router.get(
    "/admin/security/blocked_ips",
    summary="List blocked source IPs",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def list_security_blocked_ips(
    cleanup_expired: int = 1,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[int, List[Dict[str, Union[int, str]]]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    now_ts = int(time.time())
    cleanup_report: Dict[str, Union[int, List[str]]] = {"released": 0, "failed": []}
    with get_connection() as conn:
        if int(cleanup_expired) == 1:
            cleanup_report = cleanup_expired_ip_blocks(conn, now_ts=now_ts)
        rows = conn.execute(
            """
            SELECT source_ip, created_at, expire_at, reason, operator
            FROM security_ip_blocks
            WHERE expire_at = 0 OR expire_at > ?
            ORDER BY created_at DESC, source_ip ASC
            LIMIT 200
            """,
            (now_ts,),
        ).fetchall()
        conn.commit()
    items: List[Dict[str, Union[int, str]]] = []
    for row in rows:
        expire_at = int(row["expire_at"] or 0)
        remaining_seconds = 0 if expire_at == 0 else max(0, expire_at - now_ts)
        items.append(
            {
                "source_ip": str(row["source_ip"] or ""),
                "created_at": int(row["created_at"] or 0),
                "expire_at": expire_at,
                "remaining_seconds": int(remaining_seconds),
                "reason": str(row["reason"] or ""),
                "operator": str(row["operator"] or ""),
            }
        )
    return {
        "ok": True,
        "controller_port": int(CONTROLLER_PORT),
        "count": len(items),
        "items": items,
        "cleanup_released": int(cleanup_report.get("released", 0) or 0),
        "cleanup_failed": cleanup_report.get("failed", []),
    }


@router.get(
    "/admin/audit",
    summary="List audit logs",
    description=ADMIN_AUTH_DESCRIPTION,
    response_model=None,
)
def list_admin_audit_logs(
    limit: int = 50,
    action: str = "",
    action_prefix: str = "",
    actor: str = "",
    source_ip: str = "",
    window_seconds: int = 0,
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
    action_prefix_value = str(action_prefix or "").strip()
    actor_value = str(actor or "").strip()
    source_ip_value = str(source_ip or "").strip()
    if action_value and (len(action_value) > 96 or not re.fullmatch(r"[A-Za-z0-9._:-]+", action_value)):
        raise HTTPException(status_code=400, detail="invalid action")
    if action_prefix_value and (len(action_prefix_value) > 64 or not re.fullmatch(r"[A-Za-z0-9._:-]+", action_prefix_value)):
        raise HTTPException(status_code=400, detail="invalid action_prefix")
    if actor_value and (len(actor_value) > 64 or _has_control_characters(actor_value)):
        raise HTTPException(status_code=400, detail="invalid actor")
    if source_ip_value and (len(source_ip_value) > 128 or _has_control_characters(source_ip_value)):
        raise HTTPException(status_code=400, detail="invalid source_ip")
    window_value = int(window_seconds or 0)
    if window_value < 0:
        window_value = 0
    if window_value > 30 * 86400:
        window_value = 30 * 86400

    where_clauses: List[str] = []
    params: List[Union[int, str]] = []
    if action_value:
        where_clauses.append("action = ?")
        params.append(action_value)
    elif action_prefix_value:
        where_clauses.append("action LIKE ?")
        params.append("{0}%".format(action_prefix_value))
    if actor_value:
        where_clauses.append("actor = ?")
        params.append(actor_value)
    if source_ip_value:
        where_clauses.append("source_ip = ?")
        params.append(source_ip_value)
    if window_value > 0:
        where_clauses.append("created_at >= ?")
        params.append(int(time.time()) - window_value)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE {0}".format(" AND ".join(where_clauses))

    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, actor, action, resource_type, resource_id, detail, source_ip, created_at
            FROM audit_logs
            {0}
            ORDER BY id DESC
            LIMIT ?
            """.format(where_sql),
            tuple(params),
        ).fetchall()
    response_rows: List[Dict[str, Union[int, str]]] = []
    for row in rows:
        item = dict(row)
        item["detail"] = _mask_sensitive_audit_detail(str(item.get("detail") or ""))
        response_rows.append(item)
    return response_rows


@router.post(
    "/admin/audit/event",
    summary="Write custom admin audit event",
    description=(
        ADMIN_AUTH_DESCRIPTION +
        "用于 bot 或运维脚本写入自定义审计事件（action 需以 bot. 开头）。"
    ),
    response_model=None,
)
def write_admin_audit_event(
    payload: AuditEventRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Union[bool, int, str]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error

    action_value = str(payload.action or "").strip()
    if not (action_value.startswith("bot.") or action_value.startswith("ops.")):
        raise HTTPException(status_code=400, detail="action must start with bot. or ops.")

    resource_type = str(payload.resource_type or "bot").strip() or "bot"
    resource_id = str(payload.resource_id or "").strip()
    detail_obj = payload.detail if isinstance(payload.detail, dict) else {}

    with get_connection() as conn:
        write_audit_log(
            conn,
            action=action_value,
            resource_type=resource_type,
            resource_id=resource_id,
            detail=detail_obj,
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {
        "ok": True,
        "action": action_value,
        "resource_type": resource_type,
        "resource_id": resource_id,
    }


@router.get(
    "/admin/sub/sign/{user_code}",
    summary="Generate signed subscription URLs",
    description=(
        ADMIN_AUTH_DESCRIPTION
        + "可用于 bot 生成带签名订阅链接。"
    ),
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
    resolved_base_url = (
        _normalize_public_base_url(PANEL_BASE_URL)
        or _normalize_public_base_url(CONTROLLER_PUBLIC_URL)
        or str(request.base_url).rstrip("/")
    )
    signed_data = build_signed_subscription_urls(
        user_code=user_code,
        base_url=resolved_base_url,
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
