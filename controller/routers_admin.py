import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Union

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from controller.audit import get_request_actor, get_source_ip_for_audit, write_audit_log
from controller.db import BASE_DIR, get_connection
from controller.db_migration import (
    compare_snapshot_with_live,
    export_db_snapshot,
    get_db_integrity_status,
    load_export_payload,
    validate_export_payload,
)
from controller.schemas import VerifyDbExportRequest
from controller.security import (
    AUTH_TOKEN as SECURITY_AUTH_TOKEN,
    API_RATE_LIMIT_ENABLED,
    API_RATE_LIMIT_MAX_REQUESTS,
    API_RATE_LIMIT_WINDOW_SECONDS,
    TRUSTED_PROXY_IPS,
    TRUST_X_FORWARDED_FOR,
    get_auth_tokens,
    verify_admin_authorization,
)
from controller.settings import (
    BACKUP_RETENTION_COUNT,
    MIGRATE_RETENTION_COUNT,
    NODE_TASK_MAX_PENDING_PER_NODE,
    NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS,
    SECURITY_EVENTS_EXCLUDE_LOCAL,
    SUB_LINK_DEFAULT_TTL_SECONDS,
    SUB_LINK_REQUIRE_SIGNATURE,
    SUB_LINK_SIGN_KEY,
)
from controller.subscription import build_signed_subscription_urls


router = APIRouter(tags=["admin"])
# Compatibility alias for existing tests/tools that import controller.routers_admin.AUTH_TOKEN.
AUTH_TOKEN = SECURITY_AUTH_TOKEN


def build_unauthorized_events_snapshot(
    now_ts: int,
    window_seconds: int,
    top_limit: int,
    include_local: Optional[bool] = None,
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
    with get_connection() as conn:
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


def build_security_status_payload() -> Dict[str, Union[bool, int, List[str]]]:
    auth_tokens = get_auth_tokens()
    warnings: List[str] = []
    if not auth_tokens:
        warnings.append("AUTH_TOKEN 未设置：管理接口未启用鉴权")
    if len(auth_tokens) > 1:
        warnings.append("AUTH_TOKEN 处于多 token 过渡模式（建议迁移完成后移除旧 token）")
    if not SUB_LINK_SIGN_KEY:
        warnings.append("SUB_LINK_SIGN_KEY 未设置：订阅签名功能不可用")
    if SUB_LINK_SIGN_KEY and not SUB_LINK_REQUIRE_SIGNATURE:
        warnings.append("已设置 SUB_LINK_SIGN_KEY，但未强制签名（兼容模式）")
    if TRUST_X_FORWARDED_FOR and not TRUSTED_PROXY_IPS:
        warnings.append("已启用 XFF 信任，但 TRUSTED_PROXY_IPS 为空")
    if not API_RATE_LIMIT_ENABLED:
        warnings.append("轻量限流未启用")
    if not SECURITY_EVENTS_EXCLUDE_LOCAL:
        warnings.append("安全事件统计包含本机来源（可能放大测试噪声）")

    return {
        "auth_enabled": bool(auth_tokens),
        "auth_token_count": len(auth_tokens),
        "trust_x_forwarded_for": TRUST_X_FORWARDED_FOR,
        "trusted_proxy_ips": sorted(TRUSTED_PROXY_IPS),
        "sub_link_sign_enabled": bool(SUB_LINK_SIGN_KEY),
        "sub_link_require_signature": SUB_LINK_REQUIRE_SIGNATURE,
        "sub_link_default_ttl_seconds": SUB_LINK_DEFAULT_TTL_SECONDS,
        "api_rate_limit_enabled": API_RATE_LIMIT_ENABLED,
        "api_rate_limit_window_seconds": API_RATE_LIMIT_WINDOW_SECONDS,
        "api_rate_limit_max_requests": API_RATE_LIMIT_MAX_REQUESTS,
        "security_events_exclude_local": bool(SECURITY_EVENTS_EXCLUDE_LOCAL),
        "node_task_max_pending_per_node": NODE_TASK_MAX_PENDING_PER_NODE,
        "warnings": warnings,
    }


def build_admin_overview_payload(now_ts: int) -> Dict[str, Union[int, Dict, List]]:
    with get_connection() as conn:
        users_total = int(conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"] or 0)
        users_active = int(
            conn.execute("SELECT COUNT(*) AS c FROM users WHERE status = 'active'").fetchone()["c"] or 0
        )
        users_disabled = int(
            conn.execute("SELECT COUNT(*) AS c FROM users WHERE status = 'disabled'").fetchone()["c"] or 0
        )
        nodes_total = int(conn.execute("SELECT COUNT(*) AS c FROM nodes").fetchone()["c"] or 0)
        nodes_enabled = int(
            conn.execute("SELECT COUNT(*) AS c FROM nodes WHERE enabled = 1").fetchone()["c"] or 0
        )
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
    unauthorized_24h_snapshot = build_unauthorized_events_snapshot(
        now_ts=now_ts,
        window_seconds=86400,
        top_limit=5,
    )
    unauthorized_1h_snapshot = build_unauthorized_events_snapshot(
        now_ts=now_ts,
        window_seconds=3600,
        top_limit=3,
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
        },
        "security": build_security_status_payload(),
        "security_events": {
            "unauthorized_1h": int(unauthorized_1h_snapshot["unauthorized"]),
            "unauthorized_24h": int(unauthorized_24h_snapshot["unauthorized"]),
            "top_unauthorized_ips": unauthorized_24h_snapshot["top_unauthorized_ips"],
        },
    }


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
    "/admin/db/export",
    summary="Create logical DB export snapshot",
    description=(
        "AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。"
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
        "AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。"
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
        "AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。"
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


@router.get(
    "/admin/overview",
    summary="Admin overview status",
    description=(
        "AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。"
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
    return build_admin_overview_payload(now_ts=int(time.time()))


@router.get(
    "/admin/security/events",
    summary="Security event statistics",
    description=(
        "AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。"
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
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
    response_model=None,
)
def get_admin_security_status(
    authorization: Optional[str] = Header(default=None, alias="Authorization")
) -> Union[Dict[str, Union[bool, int, str, List[str]]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return build_security_status_payload()


@router.get(
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


@router.get(
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
