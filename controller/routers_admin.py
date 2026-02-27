import ipaddress
import subprocess
import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

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
from controller.schemas import BlockIpRequest, UnblockIpRequest, VerifyDbExportRequest
from controller.security import (
    AUTH_TOKEN as SECURITY_AUTH_TOKEN,
    API_RATE_LIMIT_ENABLED,
    API_RATE_LIMIT_MAX_REQUESTS,
    API_RATE_LIMIT_WINDOW_SECONDS,
    TRUSTED_PROXY_IPS,
    TRUST_X_FORWARDED_FOR,
    UNAUTHORIZED_AUDIT_SAMPLE_SECONDS,
    get_auth_tokens,
    verify_admin_authorization,
)
from controller.settings import (
    AUDIT_LOG_CLEANUP_BATCH_SIZE,
    AUDIT_LOG_CLEANUP_INTERVAL_SECONDS,
    AUDIT_LOG_RETENTION_DAYS,
    BACKUP_RETENTION_COUNT,
    CONTROLLER_PORT_WHITELIST_ITEMS,
    CONTROLLER_PORT,
    MIGRATE_RETENTION_COUNT,
    NODE_TASK_MAX_PENDING_PER_NODE,
    NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS,
    SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS,
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
    now_ts = int(time.time())
    with get_connection() as conn:
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
    if UNAUTHORIZED_AUDIT_SAMPLE_SECONDS <= 0:
        warnings.append("未授权审计采样已关闭（高扫描场景下 audit_logs 增长会更快）")
    if AUDIT_LOG_RETENTION_DAYS < 7:
        warnings.append("审计日志保留天数过短（小于 7 天）")

    return {
        "auth_enabled": bool(auth_tokens),
        "auth_token_count": len(auth_tokens),
        "controller_port_whitelist": CONTROLLER_PORT_WHITELIST_ITEMS,
        "controller_port_whitelist_count": len(CONTROLLER_PORT_WHITELIST_ITEMS),
        "trust_x_forwarded_for": TRUST_X_FORWARDED_FOR,
        "trusted_proxy_ips": sorted(TRUSTED_PROXY_IPS),
        "sub_link_sign_enabled": bool(SUB_LINK_SIGN_KEY),
        "sub_link_require_signature": SUB_LINK_REQUIRE_SIGNATURE,
        "sub_link_default_ttl_seconds": SUB_LINK_DEFAULT_TTL_SECONDS,
        "api_rate_limit_enabled": API_RATE_LIMIT_ENABLED,
        "api_rate_limit_window_seconds": API_RATE_LIMIT_WINDOW_SECONDS,
        "api_rate_limit_max_requests": API_RATE_LIMIT_MAX_REQUESTS,
        "unauthorized_audit_sample_seconds": UNAUTHORIZED_AUDIT_SAMPLE_SECONDS,
        "unauthorized_audit_sampling_enabled": bool(UNAUTHORIZED_AUDIT_SAMPLE_SECONDS > 0),
        "audit_log_retention_days": AUDIT_LOG_RETENTION_DAYS,
        "audit_log_cleanup_interval_seconds": AUDIT_LOG_CLEANUP_INTERVAL_SECONDS,
        "audit_log_cleanup_batch_size": AUDIT_LOG_CLEANUP_BATCH_SIZE,
        "security_block_cleanup_interval_seconds": SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS,
        "blocked_ip_count": active_block_count,
        "security_events_exclude_local": bool(SECURITY_EVENTS_EXCLUDE_LOCAL),
        "node_task_max_pending_per_node": NODE_TASK_MAX_PENDING_PER_NODE,
        "warnings": warnings,
    }


def normalize_source_ip(value: str) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        raise HTTPException(status_code=400, detail="source_ip is required")
    try:
        return str(ipaddress.ip_address(raw_value))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="source_ip must be valid IPv4/IPv6") from exc


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


def apply_ufw_ip_block(source_ip: str) -> Dict[str, Union[bool, str]]:
    last_error = ""
    for args in build_ufw_deny_arg_sets(source_ip):
        code, stdout, stderr = run_ufw_command(args)
        merged = "{0}\n{1}".format(stdout, stderr).strip()
        if code == 0:
            return {"ok": True, "result": (stdout or stderr or "ok").strip()}
        if code == 127:
            raise HTTPException(status_code=503, detail="ufw is not available on controller host")
        if is_ufw_rule_exists_message(merged):
            return {"ok": True, "result": (stdout or stderr or "existing").strip()}
        last_error = (stderr or stdout or "unknown error").strip()
        if not is_ufw_invalid_syntax_message(merged):
            break
    raise HTTPException(
        status_code=500,
        detail="ufw deny failed: {0}".format(last_error or "unknown error"),
    )


def remove_ufw_ip_block(source_ip: str) -> Dict[str, Union[int, str]]:
    removed = 0
    last_output = ""
    for _ in range(6):
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
    protected = {
        "127.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
    }
    request_ip = str(request_source_ip or "").strip()
    if request_ip:
        try:
            protected.add(str(ipaddress.ip_address(request_ip)))
        except ValueError:
            protected.add(request_ip)

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
        value = str(row["agent_ip"] or "").strip()
        if not value:
            continue
        try:
            protected.add(str(ipaddress.ip_address(value)))
        except ValueError:
            continue

    for item in CONTROLLER_PORT_WHITELIST_ITEMS:
        raw = str(item or "").strip()
        if not raw:
            continue
        try:
            network = ipaddress.ip_network(raw, strict=False)
            if int(network.num_addresses) == 1:
                protected.add(str(network.network_address))
        except ValueError:
            continue

    return sorted(protected)


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


@router.post(
    "/admin/security/block_ip",
    summary="Block source IP on controller port",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
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
        protected_ips = set(get_protected_source_ips(conn, request_source_ip=request_ip))
        if source_ip in protected_ips:
            raise HTTPException(status_code=400, detail="该IP受保护，拒绝封禁")
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
            },
            actor=get_request_actor(request),
            source_ip=request_ip,
        )
        conn.commit()
    return {
        "ok": True,
        "source_ip": source_ip,
        "duration_seconds": duration_seconds,
        "expire_at": int(expire_at),
        "controller_port": int(CONTROLLER_PORT),
    }


@router.post(
    "/admin/security/unblock_ip",
    summary="Unblock source IP on controller port",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
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
    return {
        "ok": True,
        "source_ip": source_ip,
        "removed_rules": int(remove_result.get("removed", 0) or 0),
    }


@router.post(
    "/admin/security/maintenance_cleanup",
    summary="Run manual security maintenance cleanup",
    description=(
        "AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。"
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


@router.get(
    "/admin/security/blocked_ips",
    summary="List blocked source IPs",
    description="AUTH_TOKEN 为空时不校验；非空时需要请求头 Authorization: Bearer <AUTH_TOKEN>。",
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
