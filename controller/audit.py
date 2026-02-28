import json
import sqlite3
import time
from typing import Any, Optional

from fastapi import Request

from controller.security import get_request_ip

MAX_AUDIT_ACTOR_LENGTH = 120
MAX_AUDIT_ACTION_LENGTH = 160
MAX_AUDIT_RESOURCE_TYPE_LENGTH = 80
MAX_AUDIT_RESOURCE_ID_LENGTH = 240
MAX_AUDIT_SOURCE_IP_LENGTH = 80


def _safe_trimmed_text(value: Any, max_length: int) -> str:
    text = str(value or "").strip()
    if max_length < 1:
        return ""
    if len(text) > max_length:
        text = text[:max_length]
    return text


def get_request_actor(request: Optional[Request]) -> str:
    if request is None:
        return ""
    return _safe_trimmed_text(request.headers.get("X-Actor", ""), MAX_AUDIT_ACTOR_LENGTH)


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
    action_text = _safe_trimmed_text(action, MAX_AUDIT_ACTION_LENGTH)
    if not action_text:
        return
    ts = int(created_at or time.time())
    conn.execute(
        """
        INSERT INTO audit_logs(actor, action, resource_type, resource_id, detail, source_ip, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _safe_trimmed_text(actor, MAX_AUDIT_ACTOR_LENGTH),
            action_text,
            _safe_trimmed_text(resource_type, MAX_AUDIT_RESOURCE_TYPE_LENGTH),
            _safe_trimmed_text(resource_id, MAX_AUDIT_RESOURCE_ID_LENGTH),
            normalize_audit_detail(detail),
            _safe_trimmed_text(source_ip, MAX_AUDIT_SOURCE_IP_LENGTH),
            ts,
        ),
    )


def cleanup_old_audit_logs(
    conn: sqlite3.Connection,
    now_ts: int,
    retention_days: int,
    batch_size: int = 2000,
) -> int:
    try:
        retention_value = int(retention_days)
    except (TypeError, ValueError):
        retention_value = 30
    if retention_value < 1:
        retention_value = 1
    try:
        batch_value = int(batch_size)
    except (TypeError, ValueError):
        batch_value = 2000
    if batch_value < 1:
        batch_value = 1

    cutoff_ts = int(now_ts) - retention_value * 86400
    cursor = conn.execute(
        """
        DELETE FROM audit_logs
        WHERE id IN (
            SELECT id
            FROM audit_logs
            WHERE created_at < ?
            ORDER BY id ASC
            LIMIT ?
        )
        """,
        (int(cutoff_ts), int(batch_value)),
    )
    return int(cursor.rowcount or 0)
