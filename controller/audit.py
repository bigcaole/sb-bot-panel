import json
import sqlite3
import time
from typing import Any, Optional

from fastapi import Request

from controller.security import get_request_ip


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
