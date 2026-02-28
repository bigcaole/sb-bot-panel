import json
import re
import sqlite3
from typing import Any, Dict, List, Optional, Union

from fastapi import HTTPException


ALLOWED_NODE_TASK_TYPES = {
    "restart_singbox",
    "status_singbox",
    "status_agent",
    "logs_singbox",
    "logs_agent",
    "sync_time",
    "update_sync",
    "config_set",
}

MAX_NODE_TASK_PAYLOAD_BYTES = 2048
NODE_CONFIG_SET_ALLOWED_KEYS = {
    "poll_interval",
    "tuic_domain",
    "tuic_listen_port",
    "acme_email",
    "controller_url",
    "auth_token",
    "node_code",
}
SENSITIVE_PAYLOAD_KEYWORDS = (
    "token",
    "secret",
    "password",
    "private_key",
    "api_key",
    "apikey",
)
SENSITIVE_RESULT_PATTERNS = (
    re.compile(
        r'(?i)("?(?:auth[_-]?token|token|password|secret|api[_-]?key|private[_-]?key)"?\s*[:=]\s*"?)([^",\s]+)("?)'
    ),
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)([A-Za-z0-9._\-~+/=]{6,})()"),
    re.compile(r"(?i)(\bbearer\s+)([A-Za-z0-9._\-~+/=]{12,})()"),
)


def _parse_int_payload(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="{0} must be integer".format(field)) from exc


def _parse_text_payload(
    value: Any,
    field: str,
    allow_empty: bool = True,
    max_length: int = 256,
) -> str:
    raw = str(value or "").strip()
    if not allow_empty and not raw:
        raise HTTPException(status_code=400, detail="{0} cannot be empty".format(field))
    if "\n" in raw or "\r" in raw:
        raise HTTPException(status_code=400, detail="{0} must be single-line".format(field))
    if len(raw) > max_length:
        raise HTTPException(
            status_code=400,
            detail="{0} too long (max {1})".format(field, max_length),
        )
    return raw


def validate_node_task_payload(task_type: str, payload_obj: Dict[str, Any]) -> Dict[str, Any]:
    if task_type in ("restart_singbox", "status_singbox", "status_agent", "update_sync"):
        if payload_obj:
            raise HTTPException(status_code=400, detail="payload not allowed for task_type")
        return {}

    if task_type in ("logs_singbox", "logs_agent"):
        if not payload_obj:
            return {"lines": 120}
        unknown_keys = [key for key in payload_obj.keys() if key != "lines"]
        if unknown_keys:
            raise HTTPException(status_code=400, detail="unsupported payload keys")
        lines = _parse_int_payload(payload_obj.get("lines"), "lines")
        if lines < 20 or lines > 300:
            raise HTTPException(status_code=400, detail="lines must be 20-300")
        return {"lines": lines}

    if task_type == "config_set":
        if not payload_obj:
            raise HTTPException(status_code=400, detail="config_set payload required")
        if len(payload_obj.keys()) > len(NODE_CONFIG_SET_ALLOWED_KEYS):
            raise HTTPException(status_code=400, detail="too many payload keys")
        unknown_keys = [
            key for key in payload_obj.keys() if key not in NODE_CONFIG_SET_ALLOWED_KEYS
        ]
        if unknown_keys:
            raise HTTPException(status_code=400, detail="unsupported payload keys")

        sanitized: Dict[str, Any] = {}
        for key, value in payload_obj.items():
            if key == "poll_interval":
                parsed = _parse_int_payload(value, key)
                if parsed < 5 or parsed > 3600:
                    raise HTTPException(status_code=400, detail="poll_interval must be 5-3600")
                sanitized[key] = parsed
            elif key == "tuic_listen_port":
                parsed = _parse_int_payload(value, key)
                if parsed < 1 or parsed > 65535:
                    raise HTTPException(status_code=400, detail="tuic_listen_port must be 1-65535")
                sanitized[key] = parsed
            elif key == "controller_url":
                text = _parse_text_payload(value, key, allow_empty=False, max_length=512)
                if not re.match(r"^https?://", text):
                    text = "http://{0}".format(text)
                sanitized[key] = text.rstrip("/")
            elif key == "node_code":
                sanitized[key] = _parse_text_payload(value, key, allow_empty=False, max_length=64)
            elif key == "auth_token":
                sanitized[key] = _parse_text_payload(value, key, allow_empty=True, max_length=256)
            elif key in ("tuic_domain", "acme_email"):
                sanitized[key] = _parse_text_payload(value, key, allow_empty=True, max_length=256)
        if not sanitized:
            raise HTTPException(status_code=400, detail="config_set payload required")
        return sanitized

    if task_type == "sync_time":
        if not payload_obj:
            raise HTTPException(status_code=400, detail="sync_time payload required")
        unknown_keys = [key for key in payload_obj.keys() if key != "server_unix"]
        if unknown_keys:
            raise HTTPException(status_code=400, detail="unsupported payload keys")
        server_unix = _parse_int_payload(payload_obj.get("server_unix"), "server_unix")
        if server_unix < 946684800 or server_unix > 4102444800:
            raise HTTPException(status_code=400, detail="server_unix out of range")
        return {"server_unix": server_unix}

    raise HTTPException(status_code=400, detail="unsupported task_type")


def validate_node_task_payload_size(payload_json: str) -> None:
    size = len(str(payload_json or "").encode("utf-8"))
    if size > MAX_NODE_TASK_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail="payload too large (max {0} bytes)".format(MAX_NODE_TASK_PAYLOAD_BYTES),
        )


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


def _is_sensitive_payload_key(key: str) -> bool:
    normalized = str(key or "").strip().lower()
    if not normalized:
        return False
    return any(keyword in normalized for keyword in SENSITIVE_PAYLOAD_KEYWORDS)


def _redact_payload_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): ("***" if _is_sensitive_payload_key(str(key)) else _redact_payload_value(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_payload_value(item) for item in value]
    return value


def sanitize_task_payload_for_display(payload_obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload_obj, dict):
        return {}
    return _redact_payload_value(payload_obj)


def sanitize_task_result_for_display(result_text: Any) -> str:
    text = str(result_text or "")
    if not text:
        return ""
    masked = text
    for pattern in SENSITIVE_RESULT_PATTERNS:
        masked = pattern.sub(r"\1***\3", masked)
    return masked


def build_task_row_dict(row: sqlite3.Row, redact_sensitive: bool = False) -> Dict[str, Any]:
    attempts = int(row["attempts"] or 0)
    max_attempts = int(row["max_attempts"] or 1)
    if max_attempts < 1:
        max_attempts = 1
    payload_obj = parse_task_payload(row["payload_json"])
    result_text = str(row["result_text"] or "")
    if redact_sensitive:
        payload_obj = sanitize_task_payload_for_display(payload_obj)
        result_text = sanitize_task_result_for_display(result_text)
    return {
        "id": int(row["id"]),
        "node_code": str(row["node_code"]),
        "task_type": str(row["task_type"]),
        "payload": payload_obj,
        "payload_hash": str(row["payload_hash"] or ""),
        "status": str(row["status"]),
        "attempts": attempts,
        "max_attempts": max_attempts,
        "created_at": int(row["created_at"] or 0),
        "updated_at": int(row["updated_at"] or 0),
        "result_text": result_text,
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
    running_timeout_seconds: int,
    retention_seconds: int,
    node_code: Optional[str] = None,
) -> Dict[str, int]:
    timeout_before = now_ts - int(running_timeout_seconds)
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
        stale_note = "[controller] task timed out after {0}s".format(running_timeout_seconds)
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

    retention_before = now_ts - int(retention_seconds)
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
