import json
import sqlite3
from typing import Any, Dict, List, Optional, Union


ALLOWED_NODE_TASK_TYPES = {
    "restart_singbox",
    "status_singbox",
    "status_agent",
    "logs_singbox",
    "logs_agent",
    "update_sync",
    "config_set",
}


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


def build_task_row_dict(row: sqlite3.Row) -> Dict[str, Any]:
    attempts = int(row["attempts"] or 0)
    max_attempts = int(row["max_attempts"] or 1)
    if max_attempts < 1:
        max_attempts = 1
    return {
        "id": int(row["id"]),
        "node_code": str(row["node_code"]),
        "task_type": str(row["task_type"]),
        "payload": parse_task_payload(row["payload_json"]),
        "status": str(row["status"]),
        "attempts": attempts,
        "max_attempts": max_attempts,
        "created_at": int(row["created_at"] or 0),
        "updated_at": int(row["updated_at"] or 0),
        "result_text": str(row["result_text"] or ""),
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
