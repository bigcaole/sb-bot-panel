import json
import time
import hashlib
import re
from typing import Any, Dict, List, Union

from fastapi import HTTPException, Request

from controller.audit import get_request_actor, get_source_ip_for_audit, write_audit_log
from controller.db import get_connection
from controller.node_tasks import (
    ALLOWED_NODE_TASK_TYPES,
    append_task_result,
    build_task_row_dict,
    run_node_task_housekeeping,
    validate_node_task_payload,
    validate_node_task_payload_size,
)
from controller.schemas import CreateNodeTaskRequest, ReportNodeRealityRequest, ReportNodeTaskRequest
from controller.security import verify_node_agent_ip


def create_node_task_service(
    node_code: str,
    payload: CreateNodeTaskRequest,
    request: Request,
    running_timeout_seconds: int,
    retention_seconds: int,
    max_pending_per_node: int,
) -> Dict[str, Any]:
    task_type = str(payload.task_type or "").strip()
    if task_type not in ALLOWED_NODE_TASK_TYPES:
        raise HTTPException(status_code=400, detail="unsupported task_type")
    raw_payload_obj = payload.payload if isinstance(payload.payload, dict) else {}
    payload_obj = validate_node_task_payload(task_type, raw_payload_obj)
    payload_json = json.dumps(payload_obj, ensure_ascii=False)
    validate_node_task_payload_size(payload_json)
    canonical_payload_json = json.dumps(
        payload_obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    payload_hash = hashlib.sha256(canonical_payload_json.encode("utf-8")).hexdigest()
    force_new = bool(payload.force_new)
    max_attempts = int(payload.max_attempts or 1)
    if max_attempts < 1:
        max_attempts = 1
    if max_attempts > 3:
        max_attempts = 3
    now_ts = int(time.time())

    with get_connection() as conn:
        run_node_task_housekeeping(
            conn,
            now_ts,
            running_timeout_seconds=running_timeout_seconds,
            retention_seconds=retention_seconds,
        )
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        if not force_new:
            existing_row = conn.execute(
                """
                SELECT
                    id,
                    node_code,
                    task_type,
                    payload_json,
                    payload_hash,
                    status,
                    attempts,
                    max_attempts,
                    created_at,
                    updated_at,
                    result_text
                FROM node_tasks
                WHERE node_code = ?
                  AND task_type = ?
                  AND (
                    (payload_hash = ? AND payload_hash != '')
                    OR payload_json = ?
                  )
                  AND status IN ('pending', 'running')
                ORDER BY id ASC
                LIMIT 1
                """,
                (node_code, task_type, payload_hash, payload_json),
            ).fetchone()
            if existing_row is not None:
                task_data = build_task_row_dict(existing_row)
                write_audit_log(
                    conn,
                    action="node.task.deduplicated",
                    resource_type="node_task",
                    resource_id=str(task_data.get("id", "")),
                    detail={
                        "node_code": node_code,
                        "task_type": task_type,
                        "force_new": False,
                        "payload_hash": payload_hash[:16],
                    },
                    actor=get_request_actor(request),
                    source_ip=get_source_ip_for_audit(request),
                    created_at=now_ts,
                )
                conn.commit()
                task_data["deduplicated"] = True
                return task_data

        backlog_count_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM node_tasks
            WHERE node_code = ?
              AND status IN ('pending', 'running')
            """,
            (node_code,),
        ).fetchone()
        backlog_count = int(backlog_count_row["c"] or 0) if backlog_count_row else 0
        if backlog_count >= int(max_pending_per_node):
            raise HTTPException(
                status_code=429,
                detail="too many pending tasks for node",
            )

        cursor = conn.execute(
            """
            INSERT INTO node_tasks(
                node_code,
                task_type,
                payload_json,
                payload_hash,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            )
            VALUES (?, ?, ?, ?, 'pending', 0, ?, ?, ?, '')
            """,
            (node_code, task_type, payload_json, payload_hash, max_attempts, now_ts, now_ts),
        )
        task_id = int(cursor.lastrowid or 0)
        write_audit_log(
            conn,
            action="node.task.create",
            resource_type="node_task",
            resource_id=str(task_id),
            detail={
                "node_code": node_code,
                "task_type": task_type,
                "max_attempts": max_attempts,
                "payload_keys": sorted(list(payload_obj.keys())),
                "payload_size_bytes": len(payload_json.encode("utf-8")),
                "payload_hash": payload_hash[:16],
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

        created_row = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                payload_hash,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE id = ?
            """,
            (task_id,),
        ).fetchone()
    if created_row is None:
        raise HTTPException(status_code=500, detail="create task failed")
    created_data = build_task_row_dict(created_row)
    created_data["deduplicated"] = False
    return created_data


def list_node_tasks_service(
    node_code: str,
    limit: int,
    running_timeout_seconds: int,
    retention_seconds: int,
) -> List[Dict[str, Any]]:
    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    with get_connection() as conn:
        run_node_task_housekeeping(
            conn,
            int(time.time()),
            running_timeout_seconds=running_timeout_seconds,
            retention_seconds=retention_seconds,
            node_code=node_code,
        )
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        rows = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                payload_hash,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE node_code = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (node_code, limit),
        ).fetchall()
    return [build_task_row_dict(row) for row in rows]


def get_next_node_task_service(
    node_code: str,
    request: Request,
    running_timeout_seconds: int,
    retention_seconds: int,
) -> Dict[str, Any]:
    now_ts = int(time.time())
    with get_connection() as conn:
        run_node_task_housekeeping(
            conn,
            now_ts,
            running_timeout_seconds=running_timeout_seconds,
            retention_seconds=retention_seconds,
            node_code=node_code,
        )
        node_row = conn.execute(
            "SELECT node_code, agent_ip FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])

        row = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                payload_hash,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE node_code = ? AND status = 'pending' AND attempts < max_attempts
            ORDER BY id ASC
            LIMIT 1
            """,
            (node_code,),
        ).fetchone()
        if row is None:
            return {"ok": True, "task": None}

        cursor = conn.execute(
            """
            UPDATE node_tasks
            SET status = 'running', attempts = attempts + 1, updated_at = ?
            WHERE id = ? AND status = 'pending' AND attempts < max_attempts
            """,
            (now_ts, int(row["id"])),
        )
        if int(cursor.rowcount or 0) <= 0:
            return {"ok": True, "task": None}
        conn.commit()

        running_row = conn.execute(
            """
            SELECT
                id,
                node_code,
                task_type,
                payload_json,
                payload_hash,
                status,
                attempts,
                max_attempts,
                created_at,
                updated_at,
                result_text
            FROM node_tasks
            WHERE id = ?
            """,
            (int(row["id"]),),
        ).fetchone()

    if running_row is None:
        return {"ok": True, "task": None}
    return {"ok": True, "task": build_task_row_dict(running_row)}


def report_node_task_service(
    node_code: str,
    task_id: int,
    payload: ReportNodeTaskRequest,
    request: Request,
) -> Dict[str, Any]:
    status_value = str(payload.status or "").strip().lower()
    if status_value not in ("running", "success", "failed"):
        raise HTTPException(status_code=400, detail="status must be running/success/failed")
    result_text = str(payload.result or "")
    if len(result_text) > 12000:
        result_text = result_text[:12000]
    now_ts = int(time.time())

    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code, agent_ip FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])

        task_row = conn.execute(
            """
            SELECT id, attempts, max_attempts, result_text
            FROM node_tasks
            WHERE id = ? AND node_code = ?
            """,
            (task_id, node_code),
        ).fetchone()
        if task_row is None:
            raise HTTPException(status_code=404, detail="Task not found")

        attempts = int(task_row["attempts"] or 0)
        max_attempts = int(task_row["max_attempts"] or 1)
        if max_attempts < 1:
            max_attempts = 1
        next_status = status_value
        next_result = result_text
        if status_value == "failed" and attempts < max_attempts:
            next_status = "pending"
            retry_note = "[controller] auto retry scheduled ({0}/{1})".format(
                attempts,
                max_attempts,
            )
            next_result = append_task_result(result_text, retry_note)

        conn.execute(
            """
            UPDATE node_tasks
            SET status = ?, result_text = ?, updated_at = ?
            WHERE id = ? AND node_code = ?
            """,
            (next_status, next_result, now_ts, task_id, node_code),
        )
        write_audit_log(
            conn,
            action="node.task.report",
            resource_type="node_task",
            resource_id=str(task_id),
            detail={"node_code": node_code, "status": next_status},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

    return {"ok": True, "node_code": node_code, "task_id": task_id, "status": next_status}


def get_node_sync_service(node_code: str, request: Request) -> Dict[str, Union[Dict, List, int]]:
    generated_at = int(time.time())
    with get_connection() as conn:
        node_row = conn.execute(
            """
            SELECT
                node_code,
                enabled,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                supports_reality,
                supports_tuic,
                tuic_port_start,
                tuic_port_end,
                reality_public_key,
                reality_short_id
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])
        conn.execute(
            "UPDATE nodes SET last_seen_at = ? WHERE node_code = ?",
            (generated_at, node_code),
        )
        node_enabled = int(node_row["enabled"] or 0) == 1
        if node_enabled:
            user_rows = conn.execute(
                """
                SELECT
                    u.user_code,
                    u.display_name,
                    u.status,
                    u.expire_at,
                    u.speed_mbps,
                    u.limit_mode,
                    u.vless_uuid,
                    u.tuic_secret,
                    un.tuic_port,
                    un.created_at AS bound_at
                FROM user_nodes un
                JOIN users u ON u.user_code = un.user_code
                WHERE un.node_code = ?
                  AND u.status = 'active'
                  AND (u.expire_at IS NULL OR u.expire_at = 0 OR u.expire_at > ?)
                ORDER BY u.user_code ASC
                """,
                (node_code, generated_at),
            ).fetchall()
        else:
            user_rows = []

    node_data = dict(node_row)
    node_data["last_seen_at"] = generated_at
    user_items = []
    for row in user_rows:
        item = dict(row)
        limit_mode = str(item.get("limit_mode") or "tc").strip().lower() or "tc"
        item["limit_mode"] = limit_mode
        if limit_mode != "tc":
            item["speed_mbps"] = 0
        user_items.append(item)

    return {
        "node": node_data,
        "users": user_items,
        "generated_at": generated_at,
    }


def report_node_reality_service(
    node_code: str,
    payload: ReportNodeRealityRequest,
    request: Request,
) -> Dict[str, Union[bool, str, int]]:
    public_key = str(payload.reality_public_key or "").strip()
    short_id_raw = str(payload.reality_short_id or "").strip().lower()
    if not public_key:
        raise HTTPException(status_code=400, detail="reality_public_key is required")
    if not re.fullmatch(r"[0-9a-f]{1,8}", short_id_raw):
        raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (1-8 chars)")

    now_ts = int(time.time())
    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code, agent_ip FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        verify_node_agent_ip(request, node_code, node_row["agent_ip"])

        conn.execute(
            """
            UPDATE nodes
            SET reality_public_key = ?, reality_short_id = ?, last_seen_at = ?
            WHERE node_code = ?
            """,
            (public_key, short_id_raw, now_ts, node_code),
        )
        write_audit_log(
            conn,
            action="node.reality.report",
            resource_type="node",
            resource_id=node_code,
            detail={
                "reality_public_key_len": len(public_key),
                "reality_short_id": short_id_raw,
            },
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

    return {
        "ok": True,
        "node_code": node_code,
        "reality_short_id": short_id_raw,
        "updated_at": now_ts,
    }
