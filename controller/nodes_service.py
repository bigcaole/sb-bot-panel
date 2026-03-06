import ipaddress
import sqlite3
from urllib.parse import urlsplit
from typing import Dict, List, Union

from fastapi import HTTPException, Request

from controller.audit import get_request_actor, get_source_ip_for_audit, write_audit_log
from controller.db import get_connection
from controller.schemas import CreateNodeRequest, UpdateNodeRequest
from controller.security import validate_agent_ip


def _normalize_host_candidate(raw_host: str) -> str:
    value = str(raw_host or "").strip()
    if not value:
        return ""
    if "://" in value:
        parsed = urlsplit(value)
        value = str(parsed.hostname or "").strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1].strip()
    # host:port (non-IPv6)
    if value.count(":") == 1:
        maybe_host, maybe_port = value.rsplit(":", 1)
        if maybe_port.isdigit():
            value = maybe_host.strip()
    return value


def _is_ip_literal(value: str) -> bool:
    candidate = str(value or "").strip()
    if not candidate:
        return False
    try:
        ipaddress.ip_address(candidate)
        return True
    except ValueError:
        return False


def derive_tuic_server_name(host: str, tuic_server_name: Union[str, None]) -> Union[str, None]:
    explicit = str(tuic_server_name or "").strip()
    if explicit:
        return explicit
    candidate = _normalize_host_candidate(host)
    if not candidate:
        return None
    if _is_ip_literal(candidate):
        return None
    return candidate


def create_node_service(payload: CreateNodeRequest, request: Request) -> Dict[str, Union[int, str, None]]:
    if payload.tuic_port_start > payload.tuic_port_end:
        raise HTTPException(status_code=400, detail="Invalid port range: start must be <= end")
    if payload.enabled not in (0, 1):
        raise HTTPException(status_code=400, detail="enabled must be 0 or 1")
    supports_reality = 1 if payload.supports_reality is None else payload.supports_reality
    supports_tuic = 1 if payload.supports_tuic is None else payload.supports_tuic
    if supports_reality not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_reality must be 0 or 1")
    if supports_tuic not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_tuic must be 0 or 1")
    monitor_enabled = 0 if payload.monitor_enabled is None else payload.monitor_enabled
    if monitor_enabled not in (0, 1):
        raise HTTPException(status_code=400, detail="monitor_enabled must be 0 or 1")
    agent_ip = validate_agent_ip(payload.agent_ip)
    tuic_server_name = derive_tuic_server_name(payload.host, payload.tuic_server_name)

    with get_connection() as conn:
        try:
            conn.execute(
                """
                INSERT INTO nodes(
                    node_code,
                    region,
                    host,
                    agent_ip,
                    reality_server_name,
                    tuic_server_name,
                    tuic_listen_port,
                    monitor_enabled,
                    tuic_port_start,
                    tuic_port_end,
                    enabled,
                    supports_reality,
                    supports_tuic,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.node_code,
                    payload.region,
                    payload.host,
                    agent_ip,
                    payload.reality_server_name,
                    tuic_server_name,
                    payload.tuic_listen_port,
                    monitor_enabled,
                    payload.tuic_port_start,
                    payload.tuic_port_end,
                    payload.enabled,
                    supports_reality,
                    supports_tuic,
                    payload.note,
                ),
            )
            write_audit_log(
                conn,
                action="node.create",
                resource_type="node",
                resource_id=payload.node_code,
                detail={
                    "region": payload.region,
                    "host": payload.host,
                    "enabled": payload.enabled,
                    "supports_reality": supports_reality,
                    "supports_tuic": supports_tuic,
                },
                actor=get_request_actor(request),
                source_ip=get_source_ip_for_audit(request),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="node_code already exists") from exc

    return {
        "node_code": payload.node_code,
        "region": payload.region,
        "host": payload.host,
        "agent_ip": agent_ip,
        "reality_server_name": payload.reality_server_name,
        "tuic_server_name": tuic_server_name,
        "tuic_listen_port": payload.tuic_listen_port,
        "monitor_enabled": monitor_enabled,
        "tuic_port_start": payload.tuic_port_start,
        "tuic_port_end": payload.tuic_port_end,
        "enabled": payload.enabled,
        "supports_reality": supports_reality,
        "supports_tuic": supports_tuic,
        "note": payload.note,
    }


def list_nodes_service() -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            ORDER BY node_code
            """
        ).fetchall()
    return [dict(row) for row in rows]


def get_node_service(node_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Node not found")
    return dict(row)


def get_node_stats_service(node_code: str) -> Dict[str, Union[int, str]]:
    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        count_row = conn.execute(
            "SELECT COUNT(*) AS bound_users FROM user_nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
    return {"node_code": node_code, "bound_users": int(count_row["bound_users"])}


def list_node_bindings_service(
    node_code: str, limit: int = 50
) -> List[Dict[str, Union[int, str, None]]]:
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200

    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        rows = conn.execute(
            """
            SELECT
                un.user_code,
                un.node_code,
                un.tuic_port,
                un.created_at AS bound_at,
                u.display_name,
                u.status,
                u.expire_at,
                u.speed_mbps,
                u.limit_mode
            FROM user_nodes un
            JOIN users u ON u.user_code = un.user_code
            WHERE un.node_code = ?
            ORDER BY un.created_at DESC, un.user_code ASC
            LIMIT ?
            """,
            (node_code, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def update_node_service(
    node_code: str, payload: UpdateNodeRequest, request: Request
) -> Dict[str, Union[int, str, None]]:
    update_data = payload.model_dump(exclude_unset=True)
    if "agent_ip" in update_data:
        update_data["agent_ip"] = validate_agent_ip(update_data.get("agent_ip"))
    if "tuic_port_start" in update_data and update_data["tuic_port_start"] is None:
        raise HTTPException(status_code=400, detail="tuic_port_start must be an integer in 1-65535")
    if "tuic_port_end" in update_data and update_data["tuic_port_end"] is None:
        raise HTTPException(status_code=400, detail="tuic_port_end must be an integer in 1-65535")
    if "enabled" in update_data and update_data["enabled"] not in (0, 1):
        raise HTTPException(status_code=400, detail="enabled must be 0 or 1")
    if "reality_private_key" in update_data:
        private_key = update_data["reality_private_key"]
        if private_key is None or str(private_key).strip() == "":
            raise HTTPException(status_code=400, detail="reality_private_key must be a non-empty string")
    if "reality_public_key" in update_data:
        public_key = update_data["reality_public_key"]
        if public_key is None or str(public_key).strip() == "":
            raise HTTPException(status_code=400, detail="reality_public_key must be a non-empty string")
    if "reality_short_id" in update_data:
        short_id = update_data["reality_short_id"]
        if short_id is None:
            raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)")
        short_id_str = str(short_id)
        if not short_id_str.isalnum() and short_id_str != "":
            raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)")
        if len(short_id_str) > 8:
            raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)")
        if short_id_str != "":
            try:
                int(short_id_str, 16)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="reality_short_id must be a hex string (0-8 chars)") from exc
    if "supports_reality" in update_data and update_data["supports_reality"] is None:
        raise HTTPException(status_code=400, detail="supports_reality must be 0 or 1")
    if "supports_tuic" in update_data and update_data["supports_tuic"] is None:
        raise HTTPException(status_code=400, detail="supports_tuic must be 0 or 1")
    if "supports_reality" in update_data and update_data["supports_reality"] not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_reality must be 0 or 1")
    if "supports_tuic" in update_data and update_data["supports_tuic"] not in (0, 1):
        raise HTTPException(status_code=400, detail="supports_tuic must be 0 or 1")
    if "monitor_enabled" in update_data and update_data["monitor_enabled"] is None:
        raise HTTPException(status_code=400, detail="monitor_enabled must be 0 or 1")
    if "monitor_enabled" in update_data and update_data["monitor_enabled"] not in (0, 1):
        raise HTTPException(status_code=400, detail="monitor_enabled must be 0 or 1")

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="Node not found")

        port_start = int(update_data.get("tuic_port_start", existing["tuic_port_start"]))
        port_end = int(update_data.get("tuic_port_end", existing["tuic_port_end"]))
        if port_start > port_end:
            raise HTTPException(status_code=400, detail="Invalid port range: start must be <= end")

        if update_data:
            allowed_fields = {
                "region",
                "host",
                "agent_ip",
                "reality_server_name",
                "tuic_server_name",
                "tuic_listen_port",
                "monitor_enabled",
                "reality_private_key",
                "reality_public_key",
                "reality_short_id",
                "tuic_port_start",
                "tuic_port_end",
                "enabled",
                "supports_reality",
                "supports_tuic",
                "note",
            }
            assignments = []
            values = []
            for key, value in update_data.items():
                if key in allowed_fields:
                    assignments.append("{0} = ?".format(key))
                    values.append(value)
            if assignments:
                values.append(node_code)
                conn.execute(
                    "UPDATE nodes SET {0} WHERE node_code = ?".format(", ".join(assignments)),
                    tuple(values),
                )
                audit_update_data = dict(update_data)
                if "reality_private_key" in audit_update_data:
                    audit_update_data["reality_private_key"] = "***"
                write_audit_log(
                    conn,
                    action="node.update",
                    resource_type="node",
                    resource_id=node_code,
                    detail=audit_update_data,
                    actor=get_request_actor(request),
                    source_ip=get_source_ip_for_audit(request),
                )
                conn.commit()

        updated = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                agent_ip,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                monitor_enabled,
                last_seen_at,
                reality_public_key,
                reality_short_id,
                tuic_port_start,
                tuic_port_end,
                enabled,
                supports_reality,
                supports_tuic,
                note
            FROM nodes
            WHERE node_code = ?
            """,
            (node_code,),
        ).fetchone()

    if updated is None:
        raise HTTPException(status_code=404, detail="Node not found")
    return dict(updated)


def delete_node_service(node_code: str, request: Request) -> Dict[str, bool]:
    with get_connection() as conn:
        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        bound_row = conn.execute(
            "SELECT 1 FROM user_nodes WHERE node_code = ? LIMIT 1",
            (node_code,),
        ).fetchone()
        if bound_row is not None:
            raise HTTPException(status_code=400, detail="该节点仍有用户绑定，请先解绑后再删除")

        running_task_row = conn.execute(
            """
            SELECT 1
            FROM node_tasks
            WHERE node_code = ? AND status = 'running'
            LIMIT 1
            """,
            (node_code,),
        ).fetchone()
        if running_task_row is not None:
            raise HTTPException(status_code=400, detail="该节点仍有运行中的任务，请稍后重试")

        task_delete_cursor = conn.execute(
            "DELETE FROM node_tasks WHERE node_code = ?",
            (node_code,),
        )
        deleted_task_count = int(task_delete_cursor.rowcount or 0)
        conn.execute("DELETE FROM nodes WHERE node_code = ?", (node_code,))
        write_audit_log(
            conn,
            action="node.delete",
            resource_type="node",
            resource_id=node_code,
            detail={"ok": True, "deleted_node_tasks": deleted_task_count},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True}
