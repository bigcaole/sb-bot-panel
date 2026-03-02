import sqlite3
import time
import uuid
from typing import Dict, List, Optional, Union

from fastapi import HTTPException, Request

from controller.audit import get_request_actor, get_source_ip_for_audit, write_audit_log
from controller.db import get_connection
from controller.schemas import (
    AssignNodeRequest,
    CreateUserRequest,
    SetUserLimitModeRequest,
    SetUserSpeedRequest,
    SetUserStatusRequest,
)


def _generate_unique_user_credential(conn: sqlite3.Connection, column_name: str) -> str:
    for _ in range(12):
        candidate = str(uuid.uuid4())
        exists_row = conn.execute(
            "SELECT 1 FROM users WHERE {0} = ? LIMIT 1".format(column_name),
            (candidate,),
        ).fetchone()
        if exists_row is None:
            return candidate
    raise HTTPException(status_code=500, detail="failed to allocate unique user credential")


def create_user_service(payload: CreateUserRequest, request: Request) -> Dict[str, Union[int, str]]:
    now = int(time.time())

    with get_connection() as conn:
        row = conn.execute("SELECT COALESCE(MAX(mark), 1000) + 1 AS next_mark FROM users").fetchone()
        mark = int(row["next_mark"])
        user_code = "u{0}".format(mark)
        vless_uuid = _generate_unique_user_credential(conn, "vless_uuid")
        tuic_secret = _generate_unique_user_credential(conn, "tuic_secret")
        expire_at = now + payload.valid_days * 86400

        try:
            conn.execute(
                """
                INSERT INTO users(
                    user_code,
                    display_name,
                    status,
                    created_at,
                    expire_at,
                    grace_days,
                    speed_mbps,
                    limit_mode,
                    mark,
                    vless_uuid,
                    tuic_secret,
                    tuic_port,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_code,
                    payload.display_name,
                    "active",
                    now,
                    expire_at,
                    3,
                    payload.speed_mbps,
                    "tc",
                    mark,
                    vless_uuid,
                    tuic_secret,
                    payload.tuic_port,
                    payload.note,
                ),
            )
            write_audit_log(
                conn,
                action="user.create",
                resource_type="user",
                resource_id=user_code,
                detail={
                    "display_name": payload.display_name,
                    "speed_mbps": payload.speed_mbps,
                    "valid_days": payload.valid_days,
                },
                actor=get_request_actor(request),
                source_ip=get_source_ip_for_audit(request),
                created_at=now,
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="Conflict: duplicate unique field") from exc

    return {
        "user_code": user_code,
        "mark": mark,
        "vless_uuid": vless_uuid,
        "tuic_secret": tuic_secret,
        "tuic_port": payload.tuic_port,
        "speed_mbps": payload.speed_mbps,
        "expire_at": expire_at,
    }


def set_user_speed_service(
    user_code: str, payload: SetUserSpeedRequest, request: Request
) -> Dict[str, Union[bool, int, str]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        conn.execute(
            "UPDATE users SET speed_mbps = ? WHERE user_code = ?",
            (payload.speed_mbps, user_code),
        )
        write_audit_log(
            conn,
            action="user.set_speed",
            resource_type="user",
            resource_id=user_code,
            detail={"speed_mbps": payload.speed_mbps},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "speed_mbps": payload.speed_mbps}


def set_user_status_service(
    user_code: str, payload: SetUserStatusRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    status_value = str(payload.status or "").strip().lower()
    if status_value not in ("active", "disabled"):
        raise HTTPException(status_code=400, detail="status must be active or disabled")

    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        conn.execute(
            "UPDATE users SET status = ? WHERE user_code = ?",
            (status_value, user_code),
        )
        write_audit_log(
            conn,
            action="user.set_status",
            resource_type="user",
            resource_id=user_code,
            detail={"status": status_value},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "status": status_value}


def set_user_limit_mode_service(
    user_code: str, payload: SetUserLimitModeRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    mode_value = str(payload.limit_mode or "").strip().lower()
    if mode_value not in ("tc", "off"):
        raise HTTPException(status_code=400, detail="limit_mode must be tc or off")

    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        conn.execute(
            "UPDATE users SET limit_mode = ? WHERE user_code = ?",
            (mode_value, user_code),
        )
        write_audit_log(
            conn,
            action="user.set_limit_mode",
            resource_type="user",
            resource_id=user_code,
            detail={"limit_mode": mode_value},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "limit_mode": mode_value}


def rotate_user_credentials_service(
    user_code: str, request: Request
) -> Dict[str, Union[bool, int, str]]:
    now_ts = int(time.time())
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        vless_uuid = _generate_unique_user_credential(conn, "vless_uuid")
        tuic_secret = _generate_unique_user_credential(conn, "tuic_secret")
        conn.execute(
            "UPDATE users SET vless_uuid = ?, tuic_secret = ? WHERE user_code = ?",
            (vless_uuid, tuic_secret, user_code),
        )
        bindings_row = conn.execute(
            "SELECT COUNT(*) AS c FROM user_nodes WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        bindings = int(bindings_row["c"] or 0) if bindings_row else 0
        write_audit_log(
            conn,
            action="user.rotate_credentials",
            resource_type="user",
            resource_id=user_code,
            detail={"bindings": bindings},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now_ts,
        )
        conn.commit()

    return {
        "ok": True,
        "user_code": user_code,
        "vless_uuid": vless_uuid,
        "tuic_secret": tuic_secret,
        "bindings": bindings,
    }


def delete_user_service(user_code: str, request: Request) -> Dict[str, Union[bool, str]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        binding_row = conn.execute(
            "SELECT 1 FROM user_nodes WHERE user_code = ? LIMIT 1",
            (user_code,),
        ).fetchone()
        if binding_row is not None:
            raise HTTPException(status_code=400, detail="该用户仍有节点绑定，请先解绑后再删除")

        conn.execute("DELETE FROM users WHERE user_code = ?", (user_code,))
        write_audit_log(
            conn,
            action="user.delete",
            resource_type="user",
            resource_id=user_code,
            detail={"ok": True},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code}


def _pick_smallest_free_port(
    conn: sqlite3.Connection, node_code: str, port_start: int, port_end: int
) -> Optional[int]:
    used_rows = conn.execute(
        "SELECT tuic_port FROM user_nodes WHERE node_code = ? ORDER BY tuic_port",
        (node_code,),
    ).fetchall()
    used_ports = set(int(row["tuic_port"]) for row in used_rows)
    for port in range(port_start, port_end + 1):
        if port not in used_ports:
            return port
    return None


def assign_node_service(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[int, str]]:
    now = int(time.time())
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_row = conn.execute(
            """
            SELECT node_code, tuic_port_start, tuic_port_end, enabled
            FROM nodes
            WHERE node_code = ?
            """,
            (payload.node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")
        if int(node_row["enabled"]) != 1:
            raise HTTPException(status_code=400, detail="Node is disabled")

        existing = conn.execute(
            """
            SELECT tuic_port
            FROM user_nodes
            WHERE user_code = ? AND node_code = ?
            """,
            (user_code, payload.node_code),
        ).fetchone()
        if existing is not None:
            raise HTTPException(status_code=409, detail="User already assigned to this node")

        tuic_port = _pick_smallest_free_port(
            conn,
            payload.node_code,
            int(node_row["tuic_port_start"]),
            int(node_row["tuic_port_end"]),
        )
        if tuic_port is None:
            raise HTTPException(status_code=409, detail="No available TUIC port in node pool")

        conn.execute(
            """
            INSERT INTO user_nodes(user_code, node_code, tuic_port, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (user_code, payload.node_code, tuic_port, now),
        )
        write_audit_log(
            conn,
            action="user.assign_node",
            resource_type="user_node",
            resource_id="{0}:{1}".format(user_code, payload.node_code),
            detail={"tuic_port": tuic_port},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
            created_at=now,
        )
        conn.commit()

    return {"user_code": user_code, "node_code": payload.node_code, "tuic_port": tuic_port}


def unassign_node_service(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_row = conn.execute(
            "SELECT node_code FROM nodes WHERE node_code = ?",
            (payload.node_code,),
        ).fetchone()
        if node_row is None:
            raise HTTPException(status_code=404, detail="Node not found")

        binding_row = conn.execute(
            """
            SELECT 1
            FROM user_nodes
            WHERE user_code = ? AND node_code = ?
            """,
            (user_code, payload.node_code),
        ).fetchone()
        if binding_row is None:
            raise HTTPException(status_code=404, detail="User-node binding not found")

        conn.execute(
            "DELETE FROM user_nodes WHERE user_code = ? AND node_code = ?",
            (user_code, payload.node_code),
        )
        write_audit_log(
            conn,
            action="user.unassign_node",
            resource_type="user_node",
            resource_id="{0}:{1}".format(user_code, payload.node_code),
            detail={"ok": True},
            actor=get_request_actor(request),
            source_ip=get_source_ip_for_audit(request),
        )
        conn.commit()

    return {"ok": True, "user_code": user_code, "node_code": payload.node_code}


def list_user_nodes_service(user_code: str) -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        user_row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        rows = conn.execute(
            """
            SELECT
                un.node_code,
                un.tuic_port,
                un.created_at,
                n.host,
                n.region,
                n.reality_server_name,
                n.enabled
            FROM user_nodes un
            JOIN nodes n ON n.node_code = un.node_code
            WHERE un.user_code = ?
            ORDER BY un.node_code
            """,
            (user_code,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_user_service(user_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_code = ?", (user_code,)).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return dict(row)


def list_users_service() -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                user_code,
                display_name,
                status,
                expire_at,
                speed_mbps
            FROM users
            ORDER BY id ASC
            LIMIT 500
            """
        ).fetchall()
    return [dict(row) for row in rows]
