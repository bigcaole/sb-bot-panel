import base64
import sqlite3
import tarfile
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import quote

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "app.db"

app = FastAPI()


class CreateUserRequest(BaseModel):
    display_name: str = Field(min_length=1)
    tuic_port: int = Field(ge=1, le=65535)
    speed_mbps: int = Field(gt=0)
    valid_days: int = Field(gt=0)
    note: str = ""


class CreateNodeRequest(BaseModel):
    node_code: str = Field(min_length=1)
    region: str = ""
    host: str = Field(min_length=1)
    reality_server_name: Optional[str] = None
    tuic_server_name: Optional[str] = None
    tuic_listen_port: Optional[int] = Field(default=None, ge=1, le=65535)
    tuic_port_start: int = Field(ge=1, le=65535)
    tuic_port_end: int = Field(ge=1, le=65535)
    enabled: int = 1
    supports_reality: Optional[int] = None
    supports_tuic: Optional[int] = None
    note: str = ""


class AssignNodeRequest(BaseModel):
    node_code: str = Field(min_length=1)


class SetUserSpeedRequest(BaseModel):
    speed_mbps: int = Field(ge=1, le=10000)


class UpdateNodeRequest(BaseModel):
    region: Optional[str] = None
    host: Optional[str] = None
    reality_server_name: Optional[str] = None
    tuic_server_name: Optional[str] = None
    tuic_listen_port: Optional[int] = Field(default=None, ge=1, le=65535)
    reality_private_key: Optional[str] = None
    reality_public_key: Optional[str] = None
    reality_short_id: Optional[str] = None
    tuic_port_start: Optional[int] = Field(default=None, ge=1, le=65535)
    tuic_port_end: Optional[int] = Field(default=None, ge=1, le=65535)
    enabled: Optional[int] = None
    supports_reality: Optional[int] = None
    supports_tuic: Optional[int] = None
    note: Optional[str] = None


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_code TEXT UNIQUE,
                display_name TEXT,
                status TEXT,
                created_at INTEGER,
                expire_at INTEGER,
                grace_days INTEGER,
                speed_mbps INTEGER,
                limit_mode TEXT,
                mark INTEGER UNIQUE,
                vless_uuid TEXT,
                tuic_secret TEXT,
                tuic_port INTEGER UNIQUE,
                note TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nodes(
                node_code TEXT PRIMARY KEY,
                region TEXT,
                host TEXT,
                reality_server_name TEXT,
                tuic_server_name TEXT,
                tuic_listen_port INTEGER,
                reality_private_key TEXT,
                reality_public_key TEXT,
                reality_short_id TEXT,
                tuic_port_start INTEGER,
                tuic_port_end INTEGER,
                enabled INTEGER,
                supports_reality INTEGER,
                supports_tuic INTEGER,
                note TEXT
            )
            """
        )
        node_columns = conn.execute("PRAGMA table_info(nodes)").fetchall()
        node_column_names = set(row["name"] for row in node_columns)
        if "reality_server_name" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_server_name TEXT")
        if "tuic_server_name" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN tuic_server_name TEXT")
        if "tuic_listen_port" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN tuic_listen_port INTEGER")
        if "reality_private_key" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_private_key TEXT")
        if "reality_public_key" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_public_key TEXT")
        if "reality_short_id" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN reality_short_id TEXT")
        if "supports_reality" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN supports_reality INTEGER")
        if "supports_tuic" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN supports_tuic INTEGER")
        conn.execute("UPDATE nodes SET supports_reality = 1 WHERE supports_reality IS NULL")
        conn.execute("UPDATE nodes SET supports_tuic = 1 WHERE supports_tuic IS NULL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_nodes(
                user_code TEXT,
                node_code TEXT,
                tuic_port INTEGER,
                created_at INTEGER,
                PRIMARY KEY (user_code, node_code),
                FOREIGN KEY (user_code) REFERENCES users(user_code),
                FOREIGN KEY (node_code) REFERENCES nodes(node_code)
            )
            """
        )
        conn.commit()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/admin/backup")
def create_backup() -> Dict[str, Union[bool, int, str]]:
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
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Backup failed: {0}".format(exc)) from exc

    return {
        "ok": True,
        "path": str(backup_path),
        "size_bytes": size_bytes,
        "created_at": created_at,
    }


@app.post("/users/create")
def create_user(payload: CreateUserRequest) -> Dict[str, Union[int, str]]:
    now = int(time.time())

    with get_connection() as conn:
        row = conn.execute("SELECT COALESCE(MAX(mark), 1000) + 1 AS next_mark FROM users").fetchone()
        mark = int(row["next_mark"])
        user_code = f"u{mark}"
        vless_uuid = str(uuid.uuid4())
        tuic_secret = str(uuid.uuid4())
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


@app.post("/users/{user_code}/set_speed")
def set_user_speed(
    user_code: str, payload: SetUserSpeedRequest
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
        conn.commit()

    return {"ok": True, "user_code": user_code, "speed_mbps": payload.speed_mbps}


@app.post("/nodes/create")
def create_node(payload: CreateNodeRequest) -> Dict[str, Union[int, str, None]]:
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

    with get_connection() as conn:
        try:
            conn.execute(
                """
                INSERT INTO nodes(
                    node_code,
                    region,
                    host,
                    reality_server_name,
                    tuic_server_name,
                    tuic_listen_port,
                    tuic_port_start,
                    tuic_port_end,
                    enabled,
                    supports_reality,
                    supports_tuic,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.node_code,
                    payload.region,
                    payload.host,
                    payload.reality_server_name,
                    payload.tuic_server_name,
                    payload.tuic_listen_port,
                    payload.tuic_port_start,
                    payload.tuic_port_end,
                    payload.enabled,
                    supports_reality,
                    supports_tuic,
                    payload.note,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="node_code already exists") from exc

    return {
        "node_code": payload.node_code,
        "region": payload.region,
        "host": payload.host,
        "reality_server_name": payload.reality_server_name,
        "tuic_server_name": payload.tuic_server_name,
        "tuic_listen_port": payload.tuic_listen_port,
        "tuic_port_start": payload.tuic_port_start,
        "tuic_port_end": payload.tuic_port_end,
        "enabled": payload.enabled,
        "supports_reality": supports_reality,
        "supports_tuic": supports_tuic,
        "note": payload.note,
    }


@app.get("/nodes")
def list_nodes() -> List[Dict[str, Union[int, str, None]]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                reality_private_key,
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


@app.get("/nodes/{node_code}")
def get_node(node_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                reality_private_key,
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


@app.get("/nodes/{node_code}/stats")
def get_node_stats(node_code: str) -> Dict[str, Union[int, str]]:
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


# Used by node-side agent polling periodically to sync node and bound-user config.
@app.get("/nodes/{node_code}/sync")
def get_node_sync(node_code: str) -> Dict[str, Union[Dict, List, int]]:
    generated_at = int(time.time())
    with get_connection() as conn:
        node_row = conn.execute(
            """
            SELECT
                node_code,
                enabled,
                region,
                host,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
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

        user_rows = conn.execute(
            """
            SELECT
                u.user_code,
                u.display_name,
                u.status,
                u.expire_at,
                u.speed_mbps,
                u.vless_uuid,
                u.tuic_secret,
                un.tuic_port,
                un.created_at AS bound_at
            FROM user_nodes un
            JOIN users u ON u.user_code = un.user_code
            WHERE un.node_code = ?
            ORDER BY u.user_code ASC
            """,
            (node_code,),
        ).fetchall()

    return {
        "node": dict(node_row),
        "users": [dict(row) for row in user_rows],
        "generated_at": generated_at,
    }


@app.patch("/nodes/{node_code}")
def update_node(
    node_code: str, payload: UpdateNodeRequest
) -> Dict[str, Union[int, str, None]]:
    update_data = payload.model_dump(exclude_unset=True)
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

    with get_connection() as conn:
        existing = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                reality_private_key,
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
                "reality_server_name",
                "tuic_server_name",
                "tuic_listen_port",
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
                conn.commit()

        updated = conn.execute(
            """
            SELECT
                node_code,
                region,
                host,
                reality_server_name,
                tuic_server_name,
                tuic_listen_port,
                reality_private_key,
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


@app.delete("/nodes/{node_code}")
def delete_node(node_code: str) -> Dict[str, bool]:
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

        conn.execute("DELETE FROM nodes WHERE node_code = ?", (node_code,))
        conn.commit()

    return {"ok": True}


# Strategy B: each node owns an independent TUIC port pool.
# Port allocation is recorded in user_nodes, and assignment picks
# the smallest free port in [tuic_port_start, tuic_port_end].
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


@app.post("/users/{user_code}/assign_node")
def assign_node(
    user_code: str, payload: AssignNodeRequest
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
        conn.commit()

    return {"user_code": user_code, "node_code": payload.node_code, "tuic_port": tuic_port}


@app.post("/users/{user_code}/unassign_node")
def unassign_node(
    user_code: str, payload: AssignNodeRequest
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
        conn.commit()

    return {"ok": True, "user_code": user_code, "node_code": payload.node_code}


@app.get("/users/{user_code}/nodes")
def list_user_nodes(user_code: str) -> List[Dict[str, Union[int, str, None]]]:
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


@app.get("/users/{user_code}")
def get_user(user_code: str) -> Dict[str, Union[int, str, None]]:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_code = ?", (user_code,)).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return dict(row)


@app.get("/users")
def list_users() -> List[Dict[str, Union[int, str, None]]]:
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


def _build_subscription_links_text(user_code: str) -> str:
    with get_connection() as conn:
        user_row = conn.execute(
            """
            SELECT user_code, vless_uuid, tuic_secret
            FROM users
            WHERE user_code = ?
            """,
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_rows = conn.execute(
            """
            SELECT
                un.node_code,
                un.tuic_port,
                n.host,
                n.reality_server_name,
                n.tuic_server_name,
                n.tuic_listen_port,
                n.reality_public_key,
                n.reality_short_id,
                n.supports_reality,
                n.supports_tuic
            FROM user_nodes un
            JOIN nodes n ON n.node_code = un.node_code
            WHERE un.user_code = ? AND n.enabled = 1
            ORDER BY un.node_code ASC
            """,
            (user_code,),
        ).fetchall()

    lines: List[str] = []
    generated_links = 0

    for row in node_rows:
        node_code = str(row["node_code"])
        host = str(row["host"])
        reality_sni_raw = row["reality_server_name"] or ""
        tuic_sni_raw = row["tuic_server_name"] or ""
        supports_reality = 1 if row["supports_reality"] is None else int(row["supports_reality"])
        supports_tuic = 1 if row["supports_tuic"] is None else int(row["supports_tuic"])

        if supports_reality == 1:
            pbk_raw = row["reality_public_key"] or ""
            sid_raw = row["reality_short_id"] or ""
            if reality_sni_raw and pbk_raw and sid_raw:
                sni = quote(str(reality_sni_raw), safe="")
                pbk = quote(str(pbk_raw), safe="")
                sid = quote(str(sid_raw), safe="")
                name = quote("{0}-R".format(node_code), safe="")
                lines.append(
                    "vless://{0}@{1}:443?encryption=none&type=tcp&security=reality&sni={2}"
                    "&fp=chrome&pbk={3}&sid={4}#{5}".format(
                        user_row["vless_uuid"],
                        host,
                        sni,
                        pbk,
                        sid,
                        name,
                    )
                )
                generated_links += 1
            else:
                lines.append("# {0}: REALITY params missing, skipped vless link".format(node_code))

        # A 模式：节点单端口监听，TUIC 端口使用 nodes.tuic_listen_port（默认 8443）。
        # B 模式：节点端口池，才使用 user_nodes.tuic_port（当前生成链接逻辑中忽略该字段，仅保留兼容）。
        if supports_tuic == 1:
            try:
                tuic_port = int(row["tuic_listen_port"] or 8443)
            except (TypeError, ValueError):
                tuic_port = 8443
            name = quote("{0}-T".format(node_code), safe="")
            tuic_link = "tuic://{0}:{0}@{1}:{2}?alpn=h3".format(
                user_row["tuic_secret"], host, tuic_port
            )
            tuic_sni_final = str(tuic_sni_raw or host)
            tuic_link = "{0}&sni={1}".format(
                tuic_link, quote(str(tuic_sni_final), safe="")
            )
            lines.append("{0}#{1}".format(tuic_link, name))
            generated_links += 1

    if generated_links == 0:
        return "# no available links"

    return "\n".join(lines)


@app.get("/sub/links/{user_code}", response_class=PlainTextResponse)
def get_sub_links(user_code: str) -> PlainTextResponse:
    text = _build_subscription_links_text(user_code)
    return PlainTextResponse(content=text)


@app.get("/sub/base64/{user_code}", response_class=PlainTextResponse)
def get_sub_base64(user_code: str) -> PlainTextResponse:
    text = _build_subscription_links_text(user_code)
    encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
    return PlainTextResponse(content=encoded)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, reload=False)
