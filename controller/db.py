import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "app.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        # WAL + NORMAL 同步可显著降低读写阻塞，适合本项目这种高读低写场景。
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA wal_autocheckpoint = 1000")
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
                agent_ip TEXT,
                reality_server_name TEXT,
                tuic_server_name TEXT,
                tuic_listen_port INTEGER,
                monitor_enabled INTEGER,
                last_seen_at INTEGER,
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
        if "agent_ip" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN agent_ip TEXT")
        if "tuic_server_name" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN tuic_server_name TEXT")
        if "tuic_listen_port" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN tuic_listen_port INTEGER")
        if "monitor_enabled" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN monitor_enabled INTEGER")
        if "last_seen_at" not in node_column_names:
            conn.execute("ALTER TABLE nodes ADD COLUMN last_seen_at INTEGER")
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
        conn.execute("UPDATE nodes SET monitor_enabled = 0 WHERE monitor_enabled IS NULL")
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS node_tasks(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_code TEXT NOT NULL,
                task_type TEXT NOT NULL,
                payload_json TEXT,
                status TEXT NOT NULL,
                attempts INTEGER,
                max_attempts INTEGER,
                created_at INTEGER,
                updated_at INTEGER,
                result_text TEXT,
                FOREIGN KEY (node_code) REFERENCES nodes(node_code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT,
                action TEXT NOT NULL,
                resource_type TEXT,
                resource_id TEXT,
                detail TEXT,
                source_ip TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        node_task_columns = conn.execute("PRAGMA table_info(node_tasks)").fetchall()
        node_task_column_names = set(row["name"] for row in node_task_columns)
        if "attempts" not in node_task_column_names:
            conn.execute("ALTER TABLE node_tasks ADD COLUMN attempts INTEGER")
        if "max_attempts" not in node_task_column_names:
            conn.execute("ALTER TABLE node_tasks ADD COLUMN max_attempts INTEGER")
        conn.execute("UPDATE node_tasks SET attempts = 0 WHERE attempts IS NULL")
        conn.execute("UPDATE node_tasks SET max_attempts = 1 WHERE max_attempts IS NULL")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_node_tasks_node_status_id
            ON node_tasks(node_code, status, id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_node_tasks_status_updated_at
            ON node_tasks(status, updated_at DESC, id DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_nodes_node_code
            ON user_nodes(node_code)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_status_expire_at
            ON users(status, expire_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_nodes_monitor_last_seen
            ON nodes(monitor_enabled, last_seen_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at
            ON audit_logs(created_at DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_logs_action_created_at
            ON audit_logs(action, created_at DESC)
            """
        )
        conn.commit()
