import sqlite3
import unittest

from controller.audit import cleanup_old_audit_logs, write_audit_log


class AuditCleanupTestCase(unittest.TestCase):
    def _build_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE audit_logs(
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
        return conn

    def test_cleanup_old_audit_logs_by_retention(self) -> None:
        now_ts = 1_700_000_000
        with self._build_conn() as conn:
            write_audit_log(conn, action="x.a", created_at=now_ts - 40 * 86400)
            write_audit_log(conn, action="x.b", created_at=now_ts - 20 * 86400)
            write_audit_log(conn, action="x.c", created_at=now_ts - 1 * 86400)
            conn.commit()

            removed = cleanup_old_audit_logs(
                conn,
                now_ts=now_ts,
                retention_days=30,
                batch_size=100,
            )
            conn.commit()

            self.assertEqual(1, removed)
            remaining = int(conn.execute("SELECT COUNT(*) AS c FROM audit_logs").fetchone()["c"] or 0)
            self.assertEqual(2, remaining)

    def test_cleanup_old_audit_logs_honors_batch_size(self) -> None:
        now_ts = 1_700_000_000
        with self._build_conn() as conn:
            for idx in range(5):
                write_audit_log(conn, action=f"x.{idx}", created_at=now_ts - 50 * 86400 - idx)
            conn.commit()

            removed_first = cleanup_old_audit_logs(
                conn,
                now_ts=now_ts,
                retention_days=30,
                batch_size=2,
            )
            conn.commit()
            removed_second = cleanup_old_audit_logs(
                conn,
                now_ts=now_ts,
                retention_days=30,
                batch_size=2,
            )
            conn.commit()

            self.assertEqual(2, removed_first)
            self.assertEqual(2, removed_second)
            remaining = int(conn.execute("SELECT COUNT(*) AS c FROM audit_logs").fetchone()["c"] or 0)
            self.assertEqual(1, remaining)

    def test_write_audit_log_clamps_long_fields(self) -> None:
        with self._build_conn() as conn:
            write_audit_log(
                conn,
                action="a" * 300,
                resource_type="t" * 200,
                resource_id="r" * 500,
                detail={"k": "v"},
                actor="u" * 300,
                source_ip="x" * 200,
                created_at=1_700_000_000,
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT actor, action, resource_type, resource_id, detail, source_ip
                FROM audit_logs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(120, len(str(row["actor"] or "")))
            self.assertEqual(160, len(str(row["action"] or "")))
            self.assertEqual(80, len(str(row["resource_type"] or "")))
            self.assertEqual(240, len(str(row["resource_id"] or "")))
            self.assertEqual('{"k":"v"}', str(row["detail"] or ""))
            self.assertEqual(80, len(str(row["source_ip"] or "")))

    def test_write_audit_log_normalizes_control_chars(self) -> None:
        with self._build_conn() as conn:
            write_audit_log(
                conn,
                action="node.task\ncreate\tok",
                resource_type="http\radmin",
                resource_id=" /nodes/\nJP1\t ",
                detail="x",
                actor="bot\nops\t1",
                source_ip="127.0.0.1\n",
                created_at=1_700_000_000,
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT actor, action, resource_type, resource_id, source_ip
                FROM audit_logs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual("bot ops 1", str(row["actor"] or ""))
            self.assertEqual("node.task create ok", str(row["action"] or ""))
            self.assertEqual("http admin", str(row["resource_type"] or ""))
            self.assertEqual("/nodes/ JP1", str(row["resource_id"] or ""))
            self.assertEqual("127.0.0.1", str(row["source_ip"] or ""))


if __name__ == "__main__":
    unittest.main()
