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


if __name__ == "__main__":
    unittest.main()
