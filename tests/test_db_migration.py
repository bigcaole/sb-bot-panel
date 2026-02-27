import tempfile
import time
import unittest
from pathlib import Path

from controller import db as db_module
from controller import db_migration


class DbMigrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._old_db_path = db_module.DB_PATH
        db_module.DB_PATH = Path(self._tmpdir.name) / "app.db"
        db_module.init_db()

        now_ts = int(time.time())
        with db_module.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO users(
                    user_code, display_name, status, created_at, expire_at, grace_days,
                    speed_mbps, limit_mode, mark, vless_uuid, tuic_secret, tuic_port, note
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "u1001",
                    "Alice",
                    "active",
                    now_ts,
                    now_ts + 3600,
                    3,
                    20,
                    "tc",
                    1001,
                    "00000000-0000-4000-8000-000000000001",
                    "secret-1",
                    20010,
                    "",
                ),
            )
            conn.execute(
                """
                INSERT INTO nodes(
                    node_code, region, host, enabled, supports_reality, supports_tuic,
                    tuic_port_start, tuic_port_end, monitor_enabled
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("JP1", "JP", "jp1.example.com", 1, 1, 1, 20010, 20019, 0),
            )
            conn.execute(
                """
                INSERT INTO user_nodes(user_code, node_code, tuic_port, created_at)
                VALUES(?, ?, ?, ?)
                """,
                ("u1001", "JP1", 20010, now_ts),
            )
            conn.commit()

    def tearDown(self) -> None:
        db_module.DB_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def test_export_and_verify_roundtrip(self) -> None:
        export_dir = Path(self._tmpdir.name) / "exports"
        result = db_migration.export_db_snapshot(export_dir, keep_count=5)
        self.assertTrue(str(result.get("path", "")).endswith(".json.gz"))

        payload = db_migration.load_export_payload(Path(str(result["path"])))
        validation = db_migration.validate_export_payload(payload)
        self.assertTrue(bool(validation.get("snapshot_valid")))

        compare = db_migration.compare_snapshot_with_live(payload)
        self.assertTrue(bool(compare.get("snapshot_valid")))
        self.assertTrue(bool(compare.get("live_match")))

    def test_compare_detects_data_drift(self) -> None:
        export_dir = Path(self._tmpdir.name) / "exports"
        result = db_migration.export_db_snapshot(export_dir, keep_count=5)
        payload = db_migration.load_export_payload(Path(str(result["path"])))

        with db_module.get_connection() as conn:
            conn.execute("UPDATE users SET speed_mbps = 30 WHERE user_code = 'u1001'")
            conn.commit()

        compare = db_migration.compare_snapshot_with_live(payload)
        self.assertFalse(bool(compare.get("live_match")))
        mismatches = compare.get("mismatches", [])
        self.assertTrue(isinstance(mismatches, list) and len(mismatches) >= 1)


if __name__ == "__main__":
    unittest.main()
