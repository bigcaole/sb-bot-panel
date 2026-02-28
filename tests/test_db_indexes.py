import tempfile
import unittest
from pathlib import Path

from controller import db as db_module


class DbIndexesTestCase(unittest.TestCase):
    def test_audit_log_source_ip_index_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_db_path = db_module.DB_PATH
            try:
                db_module.DB_PATH = Path(tmp_dir) / "app.db"
                db_module.init_db()
                with db_module.get_connection() as conn:
                    row = conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name = 'idx_audit_logs_action_source_ip_created_at'
                        """
                    ).fetchone()
                self.assertIsNotNone(row)
            finally:
                db_module.DB_PATH = old_db_path

    def test_audit_log_actor_index_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_db_path = db_module.DB_PATH
            try:
                db_module.DB_PATH = Path(tmp_dir) / "app.db"
                db_module.init_db()
                with db_module.get_connection() as conn:
                    row = conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name = 'idx_audit_logs_actor_created_at'
                        """
                    ).fetchone()
                self.assertIsNotNone(row)
            finally:
                db_module.DB_PATH = old_db_path

    def test_audit_log_action_created_source_index_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_db_path = db_module.DB_PATH
            try:
                db_module.DB_PATH = Path(tmp_dir) / "app.db"
                db_module.init_db()
                with db_module.get_connection() as conn:
                    row = conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name = 'idx_audit_logs_action_created_at_source_ip'
                        """
                    ).fetchone()
                self.assertIsNotNone(row)
            finally:
                db_module.DB_PATH = old_db_path

    def test_audit_log_action_actor_index_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_db_path = db_module.DB_PATH
            try:
                db_module.DB_PATH = Path(tmp_dir) / "app.db"
                db_module.init_db()
                with db_module.get_connection() as conn:
                    row = conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name = 'idx_audit_logs_action_actor_created_at'
                        """
                    ).fetchone()
                self.assertIsNotNone(row)
            finally:
                db_module.DB_PATH = old_db_path

    def test_security_ip_blocks_created_at_index_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            old_db_path = db_module.DB_PATH
            try:
                db_module.DB_PATH = Path(tmp_dir) / "app.db"
                db_module.init_db()
                with db_module.get_connection() as conn:
                    row = conn.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type = 'index'
                          AND name = 'idx_security_ip_blocks_created_at'
                        """
                    ).fetchone()
                self.assertIsNotNone(row)
            finally:
                db_module.DB_PATH = old_db_path


if __name__ == "__main__":
    unittest.main()
