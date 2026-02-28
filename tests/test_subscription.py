import tempfile
import time
import unittest
from pathlib import Path

from fastapi import HTTPException

from controller import db
from controller.subscription import (
    build_subscription_links_text,
    build_signed_subscription_urls,
    build_sub_signature,
    verify_sub_access,
)


class SubscriptionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmpdir.name) / "app.db"
        db.init_db()
        now_ts = int(time.time())
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO users(
                    user_code, display_name, status, created_at, expire_at, grace_days, speed_mbps,
                    limit_mode, mark, vless_uuid, tuic_secret, tuic_port, note
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "u1001",
                    "test",
                    "active",
                    now_ts,
                    now_ts + 86400,
                    3,
                    10,
                    "tc",
                    1001,
                    "00000000-0000-4000-8000-000000000001",
                    "secret-1",
                    20010,
                    "",
                ),
            )
            conn.commit()

    def tearDown(self) -> None:
        db.DB_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def test_build_sub_signature_is_deterministic(self) -> None:
        sig1 = build_sub_signature("u1001", 1234567890, "k1")
        sig2 = build_sub_signature("u1001", 1234567890, "k1")
        self.assertEqual(sig1, sig2)
        self.assertEqual(len(sig1), 64)

    def test_verify_sub_access_requires_signature_when_enabled(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            verify_sub_access("u1001", sign_key="k1", require_signature=True, exp="", sig="")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_verify_sub_access_accepts_valid_signature(self) -> None:
        expire_at = int(time.time()) + 300
        sig = build_sub_signature("u1001", expire_at, "k1")
        verify_sub_access(
            "u1001",
            sign_key="k1",
            require_signature=True,
            exp=str(expire_at),
            sig=sig,
        )

    def test_build_signed_subscription_urls_unsigned_when_key_empty(self) -> None:
        result = build_signed_subscription_urls(
            user_code="u1001",
            base_url="http://127.0.0.1:8080",
            ttl_seconds=0,
            default_ttl_seconds=600,
            sign_key="",
        )
        self.assertEqual(result["signed"], False)
        self.assertIn("/sub/links/u1001", result["links_url"])
        self.assertNotIn("?exp=", result["links_url"])

    def test_verify_sub_access_rejects_disabled_user(self) -> None:
        with db.get_connection() as conn:
            conn.execute("UPDATE users SET status = 'disabled' WHERE user_code = ?", ("u1001",))
            conn.commit()

        with self.assertRaises(HTTPException) as ctx:
            verify_sub_access("u1001", sign_key="", require_signature=False, exp="", sig="")
        self.assertEqual(403, ctx.exception.status_code)

    def test_verify_sub_access_rejects_expired_user(self) -> None:
        with db.get_connection() as conn:
            conn.execute(
                "UPDATE users SET status = 'active', expire_at = ? WHERE user_code = ?",
                (int(time.time()) - 1, "u1001"),
            )
            conn.commit()

        with self.assertRaises(HTTPException) as ctx:
            verify_sub_access("u1001", sign_key="", require_signature=False, exp="", sig="")
        self.assertEqual(403, ctx.exception.status_code)

    def test_links_text_explains_no_binding_reason(self) -> None:
        text = build_subscription_links_text("u1001")
        self.assertIn("# no available links", text)
        self.assertIn("user has no node bindings", text)

    def test_links_text_explains_all_nodes_disabled_reason(self) -> None:
        now_ts = int(time.time())
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO nodes(
                    node_code, region, host, reality_server_name, tuic_server_name,
                    tuic_listen_port, monitor_enabled, tuic_port_start, tuic_port_end,
                    enabled, supports_reality, supports_tuic, note
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "JP1",
                    "JP",
                    "jp1.example.com",
                    "www.cloudflare.com",
                    "jp1.example.com",
                    8443,
                    0,
                    20010,
                    20019,
                    0,
                    1,
                    1,
                    "",
                ),
            )
            conn.execute(
                """
                INSERT INTO user_nodes(user_code, node_code, tuic_port, created_at)
                VALUES(?, ?, ?, ?)
                """,
                ("u1001", "JP1", 20010, now_ts),
            )
            conn.commit()

        text = build_subscription_links_text("u1001")
        self.assertIn("# no available links", text)
        self.assertIn("all bound nodes are disabled", text)


if __name__ == "__main__":
    unittest.main()
