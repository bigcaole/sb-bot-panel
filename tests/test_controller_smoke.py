import tempfile
import time
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from controller import app as app_module
from controller import db as db_module
from controller import routers_admin as admin_router_module
from controller import routers_nodes as nodes_router_module
from controller import routers_sub as sub_router_module
from controller import security as security_module


class ControllerSmokeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._old_db_path = db_module.DB_PATH
        db_module.DB_PATH = Path(self._tmpdir.name) / "app.db"

        self._old_values = {
            "app.AUTH_TOKEN": app_module.AUTH_TOKEN,
            "app.API_RATE_LIMIT_ENABLED": app_module.API_RATE_LIMIT_ENABLED,
            "security.AUTH_TOKEN": security_module.AUTH_TOKEN,
            "security.API_RATE_LIMIT_ENABLED": security_module.API_RATE_LIMIT_ENABLED,
            "routers_sub.SUB_LINK_SIGN_KEY": sub_router_module.SUB_LINK_SIGN_KEY,
            "routers_sub.SUB_LINK_REQUIRE_SIGNATURE": sub_router_module.SUB_LINK_REQUIRE_SIGNATURE,
            "routers_admin.AUTH_TOKEN": admin_router_module.AUTH_TOKEN,
            "routers_admin.SUB_LINK_SIGN_KEY": admin_router_module.SUB_LINK_SIGN_KEY,
            "routers_admin.SUB_LINK_REQUIRE_SIGNATURE": admin_router_module.SUB_LINK_REQUIRE_SIGNATURE,
            "routers_admin.SUB_LINK_DEFAULT_TTL_SECONDS": admin_router_module.SUB_LINK_DEFAULT_TTL_SECONDS,
            "routers_nodes.NODE_TASK_MAX_PENDING_PER_NODE": nodes_router_module.NODE_TASK_MAX_PENDING_PER_NODE,
        }

        app_module.AUTH_TOKEN = "test-token"
        app_module.API_RATE_LIMIT_ENABLED = False
        security_module.AUTH_TOKEN = "test-token"
        security_module.API_RATE_LIMIT_ENABLED = False
        sub_router_module.SUB_LINK_SIGN_KEY = "sign-key"
        sub_router_module.SUB_LINK_REQUIRE_SIGNATURE = True
        admin_router_module.AUTH_TOKEN = "test-token"
        admin_router_module.SUB_LINK_SIGN_KEY = "sign-key"
        admin_router_module.SUB_LINK_REQUIRE_SIGNATURE = True
        admin_router_module.SUB_LINK_DEFAULT_TTL_SECONDS = 600
        nodes_router_module.NODE_TASK_MAX_PENDING_PER_NODE = 2

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
                    now_ts + 86400,
                    3,
                    20,
                    "tc",
                    1001,
                    "00000000-0000-4000-8000-000000000001",
                    "tuic-secret-1",
                    20010,
                    "",
                ),
            )
            conn.execute(
                """
                INSERT INTO nodes(
                    node_code, region, host, reality_server_name, tuic_server_name, tuic_listen_port,
                    monitor_enabled, last_seen_at, reality_public_key, reality_short_id,
                    tuic_port_start, tuic_port_end, enabled, supports_reality, supports_tuic, note
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "JP1",
                    "JP",
                    "jp1.example.com",
                    "www.cloudflare.com",
                    "jp1.example.com",
                    8443,
                    1,
                    now_ts,
                    "pubkey",
                    "1a2b3c4d",
                    20010,
                    20019,
                    1,
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

    def tearDown(self) -> None:
        app_module.AUTH_TOKEN = self._old_values["app.AUTH_TOKEN"]
        app_module.API_RATE_LIMIT_ENABLED = self._old_values["app.API_RATE_LIMIT_ENABLED"]
        security_module.AUTH_TOKEN = self._old_values["security.AUTH_TOKEN"]
        security_module.API_RATE_LIMIT_ENABLED = self._old_values["security.API_RATE_LIMIT_ENABLED"]
        sub_router_module.SUB_LINK_SIGN_KEY = self._old_values["routers_sub.SUB_LINK_SIGN_KEY"]
        sub_router_module.SUB_LINK_REQUIRE_SIGNATURE = self._old_values[
            "routers_sub.SUB_LINK_REQUIRE_SIGNATURE"
        ]
        admin_router_module.AUTH_TOKEN = self._old_values["routers_admin.AUTH_TOKEN"]
        admin_router_module.SUB_LINK_SIGN_KEY = self._old_values["routers_admin.SUB_LINK_SIGN_KEY"]
        admin_router_module.SUB_LINK_REQUIRE_SIGNATURE = self._old_values[
            "routers_admin.SUB_LINK_REQUIRE_SIGNATURE"
        ]
        admin_router_module.SUB_LINK_DEFAULT_TTL_SECONDS = self._old_values[
            "routers_admin.SUB_LINK_DEFAULT_TTL_SECONDS"
        ]
        nodes_router_module.NODE_TASK_MAX_PENDING_PER_NODE = self._old_values[
            "routers_nodes.NODE_TASK_MAX_PENDING_PER_NODE"
        ]
        db_module.DB_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def _auth_header(self) -> dict:
        return {"Authorization": "Bearer test-token"}

    def test_health_and_auth_smoke(self) -> None:
        with TestClient(app_module.app) as client:
            health_resp = client.get("/health")
            self.assertEqual(200, health_resp.status_code)
            self.assertEqual({"ok": True}, health_resp.json())

            unauthorized_nodes = client.get("/nodes")
            self.assertEqual(401, unauthorized_nodes.status_code)

            authorized_nodes = client.get("/nodes", headers=self._auth_header())
            self.assertEqual(200, authorized_nodes.status_code)
            nodes = authorized_nodes.json()
            self.assertEqual("JP1", nodes[0]["node_code"])

            users_resp = client.get("/users", headers=self._auth_header())
            self.assertEqual(200, users_resp.status_code)
            self.assertEqual("u1001", users_resp.json()[0]["user_code"])

            admin_sec = client.get("/admin/security/status", headers=self._auth_header())
            self.assertEqual(200, admin_sec.status_code)
            self.assertTrue(bool(admin_sec.json().get("auth_enabled")))
            self.assertIn("security_events_exclude_local", admin_sec.json())
            self.assertIn("controller_port_whitelist_count", admin_sec.json())

            overview_resp = client.get("/admin/overview", headers=self._auth_header())
            self.assertEqual(200, overview_resp.status_code)
            overview_payload = overview_resp.json()
            self.assertIn("totals", overview_payload)
            self.assertEqual(1, int(overview_payload["totals"]["users"]))
            self.assertEqual(1, int(overview_payload["totals"]["active_users"]))
            self.assertEqual(0, int(overview_payload["totals"]["disabled_users"]))
            self.assertEqual(1, int(overview_payload["totals"]["nodes"]))
            self.assertIn("monitor", overview_payload)
            self.assertIn("tasks", overview_payload)
            self.assertIn("queue_cap_per_node", overview_payload["tasks"])
            self.assertIn("near_cap_threshold", overview_payload["tasks"])
            self.assertIn("near_cap_nodes", overview_payload["tasks"])
            self.assertIn("security", overview_payload)
            self.assertIn("security_events", overview_payload)
            self.assertIn("unauthorized_1h", overview_payload["security_events"])
            self.assertIn("unauthorized_24h", overview_payload["security_events"])
            self.assertIn("top_unauthorized_ips", overview_payload["security_events"])

            db_integrity = client.get("/admin/db/integrity", headers=self._auth_header())
            self.assertEqual(200, db_integrity.status_code)
            self.assertTrue(bool(db_integrity.json().get("ok")))

            sec_events = client.get(
                "/admin/security/events?window_seconds=3600&top=3",
                headers=self._auth_header(),
            )
            self.assertEqual(200, sec_events.status_code)
            sec_payload = sec_events.json()
            self.assertEqual(3600, int(sec_payload.get("window_seconds", 0)))
            self.assertIn("include_local", sec_payload)
            self.assertIn("since", sec_payload)
            self.assertIn("unauthorized", sec_payload)
            self.assertIn("top_unauthorized_ips", sec_payload)

            node_access = client.get("/admin/node_access/status", headers=self._auth_header())
            self.assertEqual(200, node_access.status_code)
            node_access_payload = node_access.json()
            self.assertIn("controller_port_whitelist", node_access_payload)
            self.assertIn("whitelist_missing_nodes", node_access_payload)
            self.assertIn("enabled_nodes", node_access_payload)
            self.assertIn("locked_enabled_nodes", node_access_payload)
            self.assertIn("unlocked_enabled_nodes", node_access_payload)

    def test_subscription_sign_and_access_smoke(self) -> None:
        with TestClient(app_module.app) as client:
            direct = client.get("/sub/links/u1001")
            self.assertEqual(403, direct.status_code)

            sign_resp = client.get(
                "/admin/sub/sign/u1001",
                headers=self._auth_header(),
            )
            self.assertEqual(200, sign_resp.status_code)
            signed = sign_resp.json()
            self.assertTrue(bool(signed.get("signed")))

            signed_links_url = str(signed["links_url"])
            signed_path = signed_links_url.replace("http://testserver", "")
            signed_resp = client.get(signed_path)
            self.assertEqual(200, signed_resp.status_code)
            body = signed_resp.text
            self.assertIn("vless://", body)
            self.assertIn("tuic://", body)

    def test_db_export_and_verify_smoke(self) -> None:
        with TestClient(app_module.app) as client:
            export_resp = client.post("/admin/db/export", headers=self._auth_header())
            self.assertEqual(200, export_resp.status_code)
            export_payload = export_resp.json()
            self.assertTrue(bool(export_payload.get("ok")))
            export_path = str(export_payload.get("path", ""))
            self.assertTrue(export_path.endswith(".json.gz"))

            verify_resp = client.post(
                "/admin/db/verify_export",
                headers=self._auth_header(),
                json={"path": export_path, "compare_live": True},
            )
            self.assertEqual(200, verify_resp.status_code)
            verify_payload = verify_resp.json()
            self.assertTrue(bool(verify_payload.get("ok")))
            self.assertTrue(bool(verify_payload.get("snapshot_valid")))
            self.assertTrue(bool(verify_payload.get("live_match")))

    def test_node_task_deduplicate_smoke(self) -> None:
        with TestClient(app_module.app) as client:
            first_resp = client.post(
                "/nodes/JP1/tasks/create",
                headers=self._auth_header(),
                json={"task_type": "status_agent"},
            )
            self.assertEqual(200, first_resp.status_code)
            first_task = first_resp.json()
            self.assertFalse(bool(first_task.get("deduplicated")))
            first_id = int(first_task["id"])

            second_resp = client.post(
                "/nodes/JP1/tasks/create",
                headers=self._auth_header(),
                json={"task_type": "status_agent"},
            )
            self.assertEqual(200, second_resp.status_code)
            second_task = second_resp.json()
            self.assertTrue(bool(second_task.get("deduplicated")))
            self.assertEqual(first_id, int(second_task["id"]))

            force_resp = client.post(
                "/nodes/JP1/tasks/create",
                headers=self._auth_header(),
                json={"task_type": "status_agent", "force_new": True},
            )
            self.assertEqual(200, force_resp.status_code)
            force_task = force_resp.json()
            self.assertFalse(bool(force_task.get("deduplicated")))
            self.assertNotEqual(first_id, int(force_task["id"]))

    def test_node_task_backlog_limit_smoke(self) -> None:
        with TestClient(app_module.app) as client:
            first_resp = client.post(
                "/nodes/JP1/tasks/create",
                headers=self._auth_header(),
                json={"task_type": "status_agent", "force_new": True},
            )
            self.assertEqual(200, first_resp.status_code)
            second_resp = client.post(
                "/nodes/JP1/tasks/create",
                headers=self._auth_header(),
                json={"task_type": "status_singbox", "force_new": True},
            )
            self.assertEqual(200, second_resp.status_code)
            third_resp = client.post(
                "/nodes/JP1/tasks/create",
                headers=self._auth_header(),
                json={"task_type": "restart_singbox", "force_new": True},
            )
            self.assertEqual(429, third_resp.status_code)
            self.assertEqual("too many pending tasks for node", str(third_resp.json().get("detail")))


if __name__ == "__main__":
    unittest.main()
