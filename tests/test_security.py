import unittest

from controller import security


class SecurityTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_auth_token = security.AUTH_TOKEN
        self._old_admin_auth_token = security.ADMIN_AUTH_TOKEN
        self._old_node_auth_token = security.NODE_AUTH_TOKEN
        self._old_api_docs_enabled = security.API_DOCS_ENABLED
        self._old_window = security.API_RATE_LIMIT_WINDOW_SECONDS
        self._old_max = security.API_RATE_LIMIT_MAX_REQUESTS
        self._old_state_max_keys = security.RATE_LIMIT_STATE_MAX_KEYS
        self._old_unauth_sample = security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS
        self._old_unauth_state_max_keys = security.UNAUTHORIZED_AUDIT_STATE_MAX_KEYS
        security._RATE_LIMIT_STATE.clear()
        security._RATE_LIMIT_LAST_CLEANUP_AT = 0
        security._UNAUTH_AUDIT_STATE.clear()
        security._UNAUTH_AUDIT_LAST_CLEANUP_AT = 0

    def tearDown(self) -> None:
        security.AUTH_TOKEN = self._old_auth_token
        security.ADMIN_AUTH_TOKEN = self._old_admin_auth_token
        security.NODE_AUTH_TOKEN = self._old_node_auth_token
        security.API_DOCS_ENABLED = self._old_api_docs_enabled
        security.API_RATE_LIMIT_WINDOW_SECONDS = self._old_window
        security.API_RATE_LIMIT_MAX_REQUESTS = self._old_max
        security.RATE_LIMIT_STATE_MAX_KEYS = self._old_state_max_keys
        security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = self._old_unauth_sample
        security.UNAUTHORIZED_AUDIT_STATE_MAX_KEYS = self._old_unauth_state_max_keys
        security._RATE_LIMIT_STATE.clear()
        security._RATE_LIMIT_LAST_CLEANUP_AT = 0
        security._UNAUTH_AUDIT_STATE.clear()
        security._UNAUTH_AUDIT_LAST_CLEANUP_AT = 0

    def test_is_auth_exempt_path(self) -> None:
        security.API_DOCS_ENABLED = True
        self.assertTrue(security.is_auth_exempt_path("/health"))
        self.assertTrue(security.is_auth_exempt_path("/docs"))
        self.assertTrue(security.is_auth_exempt_path("/favicon.ico"))
        self.assertTrue(security.is_auth_exempt_path("/sub/links/u1001"))
        self.assertFalse(security.is_auth_exempt_path("/nodes"))

    def test_is_auth_exempt_path_when_docs_disabled(self) -> None:
        security.API_DOCS_ENABLED = False
        self.assertFalse(security.is_auth_exempt_path("/docs"))
        self.assertFalse(security.is_auth_exempt_path("/openapi.json"))
        self.assertFalse(security.is_auth_exempt_path("/redoc"))

    def test_verify_admin_authorization_optional_when_no_token(self) -> None:
        security.AUTH_TOKEN = ""
        security.ADMIN_AUTH_TOKEN = ""
        security.NODE_AUTH_TOKEN = ""
        self.assertIsNone(security.verify_admin_authorization(None))

    def test_verify_admin_authorization_strict_when_token_set(self) -> None:
        security.AUTH_TOKEN = "abc123"
        security.ADMIN_AUTH_TOKEN = ""
        security.NODE_AUTH_TOKEN = ""
        self.assertIsNotNone(security.verify_admin_authorization(None))
        self.assertIsNotNone(security.verify_admin_authorization("Bearer wrong"))
        self.assertIsNone(security.verify_admin_authorization("Bearer abc123"))

    def test_verify_admin_authorization_accepts_token_rotation_list(self) -> None:
        security.AUTH_TOKEN = "newtoken,oldtoken"
        security.ADMIN_AUTH_TOKEN = ""
        security.NODE_AUTH_TOKEN = ""
        self.assertIsNone(security.verify_admin_authorization("Bearer newtoken"))
        self.assertIsNone(security.verify_admin_authorization("Bearer oldtoken"))
        self.assertIsNotNone(security.verify_admin_authorization("Bearer invalid"))

    def test_split_tokens_admin_and_node_are_isolated(self) -> None:
        security.AUTH_TOKEN = ""
        security.ADMIN_AUTH_TOKEN = "admin-only"
        security.NODE_AUTH_TOKEN = "node-only"
        self.assertIsNone(security.verify_admin_authorization("Bearer admin-only"))
        self.assertIsNotNone(security.verify_admin_authorization("Bearer node-only"))
        self.assertIsNone(security.verify_node_authorization("Bearer node-only"))
        self.assertIsNotNone(security.verify_node_authorization("Bearer admin-only"))

    def test_split_tokens_fallback_to_legacy_when_missing(self) -> None:
        security.AUTH_TOKEN = "legacy-token"
        security.ADMIN_AUTH_TOKEN = ""
        security.NODE_AUTH_TOKEN = ""
        self.assertIsNone(security.verify_admin_authorization("Bearer legacy-token"))
        self.assertIsNone(security.verify_node_authorization("Bearer legacy-token"))

    def test_is_node_agent_auth_path(self) -> None:
        self.assertTrue(security.is_node_agent_auth_path("/nodes/JP1/sync"))
        self.assertTrue(security.is_node_agent_auth_path("/nodes/JP1/tasks/next"))
        self.assertTrue(security.is_node_agent_auth_path("/nodes/JP1/tasks/12/report"))
        self.assertTrue(security.is_node_agent_auth_path("/nodes/JP1/report_reality"))
        self.assertFalse(security.is_node_agent_auth_path("/nodes/JP1/tasks/create"))
        self.assertFalse(security.is_node_agent_auth_path("/admin/nodes/JP1/sync_preview"))

    def test_check_and_consume_rate_limit_window(self) -> None:
        security.API_RATE_LIMIT_WINDOW_SECONDS = 10
        security.API_RATE_LIMIT_MAX_REQUESTS = 2

        limited, retry_after = security.check_and_consume_rate_limit("ip:/nodes", 100)
        self.assertEqual((limited, retry_after), (False, 0))
        limited, retry_after = security.check_and_consume_rate_limit("ip:/nodes", 101)
        self.assertEqual((limited, retry_after), (False, 0))
        limited, retry_after = security.check_and_consume_rate_limit("ip:/nodes", 102)
        self.assertTrue(limited)
        self.assertGreaterEqual(retry_after, 1)

        limited, retry_after = security.check_and_consume_rate_limit("ip:/nodes", 111)
        self.assertEqual((limited, retry_after), (False, 0))

    def test_build_rate_limit_path_key(self) -> None:
        self.assertEqual("/nodes/*", security.build_rate_limit_path_key("/nodes/JP1"))
        self.assertEqual(
            "/nodes/*/tasks/*/report",
            security.build_rate_limit_path_key("/nodes/JP1/tasks/123/report"),
        )
        self.assertEqual(
            "/users/*/set_speed",
            security.build_rate_limit_path_key("/users/u1001/set_speed"),
        )
        self.assertEqual(
            "/users/*/set_limit_mode",
            security.build_rate_limit_path_key("/users/u1001/set_limit_mode"),
        )
        self.assertEqual(
            "/admin/security/status",
            security.build_rate_limit_path_key("/admin/security/status"),
        )

    def test_is_rate_limit_target_path(self) -> None:
        self.assertTrue(security.is_rate_limit_target_path("/nodes"))
        self.assertTrue(security.is_rate_limit_target_path("/users"))
        self.assertTrue(security.is_rate_limit_target_path("/admin/overview"))
        self.assertFalse(security.is_rate_limit_target_path("/nodes/JP1/sync"))
        self.assertFalse(security.is_rate_limit_target_path("/sub/links/u1001"))

    def test_should_write_unauthorized_audit_sampling_window(self) -> None:
        security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = 30
        key = security.build_unauthorized_audit_key("1.2.3.4", "/nodes", "GET")

        should_write, dropped = security.should_write_unauthorized_audit(key, 100)
        self.assertEqual((should_write, dropped), (True, 0))

        should_write, dropped = security.should_write_unauthorized_audit(key, 101)
        self.assertEqual((should_write, dropped), (False, 1))

        should_write, dropped = security.should_write_unauthorized_audit(key, 102)
        self.assertEqual((should_write, dropped), (False, 2))

        should_write, dropped = security.should_write_unauthorized_audit(key, 131)
        self.assertEqual((should_write, dropped), (True, 2))

    def test_should_write_unauthorized_audit_sampling_disabled(self) -> None:
        security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = 0
        key = security.build_unauthorized_audit_key("1.2.3.4", "/nodes", "GET")
        self.assertEqual(security.should_write_unauthorized_audit(key, 100), (True, 0))
        self.assertEqual(security.should_write_unauthorized_audit(key, 101), (True, 0))

    def test_rate_limit_state_prunes_when_exceeds_max_keys(self) -> None:
        security.API_RATE_LIMIT_WINDOW_SECONDS = 3600
        security.API_RATE_LIMIT_MAX_REQUESTS = 100
        security.RATE_LIMIT_STATE_MAX_KEYS = 100
        now_ts = 100
        for index in range(101):
            identity = f"ip{index}:/admin/overview"
            limited, retry_after = security.check_and_consume_rate_limit(identity, now_ts + index)
            self.assertEqual((limited, retry_after), (False, 0))

        self.assertLessEqual(len(security._RATE_LIMIT_STATE), 100)
        self.assertNotIn("ip0:/admin/overview", security._RATE_LIMIT_STATE)

    def test_unauth_audit_state_prunes_when_exceeds_max_keys(self) -> None:
        security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = 30
        security.UNAUTHORIZED_AUDIT_STATE_MAX_KEYS = 100
        for index in range(101):
            key = security.build_unauthorized_audit_key(f"198.51.100.{index}", "/nodes", "GET")
            should_write, dropped = security.should_write_unauthorized_audit(key, 100 + index)
            self.assertEqual((should_write, dropped), (True, 0))

        self.assertLessEqual(len(security._UNAUTH_AUDIT_STATE), 100)
        old_key = security.build_unauthorized_audit_key("198.51.100.0", "/nodes", "GET")
        self.assertNotIn(old_key, security._UNAUTH_AUDIT_STATE)


if __name__ == "__main__":
    unittest.main()
