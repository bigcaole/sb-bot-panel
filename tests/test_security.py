import unittest

from controller import security


class SecurityTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_auth_token = security.AUTH_TOKEN
        self._old_window = security.API_RATE_LIMIT_WINDOW_SECONDS
        self._old_max = security.API_RATE_LIMIT_MAX_REQUESTS
        self._old_unauth_sample = security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS
        security._RATE_LIMIT_STATE.clear()
        security._RATE_LIMIT_LAST_CLEANUP_AT = 0
        security._UNAUTH_AUDIT_STATE.clear()
        security._UNAUTH_AUDIT_LAST_CLEANUP_AT = 0

    def tearDown(self) -> None:
        security.AUTH_TOKEN = self._old_auth_token
        security.API_RATE_LIMIT_WINDOW_SECONDS = self._old_window
        security.API_RATE_LIMIT_MAX_REQUESTS = self._old_max
        security.UNAUTHORIZED_AUDIT_SAMPLE_SECONDS = self._old_unauth_sample
        security._RATE_LIMIT_STATE.clear()
        security._RATE_LIMIT_LAST_CLEANUP_AT = 0
        security._UNAUTH_AUDIT_STATE.clear()
        security._UNAUTH_AUDIT_LAST_CLEANUP_AT = 0

    def test_is_auth_exempt_path(self) -> None:
        self.assertTrue(security.is_auth_exempt_path("/health"))
        self.assertTrue(security.is_auth_exempt_path("/docs"))
        self.assertTrue(security.is_auth_exempt_path("/favicon.ico"))
        self.assertTrue(security.is_auth_exempt_path("/sub/links/u1001"))
        self.assertFalse(security.is_auth_exempt_path("/nodes"))

    def test_verify_admin_authorization_optional_when_no_token(self) -> None:
        security.AUTH_TOKEN = ""
        self.assertIsNone(security.verify_admin_authorization(None))

    def test_verify_admin_authorization_strict_when_token_set(self) -> None:
        security.AUTH_TOKEN = "abc123"
        self.assertIsNotNone(security.verify_admin_authorization(None))
        self.assertIsNotNone(security.verify_admin_authorization("Bearer wrong"))
        self.assertIsNone(security.verify_admin_authorization("Bearer abc123"))

    def test_verify_admin_authorization_accepts_token_rotation_list(self) -> None:
        security.AUTH_TOKEN = "newtoken,oldtoken"
        self.assertIsNone(security.verify_admin_authorization("Bearer newtoken"))
        self.assertIsNone(security.verify_admin_authorization("Bearer oldtoken"))
        self.assertIsNotNone(security.verify_admin_authorization("Bearer invalid"))

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
            "/admin/security/status",
            security.build_rate_limit_path_key("/admin/security/status"),
        )

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


if __name__ == "__main__":
    unittest.main()
