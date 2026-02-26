import unittest

from controller import security


class SecurityTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_auth_token = security.AUTH_TOKEN
        self._old_window = security.API_RATE_LIMIT_WINDOW_SECONDS
        self._old_max = security.API_RATE_LIMIT_MAX_REQUESTS
        security._RATE_LIMIT_STATE.clear()
        security._RATE_LIMIT_LAST_CLEANUP_AT = 0

    def tearDown(self) -> None:
        security.AUTH_TOKEN = self._old_auth_token
        security.API_RATE_LIMIT_WINDOW_SECONDS = self._old_window
        security.API_RATE_LIMIT_MAX_REQUESTS = self._old_max
        security._RATE_LIMIT_STATE.clear()
        security._RATE_LIMIT_LAST_CLEANUP_AT = 0

    def test_is_auth_exempt_path(self) -> None:
        self.assertTrue(security.is_auth_exempt_path("/health"))
        self.assertTrue(security.is_auth_exempt_path("/docs"))
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


if __name__ == "__main__":
    unittest.main()
