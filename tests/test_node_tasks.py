import unittest

from fastapi import HTTPException

from controller import node_tasks


class NodeTasksValidationTestCase(unittest.TestCase):
    def test_logs_payload_validation(self) -> None:
        payload = node_tasks.validate_node_task_payload("logs_agent", {"lines": 120})
        self.assertEqual({"lines": 120}, payload)

    def test_logs_payload_reject_unknown_keys(self) -> None:
        with self.assertRaises(HTTPException):
            node_tasks.validate_node_task_payload("logs_singbox", {"lines": 100, "x": 1})

    def test_config_set_payload_validation(self) -> None:
        payload = node_tasks.validate_node_task_payload(
            "config_set",
            {"controller_url": "127.0.0.1:8080", "poll_interval": 10},
        )
        self.assertEqual("http://127.0.0.1:8080", payload.get("controller_url"))
        self.assertEqual(10, payload.get("poll_interval"))

    def test_config_set_payload_validation_enable_protocol_flags(self) -> None:
        payload = node_tasks.validate_node_task_payload(
            "config_set",
            {"enable_tuic": "false", "enable_vless": True},
        )
        self.assertEqual(False, payload.get("enable_tuic"))
        self.assertEqual(True, payload.get("enable_vless"))

    def test_config_set_payload_reject_invalid_boolean(self) -> None:
        with self.assertRaises(HTTPException):
            node_tasks.validate_node_task_payload(
                "config_set",
                {"enable_tuic": "not-bool"},
            )

    def test_config_set_reject_unsupported_key(self) -> None:
        with self.assertRaises(HTTPException):
            node_tasks.validate_node_task_payload("config_set", {"bad_key": "x"})

    def test_sync_time_payload_validation(self) -> None:
        payload = node_tasks.validate_node_task_payload("sync_time", {"server_unix": 1772200000})
        self.assertEqual({"server_unix": 1772200000}, payload)

    def test_payload_size_limit(self) -> None:
        oversized = "a" * (node_tasks.MAX_NODE_TASK_PAYLOAD_BYTES + 1)
        with self.assertRaises(HTTPException):
            node_tasks.validate_node_task_payload_size(oversized)

    def test_sanitize_task_payload_for_display_redacts_sensitive_keys(self) -> None:
        payload = {
            "auth_token": "abc123",
            "controller_url": "http://127.0.0.1:8080",
            "nested": {"api_key": "k-1", "note": "ok"},
            "items": [{"secret": "s1"}, {"value": 1}],
        }
        masked = node_tasks.sanitize_task_payload_for_display(payload)
        self.assertEqual("***", masked.get("auth_token"))
        self.assertEqual("http://127.0.0.1:8080", masked.get("controller_url"))
        self.assertEqual("***", (masked.get("nested") or {}).get("api_key"))
        self.assertEqual("ok", (masked.get("nested") or {}).get("note"))
        self.assertEqual("***", ((masked.get("items") or [])[0]).get("secret"))
        self.assertEqual(1, ((masked.get("items") or [None, {}])[1]).get("value"))

    def test_sanitize_task_result_for_display_redacts_sensitive_values(self) -> None:
        raw = (
            "auth_token=abcdef123456\n"
            "Authorization: Bearer abcdef.123456\n"
            '{"auth_token":"xyz","controller_url":"http://127.0.0.1:8080"}\n'
            "normal_line=ok"
        )
        masked = node_tasks.sanitize_task_result_for_display(raw)
        self.assertIn("auth_token=***", masked)
        self.assertIn("Authorization: Bearer ***", masked)
        self.assertIn('"auth_token":"***"', masked)
        self.assertIn("normal_line=ok", masked)


if __name__ == "__main__":
    unittest.main()
