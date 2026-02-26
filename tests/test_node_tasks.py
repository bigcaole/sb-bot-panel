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

    def test_config_set_reject_unsupported_key(self) -> None:
        with self.assertRaises(HTTPException):
            node_tasks.validate_node_task_payload("config_set", {"bad_key": "x"})

    def test_payload_size_limit(self) -> None:
        oversized = "a" * (node_tasks.MAX_NODE_TASK_PAYLOAD_BYTES + 1)
        with self.assertRaises(HTTPException):
            node_tasks.validate_node_task_payload_size(oversized)


if __name__ == "__main__":
    unittest.main()
