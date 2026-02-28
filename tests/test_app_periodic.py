import unittest
from threading import Lock

from controller import app as app_module


class AppPeriodicTaskTestCase(unittest.TestCase):
    def test_skip_when_interval_not_reached(self) -> None:
        called = {"count": 0}

        def _runner() -> None:
            called["count"] += 1

        lock = Lock()
        updated = app_module._maybe_run_periodic_task(
            now_ts=100,
            interval_seconds=60,
            last_at=80,
            task_lock=lock,
            task_runner=_runner,
        )
        self.assertEqual(80, updated)
        self.assertEqual(0, called["count"])

    def test_run_when_due(self) -> None:
        called = {"count": 0}

        def _runner() -> None:
            called["count"] += 1

        lock = Lock()
        updated = app_module._maybe_run_periodic_task(
            now_ts=100,
            interval_seconds=60,
            last_at=30,
            task_lock=lock,
            task_runner=_runner,
        )
        self.assertEqual(100, updated)
        self.assertEqual(1, called["count"])

    def test_skip_when_lock_is_held(self) -> None:
        called = {"count": 0}

        def _runner() -> None:
            called["count"] += 1

        lock = Lock()
        lock.acquire()
        try:
            updated = app_module._maybe_run_periodic_task(
                now_ts=100,
                interval_seconds=60,
                last_at=30,
                task_lock=lock,
                task_runner=_runner,
            )
        finally:
            lock.release()
        self.assertEqual(30, updated)
        self.assertEqual(0, called["count"])

    def test_runner_failure_does_not_break_timestamp_update(self) -> None:
        def _runner() -> None:
            raise RuntimeError("boom")

        lock = Lock()
        with self.assertLogs(app_module.logger, level="WARNING"):
            updated = app_module._maybe_run_periodic_task(
                now_ts=100,
                interval_seconds=60,
                last_at=30,
                task_lock=lock,
                task_runner=_runner,
            )
        self.assertEqual(100, updated)
        self.assertFalse(lock.locked())

    def test_runner_failure_writes_warning_log(self) -> None:
        def _runner() -> None:
            raise RuntimeError("boom")

        lock = Lock()
        with self.assertLogs(app_module.logger, level="WARNING") as cm:
            updated = app_module._maybe_run_periodic_task(
                now_ts=100,
                interval_seconds=60,
                last_at=30,
                task_lock=lock,
                task_runner=_runner,
                task_name="unit-test-task",
            )
        self.assertEqual(100, updated)
        self.assertFalse(lock.locked())
        merged = "\n".join(cm.output)
        self.assertIn("periodic task failed", merged)
        self.assertIn("unit-test-task", merged)
        self.assertIn("boom", merged)


if __name__ == "__main__":
    unittest.main()
