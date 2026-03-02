import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock

from fastapi import HTTPException

from controller import db
from controller.schemas import CreateUserRequest
from controller.users_service import create_user_service, rotate_user_credentials_service


class UsersServiceTestCase(unittest.TestCase):
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
                    10,
                    "tc",
                    1001,
                    "11111111-1111-4111-8111-111111111111",
                    "22222222-2222-4222-8222-222222222222",
                    20010,
                    "",
                ),
            )
            conn.commit()

    def tearDown(self) -> None:
        db.DB_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def test_create_user_retries_when_uuid_or_secret_collides(self) -> None:
        payload = CreateUserRequest(
            display_name="Bob",
            tuic_port=20011,
            speed_mbps=20,
            valid_days=7,
            note="",
        )
        with mock.patch(
            "controller.users_service.uuid.uuid4",
            side_effect=[
                uuid.UUID("11111111-1111-4111-8111-111111111111"),
                uuid.UUID("33333333-3333-4333-8333-333333333333"),
                uuid.UUID("22222222-2222-4222-8222-222222222222"),
                uuid.UUID("44444444-4444-4444-8444-444444444444"),
            ],
        ):
            created = create_user_service(payload, request=None)  # type: ignore[arg-type]

        self.assertEqual("33333333-3333-4333-8333-333333333333", created["vless_uuid"])
        self.assertEqual("44444444-4444-4444-8444-444444444444", created["tuic_secret"])

    def test_create_user_raises_when_cannot_allocate_unique_credentials(self) -> None:
        payload = CreateUserRequest(
            display_name="Carol",
            tuic_port=20012,
            speed_mbps=20,
            valid_days=7,
            note="",
        )
        with mock.patch(
            "controller.users_service.uuid.uuid4",
            return_value=uuid.UUID("11111111-1111-4111-8111-111111111111"),
        ):
            with self.assertRaises(HTTPException) as exc:
                create_user_service(payload, request=None)  # type: ignore[arg-type]
        self.assertEqual(500, exc.exception.status_code)
        self.assertIn("unique user credential", str(exc.exception.detail))

    def test_rotate_user_credentials_updates_both_fields(self) -> None:
        with mock.patch(
            "controller.users_service.uuid.uuid4",
            side_effect=[
                uuid.UUID("33333333-3333-4333-8333-333333333333"),
                uuid.UUID("44444444-4444-4444-8444-444444444444"),
            ],
        ):
            result = rotate_user_credentials_service("u1001", request=None)  # type: ignore[arg-type]
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual("u1001", result.get("user_code"))
        self.assertEqual("33333333-3333-4333-8333-333333333333", result.get("vless_uuid"))
        self.assertEqual("44444444-4444-4444-8444-444444444444", result.get("tuic_secret"))

        with db.get_connection() as conn:
            row = conn.execute(
                "SELECT vless_uuid, tuic_secret FROM users WHERE user_code = ?",
                ("u1001",),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual("33333333-3333-4333-8333-333333333333", row["vless_uuid"])
        self.assertEqual("44444444-4444-4444-8444-444444444444", row["tuic_secret"])


if __name__ == "__main__":
    unittest.main()
