import tempfile
import time
import unittest
from pathlib import Path

from controller.routers_admin import cleanup_archives_by_count


class ArchiveRetentionTestCase(unittest.TestCase):
    def test_cleanup_archives_by_count_keeps_latest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for idx in range(5):
                file_path = root / "backup-20260101-00000{0}.tar.gz".format(idx)
                file_path.write_text("x", encoding="utf-8")
                ts = time.time() + idx
                # 让文件 mtime 递增，idx 越大越新。
                file_path.touch()
                try:
                    import os

                    os.utime(str(file_path), (ts, ts))
                except OSError:
                    pass

            removed = cleanup_archives_by_count(root, "backup-", 2)
            self.assertEqual(3, removed)

            remain = sorted(p.name for p in root.glob("backup-*.tar.gz"))
            self.assertEqual(
                ["backup-20260101-000003.tar.gz", "backup-20260101-000004.tar.gz"],
                remain,
            )


if __name__ == "__main__":
    unittest.main()
