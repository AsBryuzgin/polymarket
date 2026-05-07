from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from execution.state_backup import backup_state_db


class StateBackupTests(unittest.TestCase):
    def test_backup_state_db_copies_configured_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db_path = root / "executor_state_live.db"
            db_path.write_text("sqlite bytes", encoding="utf-8")
            backup_dir = root / "backups"

            result = backup_state_db(
                config={
                    "state_backup": {
                        "enabled": True,
                        "dir": str(backup_dir),
                    }
                },
                label="before_submit",
                db_path=db_path,
            )

            self.assertTrue(result.created)
            self.assertIsNotNone(result.backup_path)
            backup_path = Path(result.backup_path or "")
            self.assertTrue(backup_path.exists())
            self.assertEqual(backup_path.read_text(encoding="utf-8"), "sqlite bytes")

    def test_backup_state_db_respects_disabled_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "executor_state_live.db"
            db_path.write_text("sqlite bytes", encoding="utf-8")

            result = backup_state_db(
                config={"state_backup": {"enabled": False}},
                label="manual",
                db_path=db_path,
            )

            self.assertFalse(result.created)
            self.assertIn("disabled", result.reason)

    def test_backup_retention_prunes_old_files_for_same_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "executor_state_paper.db"
            source.write_text("state", encoding="utf-8")
            config = {
                "state_backup": {
                    "enabled": True,
                    "dir": str(root / "backups"),
                    "retention_keep_last": 2,
                    "retention_max_total_mb": 100,
                }
            }

            results = [
                backup_state_db(config=config, label=f"hourly_{idx}", db_path=source)
                for idx in range(5)
            ]

            self.assertTrue(all(row.created for row in results))
            backups = sorted((root / "backups").glob("executor_state_paper_*.db"))
            self.assertEqual(len(backups), 2)
            self.assertGreater(results[-1].pruned_count, 0)

    def test_backup_retention_prunes_zero_byte_backups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "executor_state_paper.db"
            source.write_text("state", encoding="utf-8")
            backup_dir = root / "backups"
            backup_dir.mkdir()
            broken = backup_dir / "executor_state_paper_paper_hourly_20260101T000000Z.db"
            broken.write_bytes(b"")
            config = {
                "state_backup": {
                    "enabled": True,
                    "dir": str(backup_dir),
                    "retention_keep_last": 6,
                    "retention_max_total_mb": 100,
                }
            }

            result = backup_state_db(config=config, label="hourly", db_path=source)

            self.assertTrue(result.created)
            self.assertFalse(broken.exists())
            self.assertEqual(result.pruned_count, 1)

    def test_failed_backup_removes_partial_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "executor_state_paper.db"
            source.write_text("state", encoding="utf-8")
            backup_dir = root / "backups"
            config = {
                "state_backup": {
                    "enabled": True,
                    "dir": str(backup_dir),
                    "retention_keep_last": 2,
                    "retention_max_total_mb": 100,
                }
            }

            def fail_with_partial(_src: Path, dst: Path) -> None:
                Path(dst).write_text("partial", encoding="utf-8")
                raise OSError("disk full")

            with patch("execution.state_backup.shutil.copy2", side_effect=fail_with_partial):
                result = backup_state_db(config=config, label="hourly", db_path=source)

            self.assertFalse(result.created)
            self.assertIn("disk full", result.reason)
            self.assertEqual(list(backup_dir.glob("*")), [])


if __name__ == "__main__":
    unittest.main()
