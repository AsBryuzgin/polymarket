from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
