import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.storage.backup import backup_database


def _make_db(path: str, value: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
    conn.execute("INSERT INTO t VALUES (?)", (value,))
    conn.commit()
    conn.close()


class BackupTest(unittest.TestCase):
    def test_backup_is_a_valid_consistent_copy(self):
        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "portfolio.db"
            _make_db(str(db), 42)

            dest = backup_database(str(db), backup_dir=str(Path(d) / "backups"))

            conn = sqlite3.connect(str(dest))
            self.assertEqual(conn.execute("SELECT x FROM t").fetchone()[0], 42)
            conn.close()

    def test_rotation_keeps_only_newest_n(self):
        with tempfile.TemporaryDirectory() as d:
            db = Path(d) / "portfolio.db"
            _make_db(str(db), 1)
            backup_dir = Path(d) / "backups"

            # 9 backups with distinct, increasing timestamps; retention 7.
            for i in range(9):
                backup_database(
                    str(db),
                    backup_dir=str(backup_dir),
                    retention=7,
                    now=datetime(2026, 1, 1, 0, 0, i),
                )

            backups = sorted(backup_dir.glob("portfolio_*.db"))
            self.assertEqual(len(backups), 7)
            # The two oldest (seconds 0 and 1) should have been pruned.
            self.assertTrue(backups[0].name.endswith("000002.db"))
            self.assertTrue(backups[-1].name.endswith("000008.db"))

    def test_missing_source_raises(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):
                backup_database(str(Path(d) / "nope.db"))


if __name__ == "__main__":
    unittest.main()
