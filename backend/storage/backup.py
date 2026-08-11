"""Rotating backups of the portfolio SQLite database.

A backup is taken right before the daily run writes the new snapshot, so the
most recent backup is always the last known-good state (it does not yet contain
the row the current run is about to write). This gives a cheap rollback target
if a crawl writes bad data.
"""
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


def backup_database(
    db_path: str,
    backup_dir: Optional[str] = None,
    retention: int = 7,
    now: Optional[datetime] = None,
) -> Path:
    """Make a consistent copy of the SQLite DB and keep only the newest copies.

    Uses SQLite's online backup API so the copy is consistent even while other
    processes (e.g. the dashboard webapp) read the database concurrently.

    Args:
        db_path: Path to the source SQLite database.
        backup_dir: Directory for backups. Defaults to ``<db parent>/backups``.
        retention: Number of most-recent backups to keep; older ones are pruned.
        now: Timestamp used for the backup filename (injectable for tests).

    Returns:
        Path to the backup file that was created.
    """
    src = Path(db_path)
    if not src.exists():
        raise FileNotFoundError(f"Database not found: {src}")

    out_dir = Path(backup_dir) if backup_dir is not None else src.parent / "backups"
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = (now or datetime.now()).strftime("%Y%m%d_%H%M%S")
    dest = out_dir / f"{src.stem}_{stamp}{src.suffix}"

    source_conn = sqlite3.connect(str(src))
    dest_conn = sqlite3.connect(str(dest))
    try:
        source_conn.backup(dest_conn)
    finally:
        dest_conn.close()
        source_conn.close()

    log.info(f"Database backed up to {dest}")
    _prune_old_backups(out_dir, src.stem, src.suffix, retention)
    return dest


def _prune_old_backups(backup_dir: Path, stem: str, suffix: str, retention: int) -> None:
    # Timestamp is zero-padded, so lexicographic order == chronological order.
    backups = sorted(backup_dir.glob(f"{stem}_*{suffix}"))
    excess = len(backups) - retention
    for old in backups[: max(0, excess)]:
        try:
            old.unlink()
            log.info(f"Pruned old backup {old}")
        except OSError as e:
            log.warning(f"Could not remove old backup {old}: {e}")
