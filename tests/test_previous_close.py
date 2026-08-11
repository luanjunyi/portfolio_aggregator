import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.models.portfolio import Holding, Portfolio, day_change_consistency_warning
from backend.storage.database import DatabaseManager


def _holding(**kw) -> Holding:
    base = dict(
        symbol="AAPL", description="Apple", quantity=100, price=100.0,
        unit_cost=90.0, cost_basis=9000.0, current_value=10000.0,
        day_change_percent=-0.01, day_change_dollars=-100.0,
        unrealized_gain_loss=1000.0, unrealized_gain_loss_percent=0.11,
    )
    base.update(kw)
    return Holding(**base)


class PreviousCloseDerivationTest(unittest.TestCase):
    def test_derived_from_day_change(self):
        # prior value = 10000 - (-100) = 10100 over 100 shares -> 101.0
        h = _holding()
        self.assertAlmostEqual(h.previous_close, 101.0, places=6)
        # invariant: day_change_dollars == (price - previous_close) * quantity
        self.assertAlmostEqual(
            (h.price - h.previous_close) * h.quantity, h.day_change_dollars, places=6
        )

    def test_explicit_value_is_not_overwritten(self):
        h = _holding(previous_close=123.45)
        self.assertEqual(h.previous_close, 123.45)

    def test_zero_quantity_leaves_none(self):
        h = _holding(quantity=0, current_value=0.0, day_change_dollars=0.0)
        self.assertIsNone(h.previous_close)

    def test_short_option_negative_quantity(self):
        # short call: prior value -180 over -24 contracts -> 7.5
        h = _holding(symbol="QXO C", quantity=-24, price=5.0, current_value=-120.0,
                     day_change_dollars=60.0, day_change_percent=-0.3333,
                     unit_cost=7.5, cost_basis=-180.0)
        self.assertAlmostEqual(h.previous_close, 7.5, places=6)


class DayChangeAuditTest(unittest.TestCase):
    def test_consistent_holding_no_warning(self):
        self.assertIsNone(day_change_consistency_warning(_holding()))

    def test_per_share_bug_is_flagged(self):
        # day_change_dollars holds the per-share change (forgot * quantity).
        bug = _holding(day_change_dollars=-1.0, day_change_percent=-0.01)
        warning = day_change_consistency_warning(bug)
        self.assertIsNotNone(warning)
        self.assertIn("inconsistency", warning)

    def test_zero_change_skipped(self):
        self.assertIsNone(
            day_change_consistency_warning(
                _holding(day_change_dollars=0.0, day_change_percent=0.0)
            )
        )


class DatabaseRoundTripTest(unittest.TestCase):
    def test_previous_close_persisted(self):
        with tempfile.TemporaryDirectory() as d:
            # DatabaseManager reads/creates encryption.key relative to cwd;
            # run in the temp dir and restore cwd afterward to stay hermetic.
            prev_cwd = os.getcwd()
            self.addCleanup(os.chdir, prev_cwd)
            os.chdir(d)
            dm = DatabaseManager(db_path=str(Path(d) / "portfolio.db"))
            portfolio = Portfolio(
                holdings=[_holding()],
                total_value=10000.0, total_cost_basis=9000.0,
                total_unrealized_gain_loss=1000.0, total_unrealized_gain_loss_percent=0.11,
                last_updated=datetime(2026, 6, 24, 16, 0, 0),
                day_change_percent=-0.01, day_change_dollars=-100.0,
            )
            dm.save_portfolio_snapshot(portfolio)
            row = sqlite3.connect(dm.db_path).execute(
                "SELECT previous_close FROM holdings_snapshots WHERE symbol='AAPL'"
            ).fetchone()
            self.assertAlmostEqual(row[0], 101.0, places=6)


if __name__ == "__main__":
    unittest.main()
