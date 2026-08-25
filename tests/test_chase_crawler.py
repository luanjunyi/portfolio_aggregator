import logging
import os
import sys
import unittest

from bs4 import BeautifulSoup

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.crawlers.chase_crawler import ChaseCrawler
from backend.models.portfolio import day_change_consistency_warning


def _crawler() -> ChaseCrawler:
    crawler = ChaseCrawler.__new__(ChaseCrawler)
    crawler.broker_name = "chase"
    crawler.log = logging.getLogger("test_chase_crawler")
    return crawler


def _row(price_cell_text: str, market_value: str, day_gain: str, insight_indicator: bool = False) -> "BeautifulSoup":
    if insight_indicator:
        price_td = f'<td><div data-testid="position-0-insight-indicator">{price_cell_text}</div></td>'
    else:
        price_td = f'<td><div data-testid="price-position-AAPL">{price_cell_text}</div></td>'
    html = f"""
    <tr data-testid="position-AAPL">
        <td><a data-testid="symbol-position-AAPL">AAPL</a></td>
        <td>Apple Inc</td>
        {price_td}
        <td>{market_value}</td>
        <td>{day_gain}</td>
        <td>500.00</td>
        <td>5.00%</td>
        <td>100</td>
        <td>9000.00</td>
        <td></td>
    </tr>
    """
    return BeautifulSoup(html, "html.parser").find("tr")


# 100 shares @ 100.00, down 0.42/share on the day -> -0.42% shown in price cell.
PRICE_CELL = "100.00Loss of -0.42-0.42Loss of -0.42%-0.42%"


class ChaseDayChangePercentTest(unittest.TestCase):
    def test_percent_comes_from_price_cell_not_dollars_over_value(self):
        holding = _crawler()._parse_position_row(_row(PRICE_CELL, "10000.00", "-42.00"))
        # The independent percent (-0.42%) from the price cell is used.
        self.assertAlmostEqual(holding.day_change_percent, -0.0042, places=4)
        self.assertAlmostEqual(holding.previous_close, 100.42, places=2)
        self.assertIsNone(day_change_consistency_warning(holding))

    def test_independent_percent_flags_a_bad_dollar_column(self):
        # Day's gain/loss column reports the per-share change (-0.42) instead of
        # the position total (-42.00). The percent still comes from the price
        # cell, so the audit can now catch the mismatch on Chase too.
        holding = _crawler()._parse_position_row(_row(PRICE_CELL, "10000.00", "-0.42"))
        self.assertAlmostEqual(holding.day_change_percent, -0.0042, places=4)
        self.assertIsNotNone(day_change_consistency_warning(holding))

    def test_audit_warning_is_logged_during_parse(self):
        with self.assertLogs("test_chase_crawler", level="WARNING") as captured:
            _crawler()._parse_position_row(_row(PRICE_CELL, "10000.00", "-0.42"))
        self.assertTrue(any("inconsistency" in m for m in captured.output))

    def test_non_trading_day_falls_back_to_dollars_over_value(self):
        # No percent shown; day gain empty -> 0.0; percent falls back to $/value.
        holding = _crawler()._parse_position_row(_row("100.00", "10000.00", ""))
        self.assertEqual(holding.day_change_percent, 0.0)

    def test_insight_indicator_format_parses_correctly(self):
        holding = _crawler()._parse_position_row(
            _row(PRICE_CELL, "10000.00", "-42.00", insight_indicator=True)
        )
        self.assertAlmostEqual(holding.price, 100.00, places=2)
        self.assertAlmostEqual(holding.day_change_percent, -0.0042, places=4)


if __name__ == "__main__":
    unittest.main()
