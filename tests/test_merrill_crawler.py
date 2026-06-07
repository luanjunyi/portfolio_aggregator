import unittest
import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.crawlers.merrill_crawler import MerrillCrawler


class MerrillCrawlerParsingTest(unittest.IsolatedAsyncioTestCase):
    async def test_unsettled_position_keeps_value_and_unknown_costs(self):
        html = """
        <table id="CustomGrid_test" class="customTable">
            <tbody>
                <tr>
                    <td><a>VOO</a></td><td></td><td>VANGUARD 500 INDEX FUND</td>
                    <td><div class="dol">$0.00</div><div class="per">0.00%</div></td>
                    <td>$685.55</td><td>173</td><td>--</td><td>--</td>
                    <td>$118,600.15</td>
                    <td><div class="dol">--</div><div class="per">--</div></td>
                    <td>1.95%</td>
                </tr>
                <tr>
                    <td><a>Money accounts</a></td><td></td><td>ML DIRECT DEPOSIT PROGRM</td>
                    <td></td><td>$1.00</td><td>22,240</td><td>--</td><td>--</td>
                    <td>$22,240.00</td><td></td><td></td>
                </tr>
                <tr>
                    <td>Pending activity</td><td></td><td></td><td></td><td></td><td></td>
                    <td></td><td></td><td>$152,296.86</td><td></td><td></td>
                </tr>
                <tr>
                    <td>Margin balance</td><td></td><td></td><td></td><td></td><td></td>
                    <td></td><td></td><td>$496.66</td><td></td><td></td>
                </tr>
                <tr>
                    <td>Total</td><td></td><td></td><td></td><td></td><td></td>
                    <td></td><td></td><td>$293,633.67</td><td></td><td></td>
                </tr>
            </tbody>
        </table>
        """

        crawler = MerrillCrawler.__new__(MerrillCrawler)
        crawler.broker_name = "merrill_edge"
        crawler.log = logging.getLogger("test_merrill_crawler")

        holdings = await crawler.parse_portfolio_html(html)
        holdings_by_symbol = {holding.symbol: holding for holding in holdings}

        self.assertEqual(holdings_by_symbol["VOO"].current_value, 118600.15)
        self.assertIsNone(holdings_by_symbol["VOO"].unit_cost)
        self.assertIsNone(holdings_by_symbol["VOO"].cost_basis)
        self.assertIsNone(holdings_by_symbol["VOO"].unrealized_gain_loss)
        self.assertIsNone(holdings_by_symbol["VOO"].unrealized_gain_loss_percent)
        self.assertEqual(holdings_by_symbol["USD_CASH"].current_value, 174536.86)
        self.assertEqual(holdings_by_symbol["USD_MARGIN_BALANCE"].current_value, 496.66)

    async def test_day_change_dollars_scaled_by_quantity(self):
        # Merrill's day-change column is per share; the crawler must scale it by
        # the position quantity to get the total dollar change.
        html = """
        <table id="CustomGrid_test" class="customTable">
            <tbody>
                <tr>
                    <td><a>TSM</a></td><td></td><td>TAIWAN SEMICONDUCTOR</td>
                    <td><div class="dol">($29.75)</div><div class="per">(6.69%)</div></td>
                    <td>$415.17</td><td>858</td><td>$200.00</td><td>$171,600.00</td>
                    <td>$356,215.86</td>
                    <td><div class="dol">$184,615.86</div><div class="per">107.59%</div></td>
                    <td>50.00%</td>
                </tr>
                <tr>
                    <td>Total</td><td></td><td></td><td></td><td></td><td></td>
                    <td></td><td></td><td>$356,215.86</td>
                    <td><div class="dol">$184,615.86</div><div class="per">107.59%</div></td><td></td>
                </tr>
            </tbody>
        </table>
        """

        crawler = MerrillCrawler.__new__(MerrillCrawler)
        crawler.broker_name = "merrill_edge"
        crawler.log = logging.getLogger("test_merrill_crawler")

        holdings = await crawler.parse_portfolio_html(html)
        tsm = {h.symbol: h for h in holdings}["TSM"]

        # -29.75 per share * 858 shares = -25,525.50
        self.assertAlmostEqual(tsm.day_change_dollars, -25525.50, places=2)
        self.assertAlmostEqual(tsm.day_change_percent, -0.0669, places=4)


if __name__ == "__main__":
    unittest.main()
