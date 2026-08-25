import unittest
import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.crawlers.merrill_crawler import MerrillCrawler


def _make_crawler():
    crawler = MerrillCrawler.__new__(MerrillCrawler)
    crawler.broker_name = "merrill_edge"
    crawler.log = logging.getLogger("test_merrill_crawler")
    return crawler


class MerrillCrawlerParsingTest(unittest.IsolatedAsyncioTestCase):
    async def test_unsettled_position_keeps_value_and_unknown_costs(self):
        html = """
        <div>Total value</div><div>$293,633.67</div>
        <ul>
            <li><button>Cash balance</button><span>$22,240.00</span></li>
            <li><button>Pending activity</button><span>$152,296.86</span></li>
            <li><button>Margin Balance</button><span>$496.66</span></li>
        </ul>
        <table class="security-holdings-table-container">
            <tbody>
                <tr>
                    <td>
                        <div class="holding-shortname">
                            <button id="row-holding-title"><span>VOO</span></button>
                        </div>
                        <div class="holding-name" aria-hidden="true">VANGUARD 500 INDEX FUND</div>
                    </td>
                    <td>173</td>
                    <td>$685.55</td>
                    <td><span>$0.00</span></td>
                    <td><span>0.00%</span></td>
                    <td>--</td>
                    <td>$118,600.15</td>
                    <td>$0.00</td>
                    <td>0.00%</td>
                    <td>--</td>
                    <td>--</td>
                    <td>--</td>
                    <td>1.95%</td>
                    <td>--</td>
                </tr>
            </tbody>
        </table>
        """

        crawler = _make_crawler()
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
        html = """
        <div>Total value</div><div>$356,215.86</div>
        <ul>
            <li><button>Cash balance</button><span>$0.00</span></li>
            <li><button>Pending activity</button><span>$0.00</span></li>
        </ul>
        <table class="security-holdings-table-container">
            <tbody>
                <tr>
                    <td>
                        <div class="holding-shortname">
                            <button id="row-holding-title"><span>TSM</span></button>
                        </div>
                        <div class="holding-name" aria-hidden="true">TAIWAN SEMICONDUCTOR</div>
                    </td>
                    <td>858</td>
                    <td>$415.17</td>
                    <td><span class="negative"><span class="ada-hidden">Negative</span>-$29.75</span></td>
                    <td><span class="negative"><span class="ada-hidden">Negative</span>-6.69%</span></td>
                    <td>$200.00</td>
                    <td>$356,215.86</td>
                    <td><span class="negative"><span class="ada-hidden">Negative</span>-$25,525.50</span></td>
                    <td><span class="negative"><span class="ada-hidden">Negative</span>-6.69%</span></td>
                    <td><span class="positive"><span class="ada-hidden">Positive</span>+$184,615.86</span></td>
                    <td><span class="positive"><span class="ada-hidden">Positive</span>+107.59%</span></td>
                    <td>$171,600.00</td>
                    <td>50.00%</td>
                    <td>--</td>
                </tr>
            </tbody>
        </table>
        """

        crawler = _make_crawler()
        holdings = await crawler.parse_portfolio_html(html)
        tsm = {h.symbol: h for h in holdings}["TSM"]

        # -29.75 per share * 858 shares = -25,525.50
        self.assertAlmostEqual(tsm.day_change_dollars, -25525.50, places=2)
        self.assertAlmostEqual(tsm.day_change_percent, -0.0669, places=4)
        # previous_close is the prior unit price: 415.17 + 29.75 = 444.92
        self.assertAlmostEqual(tsm.previous_close, 444.92, places=2)


if __name__ == "__main__":
    unittest.main()
