from typing import List, Dict, Any, Optional
import asyncio
import re
import random
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.base_crawler import BaseCrawler
from models.portfolio import Holding, day_change_consistency_warning


class MerrillCrawler(BaseCrawler):
    """Merrill Edge crawler"""


    def __init__(self):
        super().__init__("merrill_edge")
        self.login_url = "https://olui2.fs.ml.com/login/signin.aspx"
        self.portfolio_url = "https://olui2.fs.ml.com/accounts-holdings/mlaccount/"

    def get_login_url(self) -> str:
        """Get Merrill login URL"""
        return self.login_url

    async def scrape_portfolio(self) -> List[Holding]:
        """Scrape holdings from Merrill Edge"""
        self.log.info("Starting Merrill holdings scrape...")

        if "accounts-holdings" not in self.page.url:
            self.log.info("Navigating to portfolio page...")
            await self.page.goto(self.portfolio_url, wait_until='networkidle')

        self.log.info("Waiting for holdings page to load...")
        try:
            await self.page.wait_for_selector('table.security-holdings-table-container', timeout=30000)
            self.log.info("Holdings page loaded successfully")
        except Exception as e:
            self.log.warning(f"Holdings table selector not found: {e}")

        # Expand all accounts so every holding table is in the DOM
        try:
            expand_btn = self.page.get_by_role("button", name="Expand all", exact=True)
            if await expand_btn.count() > 0:
                await expand_btn.click()
                self.log.info("Clicked 'Expand all'")
                await self.page.wait_for_timeout(2000)
        except Exception as e:
            self.log.warning(f"Could not click 'Expand all': {e}")

        html = await self.page.content()
        holdings = await self.parse_portfolio_html(html)

        self.log.info(f"Found {len(holdings)} total holdings")
        return holdings

    async def login(self) -> bool:
        """Login to Merrill Edge"""
        self.log.info("Starting Merrill login...")

        # First check if we're already logged in with a valid session
        try:
            await self.page.goto(self.login_url, wait_until='domcontentloaded')
            await self.page.wait_for_load_state('networkidle', timeout=15000)

            current_url = self.page.url
            if "accounts-holdings" in current_url.lower() or "tfpholdings" in current_url.lower():
                self.log.info("Already logged in with stored session!")
                return True
        except Exception:
            pass

        credentials = self.get_credentials()
        if not credentials:
            raise RuntimeError("No credentials found")

        username = credentials['username']
        password = credentials['password']

        current_url = self.page.url
        self.log.debug(f"Current URL: {current_url}")

        try:
            await self.page.fill('#oid', username)
            self.log.debug("Filled username field")
        except Exception as e:
            raise RuntimeError(f"Username field #oid not found: {e}") from e

        try:
            await self.page.fill('#pass', password)
            self.log.debug("Filled password field")
        except Exception as e:
            raise RuntimeError(f"Password field #pass not found: {e}") from e

        try:
            await self.page.click('#secure-signin-submit', delay=random.randint(100, 200))
            self.log.debug("Clicked login button")
        except Exception as e:
            raise RuntimeError(f"Login button #secure-signin-submit not found: {e}") from e

        try:
            self.log.info("Waiting for positions page to load...")
            await self.page.wait_for_url("**/accounts-holdings/**", timeout=5 * 60000)
        except Exception as e:
            self.log.warning(f"Error waiting for positions URL: {e}")

        return True

    async def handle_2fa_if_needed(self) -> bool:
        """Handle 2FA if required"""

        return False

    async def parse_portfolio_html(self, html: str) -> List[Holding]:
        """Parse Merrill Edge portfolio HTML to extract holdings"""
        self.log.info("Parsing HTML for all holdings...")

        soup = self.parse_html_with_soup(html)
        holdings = []

        tables = soup.find_all('table', class_=lambda c: c and 'security-holdings-table-container' in c)
        tables = [t for t in tables if t.find('tbody') and t.find('tbody').find('tr')]

        if not tables:
            raise RuntimeError("Merrill holdings tables not found")

        self.log.info(f"Found {len(tables)} holdings table(s)")

        for table in tables:
            tbody = table.find('tbody')
            rows = tbody.find_all('tr')
            for row in rows:
                try:
                    holding = self._parse_position_row(row)
                    if holding:
                        holdings.append(holding)
                except Exception as e:
                    self.log.error(f"Error parsing row: {e}")

        # Cash, margin, and pending activity are now shown as page-level
        # summary items above the tables, not as table rows.
        cash_holding = self._parse_summary_cash(soup)
        if cash_holding:
            holdings.append(cash_holding)

        margin_holding = self._parse_summary_margin(soup)
        if margin_holding:
            holdings.append(margin_holding)

        self.sanity_check(soup, holdings)

        combined_holdings = self._combine_holdings_by_symbol(holdings)

        self.log.info(f"Successfully parsed {len(holdings)} individual holdings")
        self.log.info(f"Combined into {len(combined_holdings)} unique symbols")

        return combined_holdings

    def _parse_summary_value(self, soup, label: str) -> float:
        """Extract a dollar value from the page-level summary by its label text.

        The summary items live inside <li> elements:
          <li><button>Cash balance</button><div>$0.00</div></li>
        We restrict to <li>-contained buttons to avoid matching help tooltips.
        """
        for li in soup.find_all('li'):
            btn = li.find('button')
            if not btn:
                continue
            if btn.get_text(strip=True).lower() == label.lower():
                sibling = btn.find_next_sibling()
                if sibling:
                    text = sibling.get_text(strip=True)
                    try:
                        return self._clean_decimal_text(text)
                    except ValueError:
                        continue
        return 0.0

    def _parse_summary_cash(self, soup) -> Optional[Holding]:
        cash_value = self._parse_summary_value(soup, "Cash balance")
        pending = self._parse_summary_value(soup, "Pending activity")
        total = cash_value + pending
        if total == 0.0:
            return None
        self.log.info(f"Page-level cash=${cash_value}, pending=${pending}, total=${total}")
        return Holding(
            symbol="USD_CASH",
            description="Cash and pending activity",
            quantity=total,
            price=1.00,
            unit_cost=1.00,
            cost_basis=total,
            current_value=total,
            day_change_percent=0.00,
            day_change_dollars=0.00,
            unrealized_gain_loss=0.00,
            unrealized_gain_loss_percent=0.00,
            brokers={self.broker_name: total},
        )

    def _parse_summary_margin(self, soup) -> Optional[Holding]:
        value = self._parse_summary_value(soup, "Margin Balance")
        if value == 0.0:
            return None
        self.log.info(f"Page-level margin balance=${value}")
        return Holding(
            symbol="USD_MARGIN_BALANCE",
            description="Margin balance",
            quantity=value,
            price=1.00,
            unit_cost=1.00,
            cost_basis=value,
            current_value=value,
            day_change_percent=0.00,
            day_change_dollars=0.00,
            unrealized_gain_loss=0.00,
            unrealized_gain_loss_percent=0.00,
            brokers={self.broker_name: value},
        )

    def _parse_position_row(self, row) -> Holding:
        """Parse a single position row from the Merrill holdings table.

        Column layout (14 columns):
          0  Positions (symbol + description)
          1  Quantity
          2  Price (may include a trailing timestamp span)
          3  Day's price change ($) — per share, signed
          4  Day's price change (%) — signed
          5  Unit cost
          6  Value (current_value)
          7  Day's value change ($) — total position, signed
          8  Day's value change (%) — signed
          9  Unrealized gain/loss ($) — signed
         10  Unrealized gain/loss (%) — signed
         11  Total client investment
         12  % of Portfolio
         13  BofA Securities rating
        """
        cells = row.find_all('td')
        if len(cells) < 14:
            return None

        try:
            symbol_cell = cells[0]
            symbol_btn = symbol_cell.find('button', attrs={'id': 'row-holding-title'})
            if symbol_btn:
                symbol = symbol_btn.get_text(strip=True).split()[0]
            else:
                symbol = symbol_cell.get_text(strip=True).split()[0]

            if not symbol:
                raise ValueError("Missing symbol")

            if symbol.lower().startswith("money"):
                current_value = self._clean_decimal_text(cells[6].get_text(strip=True))
                self.log.info(f"Parsed money-market cash position: ${current_value}")
                return Holding(
                    symbol="USD_CASH",
                    description="Money market cash",
                    quantity=current_value,
                    price=1.00,
                    unit_cost=1.00,
                    cost_basis=current_value,
                    current_value=current_value,
                    day_change_percent=0.00,
                    day_change_dollars=0.00,
                    unrealized_gain_loss=0.00,
                    unrealized_gain_loss_percent=0.00,
                    brokers={self.broker_name: current_value},
                )

            desc_div = symbol_cell.find('div', class_='holding-name', attrs={'aria-hidden': 'true'})
            if desc_div:
                description = desc_div.get_text(" ", strip=True)
            else:
                description = symbol

            quantity = self._clean_decimal_text(cells[1].get_text(strip=True))
            if quantity == 0:
                self.log.warning(f"Found position with zero quantity: {symbol}, maybe pending clearance.")
                return None

            price = self._clean_decimal_text(self._cell_primary_text(cells[2]))

            day_change_per_share = self._parse_signed_decimal(cells[3])
            day_change_percent = self._parse_signed_percentage(cells[4])
            day_change_dollars = day_change_per_share * quantity

            unit_cost = self._clean_optional_decimal_text(cells[5].get_text(strip=True))
            current_value = self._clean_decimal_text(cells[6].get_text(strip=True))

            unrealized_gain_loss = self._parse_optional_signed_decimal(cells[9])
            unrealized_gain_loss_percent = self._parse_optional_signed_percentage(cells[10])

            cost_basis = self._clean_optional_decimal_text(cells[11].get_text(strip=True))
            if cost_basis is None and unit_cost is not None:
                cost_basis = unit_cost * quantity

            portfolio_percentage = None
            portfolio_text = cells[12].get_text(strip=True)
            if portfolio_text and portfolio_text != '--':
                portfolio_percentage = self._clean_percentage_text(portfolio_text)

            holding = Holding(
                symbol=symbol,
                description=description,
                quantity=quantity,
                price=price,
                unit_cost=unit_cost,
                cost_basis=cost_basis,
                current_value=current_value,
                day_change_percent=day_change_percent,
                day_change_dollars=day_change_dollars,
                unrealized_gain_loss=unrealized_gain_loss,
                unrealized_gain_loss_percent=unrealized_gain_loss_percent,
                portfolio_percentage=portfolio_percentage,
                brokers={self.broker_name: current_value}
            )

            warning = day_change_consistency_warning(holding)
            if warning:
                self.log.warning(warning)

            self.log.debug(f"Parsed holding: {symbol} - {quantity} @ ${price} (value ${current_value})")
            return holding

        except Exception as e:
            self.log.error(f"Error parsing position row: {e}")
            return None

    def _cell_primary_text(self, cell) -> str:
        """Return the text of a cell's first span, ignoring sub-text (e.g. timestamps)."""
        sub = cell.find('span', class_='sub-text')
        if sub:
            sub.extract()
        return cell.get_text(strip=True)

    # ------------------------------------------------------------------
    # Value parsing helpers
    # ------------------------------------------------------------------

    def _parse_signed_decimal(self, cell) -> float:
        """Parse a cell whose text looks like 'Positive+$2.37' or 'Negative-$5.07'."""
        text = cell.get_text(strip=True)
        if not text or text in ('--', 'No Data--'):
            return 0.0
        return self._clean_decimal_text(text)

    def _parse_optional_signed_decimal(self, cell) -> Optional[float]:
        text = cell.get_text(strip=True)
        return self._clean_optional_decimal_text(text)

    def _parse_signed_percentage(self, cell) -> float:
        text = cell.get_text(strip=True)
        if not text or text in ('--', 'No Data--'):
            return 0.0
        return self._clean_percentage_text(text)

    def _parse_optional_signed_percentage(self, cell) -> Optional[float]:
        text = cell.get_text(strip=True)
        return self._clean_optional_percentage_text(text)

    # ------------------------------------------------------------------
    # Sanity check — uses page-level total value
    # ------------------------------------------------------------------

    def sanity_check(self, soup, all_holdings: List[Holding]) -> bool:
        """Compare page-reported total value against the sum of parsed holdings."""
        TOTAL_CHECK_TOLERANCE = 0.01

        reported_total = self._extract_page_total_value(soup)
        if reported_total is None:
            self.log.warning("Could not find page-level total value — skipping sanity check")
            return True

        computed_total = sum(h.current_value for h in all_holdings)
        diff = abs(computed_total - reported_total)

        if diff / max(reported_total, 1.0) > TOTAL_CHECK_TOLERANCE:
            raise RuntimeError(
                f"Total value mismatch: holdings {computed_total:,.2f} vs reported {reported_total:,.2f}"
            )

        self.log.info(f"Sanity check passed: computed={computed_total:,.2f}, reported={reported_total:,.2f}")
        return True

    def _extract_page_total_value(self, soup) -> Optional[float]:
        """Find the page-level 'Total value' and return the dollar amount."""
        for el in soup.find_all(string=re.compile(r'^\s*Total value\s*$')):
            sibling = el.find_parent().find_next_sibling()
            if sibling:
                text = sibling.get_text(strip=True)
                try:
                    return self._clean_decimal_text(text)
                except ValueError:
                    continue
        return None

    # ------------------------------------------------------------------
    # Low-level text cleaning (unchanged from old crawler)
    # ------------------------------------------------------------------

    def _clean_optional_decimal_text(self, value_str: str) -> Optional[float]:
        if not value_str:
            return None
        value_str = value_str.strip()
        if value_str in {"--", "-- --", "No Data--"}:
            return None
        return self._clean_decimal_text(value_str)

    def _clean_optional_percentage_text(self, value_str: str) -> Optional[float]:
        if not value_str:
            return None
        value_str = value_str.strip()
        if value_str in {"--", "-- --", "No Data--"}:
            return None
        return self._clean_percentage_text(value_str)

    def _clean_decimal_text(self, value_str: str) -> float:
        """Clean text and extract decimal value, handling Merrill-specific formatting"""
        if not value_str:
            raise ValueError("Value string cannot be empty")

        value_str = value_str.strip()

        is_negative = False
        if '(' in value_str and ')' in value_str:
            is_negative = True
            value_str = value_str.replace('(', '').replace(')', '')

        cleaned = re.sub(r'[$,]', '', value_str)

        number_match = re.search(r'[-+]?\d+\.?\d*', cleaned)
        if number_match:
            number_str = number_match.group()
            try:
                result = float(number_str)
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert '{number_str}' to float: {e}")

        raise ValueError(f"No valid number found in text: '{value_str}'")

    def _clean_percentage_text(self, value_str: str) -> float:
        """Clean percentage text and convert to decimal (e.g. '1.5%' -> 0.015)"""
        if not value_str:
            raise ValueError("Percentage string cannot be empty")

        value_str = value_str.strip()

        is_negative = False
        if '(' in value_str and ')' in value_str:
            is_negative = True
            value_str = value_str.replace('(', '').replace(')', '')
        elif 'Loss of' in value_str:
            is_negative = True
            value_str = value_str.replace('Loss of', '').strip()
        elif 'Gain of' in value_str:
            value_str = value_str.replace('Gain of', '').strip()

        cleaned = re.sub(r'[%+]', '', value_str)

        number_match = re.search(r'-?\d+\.?\d*', cleaned)
        if number_match:
            number_str = number_match.group()
            try:
                result = float(number_str) / 100
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert percentage '{number_str}' to float: {e}")

        raise ValueError(f"No valid percentage found in text: '{value_str}'")

    def _combine_holdings_by_symbol(self, holdings: List[Holding]) -> List[Holding]:
        """Combine holdings with the same symbol by aggregating quantities and values"""
        if not holdings:
            return []

        symbol_groups = {}
        for holding in holdings:
            symbol_groups.setdefault(holding.symbol, []).append(holding)

        combined_holdings = []
        for symbol, group in symbol_groups.items():
            combined_holdings.append(self._combine_symbol_group(symbol, group))

        return combined_holdings

    def _combine_symbol_group(self, symbol: str, holdings: List[Holding]) -> Holding:
        """Combine multiple holdings of the same symbol"""
        if not holdings:
            raise ValueError(f"No holdings provided for symbol {symbol}")

        base_holding = holdings[0]

        total_quantity = sum(h.quantity for h in holdings)
        cost_basis_values = [h.cost_basis for h in holdings]
        total_cost_basis = None if any(value is None for value in cost_basis_values) else sum(cost_basis_values)
        total_current_value = sum(h.current_value for h in holdings)
        total_day_change_dollars = sum(h.day_change_dollars for h in holdings)
        unrealized_values = [h.unrealized_gain_loss for h in holdings]
        total_unrealized_gain_loss = None if any(value is None for value in unrealized_values) else sum(unrealized_values)

        if total_quantity != 0:
            weighted_avg_price = total_current_value / total_quantity
            weighted_avg_unit_cost = None if total_cost_basis is None else total_cost_basis / total_quantity
        else:
            weighted_avg_price = 0.0
            weighted_avg_unit_cost = None

        day_change_percent = 0.0
        unrealized_gain_loss_percent = None

        if total_current_value != 0:
            day_change_percent = total_day_change_dollars / (total_current_value - total_day_change_dollars)

        if total_cost_basis not in (None, 0) and total_unrealized_gain_loss is not None:
            unrealized_gain_loss_percent = total_unrealized_gain_loss / total_cost_basis
        elif total_cost_basis == 0 and total_unrealized_gain_loss is not None:
            unrealized_gain_loss_percent = 0.0

        portfolio_percentage = None
        portfolio_percentages = [h.portfolio_percentage for h in holdings if h.portfolio_percentage is not None]
        if portfolio_percentages:
            portfolio_percentage = sum(portfolio_percentages)

        combined_holding = Holding(
            symbol=symbol,
            description=base_holding.description,
            quantity=total_quantity,
            price=weighted_avg_price,
            unit_cost=weighted_avg_unit_cost,
            cost_basis=total_cost_basis,
            current_value=total_current_value,
            day_change_percent=day_change_percent,
            day_change_dollars=total_day_change_dollars,
            unrealized_gain_loss=total_unrealized_gain_loss,
            unrealized_gain_loss_percent=unrealized_gain_loss_percent,
            portfolio_percentage=portfolio_percentage,
            brokers={self.broker_name: total_current_value}
        )

        self.log.debug(f"Combined {len(holdings)} holdings for {symbol}: {total_quantity} shares @ ${weighted_avg_price:.4f} = ${total_current_value}")
        return combined_holding
