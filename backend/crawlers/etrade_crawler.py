from typing import List
import asyncio
import re
import random

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.base_crawler import BaseCrawler
from models.portfolio import Holding


class EtradeCrawler(BaseCrawler):
    """E*TRADE crawler"""

    def __init__(self):
        super().__init__("etrade")
        # Login URL provided with redirect target to positions page
        self.login_url = (
            "https://us.etrade.com/etx/pxy/login?TARGET="
            "https%3A%2F%2Fus.etrade.com%2Fetx%2Fpxy%2Fportfolios%2Fpositions"
        )
        self.portfolio_url = "https://us.etrade.com/etx/pxy/portfolios/positions"

    def get_login_url(self) -> str:
        """Get E*TRADE login URL"""
        return self.login_url

    async def scrape_portfolio(self) -> List[Holding]:
        """Scrape holdings from E*TRADE positions page"""
        self.log.info("Starting E*TRADE holdings scrape...")

        self.log.info("Navigating to portfolio positions page...")
        await self.page.goto(self.portfolio_url, wait_until='networkidle')
        await self.page.wait_for_load_state('networkidle', timeout=15000)


        holdings = await self.parse_portfolio_html()

        self.log.info(f"Found {len(holdings)} total holdings")
        return holdings

    async def login(self) -> bool:
        """Login to E*TRADE"""
        self.log.info("Starting E*TRADE login...")

        # First check if we're already logged in with a valid session
        try:
            await self.page.goto(self.login_url, wait_until='domcontentloaded')
            await self.page.wait_for_load_state('networkidle', timeout=15000)

            # If redirected straight to positions page, we're logged in
            if "/portfolios/positions" in self.page.url.lower():
                self.log.info("Already logged in with stored session!")
                return True
        except Exception:
            pass

        credentials = self.get_credentials()
        if not credentials:
            self.log.fatal("No credentials found. Please add credentials first.")
            raise RuntimeError("No credentials found")

        username = credentials['username']
        password = credentials['password']

        try:
            self.log.info("Waiting for login form to load...")

            username_selector = '#USER'
            password_selector = '#password'
            login_button_selector = '#mfaLogonButton'

            if not await self.wait_for_element(username_selector, timeout=20000):
                self.log.fatal(f"Username field not found: {username_selector}")
                raise RuntimeError(f"Username field not found: {username_selector}")
            if not await self.wait_for_element(password_selector, timeout=20000):
                self.log.fatal(f"Password field not found: {password_selector}")
                raise RuntimeError(f"Password field not found: {password_selector}")

            try:
                await self.page.fill(username_selector, username)
                self.log.debug(f"Filled username in {username_selector}")
            except Exception as e:
                self.log.fatal(f"Error filling username field: {e}")
                raise

            try:
                await self.page.fill(password_selector, password)
                self.log.debug(f"Filled password in {password_selector}")
            except Exception as e:
                self.log.fatal(f"Error filling password field: {e}")
                raise

            if not await self.wait_for_element(login_button_selector, timeout=15000):
                self.log.fatal(f"Login button not found: {login_button_selector}")
                raise RuntimeError(f"Login button not found: {login_button_selector}")

            try:
                await self.page.click(login_button_selector, delay=random.randint(100, 200))
                self.log.debug(f"Clicked login button {login_button_selector}")
            except Exception as e:
                self.log.fatal(f"Error clicking login button: {e}")
                raise

            try:
                self.log.info("Waiting for positions page to load...")
                await self.page.wait_for_url("**/portfolios/positions*", timeout=5 * 60000)
            except Exception as e:
                self.log.fatal(f"Error waiting for positions URL: {e}")
                raise

            if "/portfolios/positions" in self.page.url.lower():
                self.log.info("Login successful - reached positions page")
                return True

            self.log.fatal(f"Login failed - at URL: {self.page.url}")
            raise RuntimeError(f"Login failed - unexpected URL: {self.page.url}")
        except Exception as e:
            self.log.fatal(f"Login failed: {e}")
            raise

    async def parse_portfolio_html(self) -> List[Holding]:
        """Parse the E*TRADE positions page.

        The page is rendered with React, so we primarily rely on the live DOM. If parsing
        fails we save debug artifacts to help refine the selectors.
        """
        try:
            return await self._parse_positions_from_dom()
        except Exception as exc:
            self.log.error(f"DOM parsing error: {exc}")

        # Save debug artifacts for further analysis
        try:
            with open("etrade_dom_dump.html", "w", encoding="utf-8") as fh:
                fh.write(await self.page.content())
            app_html = await self.page.eval_on_selector('#application', 'el => el.outerHTML')
            if app_html:
                with open("etrade_application_dump.html", "w", encoding="utf-8") as fh:
                    fh.write(app_html)
            await self.page.screenshot(path="etrade_positions_page.png", full_page=True)
            self.log.info("Saved debug artifacts (HTML and screenshot)")
        except Exception as debug_exc:
            self.log.error(f"Failed to capture debug artifacts: {debug_exc}")

        return []

    async def _parse_positions_from_dom(self) -> List[Holding]:
        grid_selector = 'div[role="grid"][aria-label="Portfolios"]'
        row_selector = f"{grid_selector} div[role='row'][aria-rowindex]"
        # Wait for the grid to be loaded by React
        await self.page.wait_for_selector(row_selector, timeout=20000)

        raw_rows = await self._extract_positions_data_via_js()
        if not raw_rows:
            self.log.warning("No portfolio rows found in React grid")
            return []

        def get_value(row: dict, key: str) -> str:
            return (row.get(key) or "").strip()

        def parse_decimal(row: dict, key: str, symbol: str) -> float:
            value = get_value(row, key)
            if not value:
                raise ValueError(f"Missing value for {key} (symbol={symbol})")
            return self._clean_decimal_text(value)

        def parse_percent(row: dict, key: str, symbol: str) -> float:
            value = get_value(row, key)
            if not value:
                raise ValueError(f"Missing value for {key} (symbol={symbol})")
            return self._clean_percentage_text(value)

        def parse_decimal_optional(row: dict, key: str, symbol: str) -> float:
            value = get_value(row, key)
            if not value:
                return 0.0
            try:
                return self._clean_decimal_text(value)
            except ValueError as exc:
                self.log.warning(f"Skipping decimal field {key} for {symbol}: {exc}")
                return 0.0

        def parse_percent_optional(row: dict, key: str, symbol: str) -> float:
            value = get_value(row, key)
            if not value:
                return 0.0
            try:
                return self._clean_percentage_text(value)
            except ValueError as exc:
                self.log.warning(f"Skipping percent field {key} for {symbol}: {exc}")
                return 0.0

        non_holding_labels = {
            "transfer money",
            "add cash",
            "withdraw",
        }

        holding_list: List[Holding] = []
        for row in raw_rows:
            symbol = get_value(row, "symbol")
            if not symbol:
                raise ValueError("Symbol is required for every row")
            if symbol.lower() in non_holding_labels:
                continue
            
            # Check if this is a cash position
            if symbol.lower() == "cash":
                cash_holding = self._parse_cash_position(row)
                if cash_holding:
                    holding_list.append(cash_holding)
                continue

            description = get_value(row, "description") or symbol

            try:
                quantity = parse_decimal(row, "quantity", symbol)
                price = parse_decimal(row, "last_price", symbol)
                current_value = parse_decimal(row, "value", symbol)
            except ValueError as exc:
                self.log.warning(f"Skipping row for {symbol}: {exc}")
                continue

            unit_cost = parse_decimal_optional(row, "cost_per_share", symbol)
            cost_basis = parse_decimal_optional(row, "total_cost", symbol)
            day_change_dollars = parse_decimal_optional(row, "day_gain_dollars", symbol)
            day_change_percent = parse_percent_optional(row, "day_change_percent", symbol)
            unrealized_gain_loss = parse_decimal_optional(row, "total_gain", symbol)
            unrealized_gain_loss_percent = parse_percent_optional(row, "total_gain_percent", symbol)

            holding_list.append(
                Holding(
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
                    brokers={self.broker_name: current_value},
                )
            )

        # Perform sanity check
        await self.sanity_check(holding_list)
        
        return holding_list

    def _parse_cash_position(self, row: dict) -> Holding:
        """Parse a cash position from E*Trade data"""
        try:
            def get_value(key: str) -> str:
                return (row.get(key) or "").strip()
            
            # Try to get cash amount from value field
            cash_value_text = get_value("value")
            if not cash_value_text:
                return None
            
            cash_amount = self._clean_decimal_text(cash_value_text)
            if cash_amount <= 0:
                return None
            
            # Create cash holding
            holding = Holding(
                symbol="USD_CASH",
                description="Cash & sweep funds",
                quantity=cash_amount,  # Cash quantity equals the dollar amount
                price=1.00,  # Cash price is always $1
                unit_cost=1.00,  # Unit cost is always $1 for cash
                cost_basis=cash_amount,  # Cost basis equals current value for cash
                current_value=cash_amount,
                day_change_percent=0.00,  # Cash doesn't have daily changes
                day_change_dollars=0.00,  # Cash doesn't have daily changes
                unrealized_gain_loss=0.00,  # Cash has no unrealized gain/loss
                unrealized_gain_loss_percent=0.00,  # Cash has no unrealized gain/loss
                brokers={self.broker_name: cash_amount}
            )
            
            self.log.debug(f"Parsed cash holding: USD_CASH - ${cash_amount}")
            return holding
            
        except Exception as e:
            self.log.error(f"Error parsing cash position: {e}")
            return None

    async def _extract_positions_data_via_js(self) -> List[dict]:
        script = r'''
(() => {
  const grid = document.querySelector('div[role="grid"][aria-label="Portfolios"]');
  if (!grid) return [];

  const extractText = (row, colIndex) => {
    const cell = row.querySelector(`[aria-colindex="${colIndex}"]`);
    if (!cell) return '';
    return (cell.innerText || '').trim();
  };

  const rows = Array.from(grid.querySelectorAll('div[role="row"][aria-rowindex]'))
    .filter(row => row.querySelector('[role="gridcell"]') || row.querySelector('[role="rowheader"]'));

  const results = [];
  for (const row of rows) {
    const symbolCell = row.querySelector('[role="rowheader"][aria-colindex="1"]');
    if (!symbolCell) continue;

    // Check for cash row first by looking for the cash wrapper
    const cashWrapper = symbolCell.querySelector('.FooterCellRenderer---cash-and-transfer-wrapper---ISxOD');
    if (cashWrapper) {
      // This is definitely the cash row - extract the cash value
      const cashValue = extractText(row, 11);
      if (cashValue) {
        results.push({
          symbol: 'Cash',
          description: 'Cash & sweep funds',
          value: cashValue
        });
        continue;
      }
    }

    // Check for total row and skip it
    const totalWrapper = symbolCell.querySelector('.FooterCellRenderer---cash-and-total---hAWG4');
    if (totalWrapper && symbolCell.textContent.toLowerCase().includes('total')) {
      continue; // Skip total row
    }

    // Regular stock rows
    const symbolLink = symbolCell.querySelector('a');
    const symbolText = symbolLink ? (symbolLink.textContent || '') : (symbolCell.textContent || '');
    const symbol = symbolText.trim();
    if (!symbol) continue;

    const rawDescription = symbolLink ? (symbolLink.getAttribute('title') || symbolLink.getAttribute('aria-label') || '') : '';

    results.push({
      symbol,
      description: rawDescription.trim(),
      last_price: extractText(row, 3),
      day_change_dollars: extractText(row, 4),
      day_change_percent: extractText(row, 5),
      quantity: extractText(row, 6),
      cost_per_share: extractText(row, 7),
      day_gain_dollars: extractText(row, 8),
      total_gain: extractText(row, 9),
      total_gain_percent: extractText(row, 10),
      value: extractText(row, 11)
    });
  }

  return results;
})()
        '''

        result = await self.page.evaluate(script)
        rows: List[dict] = []
        if isinstance(result, list):
            for entry in result:
                if isinstance(entry, dict):
                    rows.append(entry)

        return rows

    def _clean_decimal_text(self, value_str: str) -> float:
        if not value_str:
            raise ValueError("Value string cannot be empty")
        value_str = value_str.strip()
        is_negative = False
        if '(' in value_str and ')' in value_str:
            is_negative = True
            value_str = value_str.replace('(', '').replace(')', '')
        cleaned = re.sub(r'[,\$,%+]', '', value_str)
        number_match = re.search(r'-?\d+\.?\d*', cleaned)
        if not number_match:
            raise ValueError(f"No valid number found in text: '{value_str}'")
        result = float(number_match.group())
        return -result if is_negative else result

    def _clean_percentage_text(self, value_str: str) -> float:
        if not value_str:
            raise ValueError("Percentage string cannot be empty")
        value_str = value_str.strip()
        is_negative = False
        if '(' in value_str and ')' in value_str:
            is_negative = True
            value_str = value_str.replace('(', '').replace(')', '')
        cleaned = re.sub(r'[%+]', '', value_str)
        number_match = re.search(r'-?\d+\.?\d*', cleaned)
        if not number_match:
            raise ValueError(f"No valid percentage found in text: '{value_str}'")
        result = float(number_match.group()) / 100
        return -result if is_negative else result

    async def sanity_check(self, holdings: List[Holding]) -> None:
        """Compare reported totals on the page with parsed holdings totals."""
        TOTAL_CHECK_TOLERANCE = 0.01
        
        total_row_data = await self._parse_total_row()
        if not total_row_data:
            self.log.fatal("Could not find or parse totals row in E*TRADE portfolio.")
            raise RuntimeError("Could not find totals row in E*TRADE portfolio")

        reported_total_value = total_row_data['total_value']
        reported_market_value = total_row_data['market_value']
        reported_unrealized_gain = total_row_data['unrealized_gain_loss']

        computed_total_value = sum(h.current_value for h in holdings)
        computed_unrealized_gain = sum(h.unrealized_gain_loss for h in holdings)

        # Check total value (should match reported total value)
        value_diff = abs(computed_total_value - reported_total_value)
        if value_diff / max(computed_total_value, 1.0) > TOTAL_CHECK_TOLERANCE:
            raise RuntimeError(
                f"Total value mismatch: holdings {computed_total_value:.2f} vs reported {reported_total_value:.2f}"
            )

        # Check unrealized gain/loss
        unrealized_diff = abs(computed_unrealized_gain - reported_unrealized_gain)
        if unrealized_diff / max(abs(computed_unrealized_gain), 1.0) > TOTAL_CHECK_TOLERANCE:
            self.log.warning(
                f"Unrealized gain mismatch: holdings {computed_unrealized_gain:.2f} vs reported {reported_unrealized_gain:.2f}. This might be due to rounding."
            )

        self.log.info(
            "Sanity check passed: holdings total %s matches reported %s",
            computed_total_value,
            reported_total_value,
        )

    async def _parse_total_row(self) -> dict:
        """Parse the totals row from the E*TRADE portfolio table."""
        script = r'''
(() => {
  const grid = document.querySelector('div[role="grid"][aria-label="Portfolios"]');
  if (!grid) return null;

  const extractText = (row, colIndex) => {
    const cell = row.querySelector(`[aria-colindex="${colIndex}"]`);
    if (!cell) return '';
    return (cell.innerText || '').trim();
  };

  // Find the totals row by looking for the "Total" text (not "Cash Total")
  const rows = Array.from(grid.querySelectorAll('div[role="row"][aria-rowindex]'));
  for (const row of rows) {
    const symbolCell = row.querySelector('[role="rowheader"][aria-colindex="1"]');
    if (!symbolCell) continue;
    
    const totalWrapper = symbolCell.querySelector('.FooterCellRenderer---cash-and-total---hAWG4');
    if (totalWrapper) {
      const text = symbolCell.textContent.trim().toLowerCase();
      // Match "Total" but not "Cash Total"
      if (text === 'total' || (text.includes('total') && !text.includes('cash'))) {
        // Found the totals row
        return {
          day_change: extractText(row, 8),      // Column 8: Day's Gain $
          market_value: extractText(row, 7),    // Column 7: Total Cost (market value)
          total_gain: extractText(row, 9),      // Column 9: Total Gain $
          total_gain_percent: extractText(row, 10), // Column 10: Total Gain %
          total_value: extractText(row, 11)     // Column 11: Value $
        };
      }
    }
  }
  
  return null;
})()
        '''
        
        try:
            result = await self.page.evaluate(script)
            if not result:
                return None

            # Parse the extracted values
            total_value = self._clean_decimal_text(result['total_value'])
            market_value = self._clean_decimal_text(result['market_value'])
            unrealized_gain = self._clean_decimal_text(result['total_gain'])

            return {
                'total_value': total_value,
                'market_value': market_value,
                'unrealized_gain_loss': unrealized_gain,
            }
        except Exception as e:
            self.log.error(f"Error parsing totals row: {e}")
            return None
