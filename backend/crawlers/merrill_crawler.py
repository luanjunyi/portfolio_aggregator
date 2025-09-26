from typing import List, Dict, Any
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
from models.portfolio import Holding


class MerrillCrawler(BaseCrawler):
    """Merrill Edge crawler"""

    
    def __init__(self):
        super().__init__("merrill_edge")
        self.login_url = "https://olui2.fs.ml.com/login/signin.aspx"
        self.portfolio_url = "https://olui2.fs.ml.com/TFPHoldings/HoldingsByAccount.aspx"
    
    def get_login_url(self) -> str:
        """Get Merrill login URL"""
        return self.login_url
    
    async def scrape_portfolio(self) -> List[Holding]:
        """Scrape holdings from Merrill Edge"""
        self.log.info("Starting Merrill holdings scrape...")      
        self.log.info("Navigating to portfolio page...")
        
        # Navigate to the portfolio page that shows all holdings
        await self.page.goto(self.portfolio_url, wait_until='networkidle')
        
        # Wait for the portfolio table to load (it has a dynamic ID starting with CustomGrid_)
        self.log.info("Waiting for portfolio table to load...")
        try:
            await self.page.wait_for_selector('table[id^="CustomGrid_"][class*="customTable"]', timeout=30000)
            self.log.info("Portfolio table loaded successfully")
        except Exception as e:
            self.log.warning(f"Portfolio table selector not found: {e}")
            # Continue anyway in case the table is there but with different attributes
        
        # Get page HTML
        html = await self.page.content()
        
        # Parse holdings from HTML
        holdings = await self.parse_portfolio_html(html)
        
        self.log.info(f"Found {len(holdings)} total holdings")
        
        return holdings
    
    async def login(self) -> bool:
        """Login to Merrill Edge"""
        self.log.info("Starting Merrill login...")
        
        # First check if we're already logged in with a valid session
        try:
            await self.page.goto(self.login_url, wait_until='domcontentloaded')
            # Give the browser a chance to follow any automatic redirect
            await self.page.wait_for_load_state('networkidle', timeout=15000)

            current_url = self.page.url
            if "tfpholdings" in current_url.lower():
                self.log.info("Already logged in with stored session!")
                return True
        except Exception:
            pass  # Continue with normal login if session check fails
        
        # Get stored credentials
        credentials = self.get_credentials()
        if not credentials:
            self.log.fatal("No credentials found. Please add credentials first.")
            raise RuntimeError("No credentials found")
        
        username = credentials['username']
        password = credentials['password']
        
        # Wait for page to fully load
        current_url = self.page.url
        self.log.debug(f"Current URL: {current_url}")
        
        # Fill username field - crash if not found
        try:
            await self.page.fill('#oid', username)
            self.log.debug("Filled username field")
        except Exception as e:
            self.log.fatal(f"Username field #oid not found: {e}")
            raise RuntimeError(f"Username field #oid not found: {e}")
        
        # Fill password field - crash if not found  
        try:
            await self.page.fill('#pass', password)
            self.log.debug("Filled password field")
        except Exception as e:
            self.log.fatal(f"Password field #pass not found: {e}")
            raise RuntimeError(f"Password field #pass not found: {e}")
        
        # Click login button - crash if not found
        try:
            await self.page.click('#secure-signin-submit', delay=random.randint(100, 200))
            self.log.debug("Clicked login button")
        except Exception as e:
            self.log.fatal(f"Login button #secure-signin-submit not found: {e}")
            raise RuntimeError(f"Login button #secure-signin-submit not found: {e}")

        # Wait for positions page or any portfolios page
        try:
            self.log.info("Waiting for positions page to load...")
            await self.page.wait_for_url("**/TFPHoldings/HoldingsByAccount.aspx", timeout=5 * 60000)
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
        
        tables = soup.find_all('table', id=re.compile(r'^CustomGrid_'))

        if not tables:
            self.log.fatal("Merrill holdings tables not found")
            debug_file = "merrill_debug_all_holdings.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            self.log.info(f"Saved HTML to {debug_file} for analysis")
            raise RuntimeError("Holdings tables not found")

        self.log.info(f"Found {len(tables)} holdings table(s)")

        for table in tables:
            tbody = table.find('tbody')
            if not tbody:
                continue

            table_holdings: List[Holding] = []
            rows = tbody.find_all('tr')
            for row in rows:
                first_cell = row.find('td')
                if first_cell:
                    symbol_preview = first_cell.get_text(strip=True).lower()
                    if 'balances' in symbol_preview:
                        self.log.info("Reached balances section, skipping this row")
                        continue
                
                # Check if this is a cash row (Money accounts)
                try:
                    cash_holding = self._parse_cash_row(row)
                    if cash_holding:
                        holdings.append(cash_holding)
                        table_holdings.append(cash_holding)
                        self.log.info("Found and parsed cash position, this should be the last")
                        break
                except Exception as e:
                    self.log.debug(f"Row is not a cash position: {e}")
                
                try:
                    holding = self._parse_position_row(row)
                    if holding:
                        holdings.append(holding)
                        table_holdings.append(holding)
                except Exception as e:
                    self.log.error(f"Error parsing row: {e}")
            self.log.debug(f"found {len(table_holdings)} holdings from table")
            self.sanity_check(table, table_holdings)

        # Combine holdings by symbol
        combined_holdings = self._combine_holdings_by_symbol(holdings)
        
        self.log.info(f"Successfully parsed {len(holdings)} individual holdings")
        self.log.info(f"Combined into {len(combined_holdings)} unique symbols")

        return combined_holdings
    
    def _parse_position_row(self, row) -> Holding:
        """Parse a single position row from Merrill portfolio table"""
        cells = row.find_all('td')
        if len(cells) < 11:
            return None

        try:
            symbol_cell = cells[0]
            symbol_link = symbol_cell.find('a')
            if symbol_link:
                symbol = symbol_link.get_text(" ", strip=True).split()[0]
            else:
                symbol = symbol_cell.get_text(strip=True)

            if not symbol:
                raise ValueError("Missing symbol")

            description = cells[2].get_text(" ", strip=True)
            if not description:
                raise ValueError(f"Missing description for symbol {symbol}")

            day_change_dollars = self._extract_dollar_change(cells[3])
            day_change_percent = self._extract_percentage_change(cells[3])

            price = self._clean_decimal_text(cells[4].get_text(" ", strip=True))
            quantity = self._clean_decimal_text(cells[5].get_text(strip=True))
            unit_cost = self._clean_decimal_text(cells[6].get_text(strip=True))
            cost_basis = self._clean_decimal_text(cells[7].get_text(strip=True))
            current_value = self._clean_decimal_text(cells[8].get_text(strip=True))

            unrealized_gain_loss = self._extract_dollar_change(cells[9])
            unrealized_gain_loss_percent = self._extract_percentage_change(cells[9])

            portfolio_percentage = None
            portfolio_text = cells[10].get_text(strip=True)
            if portfolio_text:
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

            self.log.debug(f"Parsed holding: {symbol} - {quantity} @ ${price} (value ${current_value})")
            return holding

        except Exception as e:
            self.log.error(f"Error parsing position row: {e}")
            return None

    def _parse_cash_row(self, row) -> Holding:
        """Parse a cash position row from Merrill portfolio table"""
        cells = row.find_all('td')
        if len(cells) < 9:
            return None
        
        try:
            # Check if this is a cash row by looking for "Money accounts" link
            first_cell = cells[0]
            money_accounts_link = first_cell.find('a')
            if not money_accounts_link or 'money accounts' not in money_accounts_link.get_text(strip=True).lower():
                return None
            
            # Extract description from the third cell (index 2)
            description_cell = cells[2]
            description = description_cell.get_text(strip=True) or "Cash & sweep funds"
            
            # Extract quantity from the sixth cell (index 5)
            quantity_text = cells[5].get_text(strip=True)
            if not quantity_text or quantity_text == '--':
                return None
            quantity = self._clean_decimal_text(quantity_text)
            
            # Extract current value from the ninth cell (index 8) 
            value_text = cells[8].get_text(strip=True)
            if not value_text or value_text == '--':
                return None
            current_value = self._clean_decimal_text(value_text)
            
            # Verify this is actually a cash position (quantity should equal current_value for cash)
            if abs(quantity - current_value) > 0.01:  # Allow small rounding differences
                return None
            
            # Create cash holding
            holding = Holding(
                symbol="USD_CASH",
                description=description,
                quantity=current_value,  # For cash, use the dollar amount as quantity
                price=1.00,  # Cash price is always $1
                unit_cost=1.00,  # Unit cost is always $1 for cash
                cost_basis=current_value,  # Cost basis equals current value for cash
                current_value=current_value,
                day_change_percent=0.00,  # Cash doesn't have daily changes
                day_change_dollars=0.00,  # Cash doesn't have daily changes
                unrealized_gain_loss=0.00,  # Cash has no unrealized gain/loss
                unrealized_gain_loss_percent=0.00,  # Cash has no unrealized gain/loss
                brokers={self.broker_name: current_value}
            )
            
            self.log.debug(f"Parsed cash holding: USD_CASH - ${current_value}")
            return holding
            
        except Exception as e:
            self.log.debug(f"Error parsing cash row: {e}")
            return None

    def sanity_check(self, table, table_holdings: List[Holding]) -> bool:
        """Compare reported totals within a single table against parsed holdings."""
        TOTAL_CHECK_TOLERANCE = 0.01

        total_row = self._extract_total_row(table)
        if total_row is None:
            raise RuntimeError("Total row not found in Merrill holdings table")

        reported_total_value = total_row['current_value']
        reported_unrealized_gain = total_row['unrealized_gain_loss']

        computed_total_value = sum(holding.current_value for holding in table_holdings)
        computed_unrealized_gain = sum(holding.unrealized_gain_loss for holding in table_holdings)

        value_diff = computed_total_value - reported_total_value
        unrealized_diff = computed_unrealized_gain - reported_unrealized_gain

        if abs(value_diff) / reported_total_value > TOTAL_CHECK_TOLERANCE:
            self.log.fatal(
                f"Total value mismatch: holdings {computed_total_value} vs reported {reported_total_value}"
            )
            return False

        if abs(unrealized_diff) / reported_unrealized_gain > TOTAL_CHECK_TOLERANCE:
            self.log.fatal(
                "Unrealized gain mismatch: holdings "
                f"{computed_unrealized_gain} vs reported {reported_unrealized_gain}"
            )
            return False


        return True

    def _extract_total_row(self, table) -> Dict[str, float] | None:
        tbody = table.find('tbody')
        if not tbody:
            return None

        for row in tbody.find_all('tr'):
            first_cell = row.find('td')
            if not first_cell:
                continue

            if first_cell.get_text(strip=True).lower() == 'total':
                return self._parse_total_row(row)

        return None

    def _parse_total_row(self, row) -> Dict[str, float]:
        cells = row.find_all('td')
        if len(cells) < 10:
            raise RuntimeError("Total row missing expected cells")

        total_value_text = cells[8].get_text(strip=True)
        if not total_value_text or total_value_text == '--':
            raise RuntimeError("Total row missing total value")
        reported_total_value = self._clean_decimal_text(total_value_text)

        unrealized_cell = cells[9]
        unrealized_text = unrealized_cell.get_text(strip=True)
        if not unrealized_text or unrealized_text == '--':
            reported_unrealized_gain = 0.0
        else:
            reported_unrealized_gain = self._extract_dollar_change(unrealized_cell)

        return {
            'current_value': reported_total_value,
            'unrealized_gain_loss': reported_unrealized_gain,
        }

    def _extract_dollar_change(self, cell) -> float:
        target = cell.find('div', class_=lambda value: value and 'dol' in value.split())
        text = (target.get_text(strip=True) if target else cell.get_text(strip=True))
        if not text:
            return 0.0
        return self._clean_decimal_text(text)

    def _extract_percentage_change(self, cell) -> float:
        target = cell.find('div', class_=lambda value: value and 'per' in value.split())
        text = (target.get_text(strip=True) if target else cell.get_text(strip=True))
        if not text:
            return 0.0
        return self._clean_percentage_text(text)
    
    def _clean_decimal_text(self, value_str: str) -> float:
        """Clean text and extract decimal value, handling Merrill-specific formatting"""
        if not value_str:
            raise ValueError("Value string cannot be empty")
        
        # Remove whitespace
        value_str = value_str.strip()
        
        # Handle negative values in parentheses
        is_negative = False
        if '(' in value_str and ')' in value_str:
            is_negative = True
            value_str = value_str.replace('(', '').replace(')', '')
        
        # Remove currency symbols, commas
        cleaned = re.sub(r'[$,]', '', value_str)
        
        # Extract only the numeric part
        number_match = re.search(r'[-+]?\d+\.?\d*', cleaned)
        if number_match:
            number_str = number_match.group()
            try:
                result = float(number_str)
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert '{number_str}' to float: {e}")
        
        raise ValueError(f"No valid number found in text: '{value_str}'")
    
    def _extract_first_price(self, price_text: str) -> float:
        """Extract the first price from complex text"""
        if not price_text:
            raise ValueError("Price text cannot be empty")
        
        # Look for the first decimal number at the beginning of the string
        number_match = re.match(r'^(\d+\.?\d*)', price_text.strip())
        if number_match:
            try:
                return float(number_match.group(1))
            except Exception as e:
                raise ValueError(f"Failed to convert price '{number_match.group(1)}' to float: {e}")
        
        raise ValueError(f"No valid price found at start of text: '{price_text}'")
    
    def _clean_percentage_text(self, value_str: str) -> float:
        """Clean percentage text and convert to Decimal (as decimal, not percentage)"""
        if not value_str:
            raise ValueError("Percentage string cannot be empty")
        
        # Remove whitespace
        value_str = value_str.strip()
        
        # Handle negative values in parentheses or with "Loss of" prefix
        is_negative = False
        if '(' in value_str and ')' in value_str:
            is_negative = True
            value_str = value_str.replace('(', '').replace(')', '')
        elif 'Loss of' in value_str:
            is_negative = True
            value_str = value_str.replace('Loss of', '').strip()
        elif 'Gain of' in value_str:
            value_str = value_str.replace('Gain of', '').strip()
        
        # Remove percentage symbol and other formatting
        cleaned = re.sub(r'[%+]', '', value_str)
        
        # Extract the numeric part
        number_match = re.search(r'-?\d+\.?\d*', cleaned)
        if number_match:
            number_str = number_match.group()
            try:
                result = float(number_str) / 100  # Convert percentage to decimal
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert percentage '{number_str}' to float: {e}")
        
        raise ValueError(f"No valid percentage found in text: '{value_str}'")
    
    def _combine_holdings_by_symbol(self, holdings: List[Holding]) -> List[Holding]:
        """Combine holdings with the same symbol by aggregating quantities and values"""
        if not holdings:
            return []
        
        # Group holdings by symbol
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
        
        # Use the first holding as the base for description and other metadata
        base_holding = holdings[0]
        
        # Aggregate quantities and values
        total_quantity = sum(h.quantity for h in holdings)
        total_cost_basis = sum(h.cost_basis for h in holdings)
        total_current_value = sum(h.current_value for h in holdings)
        total_day_change_dollars = sum(h.day_change_dollars for h in holdings)
        total_unrealized_gain_loss = sum(h.unrealized_gain_loss for h in holdings)
        
        # Calculate weighted averages and derived values
        if total_quantity != 0:
            weighted_avg_price = total_current_value / total_quantity
            weighted_avg_unit_cost = total_cost_basis / total_quantity
        else:
            weighted_avg_price = 0.0
            weighted_avg_unit_cost = 0.0
        
        # Calculate percentages
        day_change_percent = 0.0
        unrealized_gain_loss_percent = 0.0
        
        if total_current_value != 0:
            day_change_percent = total_day_change_dollars / (total_current_value - total_day_change_dollars)
        
        if total_cost_basis != 0:
            unrealized_gain_loss_percent = total_unrealized_gain_loss / total_cost_basis
        
        # Sum portfolio percentages if available
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
