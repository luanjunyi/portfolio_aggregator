from typing import List, Dict, Any, Optional
import asyncio
import re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.base_crawler import BaseCrawler
from models.portfolio import Holding


class ChaseCrawler(BaseCrawler):
    """Chase Self-Direct Investment crawler"""
    
    def __init__(self):
        super().__init__("chase")
        self.login_url = "https://secure.chase.com/web/auth/dashboard"
        self.portfolio_url = \
            "https://secure.chase.com/web/auth/dashboard#/dashboard/oi-portfolio/positions/render;ai=group-cwm-investment-;orderStatus=ALL"
    
    def get_login_url(self) -> str:
        """Get Chase login URL"""
        return self.login_url
    
    async def scrape_portfolio(self) -> List[Holding]:
        """Scrape holdings from Chase"""
        self.log.info("Starting Chase holdings scrape...")
        
        
        self.log.info("Navigating to portfolio page...")
        
        # Navigate to the single portfolio page that shows all holdings
        await self.page.goto(self.portfolio_url, wait_until='networkidle')
        await asyncio.sleep(5)
        
        # Get page HTML
        html = await self.page.content()
        
        # Parse holdings from HTML
        holdings = await self.parse_portfolio_html(html)
        
        self.log.info(f"Found {len(holdings)} total holdings across all accounts")
        
        return holdings
    
    async def login(self) -> bool:
        """Login to Chase"""
        self.log.info("Starting Chase login...")
        
        # First check if we're already logged in with a valid session
        try:
            await self.page.goto(self.login_url, wait_until='domcontentloaded')
            await asyncio.sleep(5)
            # If we're already on the dashboard, we're logged in
            # Look for the accounts accordion container which indicates we're on the dashboard
            dashboard_element = await self.page.query_selector('.accounts-group-accordion-container')
            if dashboard_element:
                self.log.info("Already logged in with stored session!")
                return True
        except Exception as e:
            raise RuntimeError(f"Failed to check login session: {e}") from e
        
        # Get stored credentials
        credentials = self.get_credentials()
        if not credentials:
            raise RuntimeError("No credentials found")
        
        username = credentials['username']
        password = credentials['password']
        
        try:
            # Wait for page to fully load and check for redirects
            current_url = self.page.url
            self.log.debug(f"Current URL: {current_url}")
            
            # Chase might redirect to a different login page
            if "logon" in current_url.lower():
                await asyncio.sleep(3)
            
            # Wait for login iframe to load
            self.log.debug("Looking for login iframe...")
            
            # Wait for the iframe to appear
            iframe_selector = 'iframe#logonbox'
            if not await self.wait_for_element(iframe_selector, timeout=15000):
                raise RuntimeError("Login iframe not found")
            
            # Get the iframe element and switch to its context
            iframe_element = await self.page.query_selector(iframe_selector)
            if not iframe_element:
                raise RuntimeError("Could not get iframe element")
            
            # Get the iframe's content frame
            iframe_frame = await iframe_element.content_frame()
            if not iframe_frame:
                raise RuntimeError("Could not get iframe content frame")
            
            # Wait for login form elements inside the iframe
            form_loaded = False
            max_attempts = 3
            
            for attempt in range(max_attempts):
                all_inputs = await iframe_frame.query_selector_all('input')
                if len(all_inputs) > 0:
                    form_loaded = True
                    break
                await asyncio.sleep(2)
            
            if not form_loaded:
                raise RuntimeError("Login form never loaded in iframe")
            
            # Wait for login form to load - Chase uses specific selectors
            username_selectors = [
                'input[name="userId"]',
                'input[id="userId"]', 
                '#userId',
                'input[type="text"][autocomplete="username"]',
                'input[type="text"]'
            ]
            
            password_selectors = [
                'input[name="password"]',
                'input[id="password"]',
                '#password',
                'input[type="password"]'
            ]
            
            # Fill login form
            
            # Wait for username field in iframe
            username_found = False
            for selector in username_selectors:
                try:
                    # Check if element exists in iframe
                    element = await iframe_frame.query_selector(selector)
                    if element:
                        await iframe_frame.fill(selector, username)
                        username_found = True
                        break
                except Exception:
                    continue
            
            if not username_found:
                raise RuntimeError("Username field not found in iframe")
            
            # Wait for password field in iframe
            password_found = False
            for selector in password_selectors:
                try:
                    # Check if element exists in iframe
                    element = await iframe_frame.query_selector(selector)
                    if element:
                        await iframe_frame.fill(selector, password)
                        password_found = True
                        break
                except Exception:
                    continue
            
            if not password_found:
                raise RuntimeError("Password field not found in iframe")
            
            # Click login button in iframe
            login_button_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Sign in")',
                'button:has-text("Log in")',
                '#logon-button',
                'button',  # fallback to any button
                'input[value*="Sign"]'  # fallback to input with "Sign" in value
            ]
            
            login_button_found = False
            for selector in login_button_selectors:
                try:
                    # Check if element exists in iframe
                    element = await iframe_frame.query_selector(selector)
                    if element:
                        await iframe_frame.click(selector)
                        login_button_found = True
                        break
                except Exception:
                    continue
            
            if not login_button_found:
                raise RuntimeError("Could not find login button in iframe")
            
            # Check if we're on the dashboard or need 2FA
            current_url = self.page.url
            self.log.debug(f"Current URL after login: {current_url}")
            
            # Check for 2FA or security questions
            await self.handle_2fa_if_needed()
            # Wait for dashboard URL
            try:
                self.log.info("Waiting for portfolio page to load...")
                await self.page.wait_for_url("**/dashboard/overview", timeout=5 * 60000)
            except TimeoutError:
                self.log.warning("Timed out waiting for dashboard URL")

            
            # Check if we're successfully logged in
            # If URL contains dashboard, we're likely logged in
            if "/dashboard/overview" in self.page.url:
                self.log.info("Login successful - reached dashboard URL!")
                # Save session for future use
                await self.save_session()
                return True
            else:
                raise RuntimeError(f"Login failed - reached URL: {self.page.url}, expected */dashboard/overview")

                
        except Exception as e:
            raise
    
    async def handle_2fa_if_needed(self) -> bool:
        """Handle 2FA if required"""
        # Check if we're on a 2FA page by looking for the specific text in page content and iframes
        is_2fa_required = False
        
        # Check main page content
        page_content = await self.page.content()
        if "we need to confirm your identity" in page_content.lower():
            is_2fa_required = True
        else:
            # Check all iframes
            frames = self.page.frames
            for frame in frames:
                try:
                    frame_content = await frame.content()
                    if "we need to confirm your identity" in frame_content.lower():
                        is_2fa_required = True
                        break
                except Exception:
                    # Skip frames that can't be accessed
                    continue
        
        if not is_2fa_required:
            # No 2FA required, we're good to go
            self.log.info("No 2FA required, we're good to go!")
            return True
        
        # 2FA is required
        self.log.warning("🔐 2FA Challenge detected!")
        self.log.info("📱 Please complete the 2FA challenge in your browser window.")
        self.log.info("⏳ Waiting for redirect to dashboard/overview...")
        
        # Wait for user to complete 2FA - page should redirect to dashboard/overview
        max_wait_time = 300  # 5 minutes
        wait_interval = 2
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            await asyncio.sleep(wait_interval)
            elapsed_time += wait_interval
            
            # Check if we've been redirected to dashboard/overview
            current_url = self.page.url
            if current_url.endswith('dashboard/overview'):
                self.log.info("✅ 2FA completed successfully!")
                return True
            
            # Show progress every 30 seconds
            if elapsed_time % 30 == 0:
                remaining = (max_wait_time - elapsed_time) // 60
                self.log.info(f"⏳ Still waiting for 2FA completion... ({remaining} minutes remaining)")
                self.log.debug(f"   Current URL: {current_url}")
        
        self.log.error(f"⏰ 2FA timeout after {max_wait_time//60} minutes")
        return False
    
    
    async def parse_portfolio_html(self, html: str) -> List[Holding]:
        """Parse Chase portfolio HTML to extract holdings"""
        self.log.info("Parsing HTML for all holdings...")
        
        soup = self.parse_html_with_soup(html)
        holdings = []
        
        # Look for the specific Chase portfolio table
        portfolio_table = soup.find('table', {'id': 'ssv-table', 'data-testid': 'ssv-table'})
        
        if not portfolio_table:
            # Save HTML for debugging
            debug_file = f"chase_debug_all_holdings.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            self.log.info(f"Saved HTML to {debug_file} for analysis")
            raise RuntimeError("Chase portfolio table not found")
        
        self.log.info("Found Chase portfolio table")
        
        # Find all position rows (exclude cash and totals rows)
        tbody = portfolio_table.find('tbody')
        if not tbody:
            raise RuntimeError("No tbody found in portfolio table")
        
        position_rows = tbody.find_all('tr', {'data-testid': lambda x: x and x.startswith('position-')})
        self.log.info(f"Found {len(position_rows)} position rows")
        
        for row in position_rows:
            try:
                holding = self._parse_position_row(row)
                if holding:
                    holdings.append(holding)
            except Exception as e:
                self.log.error(f"Error parsing row: {e}")
        
        # Look for cash positions
        cash_rows = tbody.find_all('tr')
        for row in cash_rows:
            try:
                # Check if this is a cash row by looking for the cash link
                cash_link = row.find('a', {'data-testid': 'cash-and-sweep-link'})
                if cash_link:
                    cash_holding = self._parse_cash_row(row)
                    if cash_holding:
                        holdings.append(cash_holding)
                        self.log.info("Found and parsed cash position")
            except Exception as e:
                self.log.error(f"Error parsing cash row: {e}")
        
        self.log.info(f"Successfully parsed {len(holdings)} holdings")
        self.sanity_check(soup, holdings)


        return holdings
    
    def _parse_position_row(self, row) -> Holding:
        """Parse a single position row from Chase portfolio table"""
        cells = row.find_all('td')
        if len(cells) < 10:
            self.log.warning(f"Row has insufficient cells: {len(cells)}")
            return None
        
        try:
            # Extract symbol from first cell
            symbol_cell = cells[0]
            symbol_link = symbol_cell.find('a', {'data-testid': lambda x: x and x.startswith('symbol-position-')})
            symbol = symbol_link.get_text(strip=True)
            
            # Extract description from second cell
            description = cells[1].get_text(strip=True) if cells[1] else ""
            
            # Extract price from third cell (extract just the price from complex text)
            price_cell = cells[2]
            price_div = price_cell.find('div', {'data-testid': lambda x: x and x.startswith('price-position-')})
            if price_div:
                price_text = price_div.get_text(strip=True)
                # Extract just the first number from complex text like '121.61Loss of -0.51-0.51Loss of -0.42%-0.42%'
                price = self._extract_first_price(price_text)
            else:
                raise ValueError(f"Price cell not found in row for symbol {symbol}")
            
            # Extract market value from fourth cell
            market_value_text = cells[3].get_text(strip=True)
            market_value = self._clean_decimal_text(market_value_text)
            current_value = market_value  # Same as market value
            
            # Extract day's gain/loss from fifth cell (Day's gain/loss $)
            day_change_dollars_text = cells[4].get_text(strip=True)
            try:
                day_change_dollars = self._clean_decimal_text(day_change_dollars_text)
            except ValueError:
                # On non-trading days, Chase show change as empty cell
                day_change_dollars = 0.0
            
            # Calculate day change percent from day_change_dollars / current_value
            if current_value != 0:
                day_change_percent = day_change_dollars / current_value
            else:
                raise ValueError(f"Cannot calculate day change percent: current_value is zero for {symbol}")
            
            # Extract unrealized gain/loss from sixth cell (Total gain/loss $)
            unrealized_gain_loss_text = cells[5].get_text(strip=True)
            unrealized_gain_loss = self._clean_decimal_text(unrealized_gain_loss_text)
            
            # Extract unrealized gain/loss percent from seventh cell (Total gain/loss %)
            unrealized_gain_loss_percent_text = cells[6].get_text(strip=True)
            unrealized_gain_loss_percent = self._clean_percentage_text(unrealized_gain_loss_percent_text)
            
            # Extract quantity from eighth cell
            quantity_text = cells[7].get_text(strip=True)
            quantity = self._clean_decimal_text(quantity_text)
            
            # Extract cost basis from ninth cell
            cost_cell = cells[8]
            cost_text = cost_cell.get_text(strip=True)
            cost_basis = self._clean_decimal_text(cost_text)
            
            # Calculate unit cost from cost basis and quantity
            if quantity != 0:
                unit_cost = abs(cost_basis / quantity)  # Use abs to handle negative quantities
            else:
                raise ValueError(f"Cannot calculate unit cost: quantity is zero for {symbol}")
            
            # Create holding object
            holding = Holding(
                symbol=symbol,
                description=description,
                quantity=quantity,
                price=price,
                cost_basis=cost_basis,
                unit_cost=unit_cost,
                current_value=current_value,
                day_change_percent=day_change_percent,
                day_change_dollars=day_change_dollars,
                unrealized_gain_loss=unrealized_gain_loss,
                unrealized_gain_loss_percent=unrealized_gain_loss_percent,
                brokers={self.broker_name: current_value}
            )
            
            self.log.debug(f"Parsed holding: {symbol} - {quantity} shares @ ${price} = ${market_value}")
            return holding
            
        except Exception as e:
            self.log.error(f"Error parsing position row: {e}")
            return None
    
    def _parse_cash_row(self, row) -> Holding:
        """Parse a cash position row from Chase portfolio table"""
        cells = row.find_all('td')
        if len(cells) < 9:
            self.log.warning(f"Cash row has insufficient cells: {len(cells)}")
            return None
        
        try:
            # Cash symbol is always USD_CASH
            symbol = "USD_CASH"
            
            # Description is "Cash & sweep funds"
            description = "Cash & sweep funds"
            
            # For cash positions:
            # - quantity = cash amount (same as current value)
            # - price = 1.00 (cash is always $1 per unit)
            # - unit_cost = 1.00 (cash cost basis is always $1 per unit)
            # - cost_basis = cash amount (same as current value)
            # - current_value = cash amount
            # - day_change = 0 (cash doesn't change daily)
            # - unrealized_gain_loss = 0 (cash has no gain/loss)
            
            # Extract cash value from the 4th cell (index 3) based on the HTML structure
            cash_value_text = cells[3].get_text(strip=True)
            cash_amount = self._clean_decimal_text(cash_value_text)
            
            # Create cash holding
            holding = Holding(
                symbol=symbol,
                description=description,
                quantity=cash_amount,  # Cash quantity equals the dollar amount
                price=1.00,  # Cash price is always $1
                cost_basis=cash_amount,  # Cost basis equals current value for cash
                unit_cost=1.00,  # Unit cost is always $1 for cash
                current_value=cash_amount,
                day_change_percent=0.00,  # Cash doesn't have daily changes
                day_change_dollars=0.00,  # Cash doesn't have daily changes
                unrealized_gain_loss=0.00,  # Cash has no unrealized gain/loss
                unrealized_gain_loss_percent=0.00,  # Cash has no unrealized gain/loss
                brokers={self.broker_name: cash_amount}
            )
            
            self.log.debug(f"Parsed cash holding: {symbol} - ${cash_amount}")
            return holding
            
        except Exception as e:
            self.log.error(f"Error parsing cash row: {e}")
            return None
    
    def _clean_decimal_text(self, value_str: str) -> float:
        """Clean text and extract decimal value, handling Chase-specific formatting"""
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
        
        # Extract only the numeric part (handle cases like "63.41Loss" or "63.41Gain")
        # Look for the first decimal number in the string
        number_match = re.search(r'-?\d+\.?\d*', cleaned)
        if number_match:
            number_str = number_match.group()
            try:
                result = float(number_str)
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert '{number_str}' to float: {e}")
        
        raise ValueError(f"No valid number found in text: '{value_str}'")
    
    def _extract_first_price(self, price_text: str) -> float:
        """Extract the first price from complex text like '121.61Loss of -0.51-0.51Loss of -0.42%-0.42%'"""
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
    
    def sanity_check(self, soup: BeautifulSoup, holdings: List[Holding]) -> None:
        """Compare reported totals on the page with parsed holdings totals."""
        TOTAL_CHECK_TOLERANCE = 0.01
        total_row_data = self._parse_total_row(soup)

        reported_total_value = total_row_data['current_value']
        reported_unrealized_gain = total_row_data['unrealized_gain_loss']

        computed_total_value = sum(h.current_value for h in holdings)
        computed_unrealized_gain = sum(h.unrealized_gain_loss for h in holdings)

        value_diff = abs(computed_total_value - reported_total_value)
        if value_diff / computed_total_value > TOTAL_CHECK_TOLERANCE:
            raise RuntimeError(
                f"Total value mismatch: holdings {computed_total_value:.2f} vs reported {reported_total_value:.2f}"
            )

        unrealized_diff = abs(computed_unrealized_gain - reported_unrealized_gain)
        # Use a slightly larger tolerance for unrealized gain due to potential rounding differences
        if unrealized_diff / computed_unrealized_gain > TOTAL_CHECK_TOLERANCE:
            raise RuntimeError(
                f"Unrealized gain mismatch: holdings {computed_unrealized_gain:.2f} vs reported {reported_unrealized_gain:.2f}. This might be due to rounding."
            )

        self.log.info(
            "Sanity check passed: holdings total %s matches reported %s",
            computed_total_value,
            reported_total_value,
        )

    def _parse_total_row(self, soup: BeautifulSoup) -> Optional[Dict[str, float]]:
        """Parse the totals row from the Chase portfolio table."""
        try:
            totals_row = soup.find('tr', {'data-testid': 'position-totals-row'})
            if not totals_row:
                raise RuntimeError("Could not find totals row in Chase portfolio")

            cells = totals_row.find_all('td')
            if len(cells) < 7:
                raise RuntimeError("Could not find totals row in Chase portfolio")

            # Total Market Value is in the 4th cell (index 3)
            total_value_text = cells[3].get_text(strip=True)
            reported_total_value = self._clean_decimal_text(total_value_text)

            # Total Unrealized Gain/Loss is in the 6th cell (index 5)
            unrealized_gain_loss_text = cells[5].get_text(strip=True)
            reported_unrealized_gain = self._clean_decimal_text(unrealized_gain_loss_text)

            return {
                'current_value': reported_total_value,
                'unrealized_gain_loss': reported_unrealized_gain,
            }
        except Exception as e:
            raise RuntimeError(f"Error parsing totals row: {e}") from e

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
