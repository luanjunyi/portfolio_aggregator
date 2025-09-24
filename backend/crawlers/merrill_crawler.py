from typing import List, Dict, Any
from decimal import Decimal
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
    
    def __init__(self, headless: bool = True):
        super().__init__("merrill_edge", headless)
        self.login_url = "https://olui2.fs.ml.com/login/signin.aspx"
        self.portfolio_url = "https://olui2.fs.ml.com/TFPHoldings/HoldingsByAccount.aspx"
    
    def get_login_url(self) -> str:
        """Get Merrill login URL"""
        return self.login_url
    
    async def scrape_portfolio(self) -> List[Holding]:
        """Scrape holdings from Merrill Edge"""
        print(f"[{self.broker_name}] Starting Merrill holdings scrape...")      
        print(f"[{self.broker_name}] Navigating to portfolio page...")
        
        # Navigate to the portfolio page that shows all holdings
        await self.page.goto(self.portfolio_url, wait_until='networkidle')
        
        # Get page HTML
        html = await self.page.content()
        
        # Parse holdings from HTML
        holdings = await self.parse_portfolio_html(html)
        
        print(f"[{self.broker_name}] Found {len(holdings)} total holdings")
        
        return holdings
    
    async def login(self) -> bool:
        """Login to Merrill Edge"""
        print(f"[{self.broker_name}] Starting Merrill login...")
        
        # First check if we're already logged in with a valid session
        try:
            await self.page.goto(self.login_url, wait_until='domcontentloaded')
            # Give the browser a chance to follow any automatic redirect
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            await asyncio.sleep(2)

            current_url = self.page.url
            if "tfpholdings" in current_url.lower():
                print(f"[{self.broker_name}] Already logged in with stored session!")
                return True
        except Exception:
            pass  # Continue with normal login if session check fails
        
        # Get stored credentials
        credentials = self.get_credentials()
        if not credentials:
            print(f"[{self.broker_name}] No credentials found. Please add credentials first.")
            return False
        
        username = credentials['username']
        password = credentials['password']
        
        try:
            # Wait for page to fully load
            current_url = self.page.url
            print(f"[{self.broker_name}] Current URL: {current_url}")
            
            # Wait for the SPA to load the login form dynamically
            print(f"[{self.broker_name}] Waiting for login form to load...")
            
            
            # Wait a bit more for JavaScript to render the form
            await asyncio.sleep(5)

            if "/TFPHoldings/" in self.page.url.lower():
                print(f"[{self.broker_name}] Already logged in with stored session!")
                return True

            await self.page.mouse.move(100, 100)
            
            # Check if we need to wait for specific login form elements
            login_form_loaded = False
            max_wait_attempts = 10
            
            for attempt in range(max_wait_attempts):
                print(f"[{self.broker_name}] Checking for login form (attempt {attempt + 1}/{max_wait_attempts})...")
                
                # Look for any input fields that might be username/password
                all_inputs = await self.page.query_selector_all('input')
                text_inputs = []
                password_inputs = []
                
                for input_elem in all_inputs:
                    input_type = await input_elem.get_attribute('type')
                    input_name = await input_elem.get_attribute('name')
                    input_id = await input_elem.get_attribute('id')
                    input_placeholder = await input_elem.get_attribute('placeholder')
                    
                    print(f"[{self.broker_name}] Found input: type={input_type}, name={input_name}, id={input_id}, placeholder={input_placeholder}")
                    
                    if input_type == 'text' or input_type == 'email':
                        text_inputs.append(input_elem)
                    elif input_type == 'password':
                        password_inputs.append(input_elem)
                
                if len(text_inputs) > 0 and len(password_inputs) > 0:
                    login_form_loaded = True
                    print(f"[{self.broker_name}] Login form loaded! Found {len(text_inputs)} text inputs and {len(password_inputs)} password inputs")
                    break
                
                await asyncio.sleep(3)
            
            if not login_form_loaded:
                print(f"[{self.broker_name}] Login form never loaded after {max_wait_attempts} attempts")
                # Save HTML for debugging
                debug_html = await self.page.content()
                with open(f"merrill_debug_no_form.html", 'w', encoding='utf-8') as f:
                    f.write(debug_html)
                print(f"[{self.broker_name}] Saved HTML to merrill_debug_no_form.html for analysis")
                return False
            
            # Simulate human behavior before interacting with form
            await self.simulate_human_behavior()
            
            # Look for username field
            username_selectors = [
                '#userid',
                'input[name="userid"]',
                'input[type="text"]',
                '#username',
                'input[name="username"]'
            ]
            
            username_found = False
            for selector in username_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        await self.page.fill(selector, username)
                        username_found = True
                        print(f"[{self.broker_name}] Found username field: {selector}")
                        break
                except Exception as err:
                    print(f"[{self.broker_name}] Error filling username field: {err}")
                    continue
            
            if not username_found:
                print(f"[{self.broker_name}] Username field not found")
                return False
            
            # Look for password field
            password_selectors = [
                '#password',
                'input[name="password"]',
                'input[type="password"]',
                '#passwd'
            ]
            
            password_found = False
            for selector in password_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        await self.page.fill(selector, password)
                        password_found = True
                        print(f"[{self.broker_name}] Found password field: {selector}")
                        break
                except Exception as err:
                    print(f"[{self.broker_name}] Error filling password field: {err}")
                    continue
            
            if not password_found:
                print(f"[{self.broker_name}] Password field not found")
                return False
            
            # Click login button
            login_button_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Sign In")',
                'button:has-text("Log In")',
                'input[value*="Sign"]',
                '#signin-btn',
                '.signin-btn',
                'button'  # fallback
            ]
            
            login_button_found = False
            for selector in login_button_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        await self.page.click(selector, delay=random.randint(100, 200))
                        login_button_found = True
                        print(f"[{self.broker_name}] Clicked login button: {selector}")
                        break
                except Exception as err:
                    print(f"[{self.broker_name}] Error clicking login button: {err}")
                    continue
            
            if not login_button_found:
                print(f"[{self.broker_name}] Could not find login button")
                return False
            
            # Check for 2FA or security questions
            await self.handle_2fa_if_needed()
            
            return True
                
        except Exception as e:
            print(f"[{self.broker_name}] Login failed: {e}")
            return False
    
    async def handle_2fa_if_needed(self) -> bool:
        """Handle 2FA if required"""
        return False
    
    async def parse_portfolio_html(self, html: str) -> List[Holding]:
        """Parse Merrill Edge portfolio HTML to extract holdings"""
        print(f"[{self.broker_name}] Parsing HTML for all holdings...")
        
        soup = self.parse_html_with_soup(html)
        holdings = []
        
        tables = soup.find_all('table', id=re.compile(r'^CustomGrid_'))

        if not tables:
            print(f"[{self.broker_name}] Merrill holdings tables not found")
            debug_file = "merrill_debug_all_holdings.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"[{self.broker_name}] Saved HTML to {debug_file} for analysis")
            return []

        print(f"[{self.broker_name}] Found {len(tables)} holdings table(s)")

        for table in tables:
            tbody = table.find('tbody')
            if not tbody:
                continue

            rows = tbody.find_all('tr')
            for row in rows:
                first_cell = row.find('td')
                if first_cell:
                    symbol_preview = first_cell.get_text(strip=True).lower()
                    if 'balances' in symbol_preview:
                        print(f"[{self.broker_name}] Reached balances section, stopping row parsing")
                        break
                try:
                    holding = self._parse_position_row(row)
                    if holding:
                        holdings.append(holding)
                except Exception as e:
                    print(f"[{self.broker_name}] Error parsing row: {e}")
            print(f"found {len(holdings)} holdings from table")

        # Combine holdings by symbol
        combined_holdings = self._combine_holdings_by_symbol(holdings)
        
        print(f"[{self.broker_name}] Successfully parsed {len(holdings)} individual holdings")
        print(f"[{self.broker_name}] Combined into {len(combined_holdings)} unique symbols")
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
                broker="merrill",
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
                portfolio_percentage=portfolio_percentage
            )

            print(f"[{self.broker_name}] Parsed holding: {symbol} - {quantity} @ ${price} (value ${current_value})")
            return holding

        except Exception as e:
            print(f"[{self.broker_name}] Error parsing position row: {e}")
            return None

    def _extract_dollar_change(self, cell) -> Decimal:
        target = cell.find('div', class_=lambda value: value and 'dol' in value.split())
        text = (target.get_text(strip=True) if target else cell.get_text(strip=True))
        if not text:
            return Decimal('0')
        return self._clean_decimal_text(text)

    def _extract_percentage_change(self, cell) -> Decimal:
        target = cell.find('div', class_=lambda value: value and 'per' in value.split())
        text = (target.get_text(strip=True) if target else cell.get_text(strip=True))
        if not text:
            return Decimal('0')
        return self._clean_percentage_text(text)
    
    def _clean_decimal_text(self, value_str: str) -> Decimal:
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
                result = Decimal(number_str)
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert '{number_str}' to Decimal: {e}")
        
        raise ValueError(f"No valid number found in text: '{value_str}'")
    
    def _extract_first_price(self, price_text: str) -> Decimal:
        """Extract the first price from complex text"""
        if not price_text:
            raise ValueError("Price text cannot be empty")
        
        # Look for the first decimal number at the beginning of the string
        number_match = re.match(r'^(\d+\.?\d*)', price_text.strip())
        if number_match:
            try:
                return Decimal(number_match.group(1))
            except Exception as e:
                raise ValueError(f"Failed to convert price '{number_match.group(1)}' to Decimal: {e}")
        
        raise ValueError(f"No valid price found at start of text: '{price_text}'")
    
    def _clean_percentage_text(self, value_str: str) -> Decimal:
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
                result = Decimal(number_str) / 100  # Convert percentage to decimal
                return -result if is_negative else result
            except Exception as e:
                raise ValueError(f"Failed to convert percentage '{number_str}' to Decimal: {e}")
        
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
            weighted_avg_price = Decimal('0')
            weighted_avg_unit_cost = Decimal('0')
        
        # Calculate percentages
        day_change_percent = Decimal('0')
        unrealized_gain_loss_percent = Decimal('0')
        
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
            broker=base_holding.broker,
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
            portfolio_percentage=portfolio_percentage
        )
        
        print(f"[{self.broker_name}] Combined {len(holdings)} holdings for {symbol}: {total_quantity} shares @ ${weighted_avg_price:.4f} = ${total_current_value}")
        return combined_holding
