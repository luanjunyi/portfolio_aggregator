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
        
        # Login (navigation is handled inside login method)
        if not await self.login():
            print(f"[{self.broker_name}] Login failed")
            return []
        
        print(f"[{self.broker_name}] Navigating to portfolio page...")
        
        # Navigate to the portfolio page that shows all holdings
        await self.page.goto(self.portfolio_url, wait_until='networkidle')
        await asyncio.sleep(5)
        
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
            await self.page.goto(self.login_url, wait_until='networkidle')
            current_url = self.page.url
            
            # If we're already on the main page, we're logged in
            if "TFPHoldings" in current_url.lower():
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
            
            # Wait for the main content area to be populated
            await self.page.wait_for_selector('#mainContent', timeout=30000)
            
            # Wait a bit more for JavaScript to render the form
            await asyncio.sleep(5)

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
            
            # Wait for navigation after login
            await asyncio.sleep(5)
            
            # Check for 2FA or security questions
            await self.handle_2fa_if_needed()
            
            # Wait for main page URL
            try:
                print(f"[{self.broker_name}] Waiting for main page to load...")
                await self.page.wait_for_url("**/main**", timeout=5 * 60000)
            except Exception:
                print(f"[{self.broker_name}] Timed out waiting for main page URL")
            
            # Check if we're successfully logged in
            current_url = self.page.url
            if "main" in current_url.lower() or "dashboard" in current_url.lower():
                print(f"[{self.broker_name}] Login successful - reached main page!")
                # Save session for future use
                await self.save_session()
                return True
            else:
                print(f"[{self.broker_name}] Login failed - reached URL: {current_url}, expected main page")
                return False
                
        except Exception as e:
            print(f"[{self.broker_name}] Login failed: {e}")
            return False
    
    async def handle_2fa_if_needed(self) -> bool:
        """Handle 2FA if required"""
        # Check if we're on a 2FA page
        is_2fa_required = False
        
        # Check main page content for 2FA indicators
        page_content = await self.page.content()
        if any(phrase in page_content.lower() for phrase in [
            "verification code",
            "security code",
            "two-factor",
            "2fa",
            "authenticate",
            "verify your identity"
        ]):
            is_2fa_required = True
        
        if not is_2fa_required:
            # No 2FA required, we're good to go
            print(f"[{self.broker_name}] No 2FA required, we're good to go!")
            return True
        
        # 2FA is required
        print(f"[{self.broker_name}] 2FA detected! Please complete authentication in the browser.")
        print(f"[{self.broker_name}] Waiting for you to complete 2FA...")
        
        # Wait for user to complete 2FA (up to 10 minutes)
        max_wait_time = 10 * 60  # 10 minutes
        start_time = asyncio.get_event_loop().time()
        
        while True:
            await asyncio.sleep(5)
            current_time = asyncio.get_event_loop().time()
            elapsed_time = int(current_time - start_time)
            
            # Check if we've moved past the 2FA page
            current_url = self.page.url
            page_content = await self.page.content()
            
            # If we're on main page or no longer see 2FA content, we're done
            if ("main" in current_url.lower() or 
                not any(phrase in page_content.lower() for phrase in [
                    "verification code", "security code", "two-factor", "2fa"
                ])):
                print(f"[{self.broker_name}] 2FA completed successfully!")
                return True
            
            # Check timeout
            if elapsed_time >= max_wait_time:
                break
            
            # Show progress every 30 seconds
            if elapsed_time % 30 == 0:
                remaining = (max_wait_time - elapsed_time) // 60
                print(f"⏳ Still waiting for 2FA completion... ({remaining} minutes remaining)")
                print(f"   Current URL: {current_url}")
        
        print(f"⏰ [{self.broker_name}] 2FA timeout after {max_wait_time//60} minutes")
        return False
    
    async def parse_portfolio_html(self, html: str) -> List[Holding]:
        """Parse Merrill Edge portfolio HTML to extract holdings"""
        print(f"[{self.broker_name}] Parsing HTML for all holdings...")
        
        soup = self.parse_html_with_soup(html)
        holdings = []
        
        # Look for Merrill Edge portfolio table (will need to be updated based on actual HTML structure)
        # This is a placeholder - we'll need to inspect the actual HTML to find the right selectors
        portfolio_table = soup.find('table', {'class': 'holdings-table'}) or \
                         soup.find('table', {'id': 'holdings'}) or \
                         soup.find('table')  # fallback to any table
        
        if not portfolio_table:
            print(f"[{self.broker_name}] Merrill portfolio table not found")
            # Save HTML for debugging
            debug_file = f"merrill_debug_all_holdings.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"[{self.broker_name}] Saved HTML to {debug_file} for analysis")
            return holdings
        
        print(f"[{self.broker_name}] Found Merrill portfolio table")
        
        # Find all position rows (will need to be updated based on actual HTML structure)
        tbody = portfolio_table.find('tbody')
        if not tbody:
            print(f"[{self.broker_name}] No tbody found in portfolio table")
            return holdings
        
        # Look for data rows (placeholder selectors - need to be updated)
        position_rows = tbody.find_all('tr')
        print(f"[{self.broker_name}] Found {len(position_rows)} rows")
        
        for row in position_rows:
            try:
                holding = self._parse_position_row(row)
                if holding:
                    holdings.append(holding)
            except Exception as e:
                print(f"[{self.broker_name}] Error parsing row: {e}")
        
        print(f"[{self.broker_name}] Successfully parsed {len(holdings)} holdings")
        return holdings
    
    def _parse_position_row(self, row) -> Holding:
        """Parse a single position row from Merrill portfolio table"""
        cells = row.find_all('td')
        if len(cells) < 5:  # Minimum expected columns
            print(f"[{self.broker_name}] Row has insufficient cells: {len(cells)}")
            return None
        
        try:
            # NOTE: These are placeholder column mappings
            # Will need to be updated based on actual Merrill Edge HTML structure
            
            # Extract symbol (placeholder - column 0)
            symbol = cells[0].get_text(strip=True) if cells[0] else "UNKNOWN"
            
            # Extract description (placeholder - column 1)
            description = cells[1].get_text(strip=True) if cells[1] else ""
            
            # Extract quantity (placeholder - column 2)
            quantity_text = cells[2].get_text(strip=True)
            quantity = self._clean_decimal_text(quantity_text)
            
            # Extract price (placeholder - column 3)
            price_text = cells[3].get_text(strip=True)
            price = self._clean_decimal_text(price_text)
            
            # Extract market value (placeholder - column 4)
            market_value_text = cells[4].get_text(strip=True)
            market_value = self._clean_decimal_text(market_value_text)
            current_value = market_value
            
            # Calculate unit cost from market value and quantity
            if quantity != 0:
                unit_cost = abs(market_value / quantity)
            else:
                raise ValueError(f"Cannot calculate unit cost: quantity is zero for {symbol}")
            
            # Placeholder values for fields that might not be available initially
            cost_basis = market_value  # Will need to find actual cost basis
            day_change_dollars = Decimal('0')  # Will need to find actual day change
            day_change_percent = Decimal('0')  # Will need to calculate
            unrealized_gain_loss = Decimal('0')  # Will need to find actual G/L
            unrealized_gain_loss_percent = Decimal('0')  # Will need to calculate
            
            # Create holding object
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
                unrealized_gain_loss_percent=unrealized_gain_loss_percent
            )
            
            print(f"[{self.broker_name}] Parsed holding: {symbol} - {quantity} shares @ ${price} = ${market_value}")
            return holding
            
        except Exception as e:
            print(f"[{self.broker_name}] Error parsing position row: {e}")
            return None
    
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
        number_match = re.search(r'-?\d+\.?\d*', cleaned)
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
