from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import asyncio
import json
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.portfolio import Holding, CrawlerResult
from storage.database import DatabaseManager


class BaseCrawler(ABC):
    """Base class for all broker crawlers"""
    
    def __init__(self, broker_name: str, headless: bool = True):
        self.broker_name = broker_name
        self.headless = headless
        self.db_manager = DatabaseManager()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.attached_to_remote = False
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self._setup_browser()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
    
    async def _setup_browser(self):
        """Initialize Playwright browser with enhanced stealth measures"""
        self.playwright = await async_playwright().start()
        
        # Optionally attach to an existing Chrome instance via CDP
        cdp_url = os.environ.get('CHROME_REMOTE_DEBUG_URL') or os.environ.get('PLAYWRIGHT_CDP_URL')
        if cdp_url:
            print(f"Attaching to existing Chrome over CDP at {cdp_url}")
            self.attached_to_remote = True
            self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url)
            if self.browser.contexts:
                self.context = self.browser.contexts[0]
            else:
                self.context = await self.browser.new_context()
            if self.context.pages:
                self.page = self.context.pages[0]
            else:
                self.page = await self.context.new_page()
            await self.page.bring_to_front()
            await self._apply_stealth_scripts()
            return

        # Detect local Chrome installation
        chrome_paths = [
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            '/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta',
            '/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary',
            '/usr/bin/google-chrome',
            '/usr/bin/chrome'
        ]
        chrome_executable = next((path for path in chrome_paths if os.path.exists(path)), None)
        if not chrome_executable:
            raise RuntimeError("Unable to locate a Chrome executable. Please install Google Chrome.")

        # Resolve the user's Chrome profile directory
        default_profile_root_candidates = [
            os.path.expanduser('~/Library/Application Support/Google/Chrome'),  # macOS
            os.path.expanduser('~/Library/Application Support/Chrome'),         # Alternate macOS
            os.path.expanduser('~/.config/google-chrome'),                      # Linux
            os.path.expanduser('~/.config/chrome')                              # Alternate Linux
        ]
        chrome_user_data_dir = next((path for path in default_profile_root_candidates if os.path.exists(path)), None)
        if not chrome_user_data_dir:
            raise RuntimeError("Unable to locate Chrome user data directory.")

        profile_name = os.environ.get('CHROME_PROFILE_NAME', 'Default')
        profile_path = os.path.join(chrome_user_data_dir, profile_name)
        if not os.path.exists(profile_path):
            raise RuntimeError(f"Chrome profile '{profile_name}' not found at {profile_path}.")

        singleton_lock = os.path.join(chrome_user_data_dir, 'SingletonLock')
        if os.path.exists(singleton_lock):
            raise RuntimeError(
                "Chrome appears to be running with this profile. Please close all Chrome windows before running the crawler."
            )

        print(f"Using local Chrome: {chrome_executable}")
        print(f"Using Chrome profile: {profile_path}")

        # Launch persistent context so we inherit the real profile (extensions, cookies, etc.)
        self.context = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=chrome_user_data_dir,
            executable_path=chrome_executable,
            headless=self.headless,
            args=[
                f'--profile-directory={profile_name}',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=VizDisplayCompositor',
                '--disable-web-security',
                '--disable-features=TranslateUI',
                '--disable-iframes-during-prerender',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-component-extensions-with-background-pages',
                '--no-default-browser-check',
                '--no-first-run',
                '--disable-default-apps',
                '--remote-debugging-port=0',
                '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            ],
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/Los_Angeles',
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1'
            }
        )

        # Keep browser handle for compatibility with existing code
        self.browser = self.context.browser

        # Reuse existing page if one already exists (Chrome restores tabs); otherwise create one
        if self.context.pages:
            self.page = self.context.pages[0]
            await self.page.bring_to_front()
        else:
            self.page = await self.context.new_page()

        await self._apply_stealth_scripts()
        
        # Set up request/response logging (disabled for cleaner output)
        # self.page.on('request', self._log_request)
        # self.page.on('response', self._log_response)
    
    async def _cleanup_browser(self):
        """Clean up browser resources"""
        if self.attached_to_remote:
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
            return

        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()

    async def _apply_stealth_scripts(self):
        """Inject scripts to reduce automation detection."""
        if not self.page:
            return

        await self.page.add_init_script("""
            // Remove webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Mock chrome object
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
            };
            
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: Plugin},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    }
                ],
            });
            
            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            
            // Mock hardware concurrency
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8,
            });
            
            // Mock device memory
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8,
            });
        """)
    
    def _log_request(self, request):
        """Log outgoing requests for debugging"""
        print(f"[{self.broker_name}] Request: {request.method} {request.url}")
    
    def _log_response(self, response):
        """Log responses for debugging"""
        if response.status >= 400:
            print(f"[{self.broker_name}] Error Response: {response.status} {response.url}")
    
    async def save_session(self):
        """Save current browser session for future use"""
        try:
            if self.context:
                storage_state = await self.context.storage_state()
                expires_at = datetime.now() + timedelta(days=7)  # Sessions expire in 7 days
                
                self.db_manager.store_session(
                    self.broker_name,
                    storage_state,
                    expires_at.isoformat()
                )
                print(f"[{self.broker_name}] Session saved successfully")
        except Exception as e:
            print(f"[{self.broker_name}] Failed to save session: {e}")
    
    async def handle_2fa_prompt(self, prompt_text: str = "Enter 2FA code") -> str:
        """Handle 2FA input from user"""
        print(f"\n[{self.broker_name}] 2FA Required")
        print(f"Prompt: {prompt_text}")
        
        # In a real implementation, this could be a web interface
        # For now, we'll use console input
        code = input("Enter 2FA code: ").strip()
        return code
    
    async def wait_for_manual_action(self, message: str, timeout: int = 300):
        """Wait for user to complete manual action (like 2FA)"""
        print(f"\n[{self.broker_name}] Manual Action Required")
        print(f"Message: {message}")
        print("Press Enter when completed...")
        
        # In a production app, this would be handled via the web interface
        input()
    
    def get_credentials(self) -> Optional[Dict[str, Any]]:
        """Get stored credentials for this broker"""
        return self.db_manager.get_credentials(self.broker_name)
    
    def store_credentials(self, username: str, password: str):
        """Store credentials for this broker"""
        self.db_manager.store_credentials(self.broker_name, username, password)
    
    async def navigate_with_retry(self, url: str, max_retries: int = 3) -> bool:
        """Navigate to URL with retry logic"""
        for attempt in range(max_retries):
            try:
                await self.page.goto(url, wait_until='networkidle', timeout=30000)
                return True
            except Exception as e:
                print(f"[{self.broker_name}] Navigation attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return False
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return False
    
    async def wait_for_element(self, selector: str, timeout: int = 10000) -> bool:
        """Wait for element to appear"""
        try:
            await self.page.wait_for_selector(selector, timeout=timeout)
            return True
        except Exception:
            return False
    
    def parse_html_with_soup(self, html: str) -> BeautifulSoup:
        """Parse HTML content with BeautifulSoup"""
        return BeautifulSoup(html, 'lxml')
    
    async def human_type(self, selector: str, text: str, delay_range: tuple = (50, 150)):
        """Type text in a human-like manner to bypass detection"""
        import random
        
        element = await self.page.query_selector(selector)
        if not element:
            raise Exception(f"Element not found: {selector}")
        
        # Clear the field first by selecting all and deleting
        await element.click()
        await self.page.keyboard.press('Meta+a')  # Cmd+A on Mac
        await asyncio.sleep(random.uniform(0.1, 0.3))
        
        # Type each character with human-like delays
        for char in text:
            await self.page.keyboard.type(char)
            # Random delay between keystrokes (50-150ms)
            delay = random.uniform(delay_range[0], delay_range[1]) / 1000
            await asyncio.sleep(delay)
        
        # Small pause after typing
        await asyncio.sleep(random.uniform(0.2, 0.5))
    
    async def human_click(self, selector: str):
        """Click in a human-like manner with random delays"""
        import random
        
        # Small delay before clicking
        await asyncio.sleep(random.uniform(0.1, 0.3))
        
        element = await self.page.query_selector(selector)
        if not element:
            raise Exception(f"Element not found: {selector}")
        
        # Get element bounds for more realistic clicking
        box = await element.bounding_box()
        if box:
            # Click at a random point within the element (not center)
            x = box['x'] + random.uniform(0.2, 0.8) * box['width']
            y = box['y'] + random.uniform(0.2, 0.8) * box['height']
            await self.page.mouse.click(x, y)
        else:
            # Fallback to regular click
            await element.click()
        
        # Small delay after clicking
        await asyncio.sleep(random.uniform(0.1, 0.3))
    
    async def simulate_human_behavior(self):
        """Simulate human-like behavior before interacting with forms"""
        import random
        
        # Random mouse movements
        for _ in range(random.randint(2, 4)):
            x = random.randint(100, 700)
            y = random.randint(100, 600)
            await self.page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
        
        # Random scroll
        await self.page.mouse.wheel(0, random.randint(-200, 200))
        await asyncio.sleep(random.uniform(0.2, 0.5))
    
    @abstractmethod
    async def login(self) -> bool:
        """Login to the broker website. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    async def scrape_portfolio(self) -> List[Holding]:
        """Scrape portfolio data. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def get_login_url(self) -> str:
        """Get the login URL for this broker. Must be implemented by subclasses."""
        pass
    
    async def crawl(self) -> CrawlerResult:
        """Main crawling method that orchestrates the entire process"""
        try:
            print(f"[{self.broker_name}] Starting crawl...")
            
            # Attempt login (login method handles navigation)
            login_success = await self.login()
            if not login_success:
                return CrawlerResult(
                    broker=self.broker_name,
                    success=False,
                    error_message="Login failed",
                    requires_2fa=True  # Assume 2FA if login fails
                )
            
            # Save session after successful login
            await self.save_session()
            
            # Scrape portfolio data
            holdings = await self.scrape_portfolio()
            
            print(f"[{self.broker_name}] Successfully scraped {len(holdings)} holdings")
            
            return CrawlerResult(
                broker=self.broker_name,
                success=True,
                holdings=holdings
            )
            
        except Exception as e:
            print(f"[{self.broker_name}] Crawl failed with error: {e}")
            return CrawlerResult(
                broker=self.broker_name,
                success=False,
                error_message=str(e)
            )
