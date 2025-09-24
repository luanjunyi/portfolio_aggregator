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
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self._setup_browser()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self._cleanup_browser()
    
    async def _setup_browser(self):
        """Initialize Playwright browser with enhanced stealth measures"""
        self.playwright = await async_playwright().start()
        
        # Enhanced browser launch with stealth measures
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
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
                '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            ]
        )
        
        # Create context with enhanced settings
        context_options = {
            'viewport': {'width': 768, 'height': 1024},
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'locale': 'en-US',
            'timezone_id': 'America/Los_Angeles',
            'extra_http_headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0'
            }
        }
        
        # Load stored session if available
        session_data = self.db_manager.get_session(self.broker_name)
        if session_data and session_data.get('session_data'):
            stored_session = session_data['session_data']
            if 'cookies' in stored_session and stored_session['cookies']:
                try:
                    # Check if session is still valid (not expired)
                    expires_at = session_data.get('expires_at')
                    if expires_at:
                        expiry_time = datetime.fromisoformat(expires_at)
                        if datetime.now() > expiry_time:
                            print(f"[{self.broker_name}] Stored session has expired, clearing...")
                            self.db_manager.clear_session(self.broker_name)
                        else:
                            context_options['storage_state'] = stored_session
                            print(f"[{self.broker_name}] Loaded stored session with {len(stored_session['cookies'])} cookies")
                    else:
                        context_options['storage_state'] = stored_session
                        print(f"[{self.broker_name}] Loaded stored session with {len(stored_session['cookies'])} cookies")
                except Exception as e:
                    print(f"[{self.broker_name}] Failed to load session: {e}")
                    self.db_manager.clear_session(self.broker_name)
        
        self.context = await self.browser.new_context(**context_options)
        self.page = await self.context.new_page()
        
        # Enhanced stealth measures
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
        
        # Set up request/response logging (disabled for cleaner output)
        # self.page.on('request', self._log_request)
        # self.page.on('response', self._log_response)
    
    async def _cleanup_browser(self):
        """Clean up browser resources"""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()
    
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
