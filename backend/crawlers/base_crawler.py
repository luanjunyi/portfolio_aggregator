from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import asyncio
import json
import socket
import contextlib
import urllib.request
import logging
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging with line numbers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

from models.portfolio import Holding, CrawlerResult
from storage.database import DatabaseManager

class BaseCrawler(ABC):
    """Base class for all broker crawlers"""
    
    def __init__(self, broker_name: str):
        self.broker_name = broker_name
        self.headless = False
        self.db_manager = DatabaseManager()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.attached_to_remote = False
        self.chrome_process: Optional[asyncio.subprocess.Process] = None
        self.chrome_remote_debug_url: Optional[str] = None
        self.created_page = False
        self.log = logging.getLogger(f"{self.__class__.__name__}[{broker_name}]")
        
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
        
        # Attempt to launch a dedicated automation Chrome instance
        try:
            cdp_url = await self._ensure_automation_chrome()
            self.log.info(f"Launching automation Chrome and attaching over CDP at {cdp_url}")
            await self._connect_over_cdp(cdp_url)
            return
        except Exception as exc:
            self.log.fatal(f"Failed to launch automation Chrome: {exc}")
            raise RuntimeError(f"Failed to launch automation Chrome: {exc}")
        
        # Set up request/response logging (disabled for cleaner output)
        # self.page.on('request', self._log_request)
        # self.page.on('response', self._log_response)
    
    async def _cleanup_browser(self):
        """Clean up browser resources"""
        if self.attached_to_remote:
            if self.chrome_process:
                if self.browser:
                    try:
                        await self.browser.close()
                    except Exception:
                        pass
                self.chrome_process.terminate()
                try:
                    await asyncio.wait_for(self.chrome_process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    self.chrome_process.kill()
                    await self.chrome_process.wait()
                self.chrome_process = None
            else:
                if self.created_page and self.page:
                    try:
                        await self.page.close()
                    except Exception:
                        pass
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

    async def _connect_over_cdp(self, cdp_url: str):
        """Attach to a Chrome instance via CDP."""
        self.attached_to_remote = True
        self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url)

        if self.browser.contexts:
            self.context = self.browser.contexts[0]
        else:
            self.context = await self.browser.new_context()
        if self.context.pages:
            self.page = self.context.pages[0]
            self.created_page = False
        else:
            self.page = await self.context.new_page()
            self.created_page = True
        if self.page:
            await self.page.bring_to_front()
        await self._apply_stealth_scripts()

    async def _ensure_automation_chrome(self) -> str:
        """Launch a dedicated Chrome instance for automation if not already running."""
        if self.chrome_remote_debug_url:
            return self.chrome_remote_debug_url

        chrome_executable = '/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta'
        if not os.path.exists(chrome_executable):
            self.log.fatal(f"Chrome executable not found at {chrome_executable}")
            raise RuntimeError("Unable to locate Chrome executable for automation. Set CHROME_AUTOMATION_EXECUTABLE to the Chrome Beta path.")

        user_data_dir = os.path.expanduser('~/Library/Application Support/Chrome-Automation')
        os.makedirs(user_data_dir, exist_ok=True)

        await self._terminate_existing_automation_chrome(user_data_dir)

        port = self._find_free_port()
        self.chrome_remote_debug_url = f"http://127.0.0.1:{port}/"


        launch_args = [
            chrome_executable,
            "--window-size=1024,1024",
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            '--no-first-run',
            '--no-default-browser-check',
            '--disable-background-networking',
            '--disable-component-extensions-with-background-pages',
            '--disable-background-timer-throttling',
            '--disable-renderer-backgrounding',
            '--disable-popup-blocking',
            '--disable-sync',
        ]

        if self.headless:
            launch_args.append('--headless=new')

        self.chrome_process = await asyncio.create_subprocess_exec(
            *launch_args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )

        await self._wait_for_cdp_ready(self.chrome_remote_debug_url)
        return self.chrome_remote_debug_url

    async def _terminate_existing_automation_chrome(self, user_data_dir: str):
        import subprocess

        try:
            ps_output = subprocess.check_output(['ps', '-ax', '-o', 'pid=,ppid=,command='], text=True)
        except Exception:
            return

        pids = []
        for line in ps_output.splitlines():
            try:
                pid_str, ppid_str, command = line.strip().split(None, 2)
                pid = int(pid_str)
                if user_data_dir in command and 'remote-debugging-port' in command:
                    pids.append(pid)
            except ValueError:
                continue

        if not pids:
            return

        self.log.info(f"Terminating {len(pids)} existing automation Chrome instance(s)")
        await self._terminate_processes(pids)

    async def _terminate_processes(self, pids: List[int]):
        import signal
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
        await asyncio.sleep(1)

    async def _wait_for_cdp_ready(self, cdp_url: str, timeout: float = 15.0):
        """Wait until the Chrome debugging endpoint responds."""
        deadline = asyncio.get_running_loop().time() + timeout
        probe_url = cdp_url.rstrip('/') + '/json/version'

        while True:
            if self.chrome_process and self.chrome_process.returncode is not None:
                self.log.fatal(f"Chrome process exited early with code {self.chrome_process.returncode}")
                raise RuntimeError(f"Chrome process exited early with code {self.chrome_process.returncode}")

            if await asyncio.to_thread(self._probe_cdp_endpoint, probe_url):
                return

            if asyncio.get_running_loop().time() > deadline:
                self.log.fatal(f"Timed out waiting for Chrome debugging endpoint at {cdp_url}")
                raise RuntimeError(f"Timed out waiting for Chrome debugging endpoint at {cdp_url}")

            await asyncio.sleep(0.2)

    def _probe_cdp_endpoint(self, probe_url: str) -> bool:
        try:
            with contextlib.closing(urllib.request.urlopen(probe_url, timeout=1)) as response:
                return response.status == 200
        except Exception:
            return False

    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]


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
        self.log.debug(f"Request: {request.method} {request.url}")
    
    def _log_response(self, response):
        """Log responses for debugging"""
        if response.status >= 400:
            self.log.error(f"Error Response: {response.status} {response.url}")
    
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
                self.log.info("Session saved successfully")
        except Exception as e:
            self.log.error(f"Failed to save session: {e}")
    
    
    def get_credentials(self) -> Optional[Dict[str, Any]]:
        """Get stored credentials for this broker"""
        return self.db_manager.get_credentials(self.broker_name)
    
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
            self.log.info("Starting crawl...")
            
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
            
            self.log.info(f"Successfully scraped {len(holdings)} holdings")
            
            return CrawlerResult(
                broker=self.broker_name,
                success=True,
                holdings=holdings
            )
            
        except Exception as e:
            self.log.fatal(f"Crawl failed with error: {e}")
            raise
