from typing import List
import asyncio

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.base_crawler import BaseCrawler
from models.portfolio import Holding


class SandboxCrawler(BaseCrawler):
    """Test crawler for development and testing purposes"""
    
    def __init__(self):
        super().__init__("test_broker")
    
    def get_login_url(self) -> str:
        """Return a test URL"""
        return "https://httpbin.org/html"
    
    async def login(self) -> bool:
        """Simulate login process"""
        self.log.info("Simulating login...")
        
        # Check if we have credentials
        credentials = self.get_credentials()
        if not credentials:
            self.log.fatal("No credentials found. Please store credentials first.")
            raise RuntimeError("No credentials found")
        
        self.log.info(f"Using credentials for user: {credentials['username']}")
        
        # Simulate some delay
        await asyncio.sleep(1)
        
        # Simulate successful login
        self.log.info("Login successful!")
        return True
    
    async def scrape_portfolio(self) -> List[Holding]:
        """Generate test portfolio data"""
        self.log.info("Scraping portfolio data...")
        
        # Simulate scraping delay
        await asyncio.sleep(1)
        
        # Generate test holdings
        test_holdings = [
            Holding(
                symbol="AAPL",
                description="Apple Inc.",
                quantity=100.0,
                price=175.0,
                unit_cost=150.00,
                cost_basis=15000.00,
                current_value=17500.00,
                day_change_percent=0.015,
                day_change_dollars=261.90,
                unrealized_gain_loss=2500.00,
                unrealized_gain_loss_percent=0.1667,
                portfolio_percentage=0.35,
                brokers={self.broker_name: 17500.00}
            ),
            Holding(
                symbol="GOOGL",
                description="Alphabet Inc. Class A",
                quantity=50.0,
                price=145.0,
                unit_cost=120.00,
                cost_basis=6000.00,
                current_value=7250.00,
                day_change_percent=-0.008,
                day_change_dollars=-58.40,
                unrealized_gain_loss=1250.00,
                unrealized_gain_loss_percent=0.2083,
                portfolio_percentage=0.145,
                brokers={self.broker_name: 7250.00}
            ),
            Holding(
                symbol="TSLA",
                description="Tesla, Inc.",
                quantity=25.0,
                price=250.0,
                unit_cost=200.00,
                cost_basis=5000.00,
                current_value=6250.00,
                day_change_percent=0.023,
                day_change_dollars=140.63,
                unrealized_gain_loss=1250.00,
                unrealized_gain_loss_percent=0.25,
                portfolio_percentage=0.125,
                brokers={self.broker_name: 6250.00}
            )
        ]
        
        self.log.info(f"Found {len(test_holdings)} holdings")
        return test_holdings
