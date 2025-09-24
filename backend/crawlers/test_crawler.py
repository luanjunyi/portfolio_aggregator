from typing import List
from decimal import Decimal
import asyncio

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.base_crawler import BaseCrawler
from models.portfolio import Holding


class TestCrawler(BaseCrawler):
    """Test crawler for development and testing purposes"""
    
    def __init__(self, headless: bool = True):
        super().__init__("test_broker", headless)
    
    def get_login_url(self) -> str:
        """Return a test URL"""
        return "https://httpbin.org/html"
    
    async def login(self) -> bool:
        """Simulate login process"""
        print(f"[{self.broker_name}] Simulating login...")
        
        # Check if we have credentials
        credentials = self.get_credentials()
        if not credentials:
            print(f"[{self.broker_name}] No credentials found. Please store credentials first.")
            return False
        
        print(f"[{self.broker_name}] Using credentials for user: {credentials['username']}")
        
        # Simulate some delay
        await asyncio.sleep(2)
        
        # Simulate successful login
        print(f"[{self.broker_name}] Login successful!")
        return True
    
    async def scrape_portfolio(self) -> List[Holding]:
        """Generate test portfolio data"""
        print(f"[{self.broker_name}] Scraping portfolio data...")
        
        # Simulate scraping delay
        await asyncio.sleep(1)
        
        # Generate test holdings
        test_holdings = [
            Holding(
                symbol="AAPL",
                description="Apple Inc.",
                quantity=Decimal("100"),
                unit_cost=Decimal("150.00"),
                cost_basis=Decimal("15000.00"),
                current_value=Decimal("17500.00"),
                day_change_percent=Decimal("1.5"),
                unrealized_gain_loss=Decimal("2500.00"),
                unrealized_gain_loss_percent=Decimal("16.67"),
                portfolio_percentage=Decimal("35.0"),
                broker=self.broker_name
            ),
            Holding(
                symbol="GOOGL",
                description="Alphabet Inc. Class A",
                quantity=Decimal("50"),
                unit_cost=Decimal("120.00"),
                cost_basis=Decimal("6000.00"),
                current_value=Decimal("7250.00"),
                day_change_percent=Decimal("-0.8"),
                unrealized_gain_loss=Decimal("1250.00"),
                unrealized_gain_loss_percent=Decimal("20.83"),
                portfolio_percentage=Decimal("14.5"),
                broker=self.broker_name
            ),
            Holding(
                symbol="TSLA",
                description="Tesla, Inc.",
                quantity=Decimal("25"),
                unit_cost=Decimal("200.00"),
                cost_basis=Decimal("5000.00"),
                current_value=Decimal("6250.00"),
                day_change_percent=Decimal("2.3"),
                unrealized_gain_loss=Decimal("1250.00"),
                unrealized_gain_loss_percent=Decimal("25.0"),
                portfolio_percentage=Decimal("12.5"),
                broker=self.broker_name
            )
        ]
        
        print(f"[{self.broker_name}] Found {len(test_holdings)} holdings")
        return test_holdings
