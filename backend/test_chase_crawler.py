#!/usr/bin/env python3
"""
Test script for Chase crawler
"""

import asyncio
import sys
import os

# Add the backend directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crawlers.chase_crawler import ChaseCrawler
from storage.database import DatabaseManager


async def test_chase_crawler():
    """Test the Chase crawler"""
    print("=== Testing Chase Crawler ===")
    
    # Check if credentials exist
    db = DatabaseManager()
    creds = db.get_credentials("chase")
    
    if not creds:
        print("❌ No Chase credentials found!")
        print("Please run: python add_chase_credentials.py")
        return False
    
    print(f"✅ Found credentials for user: {creds['username']}")
    
    # Test crawler with headless=False so you can see what's happening
    async with ChaseCrawler(headless=False) as crawler:
        print("\n🚀 Starting Chase crawl...")
        result = await crawler.crawl()
        
        print(f"\n📊 Crawl Results:")
        print(f"  Success: {result.success}")
        print(f"  Broker: {result.broker}")
        print(f"  Holdings Count: {len(result.holdings)}")
        
        if result.error_message:
            print(f"  Error: {result.error_message}")
        
        if result.requires_2fa:
            print(f"  2FA Required: {result.requires_2fa}")
        
        if result.holdings:
            print(f"\n📈 Holdings:")
            for i, holding in enumerate(result.holdings):
                print(f"  {i+1}. {holding.symbol} - {holding.description}")
                print(f"     Quantity: {holding.quantity}")
                print(f"     Price: ${holding.price}")
                print(f"     Unit Cost: ${holding.unit_cost}")
                print(f"     Cost Basis: ${holding.cost_basis}")
                print(f"     Current Value: ${holding.current_value}")
                print(f"     Day Change: ${holding.day_change_dollars} ({holding.day_change_percent:.4%})")
                print(f"     Unrealized G/L: ${holding.unrealized_gain_loss} ({holding.unrealized_gain_loss_percent:.4%})")
                print(f"     Broker: {holding.broker}")
                if holding.portfolio_percentage:
                    print(f"     Portfolio %: {holding.portfolio_percentage:.4%}")
                print()
        
        return result.success


async def main():
    """Main test function"""
    print("Chase Crawler Test")
    print("=" * 30)
    
    success = await test_chase_crawler()
    
    if success:
        print("\n✅ Chase crawler test completed successfully!")
    else:
        print("\n❌ Chase crawler test failed!")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
