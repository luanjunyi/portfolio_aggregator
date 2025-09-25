#!/usr/bin/env python3
"""
Test script for Chase crawler
"""

import asyncio
import sys
import os
import logging

# Add the backend directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crawlers.chase_crawler import ChaseCrawler
from storage.database import DatabaseManager

# Set up logging
log = logging.getLogger(__name__)


async def run_chase_crawler():
    """Run the Chase crawler"""
    log.info("=== Running Chase Crawler ===")
    print("=== Running Chase Crawler ===")
    
    # Check if credentials exist
    db = DatabaseManager()
    creds = db.get_credentials("chase")
    
    if not creds:
        log.fatal("No Chase credentials found!")
        print("❌ No Chase credentials found!")
        print("Please run: python add_chase_credentials.py")
        return False
    
    log.info(f"Found credentials for user: {creds['username']}")
    print(f"✅ Found credentials for user: {creds['username']}")
    
    # Test crawler with visible browser window
    async with ChaseCrawler() as crawler:
        log.info("Starting Chase crawl...")
        print("\n🚀 Starting Chase crawl...")
        result = await crawler.crawl()
        
        log.info(f"Crawl Results: Success={result.success}, Broker={result.broker}, Holdings={len(result.holdings)}")
        print(f"\n📊 Crawl Results:")
        print(f"  Success: {result.success}")
        print(f"  Broker: {result.broker}")
        print(f"  Holdings Count: {len(result.holdings)}")
        
        if result.error_message:
            log.error(f"Crawl error: {result.error_message}")
            print(f"  Error: {result.error_message}")
        
        if result.requires_2fa:
            log.info(f"2FA Required: {result.requires_2fa}")
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
                print(f"     Brokers: {holding.brokers}")
                if holding.portfolio_percentage:
                    print(f"     Portfolio %: {holding.portfolio_percentage:.4%}")
                print()
        
        return result.success


async def main():
    """Main test function"""
    log.info("Starting Chase Crawler Test")
    print("Chase Crawler Test")
    print("=" * 30)
    
    success = await run_chase_crawler()
    
    if success:
        log.info("Chase crawler test completed successfully!")
        print("\n✅ Chase crawler test completed successfully!")
    else:
        log.error("Chase crawler test failed!")
        print("\n❌ Chase crawler test failed!")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
