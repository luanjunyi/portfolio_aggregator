#!/usr/bin/env python3
"""
CLI tool for testing the crawler infrastructure
"""

import asyncio
import sys
from typing import Dict, Any

from crawlers.sandbox_crawler import SandboxCrawler
from storage.database import DatabaseManager


async def test_credentials():
    """Test credential storage and retrieval"""
    print("=== Testing Credential Storage ===")
    
    db = DatabaseManager()
    
    # Store test credentials
    db.store_credentials(
        broker="test_broker",
        username="test_user",
        password="test_password"
    )
    
    # Retrieve credentials
    creds = db.get_credentials("test_broker")
    print(f"Stored credentials: {creds}")
    
    return creds is not None


async def test_crawler():
    """Test the crawler functionality"""
    print("\n=== Testing Crawler ===")
    
    async with SandboxCrawler(headless=False) as crawler:
        result = await crawler.crawl()
        
        print(f"Crawl Result:")
        print(f"  Success: {result.success}")
        print(f"  Broker: {result.broker}")
        print(f"  Holdings Count: {len(result.holdings)}")
        
        if result.holdings:
            print(f"  Sample Holding:")
            holding = result.holdings[0]
            print(f"    Symbol: {holding.symbol}")
            print(f"    Description: {holding.description}")
            print(f"    Quantity: {holding.quantity}")
            print(f"    Value: ${holding.current_value}")
            print(f"    Gain/Loss: ${holding.unrealized_gain_loss} ({holding.unrealized_gain_loss_percent}%)")
        
        if result.error_message:
            print(f"  Error: {result.error_message}")

        input("Press Enter to continue...")
    
    return result.success


async def test_session_storage():
    """Test session storage functionality"""
    print("\n=== Testing Session Storage ===")
    
    db = DatabaseManager()
    
    # Store test session (proper Playwright storage state format)
    test_session = {
        "cookies": [
            {
                "name": "session_id", 
                "value": "abc123", 
                "domain": "example.com",
                "path": "/",
                "httpOnly": False,
                "secure": False,
                "sameSite": "Lax"
            }
        ],
        "origins": [
            {
                "origin": "https://example.com",
                "localStorage": [
                    {"name": "user_pref", "value": "dark_mode"}
                ]
            }
        ]
    }
    
    db.store_session("test_broker", test_session)
    
    # Retrieve session
    session = db.get_session("test_broker")
    print(f"Stored session: {session is not None}")
    
    if session:
        print(f"Session data keys: {list(session['session_data'].keys())}")
    
    return session is not None


async def main():
    """Run all tests"""
    print("Portfolio Crawler Infrastructure Test")
    print("=" * 40)
    
    tests = [
        ("Credentials", test_credentials),
        ("Session Storage", test_session_storage),
        ("Crawler", test_crawler)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results[test_name] = result
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"\n{test_name}: {status}")
        except Exception as e:
            results[test_name] = False
            print(f"\n{test_name}: ❌ ERROR - {e}")
    
    print("\n" + "=" * 40)
    print("Test Summary:")
    for test_name, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {test_name}")
    
    all_passed = all(results.values())
    print(f"\nOverall: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
