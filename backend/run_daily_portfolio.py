#!/usr/bin/env python3
import asyncio
import logging
import sys
from datetime import datetime

# Add the parent directory to sys.path to allow imports
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.fetch_all_positions import fetch_all_positions
from backend.storage.database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("portfolio_cron.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("daily_portfolio_run")

async def main():
    log.info("Starting daily portfolio fetch...")
    try:
        # 1. Fetch positions
        portfolio = await fetch_all_positions()
        log.info(f"Successfully fetched portfolio. Total Value: ${portfolio.total_value:,.2f}")
        
        # 2. Save to database
        db_manager = DatabaseManager()
        snapshot_date = db_manager.save_portfolio_snapshot(portfolio)
        log.info(f"Saved portfolio snapshot to database for date: {snapshot_date}")
        
    except Exception as e:
        log.error(f"Failed to run daily portfolio fetch: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
