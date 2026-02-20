#!/usr/bin/env python3
import argparse
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.storage.database import DatabaseManager

def main():
    parser = argparse.ArgumentParser(description="Add broker credentials to the database.")
    parser.add_argument("--broker", required=True, help="Broker name (e.g., merrill_edge, chase, etrade, test_broker)")
    parser.add_argument("--username", required=True, help="Username for the broker")
    parser.add_argument("--password", required=True, help="Password for the broker")
    
    args = parser.parse_args()
    
    try:
        db = DatabaseManager()
        db.store_credentials(args.broker, args.username, args.password)
        print(f"Successfully stored credentials for {args.broker}")
    except Exception as e:
        print(f"Error storing credentials: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
