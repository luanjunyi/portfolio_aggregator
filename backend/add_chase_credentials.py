#!/usr/bin/env python3
"""
Quick script to add Chase credentials
"""

import sys
import os
import getpass

# Add the backend directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from storage.database import DatabaseManager

def add_chase_credentials():
    """Add Chase credentials to the database"""
    print("Adding Chase Credentials")
    print("=" * 25)
    
    username = input("Chase Username: ").strip()
    if not username:
        print("Error: Username is required")
        return False
    
    password = getpass.getpass("Chase Password: ")
    if not password:
        print("Error: Password is required")
        return False
    
    # Store credentials
    db = DatabaseManager()
    db.store_credentials("chase", username, password)
    
    print(f"\n✅ Chase credentials stored successfully!")
    print(f"   Username: {username}")
    
    return True

if __name__ == "__main__":
    add_chase_credentials()
