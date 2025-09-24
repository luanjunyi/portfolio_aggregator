#!/usr/bin/env python3
"""
CLI tool for managing broker credentials
"""

import sys
import getpass
from typing import List, Optional

from storage.database import DatabaseManager


def add_credentials():
    """Add credentials for a broker"""
    print("Add Broker Credentials")
    print("=" * 25)
    
    broker = input("Broker name (e.g., merrill_edge, chase, etrade): ").strip()
    if not broker:
        print("Error: Broker name is required")
        return False
    
    username = input("Username: ").strip()
    if not username:
        print("Error: Username is required")
        return False
    
    password = input("Password: ").strip()
    if not password:
        print("Error: Password is required")
        return False
    
    # Store credentials
    db = DatabaseManager()
    db.store_credentials(broker, username, password)
    
    print(f"\n✅ Credentials stored for {broker}")
    
    return True


def list_credentials():
    """List all stored broker credentials"""
    print("Stored Broker Credentials")
    print("=" * 28)
    
    db = DatabaseManager()
    brokers = db.list_brokers()
    
    if not brokers:
        print("No credentials stored yet.")
        return
    
    for broker in brokers:
        creds = db.get_credentials(broker)
        if creds:
            print(f"\n📊 {broker}")
            print(f"   Username: {creds['username']}")
            print(f"   Password: {creds['password']}")


def delete_credentials():
    """Delete credentials for a broker"""
    print("Delete Broker Credentials")
    print("=" * 28)
    
    db = DatabaseManager()
    brokers = db.list_brokers()
    
    if not brokers:
        print("No credentials stored yet.")
        return False
    
    print("Available brokers:")
    for i, broker in enumerate(brokers, 1):
        print(f"  {i}. {broker}")
    
    try:
        choice = int(input("\nSelect broker to delete (number): ")) - 1
        if 0 <= choice < len(brokers):
            broker = brokers[choice]
            confirm = input(f"Delete credentials for '{broker}'? (y/N): ").strip().lower()
            
            if confirm == 'y':
                # Delete from database (we need to add this method)
                import sqlite3
                with sqlite3.connect(db.db_path) as conn:
                    conn.execute("DELETE FROM credentials WHERE broker = ?", (broker,))
                    conn.execute("DELETE FROM sessions WHERE broker = ?", (broker,))
                    conn.commit()
                
                print(f"✅ Deleted credentials for {broker}")
                return True
            else:
                print("Cancelled.")
                return False
        else:
            print("Invalid selection.")
            return False
    except ValueError:
        print("Invalid input.")
        return False


def main():
    """Main CLI interface"""
    while True:
        print("\n" + "=" * 40)
        print("Portfolio Credential Manager")
        print("=" * 40)
        print("1. Add broker credentials")
        print("2. List stored credentials")
        print("3. Delete credentials")
        print("4. Exit")
        
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == '1':
            add_credentials()
        elif choice == '2':
            list_credentials()
        elif choice == '3':
            delete_credentials()
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-4.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExiting...")
        sys.exit(0)
