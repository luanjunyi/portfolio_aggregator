import sqlite3
import json
import base64
from typing import Optional, Dict, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os


class DatabaseManager:
    """Manages SQLite database for credentials and encrypted sessions"""
    
    def __init__(self, db_path: str = "/Users/jluan/code/portfolio/portfolio.db"):
        self.db_path = db_path
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher_suite = Fernet(self.encryption_key)
        self._init_database()
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Generate or load encryption key for session data"""
        key_file = "encryption.key"
        
        if os.path.exists(key_file):
            with open(key_file, "rb") as f:
                return f.read()
        
        # Generate new key
        password = b"portfolio_app_key"  # In production, use a proper password
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        
        # Store key and salt
        with open(key_file, "wb") as f:
            f.write(key)
        with open("salt.key", "wb") as f:
            f.write(salt)
            
        return key
    
    def _init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS credentials (
                    broker TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    broker TEXT PRIMARY KEY,
                    session_data TEXT NOT NULL,  -- Encrypted JSON
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    
    def store_credentials(self, broker: str, username: str, password: str):
        """Store broker credentials"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO credentials 
                (broker, username, password, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (broker, username, password))
            conn.commit()
    
    def get_credentials(self, broker: str) -> Optional[Dict[str, Any]]:
        """Retrieve broker credentials"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT username, password 
                FROM credentials WHERE broker = ?
            """, (broker,))
            
            row = cursor.fetchone()
            if row:
                username, password = row
                return {
                    "username": username,
                    "password": password
                }
        return None
    
    def store_session(self, broker: str, session_data: Dict[str, Any], 
                     expires_at: Optional[str] = None):
        """Store encrypted session data"""
        # Encrypt the session data
        session_json = json.dumps(session_data)
        encrypted_data = self.cipher_suite.encrypt(session_json.encode())
        encrypted_b64 = base64.b64encode(encrypted_data).decode()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO sessions 
                (broker, session_data, expires_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (broker, encrypted_b64, expires_at))
            conn.commit()
    
    def get_session(self, broker: str) -> Optional[Dict[str, Any]]:
        """Retrieve and decrypt session data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT session_data, expires_at 
                FROM sessions WHERE broker = ?
            """, (broker,))
            
            row = cursor.fetchone()
            if row:
                encrypted_b64, expires_at = row
                try:
                    # Decrypt the session data
                    encrypted_data = base64.b64decode(encrypted_b64)
                    decrypted_data = self.cipher_suite.decrypt(encrypted_data)
                    session_data = json.loads(decrypted_data.decode())
                    
                    return {
                        "session_data": session_data,
                        "expires_at": expires_at
                    }
                except Exception as e:
                    print(f"Failed to decrypt session for {broker}: {e}")
                    # Remove invalid session
                    self.clear_session(broker)
        return None
    
    def clear_session(self, broker: str):
        """Clear session data for a broker"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM sessions WHERE broker = ?", (broker,))
            conn.commit()
    
    def list_brokers(self) -> list:
        """List all brokers with stored credentials"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT broker FROM credentials")
            return [row[0] for row in cursor.fetchall()]
