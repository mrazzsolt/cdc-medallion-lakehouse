# db.py
# Handles Oracle database connection using a context manager.
# Uses oracledb in thin mode — no Oracle Client installation needed.

import oracledb
import os
import time
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

"""
Oracle connection
"""
def get_db_connection(retries=10, delay=15):
    user = os.environ["APP_USER"]
    password = os.environ["APP_USER_PASSWORD"]
    dsn = os.environ["ORACLE_DSN"]
    
    for attempt in range(1, retries + 1):
        try:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            print(f"Connected to Oracle (attempt {attempt})")
            return conn
        except oracledb.OperationalError as e:
            print(f"Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
    
    raise Exception("Could not connect to Oracle after all retries")