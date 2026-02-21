# db.py
# Handles Oracle database connection using a context manager.
# Uses oracledb in thin mode — no Oracle Client installation needed.

import oracledb
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

"""
Oracle connection
"""
def get_db_connection():
    user = os.environ["APP_USER"]
    password = os.environ["APP_USER_PASSWORD"]
    dsn = os.environ["ORACLE_DSN"]

    return oracledb.connect(user=user, password=password, dsn=dsn)