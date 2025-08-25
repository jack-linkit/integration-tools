"""
Database connection utilities for integration tools.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
from contextlib import contextmanager


@contextmanager
def get_db(uid: str, pwd: str, server: str = "10.252.24.190", database: str = "AdminReporting"):
    """
    Get database session with provided credentials.

    Args:
        uid: Database username
        pwd: Database password
        server: Database server (default: production)
        database: Database name (default: AdminReporting)

    Returns:
        Database session
    """
    # Get connection details from environment variables for security
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
        "TrustServerCertificate=yes;"
    )

    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(conn_str)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()