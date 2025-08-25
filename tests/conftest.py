"""
Test configuration and fixtures for integration tools.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.integration_tools.models import Base
from src.integration_tools.core.credential_manager import CredentialManager
from src.integration_tools.core.db_manager import DatabaseManager
from src.integration_tools.core.file_manager import FileManager
from src.integration_tools.core.request_manager import RequestManager


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def mock_credentials():
    """Mock credentials for testing."""
    return ("test_user", "test_password")


@pytest.fixture
def mock_sftp_credentials():
    """Mock SFTP credentials for testing."""
    return ("test.host.com", "sftp_user", "sftp_password")


@pytest.fixture
def credential_manager():
    """Create a credential manager for testing."""
    return CredentialManager(service_prefix="test_integration_tools")


@pytest.fixture
def mock_db_session():
    """Create a mock database session for testing."""
    # Use SQLite in-memory database for testing
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def db_manager():
    """Create a database manager for testing."""
    return DatabaseManager(server="test_server", database="test_db")


@pytest.fixture
def file_manager():
    """Create a file manager for testing."""
    return FileManager()


@pytest.fixture
def mock_sftp_client():
    """Create a mock SFTP client."""
    mock_sftp = MagicMock()
    mock_sftp.listdir.return_value = ["test_file1.csv", "test_file2.txt"]
    mock_sftp.stat.return_value = MagicMock(st_mode=0o40000)  # Directory mode
    mock_sftp.get.return_value = None
    return mock_sftp


@pytest.fixture
def request_manager(mock_credentials, mock_sftp_credentials):
    """Create a request manager for testing."""
    with patch.object(CredentialManager, 'get_db_credentials', return_value=mock_credentials), \
         patch.object(CredentialManager, 'get_sftp_credentials', return_value=mock_sftp_credentials):
        return RequestManager(server="test_server", database="test_db")


@pytest.fixture
def sample_requests():
    """Sample request data for testing."""
    from datetime import datetime
    from integration_tools.core.db_manager import RequestRow
    
    return [
        RequestRow(
            RequestID=123,
            DistrictID=456,
            DataRequestTypeID=66,
            DataRequestTypeName="SAT",
            ImportedFileName="test_path",
            Status=5,
            RequestTime=datetime.now()
        ),
        RequestRow(
            RequestID=124,
            DistrictID=457,
            DataRequestTypeID=67,
            DataRequestTypeName="PSAT",
            ImportedFileName="test_path2",
            Status=4,
            RequestTime=datetime.now()
        )
    ]


@pytest.fixture(autouse=True)
def mock_keyring():
    """Mock keyring operations to avoid system keyring interactions during tests."""
    with patch('keyring.get_password', return_value=None), \
         patch('keyring.set_password'), \
         patch('keyring.delete_password'):
        yield


@pytest.fixture(autouse=True)
def mock_environment_variables():
    """Set up mock environment variables for tests."""
    test_env = {
        'DB_UID': 'test_db_user',
        'DB_PWD': 'test_db_password'
    }
    
    with patch.dict(os.environ, test_env, clear=False):
        yield