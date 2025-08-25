"""
Tests for credential management functionality.
"""

import os
from unittest.mock import patch, MagicMock
import pytest

from integration_tools.core.credential_manager import CredentialManager


class TestCredentialManager:
    """Test cases for CredentialManager."""
    
    def test_init(self):
        """Test credential manager initialization."""
        cm = CredentialManager("test_service")
        assert cm.service_prefix == "test_service"
        assert cm.db_service == "test_service_db"
        assert cm.sftp_service == "test_service_sftp"
    
    def test_get_db_credentials_from_env(self, credential_manager):
        """Test getting DB credentials from environment variables."""
        with patch.dict(os.environ, {'DB_UID': 'env_user', 'DB_PWD': 'env_pass'}):
            username, password = credential_manager.get_db_credentials()
            assert username == 'env_user'
            assert password == 'env_pass'
    
    @patch('keyring.get_password')
    def test_get_db_credentials_from_keyring(self, mock_get_password, credential_manager):
        """Test getting DB credentials from keyring."""
        mock_get_password.side_effect = lambda service, account: {
            'test_integration_tools_db': {'username': 'keyring_user', 'password': 'keyring_pass'}
        }.get(service, {}).get(account)
        
        with patch.dict(os.environ, {}, clear=True):  # Clear env vars
            username, password = credential_manager.get_db_credentials()
            assert username == 'keyring_user'
            assert password == 'keyring_pass'
    
    @patch('builtins.input')
    @patch('getpass.getpass')
    @patch('keyring.set_password')
    def test_get_db_credentials_prompt(self, mock_set_password, mock_getpass, 
                                     mock_input, credential_manager):
        """Test prompting for DB credentials."""
        mock_input.side_effect = ['prompt_user', 'y']  # username, save choice
        mock_getpass.return_value = 'prompt_pass'
        
        with patch.dict(os.environ, {}, clear=True), \
             patch('keyring.get_password', return_value=None):
            username, password = credential_manager.get_db_credentials()
            
            assert username == 'prompt_user'
            assert password == 'prompt_pass'
            mock_set_password.assert_called()
    
    @patch('keyring.get_password')
    @patch('builtins.input')
    def test_get_sftp_credentials_from_keyring(self, mock_input, mock_get_password, credential_manager):
        """Test getting SFTP credentials from keyring."""
        mock_get_password.side_effect = lambda service, account: {
            'test_integration_tools_sftp': {
                'host': 'saved_host.com',
                'username': 'saved_user',
                'password': 'saved_pass'
            }
        }.get(service, {}).get(account)
        
        mock_input.return_value = 'y'  # Use saved credentials
        
        host, username, password = credential_manager.get_sftp_credentials()
        assert host == 'saved_host.com'
        assert username == 'saved_user'
        assert password == 'saved_pass'
    
    @patch('builtins.input')
    @patch('getpass.getpass')
    @patch('keyring.set_password')
    def test_get_sftp_credentials_prompt(self, mock_set_password, mock_getpass, 
                                       mock_input, credential_manager):
        """Test prompting for SFTP credentials."""
        mock_input.side_effect = ['test.host.com', 'test_user', 'y']  # host, username, save choice
        mock_getpass.return_value = 'test_pass'
        
        with patch('keyring.get_password', return_value=None):
            host, username, password = credential_manager.get_sftp_credentials()
            
            assert host == 'test.host.com'
            assert username == 'test_user'
            assert password == 'test_pass'
            mock_set_password.assert_called()
    
    def test_get_sftp_credentials_with_default_host(self, credential_manager):
        """Test SFTP credentials with default host."""
        with patch('builtins.input') as mock_input, \
             patch('getpass.getpass') as mock_getpass, \
             patch('keyring.get_password', return_value=None):
            
            mock_input.side_effect = ['', 'test_user', 'n']  # empty host (use default), username, don't save
            mock_getpass.return_value = 'test_pass'
            
            host, username, password = credential_manager.get_sftp_credentials(
                default_host='default.host.com'
            )
            
            assert host == 'default.host.com'
            assert username == 'test_user'
            assert password == 'test_pass'
    
    @patch('keyring.delete_password')
    def test_clear_saved_credentials_db_only(self, mock_delete_password, credential_manager):
        """Test clearing only DB credentials."""
        credential_manager.clear_saved_credentials('db')
        
        # Should delete DB credentials but not SFTP
        assert mock_delete_password.call_count == 2  # username and password
        calls = mock_delete_password.call_args_list
        assert any('_db' in str(call) for call in calls)
        assert not any('_sftp' in str(call) for call in calls)
    
    @patch('keyring.delete_password')
    def test_clear_saved_credentials_all(self, mock_delete_password, credential_manager):
        """Test clearing all credentials."""
        credential_manager.clear_saved_credentials()
        
        # Should delete both DB and SFTP credentials
        assert mock_delete_password.call_count == 5  # 2 DB + 3 SFTP
        calls = mock_delete_password.call_args_list
        assert any('_db' in str(call) for call in calls)
        assert any('_sftp' in str(call) for call in calls)
    
    @patch('builtins.input')
    @patch('getpass.getpass')
    def test_get_db_credentials_missing_username(self, mock_getpass, mock_input, credential_manager):
        """Test error handling for missing username."""
        mock_input.return_value = ''  # Empty username
        mock_getpass.return_value = 'password'
        
        with patch.dict(os.environ, {}, clear=True), \
             patch('keyring.get_password', return_value=None), \
             pytest.raises(RuntimeError, match="DB credentials are required"):
            credential_manager.get_db_credentials()
    
    @patch('builtins.input')
    @patch('getpass.getpass')
    def test_get_sftp_credentials_missing_password(self, mock_getpass, mock_input, credential_manager):
        """Test error handling for missing SFTP password."""
        mock_input.side_effect = ['host.com', 'user']
        mock_getpass.return_value = ''  # Empty password
        
        with patch('keyring.get_password', return_value=None), \
             pytest.raises(RuntimeError, match="SFTP host/username/password are required"):
            credential_manager.get_sftp_credentials()