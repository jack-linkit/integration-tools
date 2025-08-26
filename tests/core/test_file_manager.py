"""
Tests for file management functionality.
"""

import os
import tempfile
from unittest.mock import patch, MagicMock, mock_open
import pytest
import paramiko

from integration_tools.core.file_manager import FileManager
from integration_tools.core.error_handling import SFTPConnectionError, FileOperationError


class TestFileManager:
    """Test cases for FileManager."""
    
    def test_init(self):
        """Test file manager initialization."""
        fm = FileManager()
        assert fm._sftp_client is None
        assert fm._ssh_client is None
    
    @patch('paramiko.SSHClient')
    def test_create_sftp_connection_success(self, mock_ssh_class, file_manager):
        """Test successful SFTP connection creation."""
        mock_ssh = MagicMock()
        mock_sftp = MagicMock()
        mock_ssh.open_sftp.return_value = mock_sftp
        mock_ssh_class.return_value = mock_ssh
        
        result = file_manager.create_sftp_connection("test.host.com", "user", "pass")
        
        assert result == mock_sftp
        assert file_manager._ssh_client == mock_ssh
        assert file_manager._sftp_client == mock_sftp
        mock_ssh.set_missing_host_key_policy.assert_called_once()
        mock_ssh.connect.assert_called_once_with("test.host.com", port=22, username="user", password="pass")
    
    @patch('paramiko.SSHClient')
    def test_create_sftp_connection_failure(self, mock_ssh_class, file_manager):
        """Test SFTP connection creation failure."""
        mock_ssh = MagicMock()
        mock_ssh.connect.side_effect = Exception("Connection failed")
        mock_ssh_class.return_value = mock_ssh
        
        with pytest.raises(Exception, match="Connection failed"):
            file_manager.create_sftp_connection("test.host.com", "user", "pass")
    
    def test_close_connection(self, file_manager):
        """Test closing SFTP connection."""
        # Mock the clients
        mock_sftp = MagicMock()
        mock_ssh = MagicMock()
        file_manager._sftp_client = mock_sftp
        file_manager._ssh_client = mock_ssh
        
        file_manager.close_connection()
        
        mock_sftp.close.assert_called_once()
        mock_ssh.close.assert_called_once()
        assert file_manager._sftp_client is None
        assert file_manager._ssh_client is None
    
    def test_context_manager(self, file_manager):
        """Test FileManager as context manager."""
        with patch.object(file_manager, 'close_connection') as mock_close:
            with file_manager as fm:
                assert fm == file_manager
            mock_close.assert_called_once()
    
    def test_db_windows_path_to_remote_sftp_path(self, file_manager):
        """Test converting Windows DB path to SFTP path."""
        test_cases = [
            ("F:\\FTProot\\Districts\\State\\District\\File.csv", "/Districts/State/District/File.csv"),
            ("F:/FTProot/Districts/State/District/File.csv", "/Districts/State/District/File.csv"),
            ("Districts\\State\\District\\File.csv", "/Districts/State/District/File.csv"),
            ("C:\\somewhere\\ftproot\\Districts\\Test\\file.txt", "/Districts/Test/file.txt"),
            ("", "/"),
            ("no_districts_path", "/no_districts_path")
        ]
        
        for input_path, expected in test_cases:
            result = file_manager.db_windows_path_to_remote_sftp_path(input_path)
            assert result == expected, f"Failed for input: {input_path}"
    
    def test_ensure_remote_directory_exists_already_exists(self, file_manager, mock_sftp_client):
        """Test ensuring remote directory exists when it already exists."""
        mock_sftp_client.stat.return_value = True  # Directory exists
        
        result = file_manager.ensure_remote_directory_exists(mock_sftp_client, "/test/path")
        
        assert result is True
        mock_sftp_client.stat.assert_called_once_with("/test/path")
        mock_sftp_client.mkdir.assert_not_called()
    
    def test_ensure_remote_directory_exists_create_new(self, file_manager, mock_sftp_client):
        """Test ensuring remote directory exists by creating it."""
        # First stat() call raises FileNotFoundError (directory doesn't exist)
        # Subsequent calls for parent directories should succeed or fail appropriately
        mock_sftp_client.stat.side_effect = [FileNotFoundError, FileNotFoundError, FileNotFoundError]
        
        result = file_manager.ensure_remote_directory_exists(mock_sftp_client, "/test/path")
        
        assert result is True
        assert mock_sftp_client.mkdir.call_count >= 1
    
    def test_download_requestid_raw_files_success(self, file_manager, mock_sftp_client, temp_dir):
        """Test successful download of raw files."""
        # Mock the first listdir call (listing the main directory)
        mock_sftp_client.listdir.return_value = ["123_test_folder", "other_folder"]
        mock_sftp_client.stat.return_value = MagicMock(st_mode=0o40000)  # Directory
        
        # Mock the second listdir call (listing files in the request directory)
        def listdir_side_effect(path):
            if "123_test_folder" in path:
                return ["file1.csv", "file2.txt", "file3.log"]
            return ["123_test_folder", "other_folder"]  # Default for first call
        
        mock_sftp_client.listdir.side_effect = listdir_side_effect
        
        success, count, message = file_manager.download_requestid_raw_files(
            mock_sftp_client, 123, temp_dir
        )
        
        assert success is True
        assert count == 2  # Only CSV and TXT files
        assert "Successfully downloaded" in message
        assert mock_sftp_client.get.call_count == 2
    
    def test_download_requestid_raw_files_no_directory(self, file_manager, mock_sftp_client, temp_dir):
        """Test download when no matching directory exists."""
        mock_sftp_client.listdir.return_value = ["456_other_folder", "789_another"]
        
        success, count, message = file_manager.download_requestid_raw_files(
            mock_sftp_client, 123, temp_dir
        )
        
        assert success is False
        assert count == 0
        assert "No directory found starting with 123" in message
    
    def test_download_requestid_raw_files_no_csv_files(self, file_manager, mock_sftp_client, temp_dir):
        """Test download when directory exists but no CSV/TXT files."""
        mock_sftp_client.listdir.side_effect = [
            ["123_test_folder"],  # First call - list main directory
            ["file1.log", "file2.xml"]  # Second call - list files in request directory
        ]
        mock_sftp_client.stat.return_value = MagicMock(st_mode=0o40000)  # Directory
        
        success, count, message = file_manager.download_requestid_raw_files(
            mock_sftp_client, 123, temp_dir
        )
        
        assert success is False
        assert count == 0
        assert "No CSV or TXT files found" in message
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    @patch('shutil.move')
    @patch('tarfile.open')
    def test_download_requestid_backup_file_success(self, mock_tar_open, mock_shutil_move, 
                                                  mock_makedirs, mock_file_open, 
                                                  file_manager, mock_sftp_client, temp_dir):
        """Test successful download and extraction of backup file."""
        # Mock zstandard
        with patch('integration_tools.core.file_manager.zstd') as mock_zstd:
            mock_decompressor = MagicMock()
            mock_zstd.ZstdDecompressor.return_value = mock_decompressor
            
            mock_sftp_client.listdir.return_value = ["123_backup.tar.zst", "other_file.txt"]
            
            # Mock tar file contents
            mock_member = MagicMock()
            mock_member.name = "test_file.csv"
            mock_tar = MagicMock()
            mock_tar.getmembers.return_value = [mock_member]
            mock_tar_open.return_value.__enter__.return_value = mock_tar
            
            success, count, message = file_manager.download_requestid_backup_file(
                mock_sftp_client, 123, temp_dir
            )
            
            assert success is True
            assert count == 1
            assert "Successfully processed" in message
            mock_sftp_client.get.assert_called_once()
            mock_decompressor.copy_stream.assert_called_once()
    
    def test_download_requestid_backup_file_no_zstd(self, file_manager, mock_sftp_client, temp_dir):
        """Test backup file download when zstandard is not available."""
        with patch('integration_tools.core.file_manager.zstd', None):
            success, count, message = file_manager.download_requestid_backup_file(
                mock_sftp_client, 123, temp_dir
            )
            
            assert success is False
            assert count == 0
            assert "zstandard library is required" in message
    
    def test_download_requestid_backup_file_not_found(self, file_manager, mock_sftp_client, temp_dir):
        """Test backup file download when file doesn't exist."""
        mock_sftp_client.listdir.return_value = ["456_backup.tar.zst", "other_file.txt"]
        
        success, count, message = file_manager.download_requestid_backup_file(
            mock_sftp_client, 123, temp_dir
        )
        
        assert success is False
        assert count == 0
        assert "No .tar.zst file found for 123" in message
    
    @patch('os.makedirs')
    def test_download_files_for_requests_success(self, mock_makedirs, file_manager, mock_sftp_client):
        """Test downloading files for multiple requests."""
        with patch.object(file_manager, 'download_requestid_raw_files') as mock_download_raw, \
             patch.object(file_manager, 'download_requestid_backup_file') as mock_download_backup:
            
            # Mock successful download for first request
            mock_download_raw.side_effect = [
                (True, 2, "Success"),  # Request 123
                (False, 0, "No files")  # Request 456 - will try backup
            ]
            mock_download_backup.return_value = (True, 1, "Backup success")  # Request 456 backup
            
            results = file_manager.download_files_for_requests(
                mock_sftp_client, [123, 456], "/tmp/test"
            )
            
            assert len(results) == 2
            assert results[123]["success"] is True
            assert results[123]["files_downloaded"] == 2
            assert results[456]["success"] is True
            assert results[456]["files_downloaded"] == 1
    
    def test_download_files_for_requests_directory_creation_failure(self, file_manager, mock_sftp_client):
        """Test download failure when directory creation fails."""
        with patch('os.makedirs', side_effect=Exception("Permission denied")):
            with pytest.raises(RuntimeError, match="Failed to create downloads directory"):
                file_manager.download_files_for_requests(
                    mock_sftp_client, [123], "/invalid/path"
                )