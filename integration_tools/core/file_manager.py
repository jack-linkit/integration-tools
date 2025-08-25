"""
File management utilities for SFTP operations and local file handling.
"""

import os
import shutil
import tarfile
import tempfile
from typing import Callable, Dict, List, Optional, Tuple

import paramiko

try:
    import zstandard as zstd
except ImportError:
    zstd = None


class FileManager:
    """Manages SFTP connections and file operations."""
    
    def __init__(self):
        self._sftp_client: Optional[paramiko.SFTPClient] = None
        self._ssh_client: Optional[paramiko.SSHClient] = None
    
    def create_sftp_connection(self, host: str, username: str, password: str) -> paramiko.SFTPClient:
        """
        Create SFTP connection using paramiko.
        
        Args:
            host: SFTP server hostname
            username: Username for authentication
            password: Password for authentication
            
        Returns:
            SFTP client instance
            
        Raises:
            Exception: If connection fails
        """
        try:
            self._ssh_client = paramiko.SSHClient()
            self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh_client.connect(host, port=22, username=username, password=password)
            self._sftp_client = self._ssh_client.open_sftp()
            print(f"✅ Connected to SFTP server: {host}")
            return self._sftp_client
        except Exception as e:
            print(f"❌ Error connecting to SFTP server: {e}")
            raise
    
    def close_connection(self) -> None:
        """Close SFTP and SSH connections."""
        if self._sftp_client:
            try:
                self._sftp_client.close()
            except Exception:
                pass
            self._sftp_client = None
        
        if self._ssh_client:
            try:
                self._ssh_client.close()
            except Exception:
                pass
            self._ssh_client = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
    
    def db_windows_path_to_remote_sftp_path(self, db_path: str) -> str:
        """
        Convert a Windows DB path to a remote SFTP path.
        
        Handles variations like:
        - F:\\FTProot\\Districts\\... -> /Districts/...
        - F:/FTProot/Districts/... -> /Districts/...
        - Mixed or unexpected case; falls back to the segment starting at 'Districts'.
        
        Args:
            db_path: Windows-style database path
            
        Returns:
            Unix-style SFTP path
        """
        # Normalize slashes
        p = (db_path or "").strip()
        p = p.replace("\\", "/")

        # Drop drive letter if present (e.g., F:/path or C:/path)
        if len(p) >= 2 and p[1] == ":":
            p = p[2:]

        # Trim leading slashes
        p = p.lstrip("/")

        # If we can find the 'Districts' segment, keep from there
        lower_p = p.lower()
        idx = lower_p.find("districts/")
        if idx != -1:
            p = p[idx:]
        else:
            # Fallback: also handle paths that start with 'ftproot/districts/...'
            idx2 = lower_p.find("ftproot/districts/")
            if idx2 != -1:
                p = p[idx2 + len("ftproot/"):]

        # Collapse any duplicate slashes
        while "//" in p:
            p = p.replace("//", "/")

        return f"/{p}" if p else "/"
    
    def ensure_remote_directory_exists(self, sftp: paramiko.SFTPClient, remote_path: str) -> bool:
        """
        Ensure remote directory exists, creating it if necessary.
        
        Args:
            sftp: SFTP client
            remote_path: Remote directory path
            
        Returns:
            True if directory exists or was created successfully
        """
        try:
            # Check if directory already exists
            try:
                sftp.stat(remote_path)
                return True
            except FileNotFoundError:
                pass
            
            # Create directory recursively
            current_path = ""
            for part in remote_path.strip("/").split('/'):
                if part:
                    current_path += f"/{part}" if current_path else f"/{part}"
                    try:
                        sftp.stat(current_path)
                    except FileNotFoundError:
                        sftp.mkdir(current_path)
                        print(f"Created directory: {current_path}")
            
            print(f"✅ Ensured remote directory exists: {remote_path}")
            return True
        except Exception as e:
            print(f"❌ Error creating remote directory {remote_path}: {e}")
            return False
    
    def download_requestid_raw_files(
        self, 
        sftp: paramiko.SFTPClient, 
        request_id: int, 
        local_dir: str
    ) -> Tuple[bool, int, str]:
        """
        Download raw files for a RequestID from ETLProcessedFolder.
        
        Args:
            sftp: SFTP client
            request_id: Request ID to search for
            local_dir: Local directory to save files
            
        Returns:
            Tuple of (success, files_downloaded, message)
        """
        remote_dir = "LinkIt/ETLProcessedFolder/001"
        
        try:
            files = sftp.listdir(remote_dir)
        except Exception as e:
            return False, 0, f"Error listing {remote_dir}: {str(e)}"

        # Find the directory that starts with request_id
        match_dir = None
        for f in files:
            if f.startswith(str(request_id)):
                try:
                    # Check if it's a directory
                    if sftp.stat(f"{remote_dir}/{f}").st_mode & 0o40000:
                        match_dir = f
                        break
                except Exception:
                    continue
        
        if not match_dir:
            return False, 0, f"No directory found starting with {request_id} in {remote_dir}"
        
        print(f"Found directory: {match_dir}")
        remote_folder_path = f"{remote_dir}/{match_dir}"
        
        # List all files in the matched directory
        try:
            folder_files = sftp.listdir(remote_folder_path)
        except Exception as e:
            return False, 0, f"Error listing {remote_folder_path}: {str(e)}"

        # Filter for CSV and TXT files
        data_files = [f for f in folder_files if f.endswith('.csv') or f.endswith('.txt')]
        
        if not data_files:
            return False, 0, f"No CSV or TXT files found in {remote_folder_path}"

        # Download data files to local_dir
        files_downloaded = 0
        errors = []
        
        for data_file in data_files:
            remote_source = f"{remote_folder_path}/{data_file}"
            local_dest = os.path.join(local_dir, data_file)
            
            try:
                sftp.get(remote_source, local_dest)
                files_downloaded += 1
                print(f"✓ Downloaded {data_file} for RequestID {request_id}")
            except Exception as e:
                errors.append(f"Failed to download {data_file}: {str(e)}")

        if errors:
            return False, files_downloaded, f"Completed with errors: {'; '.join(errors)}"
        return True, files_downloaded, f"Successfully downloaded {files_downloaded} data files"
    
    def download_requestid_backup_file(
        self, 
        sftp: paramiko.SFTPClient, 
        request_id: int, 
        local_dir: str
    ) -> Tuple[bool, int, str]:
        """
        Download and extract backup file for a RequestID from BackupData.
        
        Args:
            sftp: SFTP client
            request_id: Request ID to search for
            local_dir: Local directory to save files
            
        Returns:
            Tuple of (success, files_extracted, message)
        """
        if zstd is None:
            return False, 0, "zstandard library is required to decompress .zst files. Please install 'zstandard'."
        
        remote_dir = "LinkIt/BackupData/ETLProcessedFolder/001"
        try:
            files = sftp.listdir(remote_dir)
        except Exception as e:
            return False, 0, f"Error listing {remote_dir}: {str(e)}"

        # Find the file
        match = None
        for f in files:
            if f.startswith(str(request_id)) and f.endswith(".tar.zst"):
                match = f
                break
        
        if not match:
            return False, 0, f"No .tar.zst file found for {request_id} in {remote_dir}"
        
        print(f"Found backup file: {match}")
        remote_path = f"{remote_dir}/{match}"

        # Create a temporary working directory
        work_dir = os.path.join(local_dir, "temp_extract")
        try:
            os.makedirs(work_dir, exist_ok=True)
        except Exception as e:
            return False, 0, f"Failed to prepare working tmp dir '{work_dir}': {str(e)}"

        local_zst = os.path.join(work_dir, match)
        local_tar = local_zst[:-4]  # remove .zst

        # Download
        try:
            sftp.get(remote_path, local_zst)
        except Exception as e:
            return False, 0, f"Failed to download {remote_path}: {str(e)}"

        # Decompress .zst to .tar
        try:
            with open(local_zst, "rb") as compressed, open(local_tar, "wb") as out:
                dctx = zstd.ZstdDecompressor()
                dctx.copy_stream(compressed, out)
        except Exception as e:
            return False, 0, f"Failed to decompress {local_zst}: {str(e)}"

        # Extract .csv from .tar
        csv_files = []
        try:
            with tarfile.open(local_tar, "r") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".csv"):
                        tar.extract(member, path=work_dir)
                        csv_files.append(os.path.join(work_dir, member.name))
        except Exception as e:
            return False, 0, f"Failed to extract .tar: {str(e)}"

        if not csv_files:
            return False, 0, f"No .csv files found in {local_tar}"

        # Move CSVs to local_dir
        files_downloaded = 0
        errors = []
        for csv_path in csv_files:
            filename = os.path.basename(csv_path)
            local_dest = os.path.join(local_dir, filename)
            try:
                shutil.move(csv_path, local_dest)
                files_downloaded += 1
                print(f"✓ Extracted {filename} for RequestID {request_id}")
            except Exception as e:
                errors.append(f"Failed to move {filename}: {str(e)}")

        # Cleanup temporary files
        for f in [local_zst, local_tar]:
            try:
                os.remove(f)
            except Exception:
                pass

        # Remove temporary directory
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass

        if errors:
            return False, files_downloaded, f"Completed with errors: {'; '.join(errors)}"
        return True, files_downloaded, f"Successfully processed {files_downloaded} CSV files"
    
    def download_files_for_requests(
        self,
        sftp: paramiko.SFTPClient,
        request_ids: List[int],
        local_download_dir: str = "~/Downloads",
        progress_callback: Optional[Callable[[int, int, int, bool, int, str], None]] = None,
        mute_output: bool = False,
    ) -> Dict[int, Dict[str, object]]:
        """
        Download files for multiple RequestIDs to local Downloads folder.
        
        Tries raw folder first, then backup file for each request.
        
        Args:
            sftp: SFTP client
            request_ids: List of request IDs
            local_download_dir: Local directory to download to
            progress_callback: Optional progress callback function
            mute_output: Whether to suppress output
            
        Returns:
            Dictionary mapping request_id to results
        """
        # Expand ~ to user's home directory
        local_download_dir = os.path.expanduser(local_download_dir)
        
        # Create downloads directory if it doesn't exist
        try:
            os.makedirs(local_download_dir, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Failed to create downloads directory '{local_download_dir}': {e}")

        results: Dict[int, Dict[str, object]] = {}
        total = len(request_ids)
        
        for idx, request_id in enumerate(request_ids, start=1):
            # Create a subdirectory for this request
            request_dir = os.path.join(local_download_dir, f"RequestID_{request_id}")
            try:
                os.makedirs(request_dir, exist_ok=True)
            except Exception as e:
                results[request_id] = {
                    "success": False,
                    "files_downloaded": 0,
                    "message": f"Failed to create request directory: {e}"
                }
                if progress_callback:
                    try:
                        progress_callback(idx, total, request_id, False, 0, f"Failed to create request directory: {e}")
                    except Exception:
                        pass
                continue

            ok = False
            count = 0
            msg = ""
            
            try:
                # Try primary (raw files in ETLProcessedFolder)
                ok, count, msg = self.download_requestid_raw_files(sftp, request_id, request_dir)
                
                if not ok:
                    # Fallback to backup archive
                    ok, count, msg = self.download_requestid_backup_file(sftp, request_id, request_dir)
                    
            except Exception as e:
                ok = False
                count = 0
                msg = f"Exception during download: {e}"
            
            results[request_id] = {
                "success": ok,
                "files_downloaded": count,
                "message": msg,
                "local_path": request_dir
            }
            
            if progress_callback:
                try:
                    progress_callback(idx, total, request_id, ok, count, msg)
                except Exception:
                    pass
        
        return results