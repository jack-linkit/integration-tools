"""
Request Manager - Main orchestration class for request lifecycle management.
"""

import asyncio
import glob
import json
import os
import tempfile
import time
import webbrowser
from typing import Dict, List, Optional, Sequence

from .credential_manager import CredentialManager
from .db_manager import DatabaseManager, RequestRow
from .file_manager import FileManager


class RequestManager:
    """Main class for managing request lifecycle operations."""
    
    def __init__(self, server: str = "10.252.24.190", database: str = "AdminReporting"):
        self.db_manager = DatabaseManager(server, database)
        self.credential_manager = CredentialManager()
        self.file_manager = FileManager()
        self._cached_db_credentials: Optional[tuple] = None
        self._cached_sftp_credentials: Optional[tuple] = None
    
    @staticmethod
    def _cleanup_expired_email_files(max_age_hours: int = 24) -> None:
        """
        Clean up temporary HTML email files that are older than the specified age.
        
        Args:
            max_age_hours: Maximum age in hours before files are considered expired (default: 24)
        """
        temp_dir = tempfile.gettempdir()
        pattern = os.path.join(temp_dir, "*_req_*.html")
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        files_removed = 0
        
        for file_path in glob.glob(pattern):
            try:
                # Only process files that match our pattern
                if "_req_" in os.path.basename(file_path) and file_path.endswith(".html"):
                    # Check file age
                    file_age = current_time - os.path.getmtime(file_path)
                    if file_age > max_age_seconds:
                        os.remove(file_path)
                        files_removed += 1
            except Exception:
                pass  # Silently ignore cleanup failures
        
        if files_removed > 0:
            print(f"Cleaned up {files_removed} expired temporary HTML file(s) older than {max_age_hours} hours")
    
    @staticmethod
    def cleanup_all_temp_email_files() -> None:
        """
        Manually clean up ALL temporary HTML email files, regardless of age.
        Useful for immediate cleanup when needed.
        """
        temp_dir = tempfile.gettempdir()
        pattern = os.path.join(temp_dir, "*_req_*.html")
        files_removed = 0
        
        for file_path in glob.glob(pattern):
            try:
                # Only process files that match our pattern
                if "_req_" in os.path.basename(file_path) and file_path.endswith(".html"):
                    os.remove(file_path)
                    files_removed += 1
            except Exception:
                pass  # Silently ignore cleanup failures
        
        if files_removed > 0:
            print(f"Cleaned up {files_removed} temporary HTML email file(s)")
        else:
            print("No temporary HTML email files found to clean up")
    
    def get_db_credentials(self, force_prompt: bool = False) -> tuple:
        """Get database credentials with caching."""
        if self._cached_db_credentials and not force_prompt:
            return self._cached_db_credentials
        
        creds = self.credential_manager.get_db_credentials()
        self._cached_db_credentials = creds
        return creds
    
    def get_sftp_credentials(self, force_prompt: bool = False) -> tuple:
        """Get SFTP credentials with caching."""
        if self._cached_sftp_credentials and not force_prompt:
            return self._cached_sftp_credentials
        
        creds = self.credential_manager.get_sftp_credentials(force_prompt=force_prompt)
        self._cached_sftp_credentials = creds
        return creds
    
    def list_request_types(self, name_filter: Optional[str] = None) -> List[tuple]:
        """
        List all DataRequestTypes with optional filtering.
        
        Args:
            name_filter: Optional name filter (case-insensitive substring match)
            
        Returns:
            List of (DataRequestTypeID, Name) tuples
        """
        username, password = self.get_db_credentials()
        with self.db_manager.get_session(username, password) as session:
            return self.db_manager.list_request_types(session, name_filter)
    
    def find_requests(
        self,
        type_ids: Optional[List[int]] = None,
        type_names: Optional[List[str]] = None,
        district_ids: Optional[List[int]] = None,
        statuses: Sequence[int] = (4, 5),
    ) -> List[RequestRow]:
        """
        Find latest requests by various criteria.
        
        Args:
            type_ids: Optional list of DataRequestTypeIDs
            type_names: Optional list of type name prefixes
            district_ids: Optional list of DistrictIDs
            statuses: Request statuses to include (default: 4=failed, 5=success)
            
        Returns:
            List of RequestRow objects
        """
        username, password = self.get_db_credentials()
        with self.db_manager.get_session(username, password) as session:
            return self.db_manager.find_latest_requests(
                session, type_ids, type_names, district_ids, statuses
            )
    
    def show_email_content(self, request_id: int, cleanup_expired: bool = True) -> bool:
        """
        Show email content for a request in browser.
        
        Args:
            request_id: Request ID to show email for
            cleanup_expired: Whether to clean up expired temp files (default: True)
            
        Returns:
            True if email was found and opened
        """
        username, password = self.get_db_credentials()
        with self.db_manager.get_session(username, password) as session:
            content = self.db_manager.get_email_content(session, request_id)
            if not content:
                print("No email content found.")
                return False
            
            email, attach = content
            if email:
                try:
                    # Optionally clean up expired temporary HTML files (older than 24 hours)
                    if cleanup_expired:
                        self._cleanup_expired_email_files()
                    
                    html_doc = (
                        "<!doctype html><html><head><meta charset='utf-8'><title>EmailContent"
                        f" {request_id}</title></head><body>" + email + "</body></html>"
                    )
                    with tempfile.NamedTemporaryFile("w", delete=False, suffix=f"_req_{request_id}.html") as f:
                        f.write(html_doc)
                        temp_path = f.name
                    
                    webbrowser.open(f"file://{temp_path}")
                    print(f"Opened HTML in browser: {temp_path}")
                    print(f"Note: Temporary file will be automatically cleaned up after 24 hours")
                    return True
                except Exception as e:
                    print(f"Failed to open HTML in browser: {e}")
                    return False
            
            if attach:
                print(f"Attachment: {attach}")
            
            return True
    
    def download_files(
        self, 
        request_ids: List[int], 
        local_dir: str = "~/Downloads",
        show_progress: bool = True
    ) -> Dict[int, Dict[str, object]]:
        """
        Download files for multiple requests.
        
        Args:
            request_ids: List of request IDs
            local_dir: Local directory to download to
            show_progress: Whether to show progress
            
        Returns:
            Dictionary mapping request_id to download results
        """
        host, username, password = self.get_sftp_credentials()
        
        with self.file_manager as fm:
            sftp = fm.create_sftp_connection(host, username, password)
            
            def progress_callback(done: int, total: int, req_id: int, ok: bool, count: int, msg: str):
                if show_progress:
                    print(f"[{done}/{total}] RequestID {req_id}: {'✓' if ok else '✗'} {count} files - {msg}")
            
            return fm.download_files_for_requests(
                sftp, 
                request_ids, 
                local_dir,
                progress_callback=progress_callback if show_progress else None
            )
    
    def restore_files(
        self, 
        request_ids: List[int],
        temp_dir: str = "/tmp",
        show_progress: bool = True
    ) -> Dict[int, Dict[str, object]]:
        """
        Restore processed files for requests back to their SFTP directories.
        
        Args:
            request_ids: List of request IDs
            temp_dir: Temporary directory for processing
            show_progress: Whether to show progress
            
        Returns:
            Dictionary mapping request_id to restore results
        """
        username, password = self.get_db_credentials()
        host, sftp_user, sftp_pwd = self.get_sftp_credentials()
        
        # Get directory paths for requests
        request_to_path = {}
        with self.db_manager.get_session(username, password) as session:
            for req_id in request_ids:
                db_path = self.db_manager.get_directory_path_for_request(session, req_id)
                if db_path:
                    remote_path = self.file_manager.db_windows_path_to_remote_sftp_path(db_path)
                    request_to_path[req_id] = remote_path
                else:
                    print(f"Warning: Could not determine DirectoryPath for RequestID: {req_id}")
        
        # Import the restore functions (keeping existing functionality)
        from ..legacy.restore_etl_proccessed import restore_for_requestids
        
        with self.file_manager as fm:
            sftp = fm.create_sftp_connection(host, sftp_user, sftp_pwd)
            
            def progress_callback(done: int, total: int, req_id: int, ok: bool, count: int, msg: str):
                if show_progress:
                    print(f"[{done}/{total}] RequestID {req_id}: {'✓' if ok else '✗'} {count} files - {msg}")
            
            return restore_for_requestids(
                sftp,
                request_to_path,
                temp_dir,
                progress_callback=progress_callback if show_progress else None,
                mute_output=not show_progress
            )
    
    def rerun_requests(
        self,
        request_ids: List[int],
        delete_checksums: bool = False,
        checksum_keys: Optional[List[str]] = None
    ) -> Dict[str, object]:
        """
        Re-trigger runs for requests with optional checksum deletion.
        
        Args:
            request_ids: List of request IDs
            delete_checksums: Whether to delete checksums first
            checksum_keys: Optional specific checksum keys to delete
            
        Returns:
            Dictionary with operation results
        """
        username, password = self.get_db_credentials()
        
        with self.db_manager.get_session(username, password) as session:
            # Get districts and directory paths for requests
            districts = []
            dir_paths = []
            
            from integration_tools.models import Request
            
            for req_id in request_ids:
                r = session.query(Request).filter(Request.RequestID == req_id).first()
                if not r:
                    print(f"Skip {req_id}: Request not found")
                    continue
                
                db_path = r.ImportedFileName or self.db_manager.get_directory_path_for_request(session, r.RequestID)
                if not db_path:
                    print(f"Skip {req_id}: missing DirectoryPath")
                    continue
                
                districts.append(r.DistrictID)
                dir_paths.append(db_path)
            
            deleted = 0
            if delete_checksums:
                deleted = self.db_manager.clear_checksums(session, districts, dir_paths, checksum_keys)
            
            updated = self.db_manager.bump_latest_queue(session, districts, dir_paths)
            
            return {
                "request_ids": request_ids,
                "checksums_deleted": deleted if delete_checksums else 0,
                "queues_updated": updated,
            }
    
    def clear_saved_credentials(self, credential_type: Optional[str] = None) -> None:
        """Clear saved credentials and reset cache."""
        self.credential_manager.clear_saved_credentials(credential_type)
        if credential_type is None or credential_type == "db":
            self._cached_db_credentials = None
        if credential_type is None or credential_type == "sftp":
            self._cached_sftp_credentials = None