"""
Async Request Manager for handling large batch operations efficiently.
"""

import asyncio
from typing import Dict, List, Optional

from .request_manager import RequestManager


class AsyncRequestManager(RequestManager):
    """Async version of RequestManager for batch operations."""
    
    async def download_files_batch(
        self, 
        request_ids: List[int], 
        local_dir: str = "~/Downloads",
        max_concurrent: int = 5,
        show_progress: bool = True
    ) -> Dict[int, Dict[str, object]]:
        """
        Download files for multiple requests concurrently.
        
        Args:
            request_ids: List of request IDs
            local_dir: Local directory to download to
            max_concurrent: Maximum concurrent downloads
            show_progress: Whether to show progress
            
        Returns:
            Dictionary mapping request_id to download results
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def download_single(req_id: int) -> tuple:
            async with semaphore:
                # Run the sync download in a thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, 
                    lambda: self.download_files([req_id], local_dir, show_progress=False)
                )
                return req_id, result[req_id] if req_id in result else {"success": False, "message": "Unknown error"}
        
        if show_progress:
            print(f"Starting batch download of {len(request_ids)} requests with max {max_concurrent} concurrent...")
        
        # Execute all downloads concurrently
        tasks = [download_single(req_id) for req_id in request_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        final_results = {}
        success_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                print(f"Error in batch download: {result}")
                continue
            
            req_id, download_result = result
            final_results[req_id] = download_result
            if download_result.get("success"):
                success_count += 1
        
        if show_progress:
            print(f"Batch download complete: {success_count}/{len(request_ids)} successful")
        
        return final_results
    
    async def restore_files_batch(
        self,
        request_ids: List[int],
        temp_dir: str = "/tmp",
        max_concurrent: int = 3,
        show_progress: bool = True
    ) -> Dict[int, Dict[str, object]]:
        """
        Restore files for multiple requests concurrently.
        
        Args:
            request_ids: List of request IDs
            temp_dir: Temporary directory for processing
            max_concurrent: Maximum concurrent restores (lower due to SFTP limitations)
            show_progress: Whether to show progress
            
        Returns:
            Dictionary mapping request_id to restore results
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def restore_single(req_id: int) -> tuple:
            async with semaphore:
                # Run the sync restore in a thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, 
                    lambda: self.restore_files([req_id], temp_dir, show_progress=False)
                )
                return req_id, result[req_id] if req_id in result else {"success": False, "message": "Unknown error"}
        
        if show_progress:
            print(f"Starting batch restore of {len(request_ids)} requests with max {max_concurrent} concurrent...")
        
        # Execute all restores concurrently
        tasks = [restore_single(req_id) for req_id in request_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        final_results = {}
        success_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                print(f"Error in batch restore: {result}")
                continue
            
            req_id, restore_result = result
            final_results[req_id] = restore_result
            if restore_result.get("success"):
                success_count += 1
        
        if show_progress:
            print(f"Batch restore complete: {success_count}/{len(request_ids)} successful")
        
        return final_results
    
    async def batch_rerun_by_district(
        self,
        district_ids: List[int],
        type_names: Optional[List[str]] = None,
        delete_checksums: bool = False,
        checksum_keys: Optional[List[str]] = None,
        show_progress: bool = True
    ) -> Dict[str, object]:
        """
        Find and rerun latest requests for multiple districts.
        
        Args:
            district_ids: List of district IDs
            type_names: Optional list of type names to filter by
            delete_checksums: Whether to delete checksums first
            checksum_keys: Optional specific checksum keys to delete
            show_progress: Whether to show progress
            
        Returns:
            Dictionary with operation results
        """
        if show_progress:
            print(f"Finding latest requests for {len(district_ids)} districts...")
        
        # Find requests for all districts
        requests = self.find_requests(
            district_ids=district_ids,
            type_names=type_names
        )
        
        if not requests:
            return {
                "districts": district_ids,
                "requests_found": 0,
                "checksums_deleted": 0,
                "queues_updated": 0,
            }
        
        request_ids = [r.RequestID for r in requests]
        
        if show_progress:
            print(f"Found {len(request_ids)} requests to rerun")
        
        # Run the rerun operation
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.rerun_requests(request_ids, delete_checksums, checksum_keys)
        )
        
        result["districts"] = district_ids
        result["requests_found"] = len(request_ids)
        
        return result