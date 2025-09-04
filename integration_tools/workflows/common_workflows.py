"""
Common request workflows for frequent operations.
"""

import json
from dataclasses import dataclass
from typing import Dict, List, Optional

from integration_tools.core.async_request_manager import AsyncRequestManager


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    success: bool
    message: str
    data: Dict[str, object]
    steps_completed: int
    total_steps: int


class CommonWorkflows:
    """Collection of common request workflows."""
    
    def __init__(self, request_manager: Optional[AsyncRequestManager] = None):
        self.request_manager = request_manager or AsyncRequestManager()
    
    async def district_refresh_workflow(
        self,
        district_ids: List[int],
        type_names: List[str] = ["SAT", "PSAT"],
        delete_checksums: bool = True,
        restore_files: bool = True,
        show_progress: bool = True
    ) -> WorkflowResult:
        """
        Complete district refresh workflow:
        1. Find latest requests for districts
        2. Restore files to SFTP (optional)
        3. Clear checksums
        4. Rerun requests
        
        Args:
            district_ids: List of district IDs to refresh
            type_names: Request types to include
            delete_checksums: Whether to delete checksums
            restore_files: Whether to restore files first
            show_progress: Whether to show progress
            
        Returns:
            WorkflowResult with complete operation results
        """
        total_steps = 3 if restore_files else 2
        completed_steps = 0
        workflow_data = {}
        
        try:
            # Step 1: Find requests
            if show_progress:
                print("Step 1: Finding latest requests...")
            
            requests = self.request_manager.find_requests(
                district_ids=district_ids,
                type_names=type_names
            )
            
            if not requests:
                return WorkflowResult(
                    success=False,
                    message="No requests found for the specified criteria",
                    data={"districts": district_ids, "requests_found": 0},
                    steps_completed=0,
                    total_steps=total_steps
                )
            
            request_ids = [r.RequestID for r in requests]
            workflow_data["requests_found"] = len(request_ids)
            workflow_data["request_ids"] = request_ids
            
            if show_progress:
                print(f"Found {len(request_ids)} requests across {len(district_ids)} districts")
            
            completed_steps += 1
            
            # Step 2: Restore files (optional)
            if restore_files:
                if show_progress:
                    print("Step 2: Restoring files to SFTP...")
                
                restore_results = await self.request_manager.restore_files_batch(
                    request_ids, show_progress=show_progress
                )
                workflow_data["restore_results"] = restore_results
                
                successful_restores = sum(1 for r in restore_results.values() if r.get("success"))
                if show_progress:
                    print(f"Restored files for {successful_restores}/{len(request_ids)} requests")
                
                completed_steps += 1
            
            # Step 3: Rerun with checksum deletion
            if show_progress:
                step_num = 3 if restore_files else 2
                print(f"Step {step_num}: Rerunning requests{'with checksum deletion' if delete_checksums else ''}...")
            
            rerun_result = self.request_manager.rerun_requests(
                request_ids,
                delete_checksums=delete_checksums
            )
            workflow_data["rerun_result"] = rerun_result
            
            if show_progress:
                print(f"Updated {rerun_result['queues_updated']} queue entries")
                if delete_checksums:
                    print(f"Deleted {rerun_result['checksums_deleted']} checksum entries")
            
            completed_steps += 1
            
            return WorkflowResult(
                success=True,
                message=f"Successfully completed district refresh for {len(district_ids)} districts",
                data=workflow_data,
                steps_completed=completed_steps,
                total_steps=total_steps
            )
            
        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Workflow failed at step {completed_steps + 1}: {str(e)}",
                data=workflow_data,
                steps_completed=completed_steps,
                total_steps=total_steps
            )
    
    async def bulk_file_download_workflow(
        self,
        type_names: List[str],
        district_ids: Optional[List[int]] = None,
        local_dir: str = "~/Downloads/bulk_download",
        max_concurrent: int = 5,
        show_progress: bool = True
    ) -> WorkflowResult:
        """
        Bulk download workflow:
        1. Find all matching requests
        2. Download files concurrently
        3. Generate summary report
        
        Args:
            type_names: Request types to download
            district_ids: Optional district filter
            local_dir: Local directory for downloads
            max_concurrent: Maximum concurrent downloads
            show_progress: Whether to show progress
            
        Returns:
            WorkflowResult with download results
        """
        workflow_data = {}
        
        try:
            # Step 1: Find requests
            if show_progress:
                print("Step 1: Finding matching requests...")
            
            requests = self.request_manager.find_requests(
                type_names=type_names,
                district_ids=district_ids
            )
            
            if not requests:
                return WorkflowResult(
                    success=False,
                    message="No requests found for the specified criteria",
                    data={"type_names": type_names, "district_ids": district_ids},
                    steps_completed=0,
                    total_steps=2
                )
            
            request_ids = [r.RequestID for r in requests]
            workflow_data["requests_found"] = len(request_ids)
            workflow_data["request_details"] = [
                {
                    "RequestID": r.RequestID,
                    "DistrictID": r.DistrictID,
                    "TypeName": r.DataRequestTypeName,
                    "Status": r.Status
                }
                for r in requests
            ]
            
            # Step 2: Download files
            if show_progress:
                print(f"Step 2: Downloading files for {len(request_ids)} requests...")
            
            download_results = await self.request_manager.download_files_batch(
                request_ids,
                local_dir=local_dir,
                max_concurrent=max_concurrent,
                show_progress=show_progress
            )
            
            workflow_data["download_results"] = download_results
            
            # Generate summary
            successful_downloads = sum(1 for r in download_results.values() if r.get("success"))
            total_files = sum(r.get("files_downloaded", 0) for r in download_results.values())
            
            workflow_data["summary"] = {
                "successful_requests": successful_downloads,
                "total_requests": len(request_ids),
                "total_files_downloaded": total_files
            }
            
            return WorkflowResult(
                success=True,
                message=f"Downloaded {total_files} files from {successful_downloads}/{len(request_ids)} requests",
                data=workflow_data,
                steps_completed=2,
                total_steps=2
            )
            
        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Bulk download workflow failed: {str(e)}",
                data=workflow_data,
                steps_completed=1,
                total_steps=2
            )
    
    async def integration_monitoring_workflow(
        self,
        integration_types: List[str] = ["SAT", "PSAT"],
        days_back: int = 7,
        show_progress: bool = True
    ) -> WorkflowResult:
        """
        Integration monitoring workflow:
        1. Find recent requests for integration types
        2. Analyze success/failure rates
        3. Identify districts with issues
        4. Generate monitoring report
        
        Args:
            integration_types: Integration types to monitor
            days_back: Number of days to look back
            show_progress: Whether to show progress
            
        Returns:
            WorkflowResult with monitoring data
        """
        workflow_data = {}
        
        try:
            if show_progress:
                print(f"Monitoring {', '.join(integration_types)} integrations for last {days_back} days...")
            
            # Find all recent requests
            all_requests = self.request_manager.find_requests(
                type_names=integration_types,
                statuses=[4, 5]  # Failed and successful
            )
            
            if not all_requests:
                return WorkflowResult(
                    success=True,
                    message="No recent requests found for monitoring",
                    data={"integration_types": integration_types, "requests_found": 0},
                    steps_completed=1,
                    total_steps=1
                )
            
            # Analyze requests by status and type
            analysis = {
                "by_status": {"success": 0, "failed": 0},
                "by_type": {},
                "by_district": {},
                "failed_requests": [],
                "districts_with_failures": set()
            }
            
            for request in all_requests:
                # Status analysis
                if request.Status == 5:
                    analysis["by_status"]["success"] += 1
                elif request.Status == 4:
                    analysis["by_status"]["failed"] += 1
                    analysis["failed_requests"].append({
                        "RequestID": request.RequestID,
                        "DistrictID": request.DistrictID,
                        "TypeName": request.DataRequestTypeName,
                        "RequestTime": request.RequestTime.isoformat() if request.RequestTime else None,
                        "RequestTimeEST": request.RequestTimeEST.isoformat() if hasattr(request, 'RequestTimeEST') and request.RequestTimeEST else None
                    })
                    analysis["districts_with_failures"].add(request.DistrictID)
                
                # Type analysis
                type_name = request.DataRequestTypeName
                if type_name not in analysis["by_type"]:
                    analysis["by_type"][type_name] = {"success": 0, "failed": 0}
                
                if request.Status == 5:
                    analysis["by_type"][type_name]["success"] += 1
                elif request.Status == 4:
                    analysis["by_type"][type_name]["failed"] += 1
                
                # District analysis
                district_id = request.DistrictID
                if district_id not in analysis["by_district"]:
                    analysis["by_district"][district_id] = {"success": 0, "failed": 0}
                
                if request.Status == 5:
                    analysis["by_district"][district_id]["success"] += 1
                elif request.Status == 4:
                    analysis["by_district"][district_id]["failed"] += 1
            
            # Convert set to list for JSON serialization
            analysis["districts_with_failures"] = list(analysis["districts_with_failures"])
            
            # Calculate rates
            total_requests = len(all_requests)
            success_rate = (analysis["by_status"]["success"] / total_requests * 100) if total_requests > 0 else 0
            
            workflow_data = {
                "total_requests": total_requests,
                "success_rate": round(success_rate, 2),
                "analysis": analysis,
                "summary": {
                    "healthy_districts": len([d for d, stats in analysis["by_district"].items() if stats["failed"] == 0]),
                    "districts_with_issues": len(analysis["districts_with_failures"]),
                    "most_problematic_type": max(analysis["by_type"].keys(), 
                                               key=lambda k: analysis["by_type"][k]["failed"]) if analysis["by_type"] else None
                }
            }
            
            if show_progress:
                print(f"Analysis complete: {success_rate:.1f}% success rate across {total_requests} requests")
                print(f"Districts with failures: {len(analysis['districts_with_failures'])}")
            
            return WorkflowResult(
                success=True,
                message=f"Monitoring complete: {success_rate:.1f}% success rate",
                data=workflow_data,
                steps_completed=1,
                total_steps=1
            )
            
        except Exception as e:
            return WorkflowResult(
                success=False,
                message=f"Monitoring workflow failed: {str(e)}",
                data=workflow_data,
                steps_completed=0,
                total_steps=1
            )