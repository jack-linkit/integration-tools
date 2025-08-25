"""
Advanced workflow examples for integration tools.
"""

import asyncio
from integration_tools.core.async_request_manager import AsyncRequestManager
from integration_tools.workflows.common_workflows import CommonWorkflows


async def batch_operations_example():
    """Demonstrate batch operations with async support."""
    print("=== Batch Operations Example ===")
    
    # Use async request manager for better performance
    rm = AsyncRequestManager()
    
    # Find requests for multiple districts
    districts = [123, 456, 789]  # Replace with real district IDs
    requests = rm.find_requests(
        type_names=["SAT", "PSAT"],
        district_ids=districts
    )
    
    if not requests:
        print("No requests found for batch operations")
        return
    
    request_ids = [r.RequestID for r in requests]
    print(f"Found {len(request_ids)} requests for batch processing")
    
    # Batch download with concurrency control
    print("Starting batch download...")
    download_results = await rm.download_files_batch(
        request_ids,
        max_concurrent=5,
        show_progress=True
    )
    
    successful_downloads = sum(1 for r in download_results.values() if r["success"])
    print(f"Download completed: {successful_downloads}/{len(request_ids)} successful")
    
    # Batch restore with lower concurrency (SFTP limitations)
    print("\nStarting batch restore...")
    restore_results = await rm.restore_files_batch(
        request_ids,
        max_concurrent=2,
        show_progress=True
    )
    
    successful_restores = sum(1 for r in restore_results.values() if r["success"])
    print(f"Restore completed: {successful_restores}/{len(request_ids)} successful")


async def district_maintenance_workflow():
    """Comprehensive district maintenance workflow."""
    print("\n=== District Maintenance Workflow ===")
    
    workflows = CommonWorkflows()
    
    # Districts that need maintenance
    districts_to_maintain = [123, 456]  # Replace with real district IDs
    
    for district_id in districts_to_maintain:
        print(f"\nMaintaining District {district_id}...")
        
        # Step 1: Run monitoring to assess current state
        monitor_result = await workflows.integration_monitoring_workflow(
            integration_types=["SAT", "PSAT"],
            show_progress=False
        )
        
        if monitor_result.success:
            district_analysis = monitor_result.data["analysis"]["by_district"]
            district_stats = district_analysis.get(district_id, {"success": 0, "failed": 0})
            
            print(f"  Current state: {district_stats['success']} successful, {district_stats['failed']} failed")
            
            # Only proceed with refresh if there are failures
            if district_stats['failed'] > 0:
                print(f"  Running refresh workflow for {district_stats['failed']} failed requests...")
                
                refresh_result = await workflows.district_refresh_workflow(
                    district_ids=[district_id],
                    type_names=["SAT", "PSAT"],
                    delete_checksums=True,
                    restore_files=True,
                    show_progress=False
                )
                
                if refresh_result.success:
                    print(f"  ✓ District {district_id} refresh completed")
                else:
                    print(f"  ✗ District {district_id} refresh failed: {refresh_result.message}")
            else:
                print(f"  ✓ District {district_id} is healthy, skipping refresh")


async def bulk_analysis_workflow():
    """Analyze integration health across multiple types and districts."""
    print("\n=== Bulk Analysis Workflow ===")
    
    workflows = CommonWorkflows()
    
    # Analyze different integration types
    integration_types = ["SAT", "PSAT", "GenericBehavior"]
    analysis_results = {}
    
    for integration_type in integration_types:
        print(f"\nAnalyzing {integration_type} integration...")
        
        result = await workflows.integration_monitoring_workflow(
            integration_types=[integration_type],
            days_back=30,
            show_progress=False
        )
        
        if result.success:
            analysis_results[integration_type] = result.data
            print(f"  Success Rate: {result.data['success_rate']}%")
            print(f"  Total Requests: {result.data['total_requests']}")
            print(f"  Districts with Issues: {result.data['summary']['districts_with_issues']}")
        else:
            print(f"  ✗ Analysis failed: {result.message}")
    
    # Generate comparative report
    print("\n--- Integration Health Comparison ---")
    for int_type, data in analysis_results.items():
        print(f"{int_type:15} | {data['success_rate']:6.1f}% | {data['total_requests']:8d} requests | {data['summary']['districts_with_issues']:3d} problematic districts")


async def emergency_response_workflow():
    """Emergency response workflow for critical issues."""
    print("\n=== Emergency Response Workflow ===")
    
    workflows = CommonWorkflows()
    rm = AsyncRequestManager()
    
    # Identify districts with critical issues (high failure rate)
    monitor_result = await workflows.integration_monitoring_workflow(
        integration_types=["SAT", "PSAT"],
        show_progress=False
    )
    
    if not monitor_result.success:
        print("Cannot assess system health for emergency response")
        return
    
    # Find districts with >50% failure rate
    district_analysis = monitor_result.data["analysis"]["by_district"]
    critical_districts = []
    
    for district_id, stats in district_analysis.items():
        total = stats["success"] + stats["failed"]
        if total > 0:
            failure_rate = stats["failed"] / total
            if failure_rate > 0.5:  # More than 50% failure rate
                critical_districts.append(district_id)
    
    if not critical_districts:
        print("✓ No districts in critical state")
        return
    
    print(f"🚨 Found {len(critical_districts)} districts in critical state: {critical_districts}")
    
    # Emergency response for each critical district
    for district_id in critical_districts:
        print(f"\nEmergency response for District {district_id}:")
        
        # 1. Immediate rerun without restore (faster)
        print("  Step 1: Immediate rerun without file restore...")
        quick_refresh = await workflows.district_refresh_workflow(
            district_ids=[district_id],
            delete_checksums=True,
            restore_files=False,  # Skip restore for speed
            show_progress=False
        )
        
        if quick_refresh.success:
            print(f"  ✓ Quick refresh completed for District {district_id}")
            
            # 2. If quick refresh didn't work, try full restore
            print("  Step 2: Checking if full restore is needed...")
            
            # Re-check status after quick refresh
            recheck_result = await workflows.integration_monitoring_workflow(
                integration_types=["SAT", "PSAT"],
                show_progress=False
            )
            
            if recheck_result.success:
                updated_stats = recheck_result.data["analysis"]["by_district"].get(
                    district_id, {"success": 0, "failed": 0}
                )
                total = updated_stats["success"] + updated_stats["failed"]
                if total > 0:
                    updated_failure_rate = updated_stats["failed"] / total
                    
                    if updated_failure_rate > 0.3:  # Still high failure rate
                        print("  Step 3: Running full restore workflow...")
                        full_refresh = await workflows.district_refresh_workflow(
                            district_ids=[district_id],
                            delete_checksums=True,
                            restore_files=True,  # Full restore
                            show_progress=False
                        )
                        
                        if full_refresh.success:
                            print(f"  ✓ Full restore completed for District {district_id}")
                        else:
                            print(f"  ✗ Full restore failed for District {district_id}")
                            print(f"    Manual intervention required!")
                    else:
                        print(f"  ✓ District {district_id} recovered after quick refresh")
        else:
            print(f"  ✗ Emergency response failed for District {district_id}")
            print(f"    Error: {quick_refresh.message}")


async def custom_workflow_example():
    """Example of creating a custom workflow."""
    print("\n=== Custom Workflow Example ===")
    
    # Create a custom workflow for SAT-specific maintenance
    async def sat_specific_maintenance(district_ids, rm):
        """Custom workflow for SAT-specific maintenance."""
        print("Running SAT-specific maintenance...")
        
        # 1. Find SAT requests only
        sat_requests = rm.find_requests(
            type_names=["SAT"],
            district_ids=district_ids
        )
        
        if not sat_requests:
            return {"success": False, "message": "No SAT requests found"}
        
        # 2. Filter for recent failures
        failed_requests = [r for r in sat_requests if r.Status == 4]
        
        if not failed_requests:
            return {"success": True, "message": "No failed SAT requests found"}
        
        print(f"Found {len(failed_requests)} failed SAT requests")
        
        # 3. Batch restore files
        request_ids = [r.RequestID for r in failed_requests]
        restore_results = await rm.restore_files_batch(
            request_ids,
            show_progress=False
        )
        
        # 4. Rerun only the successfully restored requests
        successfully_restored = [
            req_id for req_id, result in restore_results.items() 
            if result.get("success")
        ]
        
        if successfully_restored:
            rerun_result = rm.rerun_requests(
                successfully_restored,
                delete_checksums=True
            )
            
            return {
                "success": True,
                "message": f"SAT maintenance completed for {len(successfully_restored)} requests",
                "requests_processed": len(successfully_restored),
                "queues_updated": rerun_result.get("queues_updated", 0)
            }
        else:
            return {"success": False, "message": "No requests could be restored"}
    
    # Run the custom workflow
    rm = AsyncRequestManager()
    result = await sat_specific_maintenance([123, 456], rm)  # Replace with real district IDs
    
    print(f"Custom workflow result: {result['message']}")
    if result["success"] and "requests_processed" in result:
        print(f"  Processed: {result['requests_processed']} requests")
        print(f"  Queue updates: {result['queues_updated']}")


async def main():
    """Run all advanced workflow examples."""
    print("Integration Tools - Advanced Workflow Examples")
    print("=" * 60)
    
    try:
        await batch_operations_example()
        await district_maintenance_workflow()
        await bulk_analysis_workflow()
        await emergency_response_workflow()
        await custom_workflow_example()
    except Exception as e:
        print(f"Example failed: {e}")
        print("Note: These examples require valid database credentials and data")
    
    print("\n" + "=" * 60)
    print("Advanced examples completed!")


if __name__ == "__main__":
    asyncio.run(main())