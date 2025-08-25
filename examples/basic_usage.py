"""
Basic usage examples for integration tools.
"""

import asyncio
from integration_tools import RequestManager
from integration_tools.workflows.common_workflows import CommonWorkflows


def basic_request_operations():
    """Demonstrate basic request operations."""
    print("=== Basic Request Operations ===")
    
    # Initialize request manager
    rm = RequestManager()
    
    # List request types
    print("Available Request Types:")
    types = rm.list_request_types("SAT")
    for type_id, name in types[:5]:  # Show first 5
        print(f"  {type_id}: {name}")
    
    # Find requests
    print("\nFinding SAT requests:")
    requests = rm.find_requests(type_names=["SAT"])
    for request in requests[:3]:  # Show first 3
        print(f"  RequestID: {request.RequestID}, District: {request.DistrictID}, Status: {request.Status}")
    
    return requests


def download_files_example(request_ids):
    """Demonstrate file download."""
    print("\n=== File Download Example ===")
    
    rm = RequestManager()
    
    # Download files for specific requests
    results = rm.download_files(request_ids, "~/Downloads/integration_test")
    
    for req_id, result in results.items():
        status = "✓" if result["success"] else "✗"
        print(f"  {status} RequestID {req_id}: {result['files_downloaded']} files - {result['message']}")


async def workflow_examples():
    """Demonstrate workflow usage."""
    print("\n=== Workflow Examples ===")
    
    workflows = CommonWorkflows()
    
    # District refresh workflow
    print("Running district refresh workflow...")
    result = await workflows.district_refresh_workflow(
        district_ids=[123, 456],  # Replace with real district IDs
        type_names=["SAT", "PSAT"],
        delete_checksums=True,
        restore_files=False,  # Skip file restore for example
        show_progress=True
    )
    
    if result.success:
        print(f"✓ Workflow completed: {result.message}")
        print(f"  Requests processed: {result.data.get('requests_found', 0)}")
    else:
        print(f"✗ Workflow failed: {result.message}")
    
    # Monitoring workflow
    print("\nRunning monitoring workflow...")
    monitor_result = await workflows.integration_monitoring_workflow(
        integration_types=["SAT", "PSAT"],
        show_progress=True
    )
    
    if monitor_result.success:
        data = monitor_result.data
        print(f"✓ Monitoring complete")
        print(f"  Success Rate: {data['success_rate']}%")
        print(f"  Total Requests: {data['total_requests']}")
        print(f"  Districts with Issues: {data['summary']['districts_with_issues']}")


def error_handling_example():
    """Demonstrate error handling."""
    print("\n=== Error Handling Example ===")
    
    rm = RequestManager()
    
    try:
        # This might fail due to network issues, credentials, etc.
        requests = rm.find_requests(type_names=["NONEXISTENT_TYPE"])
        print(f"Found {len(requests)} requests")
    except Exception as e:
        print(f"Handled error gracefully: {e}")


async def main():
    """Run all examples."""
    print("Integration Tools - Usage Examples")
    print("=" * 50)
    
    # Basic operations
    requests = basic_request_operations()
    
    # File operations (if we found some requests)
    if requests:
        sample_request_ids = [r.RequestID for r in requests[:2]]
        download_files_example(sample_request_ids)
    
    # Workflows
    await workflow_examples()
    
    # Error handling
    error_handling_example()
    
    print("\n" + "=" * 50)
    print("Examples completed!")


if __name__ == "__main__":
    # Run async examples
    asyncio.run(main())