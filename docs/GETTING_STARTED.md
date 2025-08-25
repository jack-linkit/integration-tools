# Getting Started

## Installation

### From Source (Development)

```bash
git clone <repository-url>
cd integration-tools
pip install -e ".[dev]"
```

### Production Install

```bash
pip install integration-tools
```

## Quick Start

### 1. Basic Request Operations

```python
from integration_tools import RequestManager

# Initialize request manager
rm = RequestManager()

# List available request types
types = rm.list_request_types("SAT")
print(f"Found {len(types)} SAT-related request types")

# Find recent requests
requests = rm.find_requests(type_names=["SAT", "PSAT"], district_ids=[123, 456])
print(f"Found {len(requests)} recent requests")

# Download files for specific requests
if requests:
    request_ids = [r.RequestID for r in requests[:3]]
    results = rm.download_files(request_ids, "~/Downloads/integration_files")
    
    successful = sum(1 for r in results.values() if r["success"])
    print(f"Downloaded files for {successful}/{len(request_ids)} requests")
```

### 2. Using the Enhanced CLI

The enhanced CLI provides rich output and batch operations:

```bash
# List request types with filtering
integration-tools list-types --filter SAT

# Find requests with rich table output
integration-tools find-requests --type-names SAT,PSAT --district-ids 123,456

# Download files with progress tracking
integration-tools download 123,456,789 --local-dir ~/Downloads/test --max-concurrent 3

# Restore files back to SFTP
integration-tools restore 123,456,789 --max-concurrent 2

# Rerun requests with checksum deletion
integration-tools rerun 123,456 --delete-checksums
```

### 3. Workflow Operations

Use predefined workflows for common operations:

```bash
# Complete district refresh
integration-tools workflow district-refresh 123,456 --type-names SAT,PSAT

# Bulk download all SAT files
integration-tools workflow bulk-download SAT --max-concurrent 5

# Monitor integration health
integration-tools workflow monitor --integration-types SAT,PSAT --json-output
```

### 4. Async Batch Operations

For large-scale operations, use the async request manager:

```python
import asyncio
from integration_tools.core.async_request_manager import AsyncRequestManager

async def batch_example():
    rm = AsyncRequestManager()
    
    # Find requests
    requests = rm.find_requests(type_names=["SAT"], district_ids=[123, 456, 789])
    request_ids = [r.RequestID for r in requests]
    
    # Batch download with controlled concurrency
    download_results = await rm.download_files_batch(
        request_ids, 
        max_concurrent=5,
        show_progress=True
    )
    
    # Batch restore
    restore_results = await rm.restore_files_batch(
        request_ids,
        max_concurrent=2,
        show_progress=True
    )
    
    return download_results, restore_results

# Run the async example
download_results, restore_results = asyncio.run(batch_example())
```

### 5. Custom Workflows

Create custom workflows for specific needs:

```python
import asyncio
from integration_tools.workflows.common_workflows import CommonWorkflows

async def custom_maintenance():
    workflows = CommonWorkflows()
    
    # District refresh workflow
    result = await workflows.district_refresh_workflow(
        district_ids=[123, 456],
        type_names=["SAT", "PSAT"],
        delete_checksums=True,
        restore_files=True,
        show_progress=True
    )
    
    if result.success:
        print(f"✓ Maintenance completed: {result.message}")
        print(f"  Requests processed: {result.data.get('requests_found', 0)}")
    else:
        print(f"✗ Maintenance failed: {result.message}")

asyncio.run(custom_maintenance())
```

## Configuration

### Environment Variables

Set these environment variables to avoid credential prompts:

```bash
export DB_UID="your_database_username"
export DB_PWD="your_database_password"
```

### Credential Storage

The tool can save credentials securely in your system keychain:

```python
from integration_tools.core.credential_manager import CredentialManager

cm = CredentialManager()

# First time - will prompt and offer to save
username, password = cm.get_db_credentials()

# Subsequent times - will use saved credentials
username, password = cm.get_db_credentials()

# Clear saved credentials
cm.clear_saved_credentials("db")  # or "sftp" or None for both
```

### Logging

Configure logging for debugging and monitoring:

```python
from integration_tools.core.error_handling import setup_logging

# Setup logging
logger = setup_logging(level="DEBUG", log_file="integration_tools.log")

# Use with request manager
rm = RequestManager()
# All operations will now be logged
```

## Common Patterns

### Error Handling

The library provides robust error handling with automatic retries:

```python
from integration_tools.core.error_handling import retry_with_backoff, DatabaseConnectionError

@retry_with_backoff(max_retries=3)
def reliable_operation():
    # This will automatically retry on transient errors
    rm = RequestManager()
    return rm.find_requests(type_names=["SAT"])

try:
    requests = reliable_operation()
except DatabaseConnectionError as e:
    print(f"Database connection failed after retries: {e}")
```

### Progress Tracking

Most operations support progress tracking:

```python
# Enable progress for all operations
rm = RequestManager()
results = rm.download_files(request_ids, show_progress=True)

# Custom progress callback for batch operations
async def custom_progress(done, total, req_id, success, count, message):
    print(f"[{done}/{total}] RequestID {req_id}: {message}")

results = await rm.download_files_batch(
    request_ids,
    show_progress=True  # Uses built-in progress display
)
```

### Resource Management

Use context managers for proper resource cleanup:

```python
from integration_tools.core.file_manager import FileManager

# SFTP connection is automatically closed
with FileManager() as fm:
    sftp = fm.create_sftp_connection("host.com", "user", "pass")
    # Use sftp...
# Connection closed here

# Database sessions are automatically managed
with db_manager.get_session("user", "pass") as session:
    results = db_manager.list_request_types(session)
# Session closed here
```

## Next Steps

- Check out the [API Reference](API_REFERENCE.md) for detailed documentation
- See [examples/](../examples/) for more usage examples
- Review [WORKFLOWS.md](WORKFLOWS.md) for workflow-specific documentation
- Read [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues