# API Reference

## Core Classes

### RequestManager

The main class for managing request lifecycle operations.

```python
from integration_tools import RequestManager

rm = RequestManager()
```

#### Methods

##### `list_request_types(name_filter=None)`
List all DataRequestTypes with optional filtering.

**Parameters:**
- `name_filter` (str, optional): Filter by name contains

**Returns:**
- `List[Tuple[int, str]]`: List of (DataRequestTypeID, Name) tuples

##### `find_requests(type_ids=None, type_names=None, district_ids=None, statuses=(4,5))`
Find latest requests by various criteria.

**Parameters:**
- `type_ids` (List[int], optional): List of DataRequestTypeIDs
- `type_names` (List[str], optional): List of type name prefixes
- `district_ids` (List[int], optional): List of DistrictIDs
- `statuses` (Sequence[int]): Request statuses to include

**Returns:**
- `List[RequestRow]`: List of matching requests

##### `download_files(request_ids, local_dir="~/Downloads", show_progress=True)`
Download files for multiple requests.

**Parameters:**
- `request_ids` (List[int]): List of request IDs
- `local_dir` (str): Local directory to download to
- `show_progress` (bool): Whether to show progress

**Returns:**
- `Dict[int, Dict[str, object]]`: Download results by request ID

##### `restore_files(request_ids, temp_dir="/tmp", show_progress=True)`
Restore processed files for requests back to their SFTP directories.

**Parameters:**
- `request_ids` (List[int]): List of request IDs
- `temp_dir` (str): Temporary directory for processing
- `show_progress` (bool): Whether to show progress

**Returns:**
- `Dict[int, Dict[str, object]]`: Restore results by request ID

##### `rerun_requests(request_ids, delete_checksums=False, checksum_keys=None)`
Re-trigger runs for requests with optional checksum deletion.

**Parameters:**
- `request_ids` (List[int]): List of request IDs
- `delete_checksums` (bool): Whether to delete checksums first
- `checksum_keys` (List[str], optional): Specific checksum keys to delete

**Returns:**
- `Dict[str, object]`: Operation results

### AsyncRequestManager

Async version of RequestManager for batch operations.

```python
from integration_tools.core.async_request_manager import AsyncRequestManager
import asyncio

rm = AsyncRequestManager()
results = asyncio.run(rm.download_files_batch([123, 456]))
```

#### Methods

##### `async download_files_batch(request_ids, local_dir="~/Downloads", max_concurrent=5, show_progress=True)`
Download files for multiple requests concurrently.

##### `async restore_files_batch(request_ids, temp_dir="/tmp", max_concurrent=3, show_progress=True)`
Restore files for multiple requests concurrently.

##### `async batch_rerun_by_district(district_ids, type_names=None, delete_checksums=False, checksum_keys=None, show_progress=True)`
Find and rerun latest requests for multiple districts.

### CommonWorkflows

Collection of common request workflows.

```python
from integration_tools.workflows.common_workflows import CommonWorkflows
import asyncio

workflows = CommonWorkflows()
result = asyncio.run(workflows.district_refresh_workflow([123, 456]))
```

#### Methods

##### `async district_refresh_workflow(district_ids, type_names=["SAT", "PSAT"], delete_checksums=True, restore_files=True, show_progress=True)`
Complete district refresh workflow.

**Workflow Steps:**
1. Find latest requests for districts
2. Restore files to SFTP (optional)
3. Clear checksums
4. Rerun requests

##### `async bulk_file_download_workflow(type_names, district_ids=None, local_dir="~/Downloads/bulk_download", max_concurrent=5, show_progress=True)`
Bulk download workflow.

**Workflow Steps:**
1. Find all matching requests
2. Download files concurrently
3. Generate summary report

##### `async integration_monitoring_workflow(integration_types=["SAT", "PSAT"], days_back=7, show_progress=True)`
Integration monitoring workflow.

**Workflow Steps:**
1. Find recent requests for integration types
2. Analyze success/failure rates
3. Identify districts with issues
4. Generate monitoring report

## Data Classes

### RequestRow

Data class for request query results.

**Attributes:**
- `RequestID` (int): Request ID
- `DistrictID` (int): District ID
- `DataRequestTypeID` (int): Data request type ID
- `DataRequestTypeName` (str): Data request type name
- `ImportedFileName` (Optional[str]): Imported file name
- `Status` (Optional[int]): Request status
- `RequestTime` (Optional[datetime]): Request timestamp

### WorkflowResult

Result of a workflow execution.

**Attributes:**
- `success` (bool): Whether the workflow succeeded
- `message` (str): Result message
- `data` (Dict[str, object]): Additional result data
- `steps_completed` (int): Number of steps completed
- `total_steps` (int): Total number of steps

## Error Handling

### Custom Exceptions

- `RetryableError`: Base class for errors that can be retried
- `DatabaseConnectionError`: Database connection related errors
- `SFTPConnectionError`: SFTP connection related errors
- `FileOperationError`: File operation related errors

### Decorators

##### `@retry_with_backoff(max_retries=3, initial_delay=1.0, backoff_factor=2.0, exceptions=(RetryableError,))`
Decorator for retrying operations with exponential backoff.

##### `@convert_database_errors`
Decorator to convert SQLAlchemy errors to custom error types.

##### `@convert_sftp_errors`
Decorator to convert paramiko SFTP errors to custom error types.

## Utilities

### CredentialManager

Manages credentials for database and SFTP connections.

```python
from integration_tools.core.credential_manager import CredentialManager

cm = CredentialManager()
username, password = cm.get_db_credentials()
host, user, pwd = cm.get_sftp_credentials()
```

### FileManager

Manages SFTP connections and file operations.

```python
from integration_tools.core.file_manager import FileManager

with FileManager() as fm:
    sftp = fm.create_sftp_connection("host.com", "user", "pass")
    # Use sftp client...
```

### DatabaseManager

Manages database connections and provides query utilities.

```python
from integration_tools.core.db_manager import DatabaseManager

db = DatabaseManager()
with db.get_session("user", "pass") as session:
    types = db.list_request_types(session)
```