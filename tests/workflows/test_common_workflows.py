"""
Tests for common workflows functionality.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from integration_tools.workflows.common_workflows import CommonWorkflows, WorkflowResult
from integration_tools.core.db_manager import RequestRow
from datetime import datetime


class TestCommonWorkflows:
    """Test cases for CommonWorkflows."""
    
    def test_init(self):
        """Test workflow initialization."""
        workflows = CommonWorkflows()
        assert workflows.request_manager is not None
    
    def test_init_with_custom_manager(self):
        """Test workflow initialization with custom request manager."""
        mock_manager = MagicMock()
        workflows = CommonWorkflows(mock_manager)
        assert workflows.request_manager == mock_manager
    
    @pytest.mark.asyncio
    async def test_district_refresh_workflow_success(self):
        """Test successful district refresh workflow."""
        mock_manager = AsyncMock()
        
        # Mock request finding
        sample_requests = [
            RequestRow(123, 456, 66, "SAT", "test_path", 5, datetime.now()),
            RequestRow(124, 456, 67, "PSAT", "test_path2", 5, datetime.now())
        ]
        mock_manager.find_requests.return_value = sample_requests
        
        # Mock restore files
        mock_manager.restore_files_batch.return_value = {
            123: {"success": True, "files_moved": 2},
            124: {"success": True, "files_moved": 1}
        }
        
        # Mock rerun
        mock_manager.rerun_requests.return_value = {
            "request_ids": [123, 124],
            "checksums_deleted": 5,
            "queues_updated": 2
        }
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.district_refresh_workflow(
            district_ids=[456],
            type_names=["SAT", "PSAT"],
            delete_checksums=True,
            restore_files=True,
            show_progress=False
        )
        
        assert result.success is True
        assert result.steps_completed == 3
        assert result.total_steps == 3
        assert "Successfully completed district refresh" in result.message
        assert result.data["requests_found"] == 2
        assert result.data["request_ids"] == [123, 124]
    
    @pytest.mark.asyncio
    async def test_district_refresh_workflow_no_requests(self):
        """Test district refresh workflow when no requests are found."""
        mock_manager = AsyncMock()
        mock_manager.find_requests.return_value = []
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.district_refresh_workflow(
            district_ids=[456],
            show_progress=False
        )
        
        assert result.success is False
        assert "No requests found" in result.message
        assert result.steps_completed == 0
        assert result.data["requests_found"] == 0
    
    @pytest.mark.asyncio
    async def test_district_refresh_workflow_without_restore(self):
        """Test district refresh workflow without file restore."""
        mock_manager = AsyncMock()
        
        sample_requests = [
            RequestRow(123, 456, 66, "SAT", "test_path", 5, datetime.now())
        ]
        mock_manager.find_requests.return_value = sample_requests
        mock_manager.rerun_requests.return_value = {
            "request_ids": [123],
            "checksums_deleted": 2,
            "queues_updated": 1
        }
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.district_refresh_workflow(
            district_ids=[456],
            restore_files=False,
            show_progress=False
        )
        
        assert result.success is True
        assert result.steps_completed == 2
        assert result.total_steps == 2  # No restore step
        mock_manager.restore_files_batch.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_district_refresh_workflow_exception(self):
        """Test district refresh workflow with exception."""
        mock_manager = AsyncMock()
        mock_manager.find_requests.side_effect = Exception("Database error")
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.district_refresh_workflow(
            district_ids=[456],
            show_progress=False
        )
        
        assert result.success is False
        assert "Workflow failed at step 1" in result.message
        assert "Database error" in result.message
        assert result.steps_completed == 0
    
    @pytest.mark.asyncio
    async def test_bulk_file_download_workflow_success(self):
        """Test successful bulk file download workflow."""
        mock_manager = AsyncMock()
        
        sample_requests = [
            RequestRow(123, 456, 66, "SAT", "test_path", 5, datetime.now()),
            RequestRow(124, 457, 67, "PSAT", "test_path2", 5, datetime.now())
        ]
        mock_manager.find_requests.return_value = sample_requests
        
        # Mock download results
        mock_manager.download_files_batch.return_value = {
            123: {"success": True, "files_downloaded": 3},
            124: {"success": True, "files_downloaded": 2}
        }
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.bulk_file_download_workflow(
            type_names=["SAT", "PSAT"],
            district_ids=[456, 457],
            show_progress=False
        )
        
        assert result.success is True
        assert result.steps_completed == 2
        assert result.total_steps == 2
        assert "Downloaded 5 files from 2/2 requests" in result.message
        assert result.data["summary"]["successful_requests"] == 2
        assert result.data["summary"]["total_files_downloaded"] == 5
    
    @pytest.mark.asyncio
    async def test_bulk_file_download_workflow_no_requests(self):
        """Test bulk file download workflow with no matching requests."""
        mock_manager = AsyncMock()
        mock_manager.find_requests.return_value = []
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.bulk_file_download_workflow(
            type_names=["SAT"],
            show_progress=False
        )
        
        assert result.success is False
        assert "No requests found" in result.message
        assert result.steps_completed == 0
        assert result.data["type_names"] == ["SAT"]
    
    @pytest.mark.asyncio
    async def test_bulk_file_download_workflow_partial_success(self):
        """Test bulk file download workflow with partial success."""
        mock_manager = AsyncMock()
        
        sample_requests = [
            RequestRow(123, 456, 66, "SAT", "test_path", 5, datetime.now()),
            RequestRow(124, 457, 67, "SAT", "test_path2", 5, datetime.now())
        ]
        mock_manager.find_requests.return_value = sample_requests
        
        # Mock partial download success
        mock_manager.download_files_batch.return_value = {
            123: {"success": True, "files_downloaded": 3},
            124: {"success": False, "files_downloaded": 0, "message": "SFTP error"}
        }
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.bulk_file_download_workflow(
            type_names=["SAT"],
            show_progress=False
        )
        
        assert result.success is True
        assert result.data["summary"]["successful_requests"] == 1
        assert result.data["summary"]["total_requests"] == 2
        assert result.data["summary"]["total_files_downloaded"] == 3
    
    @pytest.mark.asyncio
    async def test_integration_monitoring_workflow_success(self):
        """Test successful integration monitoring workflow."""
        mock_manager = AsyncMock()
        
        # Mock requests with mixed statuses
        sample_requests = [
            RequestRow(123, 456, 66, "SAT", "test_path", 5, datetime.now()),  # Success
            RequestRow(124, 456, 67, "PSAT", "test_path2", 5, datetime.now()),  # Success
            RequestRow(125, 457, 66, "SAT", "test_path3", 4, datetime.now()),  # Failed
            RequestRow(126, 457, 67, "PSAT", "test_path4", 5, datetime.now()),  # Success
        ]
        mock_manager.find_requests.return_value = sample_requests
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.integration_monitoring_workflow(
            integration_types=["SAT", "PSAT"],
            show_progress=False
        )
        
        assert result.success is True
        assert result.data["total_requests"] == 4
        assert result.data["success_rate"] == 75.0  # 3/4 successful
        
        # Check analysis details
        analysis = result.data["analysis"]
        assert analysis["by_status"]["success"] == 3
        assert analysis["by_status"]["failed"] == 1
        assert len(analysis["failed_requests"]) == 1
        assert 457 in analysis["districts_with_failures"]  # District with failed request
        
        # Check summary
        summary = result.data["summary"]
        assert summary["healthy_districts"] == 1  # District 456 has no failures
        assert summary["districts_with_issues"] == 1  # District 457 has failures
    
    @pytest.mark.asyncio
    async def test_integration_monitoring_workflow_no_requests(self):
        """Test integration monitoring workflow with no requests."""
        mock_manager = AsyncMock()
        mock_manager.find_requests.return_value = []
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.integration_monitoring_workflow(
            show_progress=False
        )
        
        assert result.success is True
        assert "No recent requests found" in result.message
        assert result.data["requests_found"] == 0
    
    @pytest.mark.asyncio
    async def test_integration_monitoring_workflow_all_successful(self):
        """Test integration monitoring workflow with all successful requests."""
        mock_manager = AsyncMock()
        
        sample_requests = [
            RequestRow(123, 456, 66, "SAT", "test_path", 5, datetime.now()),
            RequestRow(124, 457, 67, "PSAT", "test_path2", 5, datetime.now()),
        ]
        mock_manager.find_requests.return_value = sample_requests
        
        workflows = CommonWorkflows(mock_manager)
        
        result = await workflows.integration_monitoring_workflow(
            show_progress=False
        )
        
        assert result.success is True
        assert result.data["success_rate"] == 100.0
        assert result.data["analysis"]["by_status"]["failed"] == 0
        assert len(result.data["analysis"]["districts_with_failures"]) == 0
        assert result.data["summary"]["districts_with_issues"] == 0


class TestWorkflowResult:
    """Test cases for WorkflowResult dataclass."""
    
    def test_workflow_result_creation(self):
        """Test creating a WorkflowResult instance."""
        result = WorkflowResult(
            success=True,
            message="Test message",
            data={"key": "value"},
            steps_completed=2,
            total_steps=3
        )
        
        assert result.success is True
        assert result.message == "Test message"
        assert result.data == {"key": "value"}
        assert result.steps_completed == 2
        assert result.total_steps == 3