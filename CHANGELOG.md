# Changelog

All notable changes to integration-tools will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-XX

### Added
- Initial release of integration-tools
- Core request management functionality
- Async batch operations support
- Common workflows for district maintenance
- Enhanced CLI with rich terminal UI
- Comprehensive test suite
- Complete documentation

### Features
- **RequestManager**: Main orchestration class for request operations
- **AsyncRequestManager**: Concurrent batch operations
- **CommonWorkflows**: Pre-built workflows for common tasks
- **Enhanced CLI**: Rich terminal interface with progress bars
- **Error Handling**: Robust retry logic with exponential backoff
- **Credential Management**: Secure keychain storage
- **File Operations**: SFTP management with automatic fallbacks

### Workflows
- **District Refresh**: Complete maintenance workflow
- **Bulk Download**: Mass file download with analysis
- **Integration Monitoring**: Health monitoring across integrations

### CLI Commands
- `list-types`: List DataRequestTypes with filtering
- `find-requests`: Find requests by various criteria
- `download`: Download files with concurrent processing
- `restore`: Restore files to SFTP directories
- `rerun`: Re-trigger requests with checksum management
- `workflow`: Run predefined workflows

### Development
- Full test coverage with pytest
- Type hints throughout codebase
- Modern Python packaging with pyproject.toml
- Rich documentation with examples
- Backward compatibility with legacy interfaces