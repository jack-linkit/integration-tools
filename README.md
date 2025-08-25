# Integration Tools

A comprehensive suite of utilities for automating integration setup processes and managing data processing requests.

## Features

### Request Management (request_replayer.py)
- **Find and analyze requests** by type, district, or status
- **Restore processed files** from SFTP with automatic fallback to backup archives
- **Batch operations** with progress tracking and error handling
- **Re-trigger request processing** with checksum management
- **Download files** to local directories for analysis
- **Email content viewer** for request notifications

### Integration Setup
- **Automated setup** for new integrations (SAT, PSAT, etc.)
- **Database record creation** (xpsDistrictUpload, DistrictDataParm)
- **SFTP directory management** with automatic creation
- **Configuration management** with JSON templates
- **School mapping error resolution** utilities

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Quick Start

### Request Management
```bash
# Interactive mode (recommended for daily use)
request-replayer

# Command line mode
request-replayer find-requests --type-names SAT,PSAT --district-ids 123
request-replayer restore --request-ids 456,789
request-replayer rerun-by-requests --request-ids 456 --delete-checksums
```

### Integration Setup
```bash
# Interactive mode
integration-tools

# Command line mode  
integration-tools SAT  # Setup SAT integration
integration-tools PSAT # Setup PSAT integration
```

## Project Structure

```
integration-tools/
├── src/integration_tools/
│   ├── core/              # Core functionality
│   │   ├── request_manager.py    # Request lifecycle management
│   │   ├── file_manager.py       # SFTP/file operations
│   │   ├── db_manager.py         # Database operations
│   │   └── credential_manager.py # Unified credential handling
│   ├── integrations/      # Integration setup utilities
│   ├── cli/              # Command line interfaces
│   └── workflows/        # Pre-defined workflows
├── tests/                # Test suite
├── docs/                 # Documentation
└── examples/            # Usage examples
```

## Development

### Running Tests
```bash
pytest
```

### Code Formatting
```bash
black src/ tests/
isort src/ tests/
```

### Type Checking
```bash
mypy src/
```

## Contributing

1. Create a feature branch from `main`
2. Make your changes with tests
3. Ensure all tests pass and code is formatted
4. Submit a pull request

## License

MIT License - see LICENSE file for details.