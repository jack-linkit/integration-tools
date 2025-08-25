"""
Enhanced error handling and retry logic for integration tools.
"""

import asyncio
import functools
import logging
import time
from typing import Any, Callable, Optional, Type, Union

from sqlalchemy.exc import OperationalError, TimeoutError as SQLTimeoutError
import paramiko


class RetryableError(Exception):
    """Base class for errors that can be retried."""
    pass


class DatabaseConnectionError(RetryableError):
    """Database connection related errors."""
    pass


class SFTPConnectionError(RetryableError):
    """SFTP connection related errors."""
    pass


class FileOperationError(RetryableError):
    """File operation related errors."""
    pass


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (RetryableError,),
    logger: Optional[logging.Logger] = None
):
    """
    Decorator for retrying operations with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        backoff_factor: Factor by which delay increases each retry
        exceptions: Tuple of exceptions to catch and retry on
        logger: Optional logger for retry attempts
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        if logger:
                            logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise e
                    
                    if logger:
                        logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {delay}s...")
                    
                    time.sleep(delay)
                    delay *= backoff_factor
            
            # Should never reach here, but just in case
            raise last_exception
        
        return wrapper
    return decorator


async def async_retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (RetryableError,),
    logger: Optional[logging.Logger] = None
):
    """
    Async decorator for retrying operations with exponential backoff.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        if logger:
                            logger.error(f"Async function {func.__name__} failed after {max_retries} retries: {e}")
                        raise e
                    
                    if logger:
                        logger.warning(f"Async function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {delay}s...")
                    
                    await asyncio.sleep(delay)
                    delay *= backoff_factor
            
            # Should never reach here, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def convert_database_errors(func: Callable) -> Callable:
    """
    Decorator to convert SQLAlchemy errors to our custom error types.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (OperationalError, SQLTimeoutError) as e:
            raise DatabaseConnectionError(f"Database operation failed: {str(e)}") from e
        except Exception as e:
            # Re-raise other exceptions as-is
            raise e
    
    return wrapper


def convert_sftp_errors(func: Callable) -> Callable:
    """
    Decorator to convert paramiko SFTP errors to our custom error types.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (paramiko.SSHException, paramiko.AuthenticationException, 
                paramiko.BadHostKeyException, paramiko.ChannelException) as e:
            raise SFTPConnectionError(f"SFTP operation failed: {str(e)}") from e
        except FileNotFoundError as e:
            raise FileOperationError(f"File not found: {str(e)}") from e
        except PermissionError as e:
            raise FileOperationError(f"Permission denied: {str(e)}") from e
        except Exception as e:
            # Re-raise other exceptions as-is
            raise e
    
    return wrapper


class ErrorHandler:
    """Centralized error handling for integration tools."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def handle_database_error(self, error: Exception, operation: str) -> str:
        """
        Handle database-related errors with appropriate messaging.
        
        Args:
            error: The caught exception
            operation: Description of the operation that failed
            
        Returns:
            User-friendly error message
        """
        if isinstance(error, DatabaseConnectionError):
            message = f"Database connection failed during {operation}. Please check your credentials and network connection."
        elif "timeout" in str(error).lower():
            message = f"Database operation timed out during {operation}. The database may be overloaded."
        elif "login failed" in str(error).lower():
            message = f"Database authentication failed during {operation}. Please check your username and password."
        else:
            message = f"Database error during {operation}: {str(error)}"
        
        self.logger.error(f"{operation} failed: {error}", exc_info=True)
        return message
    
    def handle_sftp_error(self, error: Exception, operation: str) -> str:
        """
        Handle SFTP-related errors with appropriate messaging.
        
        Args:
            error: The caught exception
            operation: Description of the operation that failed
            
        Returns:
            User-friendly error message
        """
        if isinstance(error, SFTPConnectionError):
            if "authentication" in str(error).lower():
                message = f"SFTP authentication failed during {operation}. Please check your credentials."
            elif "connection" in str(error).lower():
                message = f"Cannot connect to SFTP server during {operation}. Please check the server address and network connection."
            else:
                message = f"SFTP connection error during {operation}: {str(error)}"
        elif isinstance(error, FileOperationError):
            message = f"File operation failed during {operation}: {str(error)}"
        else:
            message = f"SFTP error during {operation}: {str(error)}"
        
        self.logger.error(f"{operation} failed: {error}", exc_info=True)
        return message
    
    def handle_general_error(self, error: Exception, operation: str) -> str:
        """
        Handle general errors with appropriate messaging.
        
        Args:
            error: The caught exception
            operation: Description of the operation that failed
            
        Returns:
            User-friendly error message
        """
        message = f"Unexpected error during {operation}: {str(error)}"
        self.logger.error(f"{operation} failed: {error}", exc_info=True)
        return message


# Setup logging
def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """
    Setup logging configuration for integration tools.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger("integration_tools")
    logger.setLevel(getattr(logging, level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger