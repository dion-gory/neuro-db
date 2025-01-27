"""
Custom exceptions for the database framework.

This module defines all custom exceptions used throughout the framework,
providing a clear hierarchy for error handling.
"""

from typing import Optional


class DatabaseError(Exception):
    """
    Base class for all database-related exceptions.

    This hierarchy allows for specific error handling based on the type
    of database error:
    - ConnectionError for connection issues
    - QueryError for query execution issues
    - ConfigError for configuration issues

    Attributes:
        message: Human-readable error description
        original_error: Original exception that caused this error
    """

    def __init__(self, message: str, original_error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.original_error = original_error


class DatabaseConnectionError(DatabaseError):
    """
    Raised when database connection attempts fail.

    This exception is raised when:
    - Connection cannot be established
    - Authentication fails
    - Network issues occur
    - Connection pool is exhausted
    """
    pass


class QueryExecutionError(DatabaseError):
    """
    Raised when query execution fails.

    This exception is raised when:
    - SQL syntax is invalid
    - Query timeout occurs
    - Permissions are insufficient
    - Database constraints are violated
    """
    pass


class DatabaseConfigError(DatabaseError):
    """
    Raised when database configuration is invalid.

    This exception is raised when:
    - Required configuration values are missing
    - Configuration values are of wrong type
    - Configuration values are out of valid range
    - Configuration combination is invalid
    """
    pass