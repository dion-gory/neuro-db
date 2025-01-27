"""
Base connector interface module.

This module defines the abstract base class for all database connectors,
ensuring consistent interface across different database implementations.
"""

from abc import ABC, abstractmethod
from typing import Optional

from sqlalchemy.engine import Connection, Engine


class DatabaseConnector(ABC):
    """
    Abstract base class for database connection management.

    This class defines the interface for database connection handling
    and provides a template for specific database implementations.

    Implementation Requirements:
        - Connection establishment and pooling
        - Resource cleanup
        - Error handling and logging
        - Connection string building
        - Pool configuration
    """

    @abstractmethod
    def get_connection(self) -> Connection:
        """
        Establish and return a database connection.

        This method should:
        1. Build connection string from configuration
        2. Create engine with proper pool settings
        3. Establish connection from pool
        4. Handle connection errors appropriately

        Returns:
            Active database connection from the connection pool

        Raises:
            DatabaseConnectionError: If connection cannot be established
        """
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """
        Clean up database connection resources.

        This method should:
        1. Close active connection if exists
        2. Return connection to pool
        3. Dispose engine if needed
        4. Log cleanup operations
        5. Handle cleanup errors
        """
        pass

    @abstractmethod
    def _build_connection_string(self) -> str:
        """
        Build database-specific connection string.

        This method should:
        1. Use configuration parameters
        2. Follow database-specific URL format
        3. Include all necessary options
        4. Escape special characters

        Returns:
            Database-specific connection URL string
        """
        pass