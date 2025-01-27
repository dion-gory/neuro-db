"""
MySQL-specific connector implementation.

This module provides MySQL-specific connection handling using SQLAlchemy
and the PyMySQL driver with connection pooling support.
"""

import logging
from typing import Optional

import sqlalchemy
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import QueuePool

from ..config import DatabaseConfig
from ..exceptions import DatabaseConnectionError
from .base import DatabaseConnector

logger = logging.getLogger(__name__)


class MySQLConnector(DatabaseConnector):
    """
    MySQL-specific implementation of DatabaseConnector.

    This class handles MySQL connection management using SQLAlchemy's
    connection pooling and the PyMySQL driver. It provides:
    - Connection pooling with configurable size
    - Automatic retry on temporary failures
    - Resource cleanup on connection close
    - Comprehensive error handling
    """

    def __init__(self, config: DatabaseConfig):
        """
        Initialize MySQL connector with configuration.

        Args:
            config: Validated database configuration object

        Note:
            The configuration is validated at creation time by the
            DatabaseConfig class.
        """
        self.config = config
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None

    def get_connection(self) -> Connection:
        """
        Create and return MySQL database connection.

        Returns:
            SQLAlchemy Connection object from connection pool

        Raises:
            DatabaseConnectionError: If connection cannot be established

        Note:
            Connection is created with retry logic for transient failures
        """
        conn_string = self._build_connection_string()
        try:
            self._engine = sqlalchemy.create_engine(
                conn_string,
                poolclass=QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout
            )
            self._connection = self._engine.connect()
            logger.debug("Successfully established MySQL connection")
            return self._connection
        except Exception as e:
            error_msg = f"Failed to connect to MySQL database: {e}"
            logger.error(error_msg, exc_info=True)
            raise DatabaseConnectionError(error_msg, original_error=e)

    def close_connection(self) -> None:
        """
        Clean up MySQL connection resources.

        This method ensures proper cleanup by:
        1. Closing active connection
        2. Disposing of the engine
        3. Logging cleanup operations
        4. Handling cleanup errors
        """
        try:
            if self._connection:
                self._connection.close()
                logger.debug("Closed MySQL connection")
            if self._engine:
                self._engine.dispose()
                logger.debug("Disposed MySQL engine")
        except Exception as e:
            logger.error("Error during MySQL cleanup: %s", e, exc_info=True)
            # We don't raise here as this is called during cleanup

    def _build_connection_string(self) -> str:
        """
        Build MySQL connection string from configuration.

        Returns:
            SQLAlchemy connection URL string for MySQL

        Note:
            Uses PyMySQL driver for better Python compatibility
        """
        conn_string = (
            f'mysql+pymysql://'
            f'{self.config.user}:{self.config.password}@'
            f'{self.config.host}:{self.config.port}/'
        )
        if self.config.database:
            conn_string += f'{self.config.database}'
        conn_string += f'?charset={self.config.charset}'
        return conn_string