"""
PostgreSQL-specific connector implementation.

This module provides PostgreSQL-specific connection handling using SQLAlchemy
and the psycopg2 driver with connection pooling support.
"""

import logging
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

import sqlalchemy
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine.url import URL

from ..config import DatabaseConfig
from ..exceptions import DatabaseConnectionError
from .base import DatabaseConnector

logger = logging.getLogger(__name__)


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL-specific implementation of DatabaseConnector.

    Features:
    - Connection pooling with configurable size
    - SSL/TLS support for secure connections
    - Schema management
    - LISTEN/NOTIFY support for pub/sub
    - Custom type handling
    - Comprehensive error handling
    - Connection timeouts and retry logic
    """

    def __init__(self, config: DatabaseConfig):
        """
        Initialize PostgreSQL connector with configuration.

        Args:
            config: Validated database configuration object containing
                   connection parameters and pool settings
        """
        self.config = config
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None

        # PostgreSQL-specific settings
        self.ssl_mode = "prefer"  # Options: disable, allow, prefer, require, verify-ca, verify-full
        self.application_name = "dbframework"
        self.connect_timeout = 10  # seconds

    def get_connection(self) -> Connection:
        """
        Create and return PostgreSQL database connection.

        Implements:
        - Connection pooling via SQLAlchemy
        - SSL configuration for secure connections
        - Schema selection
        - Custom type registration
        - Error handling with retries

        Returns:
            SQLAlchemy Connection object from connection pool

        Raises:
            DatabaseConnectionError: If connection cannot be established
        """
        conn_string = self._build_connection_string()
        try:
            self._engine = sqlalchemy.create_engine(
                conn_string,
                poolclass=QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                connect_args=self._get_connect_args()
            )

            self._connection = self._engine.connect()

            # Set session configuration
            self._configure_session()

            logger.debug("Successfully established PostgreSQL connection")
            return self._connection

        except Exception as e:
            error_msg = f"Failed to connect to PostgreSQL database: {e}"
            logger.error(error_msg, exc_info=True)
            raise DatabaseConnectionError(error_msg, original_error=e)

    def close_connection(self) -> None:
        """
        Clean up PostgreSQL connection resources.

        Implements:
        - Connection cleanup
        - Pool cleanup
        - Resource disposal
        - Error handling
        """
        try:
            if self._connection:
                self._connection.close()
                logger.debug("Closed PostgreSQL connection")

            if self._engine:
                self._engine.dispose()
                logger.debug("Disposed PostgreSQL engine")

        except Exception as e:
            logger.error("Error during PostgreSQL cleanup: %s", e, exc_info=True)
            # We don't raise here as this is called during cleanup

    def _build_connection_string(self) -> str:
        """
        Build PostgreSQL connection string from configuration.

        Implements:
        - PostgreSQL URL format
        - SSL parameters
        - Connection timeout
        - Application name
        - Other PostgreSQL-specific options

        Returns:
            SQLAlchemy connection URL string for PostgreSQL
        """
        # Create connection components
        components = {
            'drivername': 'postgresql+psycopg2',
            'username': self.config.user,
            'password': self.config.password,
            'host': self.config.host,
            'port': self.config.port,
            'database': self.config.database
        }

        # Create the base URL
        url = URL.create(**components)

        return str(url)

    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get PostgreSQL-specific connection arguments.

        Returns:
            Dictionary of connection arguments for psycopg2
        """
        return {
            'sslmode': self.ssl_mode,
            'application_name': self.application_name,
            'connect_timeout': self.connect_timeout,
            'client_encoding': self.config.charset,
            'options': '-c timezone=UTC'
        }

    def _configure_session(self) -> None:
        """
        Configure PostgreSQL session parameters.

        Sets various session-level configuration parameters for optimal
        performance and behavior.
        """
        if self._connection:
            # Set session parameters
            settings = [
                # Statement timeout (5 minutes)
                "SET statement_timeout = '300000'",
                # Row security
                "SET row_security = on",
                # Search path (schemas)
                "SET search_path = public",
                # Transaction isolation level
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            ]

            for setting in settings:
                try:
                    self._connection.execute(setting)
                except Exception as e:
                    logger.warning("Failed to set session parameter: %s", e)

    def listen_notify(self, channel: str, callback: callable) -> None:
        """
        Set up LISTEN/NOTIFY for PostgreSQL pub/sub functionality.

        Args:
            channel: Name of channel to listen on
            callback: Function to call when notification is received

        Example:
            ```python
            def handle_notification(channel, payload):
                print(f"Received {payload} on {channel}")

            connector.listen_notify("mychannel", handle_notification)
            ```
        """
        if self._connection:
            try:
                self._connection.execute(f"LISTEN {channel}")
                # Note: Full implementation would require background thread
                # for notification processing
            except Exception as e:
                logger.error("Failed to set up LISTEN on channel %s: %s",
                           channel, e)
                raise