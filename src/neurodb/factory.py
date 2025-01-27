"""
Database factory module.

This module implements the Abstract Factory pattern for creating database
components based on the selected database type.
"""

from abc import ABC, abstractmethod
from typing import Dict, Type

from .config import DatabaseConfig, DatabaseType
from .connectors.base import DatabaseConnector
from .connectors.mysql import MySQLConnector
from .connectors.postgresql import PostgreSQLConnector


class AbstractDatabaseFactory(ABC):
    """
    Abstract factory interface for creating database components.

    This abstract class defines the interface for creating families
    of related database objects (connectors, executors, etc.).
    """

    @abstractmethod
    def create_connector(self, config: DatabaseConfig) -> DatabaseConnector:
        """
        Create appropriate database connector based on configuration.

        Args:
            config: Database configuration including type-specific settings

        Returns:
            Database connector instance

        Raises:
            ValueError: If database type is not supported
        """
        pass


class DatabaseFactory(AbstractDatabaseFactory):
    """
    Concrete factory implementation for creating database components.

    This factory creates the appropriate database connector and related
    components based on the database type specified in the configuration.
    """

    def create_connector(self, config: DatabaseConfig) -> DatabaseConnector:
        """
        Create and return appropriate database connector.

        Implementation of connector creation that:
        - Uses database type from configuration
        - Creates appropriate connector instance
        - Applies database-specific settings

        Args:
            config: Database configuration

        Returns:
            Database connector instance

        Raises:
            ValueError: If database type is not supported
        """
        connectors: Dict[DatabaseType, Type[DatabaseConnector]] = {
            DatabaseType.MYSQL: MySQLConnector,
            DatabaseType.POSTGRESQL: PostgreSQLConnector
        }

        connector_class = connectors.get(config.db_type)
        if not connector_class:
            raise ValueError(f"Unsupported database type: {config.db_type}")

        return connector_class(config)