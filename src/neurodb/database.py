"""
High-level database interface module.

This module provides the main interface for database operations,
combining all components into a cohesive API.
"""

import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, Union, Tuple, Iterator

import pandas as pd
from sqlalchemy.engine import Connection

from .config import DatabaseConfig
from .factory import DatabaseFactory
from .executor import SafeQueryExecutor
from .telemetry import create_telemetry_collector, QueryMetrics

logger = logging.getLogger(__name__)


class Database:
    """
    High-level database interface combining all components.

    This class provides a simplified interface for database operations
    while managing the complexity of connection handling, query execution,
    and performance monitoring.
    """

    def __init__(self, config: DatabaseConfig):
        """
        Initialize database interface.

        Args:
            config: Database configuration including type-specific settings
        """
        self.config = config
        self.factory = DatabaseFactory()
        self.connector = self.factory.create_connector(config)

    def select(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        capture_telemetry: bool = False
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, QueryMetrics]]:
        """
        Execute SELECT query with optional telemetry collection.

        Args:
            query: SQL query string
            params: Optional query parameters for binding
            capture_telemetry: Whether to collect query telemetry

        Returns:
            DataFrame with results or tuple of (DataFrame, QueryMetrics)

        Example:
            ```python
            # Simple query
            df = db.select(
                "SELECT * FROM users WHERE active = :active",
                {"active": True}
            )

            # Query with telemetry
            df, metrics = db.select(
                "SELECT * FROM users",
                capture_telemetry=True
            )
            print(f"Query took {metrics.execution_time:.2f} seconds")
            print(f"Used indexes: {metrics.indexes_used}")
            ```
        """
        with self._managed_connection() as connection:
            executor = SafeQueryExecutor(connection)

            if capture_telemetry:
                collector = create_telemetry_collector(
                    self.config.db_type,
                    connection,
                    executor
                )
                return collector.collect_metrics(query, params)

            return executor.execute_query(query, params, raise_on_fail=True)

    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get comprehensive database performance metrics.

        Returns dictionary containing:
        - Resource usage statistics
        - Connection pool status
        - Cache performance
        - Database-specific metrics

        Returns:
            Dictionary of performance metrics
        """
        with self._managed_connection() as connection:
            collector = create_telemetry_collector(
                self.config.db_type,
                connection,
                SafeQueryExecutor(connection)
            )
            return collector.get_resource_usage()

    def analyze_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get detailed query analysis including execution plan.

        Args:
            query: SQL query to analyze
            params: Optional query parameters

        Returns:
            Dictionary containing query analysis including:
            - Execution plan
            - Estimated cost
            - Index usage
            - Resource requirements
        """
        with self._managed_connection() as connection:
            collector = create_telemetry_collector(
                self.config.db_type,
                connection,
                SafeQueryExecutor(connection)
            )
            return collector.get_execution_plan(query, params)

    @contextmanager
    def _managed_connection(self) -> Iterator[Connection]:
        """
        Context manager for database connection lifecycle.

        Yields:
            Active database connection
        """
        try:
            connection = self.connector.get_connection()
            yield connection
        finally:
            self.connector.close_connection()