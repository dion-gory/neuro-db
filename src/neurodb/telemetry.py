"""Query telemetry collection module with improved error handling."""

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, List, Union

import pandas as pd
from sqlalchemy.engine import Connection, Result

from .executor import SafeQueryExecutor
from .config import DatabaseType

logger = logging.getLogger(__name__)


@dataclass
class QueryMetrics:
    """
    Container for query performance metrics.

    Attributes:
        query: Original SQL query
        params: Query parameters used
        execution_time: Query execution time in seconds
        row_count: Number of rows affected/returned
        timestamp: When the query was executed
        indexes_used: List of indexes used in query
        table_scans: Number of table scans performed
        temp_tables: Number of temporary tables created
        memory_used: Memory usage in bytes
    """
    query: str
    params: Optional[Dict[str, Any]]
    execution_time: float
    row_count: int
    timestamp: datetime
    indexes_used: List[str]
    table_scans: int
    temp_tables: int
    memory_used: Optional[int]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueryMetrics':
        """Create QueryMetrics instance from dictionary."""
        return cls(**data)


class MySQLTelemetryCollector:
    """MySQL-specific implementation of telemetry collection."""

    def __init__(self, connection: Connection, executor: SafeQueryExecutor):
        """Initialize telemetry collector."""
        self.connection = connection
        self.executor = executor

    def collect_metrics(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> tuple[Union[pd.DataFrame, Result], QueryMetrics]:
        """Collect MySQL-specific performance metrics."""
        # Enable profiling
        self.executor.execute_command("SET profiling = 1")

        # Execute query and measure time
        start_time = time.time()
        result = self.executor.execute_query(query, params, raise_on_fail=True)
        execution_time = time.time() - start_time

        # Get row count safely
        row_count = len(result) if isinstance(result, pd.DataFrame) else 0

        # Create metrics
        metrics = QueryMetrics(
            query=query,
            params=params,
            execution_time=execution_time,
            row_count=row_count,
            timestamp=datetime.now(),
            indexes_used=self._get_used_indexes(),
            table_scans=self._get_table_scans(),
            temp_tables=self._get_temp_tables(),
            memory_used=self._get_memory_usage()
        )

        return result, metrics

    def _get_used_indexes(self) -> List[str]:
        """Get list of indexes used in last query."""
        try:
            result = self.executor.execute_query(
                "SELECT index_name FROM information_schema.statistics "
                "WHERE table_schema = DATABASE()"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                # Check if the column exists
                if 'index_name' in result.columns:
                    return result['index_name'].tolist()
            return []
        except Exception as e:
            logger.warning("Failed to get index information: %s", e)
            return []

    def _get_table_scans(self) -> int:
        """Get number of table scans from last query."""
        try:
            result = self.executor.execute_query(
                "SHOW STATUS LIKE 'Handler_read%'"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'Value' in result.columns and not result.empty:
                    return int(result.iloc[0]['Value'])
            return 0
        except Exception as e:
            logger.warning("Failed to get table scan information: %s", e)
            return 0

    def _get_temp_tables(self) -> int:
        """Get number of temporary tables created."""
        try:
            result = self.executor.execute_query(
                "SHOW STATUS LIKE 'Created_tmp%'"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'Value' in result.columns and not result.empty:
                    return int(result.iloc[0]['Value'])
            return 0
        except Exception as e:
            logger.warning("Failed to get temp table information: %s", e)
            return 0

    def _get_memory_usage(self) -> Optional[int]:
        """Get memory usage in bytes."""
        try:
            result = self.executor.execute_query(
                "SHOW STATUS LIKE 'Memory_used'"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'Value' in result.columns and not result.empty:
                    return int(result.iloc[0]['Value'])
            return None
        except Exception as e:
            logger.warning("Failed to get memory usage information: %s", e)
            return None


class PostgreSQLTelemetryCollector:
    """PostgreSQL-specific implementation of telemetry collection."""

    def __init__(self, connection: Connection, executor: SafeQueryExecutor):
        """Initialize telemetry collector."""
        self.connection = connection
        self.executor = executor

    def collect_metrics(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> tuple[Union[pd.DataFrame, Result], QueryMetrics]:
        """Collect PostgreSQL-specific performance metrics."""
        # Enable timing
        self.executor.execute_command("SET log_statement_stats = on")

        # Execute query and measure time
        start_time = time.time()
        result = self.executor.execute_query(query, params, raise_on_fail=True)
        execution_time = time.time() - start_time

        # Get row count safely
        row_count = len(result) if isinstance(result, pd.DataFrame) else 0

        # Create metrics
        metrics = QueryMetrics(
            query=query,
            params=params,
            execution_time=execution_time,
            row_count=row_count,
            timestamp=datetime.now(),
            indexes_used=self._get_used_indexes(),
            table_scans=self._get_table_scans(),
            temp_tables=self._get_temp_tables(),
            memory_used=self._get_memory_usage()
        )

        return result, metrics

    def _get_used_indexes(self) -> List[str]:
        """Get list of indexes used in last query."""
        try:
            result = self.executor.execute_query(
                "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'indexname' in result.columns:
                    return result['indexname'].tolist()
            return []
        except Exception as e:
            logger.warning("Failed to get index information: %s", e)
            return []

    def _get_table_scans(self) -> int:
        """Get number of sequential scans."""
        try:
            result = self.executor.execute_query(
                "SELECT COALESCE(sum(seq_scan), 0) as scans FROM pg_stat_all_tables"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'scans' in result.columns and not result.empty:
                    return int(result.iloc[0]['scans'])
            return 0
        except Exception as e:
            logger.warning("Failed to get table scan information: %s", e)
            return 0

    def _get_temp_tables(self) -> int:
        """Get number of temporary tables created."""
        try:
            result = self.executor.execute_query(
                "SELECT COALESCE(sum(temp_files), 0) as temp_tables "
                "FROM pg_stat_database"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'temp_tables' in result.columns and not result.empty:
                    return int(result.iloc[0]['temp_tables'])
            return 0
        except Exception as e:
            logger.warning("Failed to get temp table information: %s", e)
            return 0

    def _get_memory_usage(self) -> Optional[int]:
        """Get memory usage in bytes."""
        try:
            result = self.executor.execute_query(
                "SELECT setting::bigint * pg_size_bytes(unit) as bytes "
                "FROM pg_settings "
                "WHERE name = 'work_mem'"
            )
            if result is not None and isinstance(result, pd.DataFrame):
                if 'bytes' in result.columns and not result.empty:
                    return int(result.iloc[0]['bytes'])
            return None
        except Exception as e:
            logger.warning("Failed to get memory usage information: %s", e)
            return None


def create_telemetry_collector(
    db_type: DatabaseType,
    connection: Connection,
    executor: SafeQueryExecutor
) -> Union[MySQLTelemetryCollector, PostgreSQLTelemetryCollector]:
    """Factory function to create appropriate telemetry collector."""
    collectors = {
        DatabaseType.MYSQL: MySQLTelemetryCollector,
        DatabaseType.POSTGRESQL: PostgreSQLTelemetryCollector
    }

    collector_class = collectors.get(db_type)
    if not collector_class:
        raise ValueError(f"Unsupported database type: {db_type}")

    return collector_class(connection, executor)