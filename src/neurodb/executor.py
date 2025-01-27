"""Query execution module with support for both SELECT and non-SELECT queries."""

import logging
from typing import Optional, Dict, Any, Union

import pandas as pd
import sqlalchemy
from sqlalchemy.engine import Connection, Result
from sqlalchemy.exc import ResourceClosedError

from .exceptions import QueryExecutionError

logger = logging.getLogger(__name__)


class SafeQueryExecutor:
    """Safe query execution with error handling."""

    def __init__(self, connection: Connection):
        """Initialize executor with database connection."""
        self.connection = connection

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        raise_on_fail: bool = False,
        return_results: bool = True
    ) -> Optional[Union[pd.DataFrame, Result]]:
        """
        Execute SQL query safely with error handling.

        Args:
            query: SQL query string
            params: Optional query parameters for binding
            raise_on_fail: Whether to raise exception on failure
            return_results: Whether to return results as DataFrame

        Returns:
            DataFrame with results, Result object, or None if query fails

        Raises:
            QueryExecutionError: If query fails and raise_on_fail is True
        """
        try:
            logger.debug("Executing query: %s with params: %s", query, params)
            query_text = sqlalchemy.text(query)

            if not return_results:
                # For SET, UPDATE, INSERT, etc.
                return self.connection.execute(query_text, params)

            try:
                # Try to get results as DataFrame
                return pd.read_sql(query_text, con=self.connection, params=params)
            except ResourceClosedError:
                # Query doesn't return results
                result = self.connection.execute(query_text, params)
                return result

        except Exception as e:
            error_msg = f"Query execution failed: {query}. Error: {e}"
            logger.error(error_msg, exc_info=True)
            if raise_on_fail:
                raise QueryExecutionError(error_msg, original_error=e)
            return None

    def execute_command(
        self,
        command: str,
        params: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Execute a command that doesn't return results (SET, UPDATE, etc.).

        Args:
            command: SQL command to execute
            params: Optional command parameters

        Returns:
            True if command executed successfully, False otherwise
        """
        try:
            self.execute_query(command, params, return_results=False)
            return True
        except Exception as e:
            logger.error("Command execution failed: %s. Error: %s", command, e)
            return False