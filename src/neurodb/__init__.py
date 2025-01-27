"""
Database Framework Package.

A robust and extensible database interaction framework with support for
multiple database types, connection pooling, and performance monitoring.

Features:
    - Multiple database support (MySQL, PostgreSQL)
    - Connection pooling
    - Query execution with parameter binding
    - Performance telemetry collection
    - Comprehensive error handling
    - Type safety with static type checking

Example:
    ```python
    from dbframework import Database, DatabaseConfig, DatabaseType

    # Create configuration
    config = DatabaseConfig(
        user="user",
        password="pass",
        host="localhost",
        port=3306,
        database="mydb"
    )

    # Create database instance
    db = Database(DatabaseType.MYSQL, config)

    # Execute query
    results = db.select("SELECT * FROM users WHERE active = :active",
                       {"active": True})
    ```
"""

from .config import DatabaseConfig, DatabaseType
from .database import Database
from .exceptions import (
    DatabaseError,
    DatabaseConnectionError,
    QueryExecutionError,
    DatabaseConfigError
)

__version__ = "1.0.0"
__author__ = "Dion Gory"
__email__ = "diongory@gmail.com"

__all__ = [
    "Database",
    "DatabaseConfig",
    "DatabaseType",
    "DatabaseError",
    "DatabaseConnectionError",
    "QueryExecutionError",
    "DatabaseConfigError"
]