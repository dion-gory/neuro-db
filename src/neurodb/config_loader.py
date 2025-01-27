"""
Configuration loader module with safe handling of optional settings.
"""

import json
from typing import Dict, Any, Optional
from pathlib import Path

from .config import (
    DatabaseConfig,
    DatabaseType,
    MySQLSpecificConfig,
    PostgreSQLSpecificConfig,
    SSLConfig
)


def create_ssl_config(ssl_data: Optional[Dict[str, Any]] = None) -> SSLConfig:
    """
    Create SSL configuration with safe defaults.

    Args:
        ssl_data: Optional dictionary of SSL configuration

    Returns:
        SSLConfig instance with provided values or defaults
    """
    if not ssl_data:
        return SSLConfig()

    return SSLConfig(**ssl_data)


def create_mysql_config(
    config_data: Optional[Dict[str, Any]] = None
) -> MySQLSpecificConfig:
    """
    Create MySQL configuration with safe defaults.

    Args:
        config_data: Optional dictionary of MySQL configuration

    Returns:
        MySQLSpecificConfig instance with provided values or defaults
    """
    if not config_data:
        return MySQLSpecificConfig()

    # Handle SSL configuration separately
    ssl_data = config_data.pop('ssl', None)
    ssl_config = create_ssl_config(ssl_data)

    return MySQLSpecificConfig(ssl=ssl_config, **config_data)


def create_postgresql_config(
    config_data: Optional[Dict[str, Any]] = None
) -> PostgreSQLSpecificConfig:
    """
    Create PostgreSQL configuration with safe defaults.

    Args:
        config_data: Optional dictionary of PostgreSQL configuration

    Returns:
        PostgreSQLSpecificConfig instance with provided values or defaults
    """
    if not config_data:
        return PostgreSQLSpecificConfig()

    # Handle SSL configuration separately
    ssl_data = config_data.pop('ssl', None)
    ssl_config = create_ssl_config(ssl_data)

    return PostgreSQLSpecificConfig(ssl=ssl_config, **config_data)


def load_config(config_path: str | Path) -> DatabaseConfig:
    """
    Load database configuration from JSON file with safe defaults.

    Args:
        config_path: Path to JSON configuration file

    Returns:
        DatabaseConfig instance

    Raises:
        ValueError: If database type is invalid or JSON is malformed
        FileNotFoundError: If configuration file doesn't exist
    """
    with open(config_path, 'r') as f:
        config_data = json.load(f)

    # Convert db_type string to Enum (required)
    db_type_str = config_data.pop('db_type', '').upper()
    try:
        db_type = DatabaseType[db_type_str]
    except KeyError:
        raise ValueError(
            f"Invalid database type: {db_type_str}. "
            f"Must be one of: {', '.join(t.name for t in DatabaseType)}"
        )

    # Create database-specific configuration
    if db_type == DatabaseType.MYSQL:
        db_specific_config = create_mysql_config(
            config_data.pop('mysql_config', None)
        )
        return DatabaseConfig(
            db_type=db_type,
            mysql_config=db_specific_config,
            **config_data
        )

    elif db_type == DatabaseType.POSTGRESQL:
        db_specific_config = create_postgresql_config(
            config_data.pop('postgresql_config', None)
        )
        return DatabaseConfig(
            db_type=db_type,
            postgresql_config=db_specific_config,
            **config_data
        )

    raise ValueError(f"Unsupported database type: {db_type}")


# Example minimal configuration files:
MINIMAL_MYSQL_CONFIG = {
    "db_type": "MYSQL",
    "user": "user",
    "password": "pass",
    "host": "localhost",
    "port": 3306
}

MINIMAL_POSTGRESQL_CONFIG = {
    "db_type": "POSTGRESQL",
    "user": "user",
    "password": "pass",
    "host": "localhost",
    "port": 5432
}