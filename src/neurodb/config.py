"""
Database configuration module with proper defaults and optional settings.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Final, Dict, Any

from .exceptions import DatabaseConfigError

# Constants
DEFAULT_CHARSET: Final[str] = 'utf8'
DEFAULT_POOL_SIZE: Final[int] = 5
DEFAULT_MAX_OVERFLOW: Final[int] = 10
DEFAULT_TIMEOUT: Final[int] = 30


class DatabaseType(Enum):
    """Enumeration of supported database types."""
    MYSQL = auto()
    POSTGRESQL = auto()


@dataclass(frozen=True)
class SSLConfig:
    """
    SSL/TLS configuration for database connections.
    All fields are optional with safe defaults.
    """
    enabled: bool = False
    verify_cert: bool = True
    ca_cert: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    cipher: Optional[str] = None


@dataclass(frozen=True)
class MySQLSpecificConfig:
    """
    MySQL-specific configuration options.
    All fields are optional with safe defaults.
    """
    ssl: SSLConfig = field(default_factory=SSLConfig)
    init_command: Optional[str] = None
    autocommit: bool = False
    local_infile: bool = False
    compress: bool = False


@dataclass(frozen=True)
class PostgreSQLSpecificConfig:
    """
    PostgreSQL-specific configuration options.
    All fields are optional with safe defaults.
    """
    ssl: SSLConfig = field(default_factory=SSLConfig)
    schema: str = 'public'
    application_name: str = 'dbframework'
    sslmode: str = 'prefer'
    target_session_attrs: str = 'any'
    async_io: bool = False


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Main database configuration class.
    Only core connection parameters are required.
    """
    # Required fields
    db_type: DatabaseType
    user: str
    password: str
    host: str
    port: int

    # Optional fields with defaults
    database: Optional[str] = None
    charset: str = DEFAULT_CHARSET
    pool_size: int = DEFAULT_POOL_SIZE
    max_overflow: int = DEFAULT_MAX_OVERFLOW
    pool_timeout: int = DEFAULT_TIMEOUT

    # Database-specific configurations with defaults
    mysql_config: MySQLSpecificConfig = field(default_factory=MySQLSpecificConfig)
    postgresql_config: PostgreSQLSpecificConfig = field(
        default_factory=PostgreSQLSpecificConfig
    )

    def __post_init__(self) -> None:
        """Validate configuration values after initialization."""
        self._validate_base_config()
        self._validate_db_specific_config()

    def _validate_base_config(self) -> None:
        """Validate required base configuration parameters."""
        if not self.user or not isinstance(self.user, str):
            raise DatabaseConfigError("Invalid username")
        if not self.password or not isinstance(self.password, str):
            raise DatabaseConfigError("Invalid password")
        if not self.host or not isinstance(self.host, str):
            raise DatabaseConfigError("Invalid host")
        if not isinstance(self.port, int) or self.port < 1 or self.port > 65535:
            raise DatabaseConfigError("Invalid port number")
        if self.database is not None and not isinstance(self.database, str):
            raise DatabaseConfigError("Invalid database name")
        if self.pool_size < 1:
            raise DatabaseConfigError("Pool size must be positive")
        if self.max_overflow < 0:
            raise DatabaseConfigError("Max overflow must be non-negative")
        if self.pool_timeout < 1:
            raise DatabaseConfigError("Pool timeout must be positive")

    def _validate_db_specific_config(self) -> None:
        """Validate database-specific configuration."""
        if self.db_type == DatabaseType.MYSQL:
            self._validate_mysql_config()
        elif self.db_type == DatabaseType.POSTGRESQL:
            self._validate_postgresql_config()

    def _validate_mysql_config(self) -> None:
        """Validate MySQL-specific configuration."""
        if self.mysql_config and self.mysql_config.ssl:
            if (self.mysql_config.ssl.enabled and
                self.mysql_config.ssl.verify_cert and
                not self.mysql_config.ssl.ca_cert):
                raise DatabaseConfigError(
                    "CA certificate required when SSL verification is enabled"
                )

    def _validate_postgresql_config(self) -> None:
        """Validate PostgreSQL-specific configuration."""
        if self.postgresql_config:
            valid_sslmodes = {'disable', 'allow', 'prefer', 'require',
                           'verify-ca', 'verify-full'}
            if self.postgresql_config.sslmode not in valid_sslmodes:
                raise DatabaseConfigError(
                    f"Invalid sslmode. Must be one of: {valid_sslmodes}"
                )