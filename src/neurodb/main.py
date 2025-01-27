from dataclasses import dataclass
from contextlib import contextmanager
import pandas as pd
import sqlalchemy
import time
import logging

_DEFAULT_CHARSET = 'utf8'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Base class for database-related errors."""
    pass

class DatabaseConnectionError(DatabaseError):
    """Raised when a database connection fails."""
    pass

class QueryExecutionError(DatabaseError):
    """Raised when a query execution fails."""
    pass

@dataclass
class Config:
    user: str
    password: str
    host: str
    port: int
    database: str = None
    charset: str = _DEFAULT_CHARSET

class DatabaseConnector:
    def __init__(self, config: Config):
        self.config = config
        self.engine = None

    def __enter__(self):
        conn_string = self._get_conn_string()
        self.engine = sqlalchemy.create_engine(conn_string)
        try:
            self.conn = self.engine.connect()
            return self.conn
        except Exception as e:
            raise DatabaseConnectionError(f"Error connecting to the database: {e}")

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()

    def _get_conn_string(self):
        conn_string = (
            f'mysql+mysqldb://'
            f'{self.config.user}:{self.config.password}@'
            f'{self.config.host}:{self.config.port}/'
        )
        if self.config.database:
            conn_string += f'{self.config.database}'
        conn_string += f'?charset={self.config.charset}'
        return conn_string

def safe_query_execution(conn, query, params=None, raise_on_fail=False):
    try:
        query_text = sqlalchemy.text(query)
        result_df = pd.read_sql(query_text, con=conn, params=params)
        return result_df
    except Exception as e:
        if raise_on_fail:
            raise QueryExecutionError(f"Error executing user query: {query}. Error: {e}")
        logger.warning(f"Permission denied or error executing query '{query}' with params '{params}': {e}")
        return None


def capture_query_telemetry(conn, query, params=None):
    telemetry_data = {'query': query, 'params': params}

    queries = {
        "execution_plan": f"EXPLAIN {query}",
        "profile": "SHOW PROFILE;",
        "io_stats": "SHOW STATUS LIKE 'Handler_read%';",
        "connections": "SHOW PROCESSLIST;",
        "buffer_pool_status": "SHOW ENGINE INNODB STATUS;",
        "query_cache": "SHOW STATUS LIKE 'Qcache%';",
        "index_usage": "SHOW STATUS LIKE 'Handler_read%index%';",
        "slow_queries": "SHOW STATUS LIKE 'Slow_queries';"
    }

    safe_query_execution(conn, "SET profiling = 1;")
    start_time = time.time()
    df = safe_query_execution(conn, query, params, raise_on_fail=True)
    end_time = time.time()
    telemetry_data['execution_time'] = end_time - start_time if df is not None else None

    for key, sql in queries.items():
        result_df = safe_query_execution(conn, sql, params)
        telemetry_data[key] = result_df.to_dict(orient='records') if result_df is not None else None

    logger.info("Query Telemetry: %s", telemetry_data)
    return df, telemetry_data

def select(config, query, params=None, capture_telemetry=False):
    with DatabaseConnector(config) as conn:
        if capture_telemetry:
            df, telemetry = capture_query_telemetry(conn, query, params)
            return df, telemetry
        else:
            df = safe_query_execution(conn, query, params, raise_on_fail=True)
            return df

class DataBase:
    def __init__(self, config: Config):
        self.config = config

    def select(self, query, params=None, capture_telemetry=False):
        return select(self.config, query, params, capture_telemetry)


if __name__ == "__main__":
    import json

    with open('database_config_files/mysql_localhost.json', 'r') as f:
        inputs = json.load(f)

    config = Config(**inputs)
    db = DataBase(config)

    query = "SELECT * FROM df WHERE TRUE;"
    df = db.select(query, )
    df, telemetry = db.select(query, capture_telemetry=True)


