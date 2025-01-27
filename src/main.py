"""
Example usage of the database framework with configuration loading.
"""

import logging
from pathlib import Path

from neurodb import Database
from neurodb.config_loader import load_config
from neurodb.exceptions import DatabaseError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_database_example(config_path: str | Path):
    """Run database operations using configuration from file."""
    try:
        # Load configuration from JSON file
        config = load_config(config_path)

        # Create database instance
        db = Database(config)

        # Example query with parameters
        query = """
            SELECT * FROM df;
        """
        params = None

        # Execute query with telemetry
        results, metrics = db.select(query, params, capture_telemetry=True)

        logger.info("Query Results:")
        logger.info(results)

        logger.info("\nPerformance Metrics:")
        logger.info(f"Execution Time: {metrics.execution_time:.2f} seconds")
        logger.info(f"Rows Returned: {metrics.row_count}")
        logger.info(f"Indexes Used: {metrics.indexes_used}")
        logger.info(f"Memory Used: {metrics.memory_used} bytes")

    except DatabaseError as e:
        logger.error("Database error occurred: %s", e)
        if e.original_error:
            logger.error("Original error: %s", e.original_error)
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)

    return results, metrics


if __name__ == "__main__":
    """Main function demonstrating configuration loading."""
    # MySQL example
    mysql_config_path = Path("database_config_files/mysql_localhost.json")
    logger.info("Running MySQL example...")
    results, metrics = run_database_example(mysql_config_path)

    # # PostgreSQL example
    # postgresql_config_path = Path("config/postgresql_config.json")
    # logger.info("\nRunning PostgreSQL example...")
    # run_database_example(postgresql_config_path)