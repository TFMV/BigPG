import logging
import pyarrow as pa
import adbc_driver_postgresql.dbapi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresService:
    """Encapsulates PostgreSQL read/write operations."""

    def __init__(self, connection_string: str):
        self.connection = adbc_driver_postgresql.dbapi.connect(connection_string)

    def fetch_data(self, table_name: str) -> pa.Table:
        """Fetches a table from PostgreSQL as an Arrow table."""
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            return cursor.fetch_arrow_table()

    def ingest_data(self, table_name: str, arrow_table: pa.Table) -> int:
        """Ingests data into PostgreSQL from an Arrow table."""
        with self.connection.cursor() as cursor:
            rows_inserted = cursor.adbc_ingest(
                table_name, arrow_table, mode="create_append"
            )
        self.connection.commit()
        logger.info(f"Inserted {rows_inserted} rows into PostgreSQL table {table_name}")
        return rows_inserted
