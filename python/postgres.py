# --------------------------------------------------------------------------------
# Author: Thomas F McGeehan V
#
# This file is part of a software project developed by Thomas F McGeehan V.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# For more information about the MIT License, please visit:
# https://opensource.org/licenses/MIT
#
# Acknowledgment appreciated but not required.
# --------------------------------------------------------------------------------

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
