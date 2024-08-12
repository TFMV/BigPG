from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Union, List
from google.cloud.bigquery_storage import (
    BigQueryReadClient,
    types,
    ArrowSerializationOptions,
)
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import pyarrow as pa
import time
import adbc_driver_postgresql.dbapi
import yaml
import uvicorn

class ConfigLoader:
    """Handles loading and accessing the configuration from a YAML file."""

    def __init__(self, config_file: str = "config.yaml") -> None:
        self.config = self.load_config(config_file)

    @staticmethod
    def load_config(config_file: str) -> Dict[str, Any]:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)

    def get(self, section: str, key: str) -> Optional[str]:
        return self.config.get(section, {}).get(key)


class BigQueryService:
    """Encapsulates BigQuery operations."""

    def __init__(self, project_id: str) -> None:
        self.client = BigQueryReadClient()
        self.project_id = project_id

    def create_read_session(
        self,
        dataset_id: str,
        table_id: str,
        max_stream_count: int,
        row_restriction: Optional[str] = None,
    ) -> types.ReadSession:
        table = f"projects/{self.project_id}/datasets/{dataset_id}/tables/{table_id}"

        requested_session = types.ReadSession(
            table=table, data_format=types.DataFormat.ARROW
        )
        requested_session.read_options.arrow_serialization_options.buffer_compression = (
            ArrowSerializationOptions.CompressionCodec.LZ4_FRAME
        )

        if row_restriction:
            requested_session.read_options.row_restriction = row_restriction

        parent = f"projects/{self.project_id}"
        return self.client.create_read_session(
            parent=parent,
            read_session=requested_session,
            max_stream_count=max_stream_count,
        )


class PostgresService:
    """Encapsulates PostgreSQL operations."""

    def __init__(self, connection_string: str) -> None:
        self.connection = adbc_driver_postgresql.dbapi.connect(connection_string)

    def ingest_data(self, table_name: str, arrow_batches) -> int:
        """Ingests data into PostgreSQL, accepting an iterable of Arrow batches."""
        with self.connection.cursor() as cursor:
            r = cursor.adbc_ingest(table_name, arrow_batches, mode="create_append")
        self.connection.commit()
        return r


    def fetch_data(self, table_name: str) -> pa.Table:
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            return cursor.fetch_arrow_table()


def generate_metadata(schema: Union[pa.Table, pa.Schema, List[SchemaField]]) -> Dict[str, Any]:
    """Generates metadata from a pyarrow schema or BigQuery schema."""
    
    # Debugging information
    print(f"Schema type: {type(schema)}")
    
    if isinstance(schema, pa.Schema):
        # If schema is a PyArrow schema
        fields = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ]
    elif isinstance(schema, pa.Table):
        # If schema is a PyArrow table
        fields = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema.schema
        ]
    elif isinstance(schema, list) and all(isinstance(field, SchemaField) for field in schema):
        # If schema is a list of BigQuery SchemaField
        fields = [
            {"name": field.name, "type": field.field_type, "nullable": field.is_nullable}
            for field in schema
        ]
    else:
        raise ValueError("Unsupported schema type provided.")
    
    return {"fields": fields}

class CopyTableRequest(BaseModel):
    dataset_id: str = Field(..., example="my_dataset")
    table_id: str = Field(..., example="my_table")
    predicates: Optional[Dict[str, str]] = Field(None, example={"state": "WA"})
    max_stream_count: int = Field(1, example=1)


class CopyTableResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]
    rows_loaded: Optional[int] = None

    class Config:
        arbitrary_types_allowed = True


app = FastAPI()
config_loader = ConfigLoader()


@app.post("/bq2pg/", response_model=CopyTableResponse)
async def bq_to_pg(request: CopyTableRequest):
    start_time = time.time()

    try:
        bigquery_service = BigQueryService(config_loader.get("gcp", "project_id"))
        postgres_service = PostgresService(config_loader.get("postgres", "conn_str"))

        row_restriction = (
            " AND ".join([f"{k} = '{v}'" for k, v in request.predicates.items()])
            if request.predicates
            else None
        )
        session = bigquery_service.create_read_session(
            dataset_id=request.dataset_id,
            table_id=request.table_id,
            max_stream_count=request.max_stream_count,
            row_restriction=row_restriction,
        )

        reader = bigquery_service.client.read_rows(session.streams[0].name)
        rows = reader.rows(session)
        arrow_table = rows.to_arrow()
        rows_loaded = postgres_service.ingest_data(request.table_id, arrow_table)

        metadata = generate_metadata(arrow_table)
        end_time = time.time()

        return CopyTableResponse(
            status="Success", time_taken=end_time - start_time, metadata=metadata, rows_loaded=rows_loaded
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bq2pg_arrow/", response_model=CopyTableResponse)
async def bq_to_pg_arrow(request: CopyTableRequest):
    start_time = time.time()

    try:
        # Initialize BigQuery client
        bq_client = bigquery.Client()

        # Run a query
        query = f"""
            SELECT *
            FROM `{request.dataset_id}.{request.table_id}`
        """
        
        # Add predicates if they exist
        if request.predicates:
            predicate_string = " AND ".join([f"{k} = '{v}'" for k, v in request.predicates.items()])
            query += f" WHERE {predicate_string}"
        
        query_job = bq_client.query(query)

        # Get the result set, and use to_arrow_iterable to stream Arrow record batches
        rows = query_job.result().to_arrow_iterable()
        # rows does not have a schema. We must get the schema from the query job
        metadata = generate_metadata(query_job.result().schema)
        # Initialize Postgres service
        postgres_service = PostgresService(config_loader.get("postgres", "conn_str"))

        # Ingest the streamed data into PostgreSQL
        rows_loaded = postgres_service.ingest_data(request.table_id, rows)

        end_time = time.time()

        return CopyTableResponse(
            status="Success", time_taken=end_time - start_time, metadata=metadata, rows_loaded=rows_loaded
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
