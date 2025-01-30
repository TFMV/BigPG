import time
import yaml
import uvicorn
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict
from bigquery import BigQueryService
from postgres import PostgresService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
class ConfigLoader:
    """Handles loading the configuration from a YAML file."""
    def __init__(self, config_file: str = "config.yaml"):
        self.config = self.load_config(config_file)

    @staticmethod
    def load_config(config_file: str):
        with open(config_file, "r") as file:
            return yaml.safe_load(file)

    def get(self, section: str, key: str):
        return self.config.get(section, {}).get(key)

# Request Models
class BQToPGRequest(BaseModel):
    dataset_id: str = Field(..., example="my_dataset")
    table_id: str = Field(..., example="my_table")
    predicates: Optional[Dict[str, str]] = Field(None, example={"state": "WA"})
    max_stream_count: int = Field(1, example=1)

class PGToBQRequest(BaseModel):
    table_name: str = Field(..., example="users")
    dataset_id: str = Field(..., example="my_dataset")
    table_id: str = Field(..., example="my_table")

# Response Model
class CopyTableResponse(BaseModel):
    status: str
    time_taken: float
    rows_loaded: Optional[int] = None

# FastAPI Initialization
app = FastAPI()
config_loader = ConfigLoader()

@app.post("/pg2bq/", response_model=CopyTableResponse)
async def pg_to_bq(request: PGToBQRequest):
    start_time = time.time()
    try:
        pg_service = PostgresService(config_loader.get("postgres", "conn_str"))
        bq_service = BigQueryService(config_loader.get("gcp", "project_id"))

        # Read data from PostgreSQL
        logger.info(f"Fetching data from PostgreSQL table: {request.table_name}")
        arrow_table = pg_service.fetch_data(request.table_name)

        # Write data to BigQuery
        logger.info(f"Writing data to BigQuery: {request.dataset_id}.{request.table_id}")
        bq_service.write_to_bigquery(request.dataset_id, request.table_id, arrow_table)

        time_taken = time.time() - start_time
        return CopyTableResponse(status="Success", time_taken=time_taken, rows_loaded=arrow_table.num_rows)

    except Exception as e:
        logger.error(f"Error in /pg2bq/: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/bq2pg/", response_model=CopyTableResponse)
async def bq_to_pg(request: BQToPGRequest):
    start_time = time.time()
    try:
        pg_service = PostgresService(config_loader.get("postgres", "conn_str"))
        bq_service = BigQueryService(config_loader.get("gcp", "project_id"))

        # Read data from BigQuery
        logger.info(f"Reading BigQuery table: {request.dataset_id}.{request.table_id}")
        arrow_table = bq_service.read_table(request.dataset_id, request.table_id, request.max_stream_count)

        # Write data to PostgreSQL
        logger.info(f"Ingesting data into PostgreSQL table: {request.table_id}")
        rows_loaded = pg_service.ingest_data(request.table_id, arrow_table)

        time_taken = time.time() - start_time
        return CopyTableResponse(status="Success", time_taken=time_taken, rows_loaded=rows_loaded)

    except Exception as e:
        logger.error(f"Error in /bq2pg/: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Uvicorn Runner
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
