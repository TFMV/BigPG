from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from google.cloud.bigquery_storage import BigQueryReadClient, types
import pyarrow as pa
import time
import adbc_driver_postgresql.dbapi
import yaml

app = FastAPI()

# Load configuration from YAML file
def load_config(config_file="config.yaml"):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

config = load_config()

class CopyTableRequest(BaseModel):
    dataset_id: str = Field(..., example="my_dataset")
    table_id: str = Field(..., example="my_table")
    predicates: Optional[Dict[str, str]] = Field(None, example={"state": "WA"})
    max_stream_count: int = Field(1, example=1)

class CopyTableResponse(BaseModel):
    status: str
    time_taken: float
    metadata: Dict[str, Any]

    class Config:
        arbitrary_types_allowed = True

def generate_metadata(schema_or_table):
    if isinstance(schema_or_table, pa.Table):
        schema = schema_or_table.schema
    elif isinstance(schema_or_table, pa.Schema):
        schema = schema_or_table
    else:
        raise ValueError("Expected a pyarrow.Table or pyarrow.Schema object")

    metadata = {
        "fields": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable
            }
            for field in schema
        ]
    }
    return metadata

@app.post("/copy-table/", response_model=CopyTableResponse)
async def copy_table(request: CopyTableRequest):
    start_time = time.time()

    try:
        # Initialize BigQuery Storage client
        client = BigQueryReadClient()

        # Configure the BigQuery table reference
        table = f"projects/{config['gcp']['project_id']}/datasets/{request.dataset_id}/tables/{request.table_id}"

        # Create the ReadSession
        requested_session = types.ReadSession()
        requested_session.table = table
        requested_session.data_format = types.DataFormat.ARROW

        # Apply predicates as row restrictions
        if request.predicates:
            conditions = " AND ".join([f"{k} = '{v}'" for k, v in request.predicates.items()])
            requested_session.read_options.row_restriction = conditions

        parent = f"projects/{config['gcp']['project_id']}"
        session = client.create_read_session(
            parent=parent,
            read_session=requested_session,
            max_stream_count=request.max_stream_count,
        )

        # Connect to PostgreSQL
        conn = adbc_driver_postgresql.dbapi.connect(config['postgres']['conn_str'])
        cur = conn.cursor()
        
        # Read the rows from BigQuery
        reader = client.read_rows(session.streams[0].name)
        rows = reader.rows(session)

        arrow_table = rows.to_arrow()
        cur.adbc_ingest(request.table_id, arrow_table, mode="create_append")

        conn.commit()
        cur.close()

        metadata = generate_metadata(arrow_table)
        end_time = time.time()

        return CopyTableResponse(
            status="Success",
            time_taken=end_time - start_time,
            metadata=metadata
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
