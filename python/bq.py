import time
import logging
import pyarrow as pa
from google.cloud import bigquery
from google.protobuf import descriptor_pb2
from pbarrow import arrow_schema_to_proto, arrow_batch_to_proto
from google.cloud.bigquery_storage_v1 import (
    types,
    writer,
    BigQueryReadClient,
    BigQueryWriteClient,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BigQueryService:
    """Encapsulates BigQuery read/write operations using the Storage API."""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.read_client = BigQueryReadClient()
        self.write_client = BigQueryWriteClient()
        self.client = bigquery.Client(project=project_id)
        self.bqstorage_client = BigQueryReadClient()

    def read_table(self, dataset_id: str, table_id: str, max_stream_count: int = 1):
        """Reads a table from BigQuery using the Storage API."""
        table = f"projects/{self.project_id}/datasets/{dataset_id}/tables/{table_id}"

        requested_session = types.ReadSession(
            table=table, data_format=types.DataFormat.ARROW
        )
        requested_session.read_options.arrow_serialization_options.buffer_compression = (
            types.ArrowSerializationOptions.CompressionCodec.LZ4_FRAME
        )

        session = self.read_client.create_read_session(
            parent=f"projects/{self.project_id}",
            read_session=requested_session,
            max_stream_count=max_stream_count,
        )

        stream = session.streams[0].name
        reader = self.read_client.read_rows(stream)
        return reader.rows(session).to_arrow()

    def write_to_bigquery(self, dataset_id: str, table_id: str, arrow_table: pa.Table):
        """Converts Arrow table to Proto and writes to BigQuery using the Storage API."""
        try:
            parent = self.write_client.table_path(self.project_id, dataset_id, table_id)
            write_stream = self.write_client.create_write_stream(
                parent=parent,
                write_stream=types.WriteStream(),
                write_stream_id=f"{parent}/_default"
            )
            stream_name = write_stream.name

            proto_schema = arrow_schema_to_proto(arrow_table.schema)
            proto_messages = arrow_batch_to_proto(arrow_table, proto_schema)

            # Prepare request template
            request_template = types.AppendRowsRequest()
            request_template.write_stream = stream_name
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.writer_schema = types.ProtoSchema(proto_descriptor=proto_schema)
            request_template.proto_rows = proto_data

            # Create append stream
            append_rows_stream = writer.AppendRowsStream(self.write_client, request_template)

            # Send the rows
            request = types.AppendRowsRequest(
                proto_rows=types.AppendRowsRequest.ProtoData(
                    rows=types.ProtoRows(serialized_rows=proto_messages)
                )
            )
            append_rows_stream.send(request)

            # Finalize the stream
            append_rows_stream.close()
            self.write_client.finalize_write_stream(name=stream_name)

            # Commit the stream
            batch_commit_response = self.write_client.batch_commit_write_streams(
                parent=parent,
                write_streams=[stream_name]
            )

            logger.info(
                f"Successfully written {len(proto_messages)} rows to BigQuery table {table_id}"
            )
            return batch_commit_response

        except Exception as e:
            logger.error(f"Error writing to BigQuery: {str(e)}")
            raise

    async def execute_query(self, query):
        """Execute a BigQuery query and return results as Arrow RecordBatch."""
        try:
            # Create BigQuery job
            job = self.client.query(query)

            # Get the destination table
            destination = job.destination

            # Create read session
            read_session = types.ReadSession()
            read_session.table = f"projects/{self.project_id}/datasets/{destination.dataset_id}/tables/{destination.table_id}"
            read_session.data_format = types.DataFormat.ARROW

            session = self.bqstorage_client.create_read_session(
                parent=f"projects/{self.project_id}",
                read_session=read_session,
                max_stream_count=1,
            )

            # Read from the stream
            stream = session.streams[0]
            reader = self.bqstorage_client.read_rows(stream.name)

            # Convert to Arrow RecordBatch
            arrow_batches = []
            for batch in reader.rows().pages:
                arrow_batches.append(batch.to_arrow())

            if not arrow_batches:
                raise ValueError("No data returned from query")

            return arrow_batches[0]  # Return first batch for now

        except Exception as e:
            logger.error(f"Error executing BigQuery query: {str(e)}")
            raise
