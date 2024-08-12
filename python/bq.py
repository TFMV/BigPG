import pyarrow as pa
from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import BigQueryWriteClient, types


def append_rows_pending(project_id: str, dataset_id: str, table_id: str):
    """Create a write stream, write some sample data, and commit the stream."""
    write_client = BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, table_id)
    write_stream = types.WriteStream()

    # When creating the stream, choose the type. Use the PENDING type to wait
    # until the stream is committed before it is visible.
    write_stream.type_ = types.WriteStream.Type.PENDING
    write_stream = write_client.create_write_stream(
        parent=parent, write_stream=write_stream
    )
    stream_name = write_stream.name

    # Define the Arrow schema
    arrow_schema = pa.schema(
        [
            ("customer_name", pa.string()),
            ("row_num", pa.int64()),
        ]
    )

    # Define Arrow record batch
    arrow_table = pa.table(
        [
            ["Alice", "Bob", "Charles"],  # customer_name
            [1, 2, 3],  # row_num
        ],
        schema=arrow_schema,
    )

    # Convert Arrow table to ArrowRecordBatch
    arrow_batches = arrow_table.to_batches()

    # Initialize the first request with the stream name and Arrow schema
    request_template = types.AppendRowsRequest()
    request_template.write_stream = stream_name
    proto_data = types.AppendRowsRequest.ProtoData()

    # Assign the Arrow schema to the request
    proto_data.writer_schema = types.ProtoSchema(
        arrow_schema=types.ArrowSchema(arrow_schema=arrow_schema.serialize())
    )
    request_template.proto_rows = proto_data

    # Some stream types support an unbounded number of requests.
    # Construct an AppendRowsStream to send an arbitrary number of requests to a stream.
    append_rows_stream = bigquery_storage.writer.AppendRowsStream(
        write_client, request_template
    )

    # Create the first batch of row data by appending the serialized Arrow record batch
    request = types.AppendRowsRequest()
    request.offset = 0
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = types.ProtoRows(serialized_rows=[arrow_batches[0].to_bytes()])
    request.proto_rows = proto_data

    response_future_1 = append_rows_stream.send(request)

    print(response_future_1.result())

    # Shutdown background threads and close the streaming connection.
    append_rows_stream.close()

    # A PENDING type stream must be "finalized" before being committed. No new
    # records can be written to the stream after this method has been called.
    write_client.finalize_write_stream(name=write_stream.name)

    # Commit the stream you created earlier.
    batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
    batch_commit_write_streams_request.parent = parent
    batch_commit_write_streams_request.write_streams = [write_stream.name]
    write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    print(f"Writes to stream: '{write_stream.name}' have been committed.")


if __name__ == "__main__":
    append_rows_pending("tfmv-371720", "tfmv", "customer_test")
