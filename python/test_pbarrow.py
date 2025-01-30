import pyarrow as pa
from pbarrow import arrow_schema_to_proto, arrow_batch_to_proto

def main():
    """
    Main function to test the conversion of Arrow schemas and RecordBatches to Proto messages.
    """
    # Define an Arrow Schema with nested fields
    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("score", pa.float32()),
        ("details", pa.struct([
            ("age", pa.int32()),
            ("address", pa.string())
        ]))
    ])

    # Create an Arrow RecordBatch
    batch = pa.RecordBatch.from_arrays([
        pa.array([1, 2, 3]),
        pa.array(["Alice", "Bob", "Charlie"]),
        pa.array([95.5, 89.2, 76.8]),
        pa.array([
            {"age": 30, "address": "123 Main St"},
            {"age": 25, "address": "456 Elm St"},
            {"age": 40, "address": "789 Oak St"}
        ], type=pa.struct([
            ("age", pa.int32()),
            ("address", pa.string())
        ]))
    ], schema=schema)

    # Convert Arrow Schema to Proto Schema
    proto_schema = arrow_schema_to_proto(schema)

    # Convert Arrow RecordBatch to Proto Messages
    proto_messages = arrow_batch_to_proto(batch, proto_schema)

    # Print the serialized messages
    for msg in proto_messages:
        print(msg)

if __name__ == "__main__":
    main()