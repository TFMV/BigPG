import time
import random
import string
import pyarrow as pa
import logging

from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import GetMessageClass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery-Compatible Type Mapping
TYPES_MAPPING = {
    pa.types.is_binary: FieldDescriptor.TYPE_BYTES,
    pa.types.is_boolean: FieldDescriptor.TYPE_BOOL,
    pa.types.is_floating: FieldDescriptor.TYPE_DOUBLE,
    pa.types.is_integer: FieldDescriptor.TYPE_INT64,
    pa.types.is_string: FieldDescriptor.TYPE_STRING,
    pa.types.is_large_string: FieldDescriptor.TYPE_STRING,
    pa.types.is_date: FieldDescriptor.TYPE_STRING,  # Convert to string for compatibility
    pa.types.is_decimal: FieldDescriptor.TYPE_STRING,  # Convert to string for compatibility
    pa.types.is_time: FieldDescriptor.TYPE_STRING,  # Convert to string for compatibility
    pa.types.is_timestamp: FieldDescriptor.TYPE_STRING,  # Convert to string for compatibility
}

def generate_unique_name(prefix="Message"):
    """Generate a unique message name."""
    timestamp = int(time.time())
    random_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"{prefix}_{timestamp}_{random_suffix}"

def create_nested_message(field_type, message_name, package_name):
    """Create a nested message descriptor for struct types."""
    nested_descriptor = descriptor_pb2.DescriptorProto()
    nested_descriptor.name = message_name

    # 🔹 Proper way to get struct field names and types
    for i, subfield in enumerate(field_type):
        field_proto = nested_descriptor.field.add()
        field_proto.name = subfield.name  # 🔹 Extract field name correctly
        field_proto.number = i + 1

        # 🔹 Extract field type properly
        if type_check := next((check for check in TYPES_MAPPING if check(subfield.type)), None):
            field_proto.type = TYPES_MAPPING[type_check]
        else:
            raise ValueError(f"Unsupported type in nested struct: {subfield.type}")

    return nested_descriptor

def arrow_schema_to_proto(schema: pa.Schema, package_name: str = "dynamic"):
    """Convert Arrow schema to Protocol Buffer descriptor."""
    message_name = generate_unique_name("ArrowMessage")
    
    descriptor_proto = descriptor_pb2.DescriptorProto()
    descriptor_proto.name = message_name

    # First pass: Create all nested message types
    nested_types = {}
    for field in schema:
        if pa.types.is_struct(field.type):
            nested_name = f"{field.name.capitalize()}Type"
            nested_types[field.name] = nested_name
            nested_message = create_nested_message(field.type, nested_name, package_name)
            descriptor_proto.nested_type.add().CopyFrom(nested_message)

    # Second pass: Create fields and reference nested types
    for i, field in enumerate(schema):
        field_proto = descriptor_proto.field.add()
        field_proto.name = field.name
        field_proto.number = i + 1

        if field.name in nested_types:
            field_proto.type = FieldDescriptor.TYPE_MESSAGE
            field_proto.type_name = nested_types[field.name]
        elif type_check := next((check for check in TYPES_MAPPING if check(field.type)), None):
            field_proto.type = TYPES_MAPPING[type_check]
        else:
            raise ValueError(f"Unsupported Arrow type: {field.type}")

    return descriptor_proto

def convert_to_proto_instance(row, message_class, nested_classes):
    """Convert a row dictionary to a Protocol Buffer message instance."""
    row_dict = {}
    
    for field_name, value in row.items():
        if value is None:
            continue

        if field_name in nested_classes and isinstance(value, dict):
            nested_class = nested_classes[field_name]
            row_dict[field_name] = nested_class(**value)
        else:
            row_dict[field_name] = value

    return message_class(**row_dict)

def arrow_batch_to_proto(batch: pa.RecordBatch, descriptor_proto, package_name="dynamic", chunk_size=1000):
    """Convert Arrow RecordBatch to Protocol Buffer messages."""
    pool = DescriptorPool()

    # Create and register the file descriptor
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = f"{package_name}.proto"
    file_proto.package = package_name
    message_proto = file_proto.message_type.add()
    message_proto.CopyFrom(descriptor_proto)

    try:
        pool.Add(file_proto)
    except Exception as e:
        logger.error(f"Failed to add proto to pool: {e}")
        raise

    # Get message classes
    message_class = GetMessageClass(pool.FindMessageTypeByName(f"{package_name}.{descriptor_proto.name}"))
    
    # Get nested message classes
    nested_classes = {}
    for nested_type in descriptor_proto.nested_type:
        type_name = f"{package_name}.{descriptor_proto.name}.{nested_type.name}"
        try:
            nested_classes[nested_type.name.lower().replace('type', '')] = GetMessageClass(
                pool.FindMessageTypeByName(type_name)
            )
        except Exception as e:
            logger.error(f"Failed to get nested class {type_name}: {e}")
            raise

    # Convert batch to messages
    proto_messages = []
    batch_list = batch.to_pylist()

    for i in range(0, len(batch_list), chunk_size):
        chunk = batch_list[i:i + chunk_size]
        try:
            chunk_messages = [
                convert_to_proto_instance(row, message_class, nested_classes).SerializeToString()
                for row in chunk
            ]
            proto_messages.extend(chunk_messages)
        except Exception as e:
            logger.error(f"Failed to convert chunk starting at index {i}: {e}")
            raise

    return proto_messages

# Example usage
if __name__ == "__main__":
    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("score", pa.float32()),
        ("metadata", pa.struct([
            ("age", pa.int32()),
            ("address", pa.string())
        ])),
    ])

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

    proto_schema = arrow_schema_to_proto(schema)
    proto_messages = arrow_batch_to_proto(batch, proto_schema, chunk_size=2)

    logger.info(f"Generated {len(proto_messages)} Proto messages successfully.")
