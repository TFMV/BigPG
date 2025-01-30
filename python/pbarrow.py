import pyarrow as pa
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import GetMessageClass

# Map Arrow types to Proto field types
ARROW_TO_PROTO = {
    pa.int8(): FieldDescriptor.TYPE_INT32,
    pa.int16(): FieldDescriptor.TYPE_INT32,
    pa.int32(): FieldDescriptor.TYPE_INT32,
    pa.int64(): FieldDescriptor.TYPE_INT64,
    pa.uint8(): FieldDescriptor.TYPE_UINT32,
    pa.uint16(): FieldDescriptor.TYPE_UINT32,
    pa.uint32(): FieldDescriptor.TYPE_UINT32,
    pa.uint64(): FieldDescriptor.TYPE_UINT64,
    pa.float16(): FieldDescriptor.TYPE_FLOAT,
    pa.float32(): FieldDescriptor.TYPE_FLOAT,
    pa.float64(): FieldDescriptor.TYPE_DOUBLE,
    pa.string(): FieldDescriptor.TYPE_STRING,
    pa.binary(): FieldDescriptor.TYPE_BYTES,
    pa.bool_(): FieldDescriptor.TYPE_BOOL,
    pa.timestamp("ms"): FieldDescriptor.TYPE_INT64,
}

def create_nested_proto(field, parent_message, package_name):
    """
    Create a nested Proto message for a nested Arrow field inside a parent message.

    Args:
        field (pa.Field): The Arrow field to convert.
        parent_message (descriptor_pb2.DescriptorProto): The parent Proto message descriptor.
        package_name (str): The package name for the Proto message.

    Returns:
        str: The full name of the nested Proto message.
    """
    nested_msg_name = f"{field.name.capitalize()}Entry"
    nested_msg = parent_message.nested_type.add()
    nested_msg.name = nested_msg_name

    for i, subfield in enumerate(field.type):
        subfield_proto = nested_msg.field.add()
        subfield_proto.name = subfield.name
        subfield_proto.number = i + 1

        arrow_type = subfield.type
        if arrow_type in ARROW_TO_PROTO:
            subfield_proto.type = ARROW_TO_PROTO[arrow_type]
        elif pa.types.is_struct(arrow_type):
            subfield_proto.type = FieldDescriptor.TYPE_MESSAGE
            subfield_proto.type_name = f".{package_name}.{parent_message.name}.{nested_msg.name}"
        else:
            raise ValueError(f"Unsupported Arrow type: {arrow_type}")

    return f"{package_name}.{parent_message.name}.{nested_msg.name}"

def arrow_schema_to_proto(schema: pa.Schema, message_name: str = "ArrowMessage", package_name: str = "dynamic"):
    """
    Dynamically generate a Proto schema from an Arrow schema.

    Args:
        schema (pa.Schema): The Arrow schema to convert.
        message_name (str): The name of the Proto message.
        package_name (str): The package name for the Proto message.

    Returns:
        descriptor_pb2.DescriptorProto: The Proto message descriptor.
    """
    descriptor_proto = descriptor_pb2.DescriptorProto()
    descriptor_proto.name = message_name

    nested_message_map = {}

    # Create nested messages first
    for field in schema:
        if pa.types.is_struct(field.type):
            nested_message_map[field.name] = create_nested_proto(field, descriptor_proto, package_name)

    # Now create the main message fields
    for i, field in enumerate(schema):
        field_proto = descriptor_proto.field.add()
        field_proto.name = field.name
        field_proto.number = i + 1

        if field.name in nested_message_map:
            field_proto.type = FieldDescriptor.TYPE_MESSAGE
            field_proto.type_name = f".{nested_message_map[field.name]}"
        elif field.type in ARROW_TO_PROTO:
            field_proto.type = ARROW_TO_PROTO[field.type]
        else:
            raise ValueError(f"Unsupported Arrow type: {field.type}")

    return descriptor_proto

def arrow_batch_to_proto(batch: pa.RecordBatch, descriptor_proto, package_name="dynamic"):
    """
    Convert an Arrow RecordBatch into a list of serialized Proto messages.

    Args:
        batch (pa.RecordBatch): The Arrow RecordBatch to convert.
        descriptor_proto (descriptor_pb2.DescriptorProto): The Proto message descriptor.
        package_name (str): The package name for the Proto message.

    Returns:
        list: A list of serialized Proto messages.
    """
    pool = DescriptorPool()

    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "dynamically_generated.proto"
    file_proto.package = package_name
    message_proto = file_proto.message_type.add()
    message_proto.CopyFrom(descriptor_proto)

    pool.Add(file_proto)

    # Retrieve the message class
    full_name = f"{package_name}.{descriptor_proto.name}"
    message_class = GetMessageClass(pool.FindMessageTypeByName(full_name))

    nested_message_classes = {
        f"{full_name}.{f.name}": GetMessageClass(pool.FindMessageTypeByName(f"{full_name}.{f.name}"))
        for f in descriptor_proto.nested_type
    }

    proto_messages = []
    for i in range(batch.num_rows):
        row_dict = {}
        for j in range(batch.num_columns):
            field_name = batch.schema.names[j]
            value = batch[j][i].as_py()

            if f"{full_name}.{field_name.capitalize()}Entry" in nested_message_classes and isinstance(value, dict):
                nested_class = nested_message_classes[f"{full_name}.{field_name.capitalize()}Entry"]
                nested_value = {k: v for k, v in value.items()}
                row_dict[field_name] = nested_class(**nested_value)
            else:
                row_dict[field_name] = value

        proto_messages.append(message_class(**row_dict).SerializeToString())

    return proto_messages
