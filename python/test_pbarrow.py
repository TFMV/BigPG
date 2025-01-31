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