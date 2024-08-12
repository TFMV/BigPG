# 🎉 Welcome to BigPG! 🚀

BigPG is your go-to FastAPI app for effortlessly moving data from Google BigQuery to PostgreSQL. Whether you're dealing with small datasets or massive tables, BigPG has got you covered with powerful data streaming capabilities powered by Apache Arrow.

## 🔥 Features

- **Speedy Transfers:** Blast your data from BigQuery to PostgreSQL in no time.
- **Memory-Efficient:** Stream Arrow record batches directly for those hefty datasets.
- **Flexibility:** Apply filters to your data with custom predicates.
- **Schema-Aware:** Automatically generates metadata to keep you informed about your data structure.

## ⚙️ Configuration

BigPG needs a config.yaml to know where to find your BigQuery and PostgreSQL databases. Here’s what it should look like:

```yaml
gcp:
  project_id: "your_gcp_project_id"
  credentials_file: "/path/to/your/gcp/credentials.json"

postgres:
  conn_str: "postgresql://postgres:password@localhost/databasename"
```

## 🎯 Endpoints

### 🔄 `/bq2pg/`

**Method:** `POST`

**What it does:** Loads your entire BigQuery table into memory (as a PyArrow Table) and then sends it to PostgreSQL.

**Request Body:**

- `dataset_id` (str): The ID of your BigQuery dataset.
- `table_id` (str): The ID of your BigQuery table.
- `predicates` (Optional[Dict[str, str]]): Add filters to zero in on the data you want.
- `max_stream_count` (int): The number of streams to use (default: 1).

**Response:**

- `status`: Whether the operation was a success or failure.
- `time_taken`: How long it took to complete the operation.
- `metadata`: All the juicy details about your data structure.
- `rows_loaded`: The number of rows that made it to PostgreSQL.

### 🚀 `/bq2pg_arrow/`

**Method:** `POST`

**What it does:** Streams your data in manageable chunks directly from BigQuery to PostgreSQL. No more running out of memory!

**Request Body:**

- `dataset_id` (str): The ID of your BigQuery dataset.
- `table_id` (str): The ID of your BigQuery table.
- `predicates` (Optional[Dict[str, str]]): Add filters to zero in on the data you want.
- `max_stream_count` (int): The number of streams to use (default: 1).

**Response:**

- `status`: Whether the operation was a success or failure.
- `time_taken`: How long it took to complete the operation.
- `metadata`: All the juicy details about your data structure.
- `rows_loaded`: The number of rows that made it to PostgreSQL.

### Real Request and Response (local machine over public internet)

Request:

```json
{
  "dataset_id": "tpch",
  "table_id": "orders",
  "max_stream_count": 1
}
```

Response:

```json
{
  "status": "Success",
  "time_taken": 56.349376916885376,
  "metadata": {
    "fields": [
      {
        "name": "o_orderkey",
        "type": "int64",
        "nullable": true
      },
      {
        "name": "o_custkey",
        "type": "int64",
        "nullable": true
      },
      {
        "name": "o_orderstatus",
        "type": "string",
        "nullable": true
      },
      {
        "name": "o_totalprice",
        "type": "double",
        "nullable": true
      },
      {
        "name": "o_orderdate",
        "type": "date32[day]",
        "nullable": true
      },
      {
        "name": "o_orderpriority",
        "type": "string",
        "nullable": true
      },
      {
        "name": "o_clerk",
        "type": "string",
        "nullable": true
      },
      {
        "name": "o_shippriority",
        "type": "int64",
        "nullable": true
      },
      {
        "name": "o_comment",
        "type": "string",
        "nullable": true
      }
    ]
  },
  "rows_loaded": 15000000
}
```
