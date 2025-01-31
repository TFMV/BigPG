# BigPG

BigPG is a high-performance FastAPI-based data replication solution that enables bidirectional data movement between PostgreSQL and Google BigQuery. Powered by Apache Arrow, ADBC, and the BigQuery Storage API, BigPG streams large datasets with minimal overhead.

## ⚙️ Configuration

BigPG needs a config.yaml to know where to find your BigQuery and PostgreSQL databases. Here’s what it should look like:

```yaml
gcp:
  project_id: "your_gcp_project_id"
  credentials_file: "/path/to/your/gcp/credentials.json"

postgres:
  conn_str: "postgresql://postgres:password@localhost/databasename"
```

## Notes

- The current implementation is a proof of concept and may require further optimization for production use.
- The code is provided as-is, without any guarantees of stability or performance.

The pbarrow implementation could be of use for other projects but it is not yet ready for production use as it has not been well tested.

🎯 Endpoints & Usage
🔄 POST /bq2pg/ - Transfer Data from BigQuery to PostgreSQL

```json
{
  "dataset_id": "my_dataset",
  "table_id": "my_table",
  "max_stream_count": 1
}
```

🔄 POST /pg2bq/ - Transfer Data from PostgreSQL to BigQuery

```json
{
  "table_name": "users",
  "dataset_id": "my_dataset",
  "table_id": "my_table"
}
```

🏎 Performance Benefits

🚀 Uses Apache Arrow for efficient, zero-copy data processing
🚀 BigQuery Storage API ensures low-latency, cost optimized streaming
🚀 Optimized batch writes prevent excessive memory usage
