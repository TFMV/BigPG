# BigPG

BigPG is a high-performance FastAPI application designed to transfer data between Google BigQuery and PostgreSQL. Leveraging Apache Arrow and Google's BigQuery Storage Write API, it enables fast, memory-efficient, and schema-aware data movement.

## ⚙️ Configuration

BigPG needs a config.yaml to know where to find your BigQuery and PostgreSQL databases. Here’s what it should look like:

```yaml
gcp:
  project_id: "your_gcp_project_id"
  credentials_file: "/path/to/your/gcp/credentials.json"

postgres:
  conn_str: "postgresql://postgres:password@localhost/databasename"
```
