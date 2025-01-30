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

- You will need to build the ADBC driver from source on the target machine.
- The current implementation is a proof of concept and may require further optimization for production use.
- The code is provided as-is, without any guarantees of stability or performance.

The pbarrow implementation could be of use for other projects but it is not yet ready for production use as it has not been well tested.

