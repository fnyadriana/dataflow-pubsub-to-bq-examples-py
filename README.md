# Pub/Sub to BigQuery Dataflow Pipelines (Python)

Apache Beam pipelines (Python) that read taxi ride data from Google Cloud Pub/Sub and write to BigQuery. Three pipeline variants demonstrate different ingestion strategies.

## Architecture

```
Pub/Sub (taxirides-realtime) -> Dataflow Pipeline -> BigQuery
```

## Pipeline Variants

### 1. Standard (Flattened)

Parses JSON fields into specific BigQuery columns with full type mapping.

```bash
./run_dataflow.sh
```

- Extracts individual fields (`ride_id`, `latitude`, `timestamp`, etc.) into typed columns
- DLQ table for malformed messages
- Timestamp parsed from ISO 8601 string to BigQuery `TIMESTAMP`

### 2. Raw JSON

Ingests the entire message payload into a BigQuery `JSON` column.

```bash
./run_dataflow_json.sh
```

- Stores raw payload in a `JSON` column for flexible querying
- Schema evolution without table updates
- DLQ table for malformed messages

### 3. Schema-Driven (Pub/Sub Schema Registry)

Fetches Avro schema from the Pub/Sub Schema Registry at startup and dynamically generates BigQuery table schema and field extraction logic.

```bash
./run_dataflow_schema_driven.sh
# In a separate terminal:
./run_mirror_publisher.sh
```

- Schema registry is the single source of truth
- Avro schema drives BQ column generation automatically
- Mirror publisher bridges the public topic to a schema-validated topic (pure pass-through)
- Custom `iso-datetime` logical type maps ISO 8601 strings to BQ `TIMESTAMP`
- No DLQ needed -- schema registry validates at publish time

For detailed design, schema evolution strategy, and architecture decisions, see [docs/schema_evolution_plan.md](docs/schema_evolution_plan.md).

## Features

- Reads from public Pub/Sub topic (`taxirides-realtime`)
- Captures Pub/Sub metadata (subscription_name, message_id, publish_time, attributes)
- Writes to partitioned and clustered BigQuery tables
- BigQuery Storage Write API with 1-second micro-batching
- Production deployment via Dataflow (DataflowRunner with Runner V2)

## Project Structure

```
dataflow-pubsub-to-bq-examples-py/
├── README.md
├── AGENTS.md                              # Guidelines for AI agents
├── pyproject.toml                         # Project configuration (uv)
├── run_dataflow.sh                        # Deployment: Standard pipeline
├── run_dataflow_json.sh                   # Deployment: JSON pipeline
├── run_dataflow_schema_driven.sh          # Deployment: Schema-driven pipeline
├── run_mirror_publisher.sh                # Mirror publisher launcher
├── publish_to_schema_topic.py             # Mirror publisher (pass-through relay)
├── schemas/
│   ├── taxi_ride_v1.avsc                  # Avro v1 schema definition
│   └── generate_bq_schema.py             # BQ schema generator from registry
├── docs/
│   └── schema_evolution_plan.md           # Schema evolution design doc
├── tests/
│   ├── test_json_to_tablerow.py           # Standard pipeline tests
│   ├── test_raw_json.py                   # JSON pipeline tests
│   └── test_schema_driven_to_tablerow.py  # Schema-driven pipeline tests
└── dataflow_pubsub_to_bq/
    ├── __init__.py
    ├── pipeline.py                        # Entry point: Standard
    ├── pipeline_json.py                   # Entry point: JSON
    ├── pipeline_schema_driven.py          # Entry point: Schema-driven
    ├── pipeline_options.py                # Options: Standard + JSON
    ├── pipeline_schema_driven_options.py  # Options: Schema-driven
    └── transforms/
        ├── __init__.py
        ├── json_to_tablerow.py            # Standard: JSON field extraction
        ├── raw_json.py                    # JSON: raw payload storage
        └── schema_driven_to_tablerow.py   # Schema-driven: Avro-based extraction
```

## Input Data Format

The pipelines read taxi ride events from the public Pub/Sub topic with this structure:

```json
{
  "ride_id": "cd3b816b-bc33-4893-a232-ea7cbbb9d0e8",
  "point_idx": 104,
  "latitude": 40.76011,
  "longitude": -73.97316,
  "timestamp": "2026-02-10T08:14:17.61844-05:00",
  "meter_reading": 4.8108845,
  "meter_increment": 0.046258505,
  "ride_status": "enroute",
  "passenger_count": 6
}
```

The `timestamp` field is an ISO 8601 string with timezone offset.

## BigQuery Output Schema

### Standard Pipeline (`taxi_events`)

```
subscription_name: STRING
message_id: STRING
publish_time: TIMESTAMP (partitioned, clustered)
processing_time: TIMESTAMP
ride_id: STRING
point_idx: INT64
latitude: FLOAT
longitude: FLOAT
timestamp: TIMESTAMP
meter_reading: FLOAT
meter_increment: FLOAT
ride_status: STRING
passenger_count: INT64
attributes: STRING
```

### JSON Pipeline (`taxi_events_json`)

```
subscription_name: STRING
message_id: STRING
publish_time: TIMESTAMP (partitioned, clustered)
processing_time: TIMESTAMP
attributes: STRING
payload: JSON
```

### Schema-Driven Pipeline (`taxi_events_schema`)

Envelope fields (static) plus dynamic payload fields generated from Avro schema:

```
# Envelope (always present)
subscription_name: STRING
message_id: STRING
publish_time: TIMESTAMP (partitioned, clustered)
processing_time: TIMESTAMP
attributes: STRING
schema_name: STRING
schema_revision_id: STRING
schema_encoding: STRING

# Payload (generated from Avro schema)
ride_id: STRING
point_idx: INT64
latitude: FLOAT64
longitude: FLOAT64
timestamp: TIMESTAMP
meter_reading: FLOAT64
meter_increment: FLOAT64
ride_status: STRING
passenger_count: INT64
```

## Prerequisites

- Google Cloud Project with billing enabled
- APIs enabled: Dataflow, Pub/Sub, BigQuery, Cloud Storage
- gcloud CLI configured
- Python 3.12+
- uv package manager

## Setup

### Install uv (if not installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Or on macOS/Linux with Homebrew:

```bash
brew install uv
```

### Install Project Dependencies

```bash
cd dataflow-pubsub-to-bq-examples-py
uv sync
```

This will create a virtual environment and install all dependencies defined in `pyproject.toml`.

## Testing

Run the pytest suite:

```bash
uv run pytest
```

## Production Deployment

### Standard Flattened Schema

```bash
./run_dataflow.sh
```

This script will:
1. Check/create GCS bucket for temp/staging
2. Check/create BigQuery dataset
3. Check/create BigQuery table with schema
4. Check/create Pub/Sub subscription
5. Install Python dependencies
6. Submit pipeline to Dataflow with Runner V2

### Raw JSON Column Schema

```bash
./run_dataflow_json.sh
```

The output table is `taxi_events_json`.

### Schema-Driven (Pub/Sub Schema Registry)

```bash
./run_dataflow_schema_driven.sh
```

This script will:
1. Create Pub/Sub schema from `schemas/taxi_ride_v1.avsc` (first run only)
2. Create schema-enabled topic and subscriptions
3. Fetch schema from registry and generate BQ table schema
4. Submit pipeline to Dataflow

Then start the mirror publisher in a separate terminal:

```bash
./run_mirror_publisher.sh
```

## Performance Optimizations

### BigQuery Storage Write API

This pipeline uses the **BigQuery Storage Write API** for optimal performance:

| Feature | Benefit |
|---------|---------|
| Auto-batching | Batches rows internally before committing |
| Exactly-once semantics | Built-in deduplication |
| Lower cost | First 2 TB/month free, then $0.025/GB (vs $0.05/GB for legacy streaming) |
| Higher throughput | Better for high-volume streaming (2,000+ RPS) |
| Micro-batching | `triggering_frequency=1` flushes every 1 second |

**Implementation:**
```python
bigquery.WriteToBigQuery(
    method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
    triggering_frequency=1,  # Flush every 1 second
)
```

This provides automatic micro-batching without manual `GroupIntoBatches` transforms.

### Alternative: Legacy Streaming API with Count-Based Batching

If you need to use the **legacy STREAMING_INSERTS API** with count-based batching (e.g., "100 records OR 1 second, whichever comes first"), you can use `GroupIntoBatches`:

```python
import random
from apache_beam.transforms.util import GroupIntoBatches

# In pipeline.py
(messages
 | 'ParseMessages' >> beam.ParDo(
     ParsePubSubMessage(custom_options.subscription_name)
 )
 | 'AddRandomKeys' >> beam.Map(lambda x: (random.randint(0, 4), x))  # 5 shards for parallelism
 | 'BatchMessages' >> GroupIntoBatches.WithShardedKey(
     batch_size=100,  # Flush after 100 records
     max_buffering_duration_secs=1,  # OR after 1 second
 )
 | 'ExtractBatches' >> beam.FlatMapTuple(lambda k, v: v)
 | 'WriteToBigQuery' >> bigquery.WriteToBigQuery(
     table=custom_options.output_table,
     schema={'fields': get_bigquery_schema()},
     write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
     create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
     method=bigquery.WriteToBigQuery.Method.STREAMING_INSERTS,  # Legacy API
 ))
```

**Trade-offs:**
- **Pros:** Exact control over batch size (100 records OR 1 second trigger)
- **Cons:** Higher cost ($0.05/GB vs $0.025/GB), requires sharding for parallelism, more complex code
- **When to use:** When you need exact count-based triggering or are using legacy systems

**Note:** For production workloads, the Storage Write API is strongly recommended over legacy streaming inserts.

## Performance Results

### Production Test Results

Tested with the configuration below, the pipeline achieved excellent performance:

| Metric | Result |
|--------|--------|
| **Target Throughput** | 2,000 RPS |
| **Sustained Throughput** | **3,400+ RPS** |
| **Performance vs Target** | **170% (exceeds target)** |
| **Sustained Range** | 3,300 - 3,500 RPS (stable) |
| **Peak Throughput (Backlog Catchup)** | **58,630+ RPS** |
| **Peak Scaling Factor** | **29x target, 17x sustained** |

**Metrics Dashboard:**

![Dataflow Metrics - Peak Throughput](images/metrics_py.jpg)

**Key Findings:**
- The Python SDK with Storage Write API and Runner V2 easily handles high-volume streaming workloads (2k+ RPS)
- During backlog catchup scenarios, the pipeline can scale to **58k+ RPS** to clear accumulated messages
- Demonstrates excellent burst capacity for handling traffic spikes or recovering from downtime

## Scaling Analysis

### Dataflow Runner V2 Configuration

The production deployment uses these settings:

| Setting | Value | Notes |
|---------|-------|-------|
| Runner | DataflowRunner with Runner V2 | Required for Python SDK |
| Machine Type | n2-standard-4 | 4 vCPUs, 16 GB RAM |
| Workers | 5 (fixed) | No auto-scaling |
| Threads per Worker | 48 (default) | 12 threads per vCPU x 4 vCPUs |
| Total Capacity | 240 threads | 5 workers x 48 threads |
| Actual Load | ~3,400 RPS | From Pub/Sub subscription |
| Per-Thread Load | ~14 messages/sec | 3,400 / 240 threads |

### Thread Configuration

The Python SDK on Dataflow uses **12 threads per vCPU** by default. For n2-standard-4 machines (4 vCPUs), this means:
- 12 threads/vCPU x 4 vCPUs = **48 threads per worker**
- 5 workers x 48 threads = **240 total threads**

This is sufficient for processing 3,400+ RPS with low system lag.

### Monitoring

Monitor the pipeline performance at:

- Dataflow console: https://console.cloud.google.com/dataflow
- BigQuery: https://console.cloud.google.com/bigquery

Key metrics to watch:
- System lag (data freshness)
- Throughput (elements/sec)
- CPU utilization per worker
- Memory usage per worker

## Configuration

### run_dataflow.sh
Edit these variables:
- `PROJECT_ID`: Your GCP project ID
- `REGION`: GCP region for Dataflow job
- `TEMP_BUCKET`: GCS bucket for temp/staging
- `BIGQUERY_DATASET`: BigQuery dataset name
- `SUBSCRIPTION_NAME`: Pub/Sub subscription name

## Troubleshooting

### High System Lag

If system lag is increasing:
1. Increase number of workers (`--num_workers`)
2. Use larger machine type (`--machine_type=n2-standard-8`)
3. Check BigQuery write quota limits

### OOM Errors

If workers run out of memory:
1. Reduce `--number_of_worker_harness_threads` (max 12)
2. Use machine type with more RAM
3. Check for memory leaks in transforms

### Slow BigQuery Writes

If BigQuery is the bottleneck:
1. Enable Streaming Engine (`--enable_streaming_engine`)
2. Check BigQuery streaming insert quota
3. Consider batch writes with windowing

## License

This project is provided as-is for educational and testing purposes.
