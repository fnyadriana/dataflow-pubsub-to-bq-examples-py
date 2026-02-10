# Implementation Plan: Google Managed Kafka Pipeline

This document is the implementation checklist for adding a Google Managed Service for Apache Kafka (GMK) pipeline variant to the repository. The Kafka pipeline mirrors data from the public Pub/Sub taxi rides topic into a GMK Kafka topic, then reads from Kafka via Dataflow and writes to BigQuery with the same flattened 14-column schema.

## Architecture

### Current

```
Pub/Sub (taxirides-realtime)
        |
   [Subscription]
        |
        v
  Dataflow (pipeline.py)
  ReadFromPubSub --> ParsePubSubMessage --> WriteToBigQuery
        |
        v
  BigQuery (taxi_events, 14 columns)
```

### Target (New Kafka Pipeline)

```
Pub/Sub (taxirides-realtime)
        |
   [Subscription: taxi_telemetry_kafka]
        |
        v
  GMK Connect Cluster
    (Pub/Sub Source Connector)
        |
        v
  GMK Kafka Topic (taxi-rides)
        |
        v
  Dataflow (pipeline_kafka.py)
  Managed I/O Kafka Read --> ParseKafkaMessage --> WriteToBigQuery
        |
        v
  BigQuery (taxi_events_kafka, 14 columns)
```

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Metadata handling | Kafka-native | Use Kafka timestamp for `publish_time`, drop Pub/Sub `message_id`. Cleaner separation; Kafka is the source of truth. |
| Kafka read API | Managed I/O (`beam.managed.Read`) | Google-recommended for GMK. Handles auth natively. Supported in Python SDK 2.61.0+ (project uses 2.70.0). |
| Infrastructure setup | Full automation in shell script | Single `run_dataflow_kafka.sh` creates all GMK infra + launches pipeline. |
| BigQuery table | Separate `taxi_events_kafka` | Keeps data isolated from the Pub/Sub pipeline for comparison and analysis. |

## File Inventory

### New Files (5)

| # | File | Purpose |
|---|------|---------|
| 1 | `dataflow_pubsub_to_bq/pipeline_kafka_options.py` | Custom `PipelineOptions` with Kafka-specific CLI args |
| 2 | `dataflow_pubsub_to_bq/transforms/kafka_to_tablerow.py` | `ParseKafkaMessage` DoFn + BigQuery schema reuse |
| 3 | `dataflow_pubsub_to_bq/pipeline_kafka.py` | Pipeline entry point: Kafka read, parse, BQ write |
| 4 | `tests/test_kafka_to_tablerow.py` | Unit tests for `ParseKafkaMessage` |
| 5 | `run_dataflow_kafka.sh` | Deployment script with full GMK infra automation |

### Files to Modify (0)

No existing files are modified. This is a purely additive change.

## Implementation Checklist

### Phase 1: Pipeline Options

- [x] **1.1** Create `dataflow_pubsub_to_bq/pipeline_kafka_options.py`
  - Extend `PipelineOptions` with:
    - `--bootstrap_server` (required): GMK cluster bootstrap address
    - `--kafka_topic` (required): Kafka topic name to consume from
    - `--output_table` (required): BigQuery output table (`PROJECT:DATASET.TABLE`)
    - `--source_name` (required): Identifier stored in `subscription_name` column for metadata tracking

### Phase 2: Kafka Transform

- [x] **2.1** Create `dataflow_pubsub_to_bq/transforms/kafka_to_tablerow.py`
  - Implement `ParseKafkaMessage(beam.DoFn)`:
    - Constructor accepts `source_name: str`
    - `process()` receives a Beam Row from Managed I/O (has `payload` bytes field)
    - Decode `payload` from bytes to UTF-8 string
    - Parse JSON, extract 9 taxi ride fields (identical to `json_to_tablerow.py`):
      - `ride_id`, `point_idx`, `latitude`, `longitude`, `timestamp`
      - `meter_reading`, `meter_increment`, `ride_status`, `passenger_count`
    - Populate metadata fields:
      - `subscription_name` = `self.source_name`
      - `message_id` = `""` (no Pub/Sub message ID available from Kafka)
      - `publish_time` = current time (Kafka timestamp not directly accessible via Managed I/O RAW format)
      - `processing_time` = current wall clock time
      - `attributes` = `"{}"`
    - Yield 14-column dict matching existing BigQuery schema
    - Handle errors: log and drop malformed messages (same pattern as `json_to_tablerow.py`)
  - Reuse `get_bigquery_schema()` from `json_to_tablerow.py` (import, do not duplicate)

### Phase 3: Pipeline Entry Point

- [x] **3.1** Create `dataflow_pubsub_to_bq/pipeline_kafka.py`
  - Import `PipelineOptions`, `KafkaToBigQueryOptions`, `ParseKafkaMessage`, `get_bigquery_schema`
  - Build pipeline:
    ```
    Read from Kafka (Managed I/O, RAW format, GMK SASL/SSL auth)
    --> Extract payload bytes from Beam Row
    --> ParseKafkaMessage DoFn
    --> WriteToBigQuery (Storage Write API, triggering_frequency=1, CREATE_NEVER)
    ```
  - Managed I/O config must include GMK auth properties:
    - `"security.protocol": "SASL_SSL"`
    - `"sasl.mechanism": "OAUTHBEARER"`
    - `"sasl.login.callback.handler.class": "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler"`
    - `"sasl.jaas.config": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"`

### Phase 4: Unit Tests

- [x] **4.1** Create `tests/test_kafka_to_tablerow.py`
  - **Test: valid JSON parsing** - Verify a well-formed taxi JSON payload produces correct 14-column output with all fields mapped
  - **Test: malformed JSON handling** - Verify broken JSON is silently dropped (no output yielded)
  - **Test: missing fields default values** - Verify partial JSON (missing some fields) uses defaults (`""` for strings, `0` for ints, `0.0` for floats)
  - **Test: source_name propagation** - Verify the `source_name` constructor arg appears in `subscription_name` field
  - Use `apache_beam.testing.test_pipeline.TestPipeline` and `assert_that` matchers

### Phase 5: Verification

- [x] **5.1** Run linter: `uv run ruff check .`
- [x] **5.2** Run formatter: `uv run ruff format .`
- [x] **5.3** Run tests: `uv run pytest` (4/4 passed)
- [ ] **5.4** Verify module imports: `uv run python -c "from dataflow_pubsub_to_bq.pipeline_kafka import run"`

### Phase 6: Deployment Script

- [x] **6.1** Create `run_dataflow_kafka.sh`
  - Configuration variables section:
    - `PROJECT_ID`, `REGION`, `TEMP_BUCKET`, `BIGQUERY_DATASET`
    - `BIGQUERY_TABLE="taxi_events_kafka"`
    - `KAFKA_CLUSTER_NAME`, `KAFKA_TOPIC_NAME="taxi-rides"`
    - `CONNECT_CLUSTER_NAME`
    - `SUBNET_NAME` (required for GMK cluster networking)
    - `PUBLIC_TOPIC` (the public Pub/Sub topic)
    - `SUBSCRIPTION_NAME="taxi_telemetry_kafka"`
    - `SOURCE_NAME="kafka-taxi-rides"`
  - Infrastructure steps (idempotent, check-before-create pattern):
    1. Check/Create GCS bucket
    2. Check/Create BigQuery dataset
    3. Check/Create BigQuery table `taxi_events_kafka` (same 14-col schema as `taxi_events`)
    4. Check/Create Pub/Sub subscription `taxi_telemetry_kafka` to public topic
    5. Check/Create GMK cluster:
       ```
       gcloud managed-kafka clusters create $KAFKA_CLUSTER_NAME \
         --location=$REGION --cpu=3 --memory=12GiB \
         --subnets=projects/$PROJECT_ID/regions/$REGION/subnetworks/$SUBNET_NAME
       ```
    6. Wait for cluster to become ACTIVE (poll with `gcloud managed-kafka clusters describe`)
    7. Check/Create Kafka topic `taxi-rides`:
       ```
       gcloud managed-kafka topics create $KAFKA_TOPIC_NAME \
         --cluster=$KAFKA_CLUSTER_NAME --location=$REGION \
         --partitions=40 --replication-factor=3
       ```
    8. Get bootstrap address:
       ```
       BOOTSTRAP=$(gcloud managed-kafka clusters describe $KAFKA_CLUSTER_NAME \
         --location=$REGION --format="value(bootstrapAddress)")
       ```
    9. Check/Create Connect cluster
    10. Grant Managed Kafka SA (`service-PROJECT_NUMBER@gcp-sa-managedkafka.iam.gserviceaccount.com`) the `roles/pubsub.subscriber` and `roles/pubsub.viewer` roles
    11. Create Pub/Sub Source connector:
        ```
        gcloud managed-kafka connectors create pubsub-taxi-source \
          --connect-cluster=$CONNECT_CLUSTER_NAME --location=$REGION \
          --configs=connector.class=com.google.pubsub.kafka.source.CloudPubSubSourceConnector,\
          cps.project=$PROJECT_ID,cps.subscription=$SUBSCRIPTION_NAME,\
          kafka.topic=$KAFKA_TOPIC_NAME,tasks.max=3,\
          value.converter=org.apache.kafka.connect.converters.ByteArrayConverter,\
          key.converter=org.apache.kafka.connect.storage.StringConverter
        ```
    12. Build wheel (`uv build --wheel`)
    13. Submit Dataflow job:
        ```
        uv run python -m dataflow_pubsub_to_bq.pipeline_kafka \
          --runner=DataflowRunner \
          --bootstrap_server=$BOOTSTRAP \
          --kafka_topic=$KAFKA_TOPIC_NAME \
          --output_table=$FULL_TABLE \
          --source_name=$SOURCE_NAME \
          ... (standard Dataflow args)
        ```
  - Post-submission: print Dataflow console URL, BigQuery monitoring URL

## BigQuery Schema (Unchanged)

The `taxi_events_kafka` table uses the same 14-column schema as `taxi_events`:

```
subscription_name   STRING    <- populated with source_name ("kafka-taxi-rides")
message_id          STRING    <- empty string (no Pub/Sub message ID from Kafka)
publish_time        TIMESTAMP <- current time at processing (partitioned, clustered)
processing_time     TIMESTAMP <- current wall clock time
ride_id             STRING
point_idx           INT64
latitude            FLOAT
longitude           FLOAT
timestamp           TIMESTAMP
meter_reading       FLOAT
meter_increment     FLOAT
ride_status         STRING
passenger_count     INT64
attributes          STRING    <- empty JSON "{}"
```

## Capacity Analysis

Sizing calculations for ~2,000 RPS of taxi ride messages (~300 bytes each).

### Message Throughput

```
Produce rate = 2,000 msg/s * 300 bytes = 0.6 MBps
```

### GMK Kafka Cluster

Using the official cluster sizing formula (https://docs.cloud.google.com/managed-service-for-apache-kafka/docs/plan-cluster-size):

| Step | Formula | Result |
|------|---------|--------|
| Total write bandwidth | 0.6 MBps * 3 replicas | 1.8 MBps |
| Total read bandwidth | 0.6 + 0.6 * (3-1) | 1.8 MBps |
| Write-equivalent rate | 1.8 + (1.8 / 4) | 2.25 MBps |
| Target utilization | 50% baseline | 0.5 |
| vCPU count | ceil(2.25 / 20 / 0.5) | 1 (min 3) |
| Memory | 3 * 4 GiB | 12 GiB |

Result: `--cpu=3 --memory=12GiB` (minimum cluster, 3 brokers, 13x headroom).

Note: Messages under 10 KB reduce per-CPU throughput, but the connector batches
messages before writing to Kafka, mitigating this for our workload.

### Kafka Topic Partitions

Dataflow parallelism is bounded by partition count:

- General rule: partitions >= `4 * max_num_workers` = 4 * 5 = 20
- Runner v2 rule: partitions = `2 * total_vCPUs` = 2 * 20 = 40

Result: `--partitions=40` with `--replication-factor=3`.
Each partition sees ~50 msg/s (2000 / 40), ensuring even load distribution.

### GMK Connect Cluster

At 2K RPS of small messages with 3 connector tasks:

- Each task handles ~667 msg/s from Pub/Sub (light workload)
- StreamingPull enabled for lowest latency
- No heavy transformation logic in the connector

Result: `--cpu=3 --memory=3GiB` (minimum cluster).

### Dataflow Pipeline

The existing config (5 workers, n2-standard-4, Runner v2) is proven at 2K+ RPS.
With 40 partitions: 40 / 5 = 8 partitions per worker, well within capacity.

### Managed I/O Redistribute

Not enabled by default. With 40 partitions >= `4 * max_workers`, Dataflow has
sufficient parallelism. Redistribute config is available as commented-out options
in `pipeline_kafka.py` if partitions become a bottleneck.

## Dependencies

No new Python dependencies required. The existing `apache-beam[gcp]==2.70.0` includes:
- `beam.managed.Read` / `beam.managed.KAFKA` (Managed I/O, available since 2.61.0)
- All GMK SASL/SSL auth is handled at the Java expansion service level (cross-language transform)

## IAM Requirements

The Dataflow worker service account needs:
- `roles/managedkafka.client` - Access the GMK cluster
- `roles/dataflow.worker` - Run Dataflow workers (existing)
- `roles/bigquery.dataEditor` - Write to BigQuery (existing)

The Managed Kafka service account (`service-PROJECT_NUMBER@gcp-sa-managedkafka.iam.gserviceaccount.com`) needs:
- `roles/pubsub.subscriber` - Pull messages from Pub/Sub subscription
- `roles/pubsub.viewer` - View Pub/Sub subscription metadata

## Post-Implementation

After all phases are complete:
- [ ] Update `README.md` to document the Kafka pipeline option
- [ ] Update `AGENTS.md` project structure section
- [ ] Update the project structure in README to include new files
