#!/bin/bash

# Dataflow Kafka to BigQuery Pipeline with Runner V2
# This script provisions Google Managed Kafka infrastructure, mirrors the public
# Pub/Sub taxi data into a Kafka topic, and runs a Dataflow pipeline that reads
# from Kafka and writes to BigQuery with the same flattened 14-column schema.
#
# Infrastructure created:
# - Pub/Sub subscription (to public taxirides-realtime topic)
# - GMK Kafka cluster + topic
# - GMK Connect cluster + Pub/Sub Source connector
# - BigQuery dataset + table (taxi_events_kafka)
#
# Prerequisites:
# - gcloud CLI configured with appropriate permissions
# - APIs enabled: Dataflow, Managed Kafka, Pub/Sub, BigQuery, Cloud Storage
# - VPC subnet accessible for GMK cluster networking

set -e

# --- Configuration ---
PROJECT_ID="your-project-id"
REGION="us-central1"
TEMP_BUCKET="gs://your-gcs-bucket"
SUBNET_NAME="default"

# BigQuery
BIGQUERY_DATASET="demo_dataset"
BIGQUERY_TABLE="taxi_events_kafka"

# Pub/Sub (source for mirroring into Kafka)
PUBLIC_TOPIC="projects/pubsub-public-data/topics/taxirides-realtime"
SUBSCRIPTION_NAME="taxi_telemetry_kafka"

# Google Managed Kafka
KAFKA_CLUSTER_NAME="taxi-kafka-cluster"
KAFKA_TOPIC_NAME="taxi-rides"
CONNECT_CLUSTER_NAME="taxi-connect-cluster"
CONNECTOR_NAME="pubsub-taxi-source"

# Pipeline
SOURCE_NAME="kafka-taxi-rides"
JOB_NAME="dataflow-kafka-to-bq-$(date +%Y%m%d-%H%M%S)"

# Full resource paths
FULL_TABLE="${PROJECT_ID}:${BIGQUERY_DATASET}.${BIGQUERY_TABLE}"

# --- Helper Functions ---

wait_for_cluster() {
    local cluster_name="$1"
    local max_attempts=60
    local attempt=0
    echo "Waiting for cluster '${cluster_name}' to become ACTIVE..."
    while [[ "${attempt}" -lt "${max_attempts}" ]]; do
        local state
        state=$(gcloud managed-kafka clusters describe "${cluster_name}" \
            --location="${REGION}" \
            --project="${PROJECT_ID}" \
            --format="value(state)" 2>/dev/null || echo "UNKNOWN")
        if [[ "${state}" == "ACTIVE" ]]; then
            echo "Cluster '${cluster_name}' is ACTIVE."
            return 0
        fi
        attempt=$((attempt + 1))
        echo "  Attempt ${attempt}/${max_attempts}: state=${state}, waiting 30s..."
        sleep 30
    done
    echo "ERROR: Cluster '${cluster_name}' did not become ACTIVE within 30 minutes."
    exit 1
}

wait_for_connect_cluster() {
    local cluster_name="$1"
    local max_attempts=60
    local attempt=0
    echo "Waiting for Connect cluster '${cluster_name}' to become ACTIVE..."
    while [[ "${attempt}" -lt "${max_attempts}" ]]; do
        local state
        state=$(gcloud managed-kafka connect-clusters describe "${cluster_name}" \
            --location="${REGION}" \
            --project="${PROJECT_ID}" \
            --format="value(state)" 2>/dev/null || echo "UNKNOWN")
        if [[ "${state}" == "ACTIVE" ]]; then
            echo "Connect cluster '${cluster_name}' is ACTIVE."
            return 0
        fi
        attempt=$((attempt + 1))
        echo "  Attempt ${attempt}/${max_attempts}: state=${state}, waiting 30s..."
        sleep 30
    done
    echo "ERROR: Connect cluster '${cluster_name}' did not become ACTIVE within 30 minutes."
    exit 1
}

# --- Main Script ---

echo "=== Dataflow Kafka to BigQuery Pipeline (Runner V2) ==="
echo "Project ID: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Job Name: ${JOB_NAME}"
echo "Kafka Cluster: ${KAFKA_CLUSTER_NAME}"
echo "Kafka Topic: ${KAFKA_TOPIC_NAME}"
echo "BigQuery Table: ${FULL_TABLE}"
echo ""
echo "Pipeline Configuration:"
echo "  - Source: Google Managed Kafka (via Managed I/O)"
echo "  - Machine Type: n2-standard-4 (4 vCPUs, 16GB RAM)"
echo "  - Workers: 5 (fixed, no auto-scaling)"
echo "  - BigQuery Write: Storage Write API with 1-second micro-batching"
echo ""
echo "Kafka Sizing (for ~2,000 RPS, ~300 bytes/msg = 0.6 MBps):"
echo "  - Kafka Cluster: 3 vCPUs / 12 GiB (min cluster, 13x headroom)"
echo "  - Kafka Partitions: 40 (Runner v2: 2 * total_vCPUs = 2 * 20)"
echo "  - Connect Cluster: 3 vCPUs / 3 GiB (min cluster, light workload)"
echo "  - Connector Tasks: 3 (one per Connect worker)"
echo ""

# 1. Check/Create GCS bucket
echo "--- Step 1/12: GCS Bucket ---"
if ! gsutil ls "${TEMP_BUCKET}" 2>/dev/null; then
    echo "Creating GCS bucket..."
    gsutil mb -p "${PROJECT_ID}" -l "${REGION}" "${TEMP_BUCKET}"
else
    echo "GCS bucket already exists."
fi

# 2. Check/Create BigQuery dataset
echo "--- Step 2/12: BigQuery Dataset ---"
if ! bq show --dataset "${PROJECT_ID}:${BIGQUERY_DATASET}" 2>/dev/null; then
    echo "Creating BigQuery dataset..."
    bq mk --dataset --location="${REGION}" "${PROJECT_ID}:${BIGQUERY_DATASET}"
else
    echo "BigQuery dataset already exists."
fi

# 3. Check/Create BigQuery table with schema
echo "--- Step 3/12: BigQuery Table ---"
if ! bq show "${FULL_TABLE}" 2>/dev/null; then
    echo "Creating BigQuery table with schema..."
    bq mk \
        --table \
        --time_partitioning_field=publish_time \
        --time_partitioning_type=DAY \
        --clustering_fields=publish_time \
        --schema=subscription_name:STRING,message_id:STRING,publish_time:TIMESTAMP,processing_time:TIMESTAMP,ride_id:STRING,point_idx:INT64,latitude:FLOAT,longitude:FLOAT,timestamp:TIMESTAMP,meter_reading:FLOAT,meter_increment:FLOAT,ride_status:STRING,passenger_count:INT64,attributes:STRING \
        "${FULL_TABLE}"
else
    echo "BigQuery table already exists."
fi

# 4. Check/Create Pub/Sub subscription (for the connector to consume)
echo "--- Step 4/12: Pub/Sub Subscription ---"
if ! gcloud pubsub subscriptions describe "${SUBSCRIPTION_NAME}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating Pub/Sub subscription..."
    gcloud pubsub subscriptions create "${SUBSCRIPTION_NAME}" \
        --project="${PROJECT_ID}" \
        --topic="${PUBLIC_TOPIC}"
else
    echo "Pub/Sub subscription already exists."
fi

# 5. Check/Create GMK Kafka cluster
echo "--- Step 5/12: GMK Kafka Cluster ---"
if ! gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating GMK Kafka cluster (this may take up to 30 minutes)..."
    # Sizing: 3 vCPUs is the minimum (1 per zone, 3 brokers).
    # At 0.6 MBps produce rate with RF=3, write-equivalent rate is ~2.25 MBps.
    # Capacity: 3 vCPUs * 20 MBps/vCPU * 50% utilization = 30 MBps (13x headroom).
    # Memory: 4 GiB per vCPU recommended (3 * 4 = 12 GiB).
    gcloud managed-kafka clusters create "${KAFKA_CLUSTER_NAME}" \
        --location="${REGION}" \
        --cpu=3 \
        --memory=12GiB \
        --subnets="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}" \
        --async
fi

# 6. Wait for Kafka cluster to be ACTIVE
echo "--- Step 6/12: Wait for Kafka Cluster ---"
wait_for_cluster "${KAFKA_CLUSTER_NAME}"

# 7. Check/Create Kafka topic
echo "--- Step 7/12: Kafka Topic ---"
if ! gcloud managed-kafka topics describe "${KAFKA_TOPIC_NAME}" \
    --cluster="${KAFKA_CLUSTER_NAME}" --location="${REGION}" \
    --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating Kafka topic..."
    # Partitions: 40 for Runner v2 parallelism (2 * total_vCPUs = 2 * 5 workers * 4 vCPUs).
    # Each partition sees ~50 msg/s (2000 / 40), ensuring even load distribution.
    # Replication factor 3 matches 3 brokers for HA.
    gcloud managed-kafka topics create "${KAFKA_TOPIC_NAME}" \
        --cluster="${KAFKA_CLUSTER_NAME}" \
        --location="${REGION}" \
        --partitions=40 \
        --replication-factor=3
else
    echo "Kafka topic already exists."
fi

# 8. Get bootstrap address
echo "--- Step 8/12: Bootstrap Address ---"
BOOTSTRAP=$(gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(bootstrapAddress)")
echo "Bootstrap address: ${BOOTSTRAP}"

# 9. Check/Create Connect cluster
echo "--- Step 9/12: GMK Connect Cluster ---"
if ! gcloud managed-kafka connect-clusters describe "${CONNECT_CLUSTER_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating GMK Connect cluster..."
    # Sizing: 3 vCPUs minimum (1 per zone). 2K RPS of ~300B messages is light.
    # Memory: 1 GiB/vCPU minimum; no heavy transformation logic in the connector.
    # Note: uses --primary-subnet (not --subnets which is for Kafka clusters).
    gcloud managed-kafka connect-clusters create "${CONNECT_CLUSTER_NAME}" \
        --location="${REGION}" \
        --kafka-cluster="projects/${PROJECT_ID}/locations/${REGION}/clusters/${KAFKA_CLUSTER_NAME}" \
        --cpu=3 \
        --memory=3GiB \
        --primary-subnet="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${SUBNET_NAME}" \
        --async
fi

# Wait for Connect cluster
wait_for_connect_cluster "${CONNECT_CLUSTER_NAME}"

# 10. Grant Managed Kafka SA permissions to read from Pub/Sub
echo "--- Step 10/12: IAM Permissions ---"
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
KAFKA_SA="service-${PROJECT_NUMBER}@gcp-sa-managedkafka.iam.gserviceaccount.com"
echo "Granting Pub/Sub Subscriber role to ${KAFKA_SA}..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${KAFKA_SA}" \
    --role="roles/pubsub.subscriber" \
    --condition=None \
    --quiet 2>/dev/null || true
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${KAFKA_SA}" \
    --role="roles/pubsub.viewer" \
    --condition=None \
    --quiet 2>/dev/null || true
echo "IAM permissions granted."

# 11. Check/Create Pub/Sub Source connector
echo "--- Step 11/12: Pub/Sub Source Connector ---"
if ! gcloud managed-kafka connectors describe "${CONNECTOR_NAME}" \
    --connect-cluster="${CONNECT_CLUSTER_NAME}" \
    --location="${REGION}" --project="${PROJECT_ID}" 2>/dev/null; then
    echo "Creating Pub/Sub Source connector..."
    # kafka.record.headers=true: Pub/Sub message attributes go to Kafka headers,
    #   value is raw message body bytes (required for ByteArrayConverter when
    #   messages have custom attributes, otherwise connector fails silently).
    # kafka.partition.count=40: Distribute across all topic partitions
    #   (default is 1, which defeats the partitioning strategy).
    # kafka.partition.scheme=round_robin: Even distribution for balanced
    #   Dataflow consumption across workers.
    gcloud managed-kafka connectors create "${CONNECTOR_NAME}" \
        --connect-cluster="${CONNECT_CLUSTER_NAME}" \
        --location="${REGION}" \
        --task-restart-min-backoff=60s \
        --task-restart-max-backoff=1800s \
        --configs=connector.class=com.google.pubsub.kafka.source.CloudPubSubSourceConnector,cps.project="${PROJECT_ID}",cps.subscription="${SUBSCRIPTION_NAME}",cps.streamingPull.enabled=true,kafka.topic="${KAFKA_TOPIC_NAME}",kafka.record.headers=true,kafka.partition.count=40,kafka.partition.scheme=round_robin,tasks.max=3,value.converter=org.apache.kafka.connect.converters.ByteArrayConverter,key.converter=org.apache.kafka.connect.storage.StringConverter
else
    echo "Pub/Sub Source connector already exists."
fi

# Allow connector to start producing messages
echo "Waiting 15 seconds for connector to initialize..."
sleep 15

# 12. Build and submit Dataflow pipeline
echo "--- Step 12/12: Dataflow Pipeline ---"

# Install Python dependencies
echo "Installing Python dependencies..."
uv sync

# Build package wheel for Dataflow workers
echo "Building package wheel for Dataflow workers..."
uv build --wheel
WHEEL_FILE=$(ls -t dist/*.whl | head -1)
echo "Built wheel: ${WHEEL_FILE}"

# Submit pipeline to Dataflow
echo ""
echo "Submitting pipeline to Dataflow with Runner V2..."
uv run python -m dataflow_pubsub_to_bq.pipeline_kafka \
    --runner=DataflowRunner \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --job_name="${JOB_NAME}" \
    --temp_location="${TEMP_BUCKET}/temp" \
    --staging_location="${TEMP_BUCKET}/staging" \
    --bootstrap_server="${BOOTSTRAP}" \
    --kafka_topic="${KAFKA_TOPIC_NAME}" \
    --output_table="${FULL_TABLE}" \
    --source_name="${SOURCE_NAME}" \
    --streaming \
    --experiments=use_runner_v2 \
    --sdk_container_image=apache/beam_python3.12_sdk:2.70.0 \
    --extra_packages="${WHEEL_FILE}" \
    --machine_type=n2-standard-4 \
    --num_workers=5 \
    --max_num_workers=5 \
    --enable_streaming_engine

echo ""
echo "=== Job Submitted Successfully ==="
echo "The pipeline has been submitted to Dataflow and is starting up..."
echo ""
echo "View job at:"
echo "https://console.cloud.google.com/dataflow/jobs/${REGION}/${JOB_NAME}?project=${PROJECT_ID}"
echo ""
echo "Monitor BigQuery table at:"
echo "https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2s${BIGQUERY_DATASET}!3s${BIGQUERY_TABLE}"
echo ""
echo "Data Flow:"
echo "  Pub/Sub (taxirides-realtime)"
echo "    -> Subscription (${SUBSCRIPTION_NAME})"
echo "    -> GMK Connect (${CONNECTOR_NAME})"
echo "    -> Kafka Topic (${KAFKA_TOPIC_NAME})"
echo "    -> Dataflow (${JOB_NAME})"
echo "    -> BigQuery (${FULL_TABLE})"
