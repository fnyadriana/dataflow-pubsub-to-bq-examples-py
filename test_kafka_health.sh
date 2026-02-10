#!/bin/bash

#######################################
# Kafka Connector Health Check
#
# Verifies that the Pub/Sub Source connector is successfully consuming
# messages by monitoring the Pub/Sub subscription backlog trend.
#
# A DECREASING backlog means the connector is consuming from Pub/Sub
# and writing to Kafka. A GROWING backlog means the connector is not
# delivering messages.
#
# Usage:
#   bash test_kafka_health.sh
#   bash test_kafka_health.sh --minutes 10
#   bash test_kafka_health.sh --project my-project --subscription my-sub
#
# Prerequisites:
#   - gcloud CLI authenticated (gcloud auth login)
#   - curl and python3 available (default in Cloud Shell)
#
# Globals:
#   PROJECT_ID, SUBSCRIPTION_NAME, KAFKA_CLUSTER_NAME,
#   CONNECT_CLUSTER_NAME, CONNECTOR_NAME
# Arguments:
#   --project: GCP project ID (optional)
#   --subscription: Pub/Sub subscription ID (optional)
#   --minutes: Number of minutes to check (optional, default 5)
#######################################

set -e

# --- Defaults ---
PROJECT_ID="johanesa-playground-326616"
REGION="us-central1"
SUBSCRIPTION_NAME="taxi_telemetry_kafka"
CONNECT_CLUSTER_NAME="taxi-connect-cluster"
CONNECTOR_NAME="pubsub-taxi-source"
MINUTES=5

# --- Parse Arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --project) PROJECT_ID="$2"; shift 2 ;;
        --subscription) SUBSCRIPTION_NAME="$2"; shift 2 ;;
        --minutes) MINUTES="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# --- Connector Status ---
echo "=== Kafka Connector Health Check ==="
echo "Project: ${PROJECT_ID}"
echo "Subscription: ${SUBSCRIPTION_NAME}"
echo ""

CONNECTOR_STATE=$(gcloud managed-kafka connectors describe "${CONNECTOR_NAME}" \
    --connect-cluster="${CONNECT_CLUSTER_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(state)" 2>/dev/null || echo "NOT_FOUND")
echo "Connector: ${CONNECTOR_NAME} (${CONNECTOR_STATE})"
echo ""

# --- Query Backlog Trend ---
TOKEN=$(gcloud auth print-access-token)

# Calculate time range
if [[ "$(uname)" == "Darwin" ]]; then
    START=$(date -u -v-"${MINUTES}"M '+%Y-%m-%dT%H:%M:%SZ')
else
    START=$(date -u -d "${MINUTES} minutes ago" '+%Y-%m-%dT%H:%M:%SZ')
fi
END=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

FILTER="metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id=\"${SUBSCRIPTION_NAME}\""
ENCODED_FILTER=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${FILTER}'))")

RESPONSE=$(curl -s -H "Authorization: Bearer ${TOKEN}" \
    "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries?filter=${ENCODED_FILTER}&interval.startTime=${START}&interval.endTime=${END}&view=FULL")

# --- Display Results ---
python3 -c "
import json
import sys

data = json.loads('''${RESPONSE}''')
ts = data.get('timeSeries', [])
if not ts:
    print('No backlog data found. The subscription may not exist or has no data yet.')
    sys.exit(1)

points = ts[0].get('points', [])
if not points:
    print('No data points in the selected time range.')
    sys.exit(1)

# Points are newest-first
values = []
for p in points:
    t = p['interval']['endTime']
    v = int(p['value']['int64Value'])
    values.append((t, v))

print(f'Backlog trend (last ${MINUTES} minutes):')
print(f'  {\"TIME\":<26} {\"MESSAGES\":>14} {\"DELTA\":>12}')
print(f'  {\"-\" * 26} {\"-\" * 14} {\"-\" * 12}')

for i, (t, v) in enumerate(values):
    if i < len(values) - 1:
        delta = v - values[i + 1][1]
        sign = '+' if delta >= 0 else ''
        print(f'  {t:<26} {v:>14,} {sign + str(delta):>12}')
    else:
        print(f'  {t:<26} {v:>14,} {\"(baseline)\":>12}')

print()

# Verdict
if len(values) >= 2:
    newest = values[0][1]
    oldest = values[-1][1]
    total_delta = newest - oldest
    minutes = len(values) - 1
    rate = abs(total_delta) // max(minutes, 1)

    if total_delta < -1000:
        print(f'Verdict: HEALTHY')
        print(f'  Backlog is DECREASING (~{rate:,} msg/min consumed).')
        print(f'  The connector is consuming from Pub/Sub and writing to Kafka.')
    elif total_delta > 1000:
        print(f'Verdict: UNHEALTHY')
        print(f'  Backlog is GROWING (~{rate:,} msg/min accumulating).')
        print(f'  The connector is NOT consuming messages. Check connector logs.')
    else:
        print(f'Verdict: STABLE')
        print(f'  Backlog is flat (delta: {total_delta:,}). This could mean:')
        print(f'  - Connector is consuming at the same rate as production (good)')
        print(f'  - Or no messages are flowing at all (check topic activity)')
else:
    print('Not enough data points for a verdict.')
"
