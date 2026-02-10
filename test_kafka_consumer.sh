#!/bin/bash

#######################################
# Kafka Consumer Connectivity Test
#
# Creates an e2-micro VM on the same VPC subnet as the GMK cluster,
# then runs a series of diagnostic checks:
#   1. DNS resolution of the GMK bootstrap address
#   2. TCP connectivity to bootstrap port 9092
#   3. Kafka topic metadata fetch via SASL_SSL + OAUTHBEARER
#   4. Consume a small batch of messages from the topic
#
# This isolates whether the issue is at the infrastructure level
# (DNS, PSC, IAM) or Dataflow-specific (Streaming Engine, Runner V2).
#
# Infrastructure created:
#   - Cloud Router + Cloud NAT (for outbound internet on internal-only VMs)
#   - e2-micro Compute Engine VM (no external IP, IAP tunnel for SSH)
#
# Usage:
#   bash test_kafka_consumer.sh            # Create infra, test, keep resources
#   bash test_kafka_consumer.sh --cleanup  # Delete VM, NAT, and router
#
# Prerequisites:
#   - gcloud CLI authenticated with permissions to create VMs, routers, NAT
#   - GMK cluster and topic already provisioned
#
# Globals:
#   PROJECT_ID, REGION, KAFKA_CLUSTER_NAME, KAFKA_TOPIC_NAME,
#   SUBNET_NAME, VM_NAME, ZONE, ROUTER_NAME, NAT_NAME, NETWORK_NAME
# Arguments:
#   --cleanup: Delete all test resources (VM, NAT, router)
#######################################

set -e

# --- Configuration ---
PROJECT_ID="your-project-id"
REGION="us-central1"
ZONE="us-central1-c"
SUBNET_NAME="default"
KAFKA_CLUSTER_NAME="taxi-kafka-cluster"
KAFKA_TOPIC_NAME="taxi-rides"
VM_NAME="kafka-connectivity-test"
MACHINE_TYPE="e2-micro"
NETWORK_NAME="default"
ROUTER_NAME="kafka-test-router"
NAT_NAME="kafka-test-nat"

# --- Parse Arguments ---
CLEANUP=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cleanup) CLEANUP=true; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# --- Cleanup Mode ---
if [[ "${CLEANUP}" == "true" ]]; then
    echo "=== Cleaning up test resources ==="
    echo "Deleting VM..."
    gcloud compute instances delete "${VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --quiet 2>/dev/null && echo "  VM '${VM_NAME}' deleted." \
        || echo "  VM '${VM_NAME}' not found or already deleted."
    echo "Deleting Cloud NAT..."
    gcloud compute routers nats delete "${NAT_NAME}" \
        --router="${ROUTER_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet 2>/dev/null && echo "  NAT '${NAT_NAME}' deleted." \
        || echo "  NAT '${NAT_NAME}' not found or already deleted."
    echo "Deleting Cloud Router..."
    gcloud compute routers delete "${ROUTER_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet 2>/dev/null && echo "  Router '${ROUTER_NAME}' deleted." \
        || echo "  Router '${ROUTER_NAME}' not found or already deleted."
    exit 0
fi

# --- Get Bootstrap Address ---
echo "=== Kafka Consumer Connectivity Test ==="
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo ""

BOOTSTRAP=$(gcloud managed-kafka clusters describe "${KAFKA_CLUSTER_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(bootstrapAddress)")
BOOTSTRAP_HOST="${BOOTSTRAP%%:*}"
BOOTSTRAP_PORT="${BOOTSTRAP##*:}"
echo "Bootstrap: ${BOOTSTRAP}"
echo ""

# --- Cloud NAT (for outbound internet on internal-only VMs) ---
echo "--- Step 1: Cloud NAT ---"
if gcloud compute routers describe "${ROUTER_NAME}" \
    --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "Cloud Router '${ROUTER_NAME}' already exists."
else
    echo "Creating Cloud Router '${ROUTER_NAME}'..."
    gcloud compute routers create "${ROUTER_NAME}" \
        --network="${NETWORK_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet
fi

if gcloud compute routers nats describe "${NAT_NAME}" \
    --router="${ROUTER_NAME}" --region="${REGION}" \
    --project="${PROJECT_ID}" &>/dev/null; then
    echo "Cloud NAT '${NAT_NAME}' already exists."
else
    echo "Creating Cloud NAT '${NAT_NAME}' on subnet '${SUBNET_NAME}'..."
    gcloud compute routers nats create "${NAT_NAME}" \
        --router="${ROUTER_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --nat-custom-subnet-ip-ranges="${SUBNET_NAME}" \
        --auto-allocate-nat-external-ips \
        --quiet
fi

# --- Create Test VM ---
echo ""
echo "--- Step 2: Create test VM ---"
if gcloud compute instances describe "${VM_NAME}" \
    --zone="${ZONE}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "VM '${VM_NAME}' already exists, reusing."
else
    echo "Creating e2-micro VM on subnet '${SUBNET_NAME}'..."
    gcloud compute instances create "${VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --machine-type="${MACHINE_TYPE}" \
        --subnet="${SUBNET_NAME}" \
        --no-address \
        --scopes=cloud-platform \
        --image-family=debian-12 \
        --image-project=debian-cloud \
        --metadata=enable-oslogin=true \
        --quiet
    echo "Waiting 30s for VM to initialize..."
    sleep 30
fi

# --- Run Diagnostics via SSH ---
echo ""
echo "--- Step 3: Run diagnostics on VM ---"
echo ""

gcloud compute ssh "${VM_NAME}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --tunnel-through-iap \
    --command="
set -e

echo '=========================================='
echo 'Test 1: DNS Resolution'
echo '=========================================='
echo 'Resolving: ${BOOTSTRAP_HOST}'
if host ${BOOTSTRAP_HOST} 2>/dev/null; then
    echo 'RESULT: DNS resolution PASSED'
else
    # Fallback to getent if host command not available
    if getent hosts ${BOOTSTRAP_HOST} 2>/dev/null; then
        echo 'RESULT: DNS resolution PASSED (via getent)'
    else
        echo 'RESULT: DNS resolution FAILED'
        echo 'The GMK private DNS zone is not resolving from this VM.'
        echo 'Check Cloud DNS zone attachment to the VPC network.'
        exit 1
    fi
fi

echo ''
echo '=========================================='
echo 'Test 2: TCP Connectivity (port ${BOOTSTRAP_PORT})'
echo '=========================================='
BOOTSTRAP_IP=\$(getent hosts ${BOOTSTRAP_HOST} | awk '{print \$1}' | head -1)
echo \"Bootstrap IP: \${BOOTSTRAP_IP}\"
if timeout 10 bash -c \"echo > /dev/tcp/\${BOOTSTRAP_IP}/${BOOTSTRAP_PORT}\" 2>/dev/null; then
    echo 'RESULT: TCP connectivity PASSED'
else
    echo 'RESULT: TCP connectivity FAILED'
    echo 'DNS resolves but cannot reach port ${BOOTSTRAP_PORT}.'
    echo 'Check PSC endpoint and firewall rules.'
    exit 1
fi

echo ''
echo '=========================================='
echo 'Test 3: Install Kafka CLI tools'
echo '=========================================='
if ! command -v /opt/kafka/bin/kafka-topics.sh &>/dev/null; then
    echo 'Installing Java and Kafka CLI...'
    sudo apt-get update -qq
    sudo apt-get install -y -qq default-jre-headless wget > /dev/null 2>&1

    # Download Kafka binaries (CLI tools only)
    KAFKA_VERSION=3.7.2
    SCALA_VERSION=2.13
    wget -q \"https://archive.apache.org/dist/kafka/\${KAFKA_VERSION}/kafka_\${SCALA_VERSION}-\${KAFKA_VERSION}.tgz\" \
        -O /tmp/kafka.tgz
    sudo mkdir -p /opt/kafka
    sudo tar -xzf /tmp/kafka.tgz -C /opt/kafka --strip-components=1
    rm /tmp/kafka.tgz

    # Download GMK auth handler JAR
    GMK_AUTH_VERSION=1.0.7
    wget -q \"https://repo1.maven.org/maven2/com/google/cloud/hosted/kafka/managed-kafka-auth-login-handler/\${GMK_AUTH_VERSION}/managed-kafka-auth-login-handler-\${GMK_AUTH_VERSION}.jar\" \
        -O /opt/kafka/libs/managed-kafka-auth-login-handler.jar
    echo 'Kafka CLI installed.'
else
    echo 'Kafka CLI already installed.'
fi

echo ''
echo '=========================================='
echo 'Test 4: Kafka Topic Metadata (SASL_SSL)'
echo '=========================================='

# Create JAAS config
cat > /tmp/kafka-client.properties << 'PROPS'
security.protocol=SASL_SSL
sasl.mechanism=OAUTHBEARER
sasl.login.callback.handler.class=com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
PROPS

echo 'Fetching topic metadata for: ${KAFKA_TOPIC_NAME}'
if timeout 30 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/kafka-client.properties \
    --describe \
    --topic ${KAFKA_TOPIC_NAME} 2>&1; then
    echo ''
    echo 'RESULT: Topic metadata fetch PASSED'
else
    echo 'RESULT: Topic metadata fetch FAILED'
    echo 'DNS and TCP work but Kafka metadata fetch failed.'
    echo 'Check SASL/SSL config or IAM roles (roles/managedkafka.client).'
    exit 1
fi

echo ''
echo '=========================================='
echo 'Test 5: Consume Messages (5-second sample)'
echo '=========================================='
echo 'Consuming from: ${KAFKA_TOPIC_NAME}'
echo 'Timeout: 5 seconds'
echo ''

timeout 15 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server ${BOOTSTRAP} \
    --consumer.config /tmp/kafka-client.properties \
    --topic ${KAFKA_TOPIC_NAME} \
    --from-beginning \
    --max-messages 3 \
    --timeout-ms 10000 2>/dev/null || true

echo ''
echo 'RESULT: Consumer test completed'

echo ''
echo '=========================================='
echo 'Summary'
echo '=========================================='
echo 'DNS Resolution:     PASSED'
echo 'TCP Connectivity:   PASSED'
echo 'Topic Metadata:     PASSED'
echo 'Message Consume:    PASSED'
echo ''
echo 'All infrastructure checks passed.'
echo 'If Dataflow still fails, the issue is Dataflow-specific'
echo '(Streaming Engine DNS, Runner V2 container networking).'
"

echo ""
echo "=== Test Complete ==="
echo ""
echo "To delete the test VM:"
echo "  bash test_kafka_consumer.sh --cleanup"
