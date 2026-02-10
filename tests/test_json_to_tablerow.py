import json
import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_empty
from dataflow_pubsub_to_bq.transforms.json_to_tablerow import (
    ParsePubSubMessage,
    get_bigquery_schema,
)


# Define a custom matcher for DLQ content
def matches_dlq_error(expected_payload_substring):
    def _matcher(elements):
        if len(elements) != 1:
            raise ValueError(f"Expected 1 DLQ element, got {len(elements)}")
        row = elements[0]
        if expected_payload_substring not in row["original_payload"]:
            raise ValueError(
                f"Expected payload to contain '{expected_payload_substring}', got '{row['original_payload']}'"
            )
        if not row.get("error_message"):
            raise ValueError("DLQ row missing 'error_message'")
        if not row.get("stack_trace"):
            raise ValueError("DLQ row missing 'stack_trace'")
        if not row.get("processing_time"):
            raise ValueError("DLQ row missing 'processing_time'")

    return _matcher


def test_process_valid_message():
    """Test that valid JSON messages go to the main output with correct fields."""
    subscription = "projects/test/subscriptions/sub"
    payload = {
        "ride_id": "123",
        "passenger_count": 1,
        "ride_status": "enroute",
        "meter_reading": 15.5,
    }
    message = PubsubMessage(
        data=json.dumps(payload).encode("utf-8"), attributes={"source": "test"}
    )

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParsePubSubMessage(subscription)).with_outputs(
                "dlq", main="success"
            )
        )

        # Check success output
        assert_that(
            results.success,
            lambda elements: len(elements) == 1
            and elements[0]["ride_id"] == "123"
            and elements[0]["passenger_count"] == 1
            and elements[0]["meter_reading"] == 15.5
            and elements[0]["subscription_name"] == subscription,
            label="CheckSuccess",
        )

        # Check DLQ is empty
        assert_that(results.dlq, is_empty(), label="CheckDLQEmpty")


def test_process_malformed_message():
    """Test that malformed JSON messages go to the DLQ output."""
    subscription = "projects/test/subscriptions/sub"
    invalid_json = '{"ride_id": "123", "passeng'  # truncated
    message = PubsubMessage(data=invalid_json.encode("utf-8"), attributes={})

    with TestPipeline() as p:
        results = (
            p
            | beam.Create([message])
            | beam.ParDo(ParsePubSubMessage(subscription)).with_outputs(
                "dlq", main="success"
            )
        )

        # Check success output is empty
        assert_that(results.success, is_empty(), label="CheckSuccessEmpty")

        # Check DLQ output
        assert_that(results.dlq, matches_dlq_error(invalid_json), label="CheckDLQ")


def test_get_bigquery_schema():
    """Tests that get_bigquery_schema returns expected field definitions."""
    schema = get_bigquery_schema()
    field_names = [f["name"] for f in schema]

    # 14 fields: 4 metadata + 9 taxi ride + 1 attributes
    assert len(schema) == 14
    assert "subscription_name" in field_names
    assert "message_id" in field_names
    assert "publish_time" in field_names
    assert "processing_time" in field_names
    assert "ride_id" in field_names
    assert "point_idx" in field_names
    assert "latitude" in field_names
    assert "longitude" in field_names
    assert "timestamp" in field_names
    assert "meter_reading" in field_names
    assert "meter_increment" in field_names
    assert "ride_status" in field_names
    assert "passenger_count" in field_names
    assert "attributes" in field_names

    # Verify key type mappings
    field_map = {f["name"]: f for f in schema}
    assert field_map["publish_time"]["type"] == "TIMESTAMP"
    assert field_map["ride_id"]["type"] == "STRING"
    assert field_map["point_idx"]["type"] == "INT64"
    assert field_map["timestamp"]["type"] == "TIMESTAMP"
