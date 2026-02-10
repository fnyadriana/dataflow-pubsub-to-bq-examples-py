import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_empty

from dataflow_pubsub_to_bq.transforms.kafka_to_tablerow import ParseKafkaMessage


def test_process_valid_message():
    """Tests that a valid JSON payload produces a correct 14-column BigQuery row."""
    source_name = "kafka-taxi-rides"
    payload = {
        "ride_id": "abc-123",
        "point_idx": 42,
        "latitude": 40.76561,
        "longitude": -73.96572,
        "timestamp": "2026-01-15T05:45:49.16882-05:00",
        "meter_reading": 11.62,
        "meter_increment": 0.043,
        "ride_status": "enroute",
        "passenger_count": 2,
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    with TestPipeline() as p:
        result = (
            p | beam.Create([raw_bytes]) | beam.ParDo(ParseKafkaMessage(source_name))
        )

        def check_valid_output(elements):
            if len(elements) != 1:
                raise ValueError(f"Expected 1 element, got {len(elements)}")
            row = elements[0]
            # Verify source metadata
            if row["subscription_name"] != source_name:
                raise ValueError(
                    f"Expected subscription_name '{source_name}', got '{row['subscription_name']}'"
                )
            if row["message_id"] != "":
                raise ValueError(
                    f"Expected empty message_id, got '{row['message_id']}'"
                )
            if row["attributes"] != "{}":
                raise ValueError(
                    f"Expected empty attributes '{{}}', got '{row['attributes']}'"
                )
            # Verify taxi ride fields
            if row["ride_id"] != "abc-123":
                raise ValueError(f"Expected ride_id 'abc-123', got '{row['ride_id']}'")
            if row["point_idx"] != 42:
                raise ValueError(f"Expected point_idx 42, got {row['point_idx']}")
            if abs(row["latitude"] - 40.76561) > 0.0001:
                raise ValueError(f"Expected latitude ~40.76561, got {row['latitude']}")
            if abs(row["longitude"] - (-73.96572)) > 0.0001:
                raise ValueError(
                    f"Expected longitude ~-73.96572, got {row['longitude']}"
                )
            if abs(row["meter_reading"] - 11.62) > 0.01:
                raise ValueError(
                    f"Expected meter_reading ~11.62, got {row['meter_reading']}"
                )
            if row["ride_status"] != "enroute":
                raise ValueError(
                    f"Expected ride_status 'enroute', got '{row['ride_status']}'"
                )
            if row["passenger_count"] != 2:
                raise ValueError(
                    f"Expected passenger_count 2, got {row['passenger_count']}"
                )
            # Verify timestamps exist
            if not row.get("publish_time"):
                raise ValueError("Missing publish_time")
            if not row.get("processing_time"):
                raise ValueError("Missing processing_time")
            if not row.get("timestamp"):
                raise ValueError("Missing ride timestamp")

        assert_that(result, check_valid_output, label="CheckValidOutput")


def test_process_malformed_message():
    """Tests that malformed JSON is silently dropped with no output."""
    source_name = "kafka-taxi-rides"
    invalid_json = b'{"ride_id": "123", "passeng'  # truncated

    with TestPipeline() as p:
        result = (
            p | beam.Create([invalid_json]) | beam.ParDo(ParseKafkaMessage(source_name))
        )

        assert_that(result, is_empty(), label="CheckMalformedDropped")


def test_process_missing_fields_use_defaults():
    """Tests that missing JSON fields fall back to default values."""
    source_name = "kafka-taxi-rides"
    # Minimal payload with only ride_id
    payload = {"ride_id": "minimal-ride"}
    raw_bytes = json.dumps(payload).encode("utf-8")

    with TestPipeline() as p:
        result = (
            p | beam.Create([raw_bytes]) | beam.ParDo(ParseKafkaMessage(source_name))
        )

        def check_defaults(elements):
            if len(elements) != 1:
                raise ValueError(f"Expected 1 element, got {len(elements)}")
            row = elements[0]
            if row["ride_id"] != "minimal-ride":
                raise ValueError(
                    f"Expected ride_id 'minimal-ride', got '{row['ride_id']}'"
                )
            # String defaults
            if row["ride_status"] != "":
                raise ValueError(
                    f"Expected empty ride_status, got '{row['ride_status']}'"
                )
            # Numeric defaults
            if row["point_idx"] != 0:
                raise ValueError(f"Expected point_idx 0, got {row['point_idx']}")
            if row["latitude"] != 0.0:
                raise ValueError(f"Expected latitude 0.0, got {row['latitude']}")
            if row["longitude"] != 0.0:
                raise ValueError(f"Expected longitude 0.0, got {row['longitude']}")
            if row["meter_reading"] != 0.0:
                raise ValueError(
                    f"Expected meter_reading 0.0, got {row['meter_reading']}"
                )
            if row["meter_increment"] != 0.0:
                raise ValueError(
                    f"Expected meter_increment 0.0, got {row['meter_increment']}"
                )
            if row["passenger_count"] != 0:
                raise ValueError(
                    f"Expected passenger_count 0, got {row['passenger_count']}"
                )
            # Missing timestamp should be None
            if row["timestamp"] is not None:
                raise ValueError(f"Expected timestamp None, got {row['timestamp']}")

        assert_that(result, check_defaults, label="CheckDefaults")


def test_source_name_propagation():
    """Tests that the source_name constructor arg populates subscription_name."""
    source_name = "my-custom-kafka-source"
    payload = {"ride_id": "test"}
    raw_bytes = json.dumps(payload).encode("utf-8")

    with TestPipeline() as p:
        result = (
            p | beam.Create([raw_bytes]) | beam.ParDo(ParseKafkaMessage(source_name))
        )

        def check_source_name(elements):
            if len(elements) != 1:
                raise ValueError(f"Expected 1 element, got {len(elements)}")
            if elements[0]["subscription_name"] != source_name:
                raise ValueError(
                    f"Expected '{source_name}', got '{elements[0]['subscription_name']}'"
                )

        assert_that(result, check_source_name, label="CheckSourceName")
