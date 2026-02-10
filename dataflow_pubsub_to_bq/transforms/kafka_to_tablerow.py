"""Transform to parse Kafka messages and convert to BigQuery TableRow."""

from datetime import datetime
import json
import logging
import time
from typing import Any

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp


class ParseKafkaMessage(beam.DoFn):
    """Parses Kafka messages and extracts taxi ride data with metadata.

    This transform receives raw bytes from Managed I/O Kafka reader,
    decodes and parses the JSON payload, and maps fields to a BigQuery
    row matching the same 14-column schema used by the Pub/Sub pipeline.

    Metadata mapping:
    - subscription_name: populated with source_name (e.g., "kafka-taxi-rides")
    - message_id: empty string (not available from Kafka via Managed I/O)
    - publish_time: current time at processing
    - processing_time: current wall clock time
    - attributes: empty JSON "{}"
    """

    def __init__(self, source_name: str):
        """Initializes the transform with a source identifier.

        Args:
            source_name: Identifier stored in the subscription_name column
                for tracking the data source in BigQuery.
        """
        self.source_name = source_name

    def process(self, element: bytes) -> Any:
        """Processes a single Kafka message payload.

        Args:
            element: Raw bytes payload from the Kafka message.

        Yields:
            Dictionary with all fields for BigQuery insertion.
        """
        try:
            # Decode the message data
            message_data = element.decode("utf-8")
            ride_data = json.loads(message_data)

            # Capture timestamps
            publish_time = Timestamp.of(time.time())
            processing_time = Timestamp.of(time.time())

            # Parse taxi ride timestamp (ISO 8601 with timezone offset)
            # Example: "2026-01-15T05:45:49.16882-05:00"
            ride_timestamp_str = ride_data.get("timestamp", "")
            if ride_timestamp_str:
                try:
                    ride_timestamp = Timestamp.of(
                        datetime.fromisoformat(ride_timestamp_str).timestamp()
                    )
                except (ValueError, AttributeError):
                    logging.warning(f"Invalid timestamp format: {ride_timestamp_str}")
                    ride_timestamp = None
            else:
                ride_timestamp = None

            # Create BigQuery row with all fields
            bq_row = {
                # Source metadata
                "subscription_name": self.source_name,
                "message_id": "",
                "publish_time": publish_time,
                "processing_time": processing_time,
                # Taxi ride data
                "ride_id": ride_data.get("ride_id", ""),
                "point_idx": ride_data.get("point_idx", 0),
                "latitude": ride_data.get("latitude", 0.0),
                "longitude": ride_data.get("longitude", 0.0),
                "timestamp": ride_timestamp,
                "meter_reading": ride_data.get("meter_reading", 0.0),
                "meter_increment": ride_data.get("meter_increment", 0.0),
                "ride_status": ride_data.get("ride_status", ""),
                "passenger_count": ride_data.get("passenger_count", 0),
                "attributes": "{}",
            }

            yield bq_row

        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON: {e}, data: {element}")
        except Exception as e:
            logging.error(f"Error processing Kafka message: {e}")
