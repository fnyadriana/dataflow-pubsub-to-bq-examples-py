"""Transform to parse Pub/Sub messages and convert to BigQuery TableRow with Raw JSON."""

import json
import logging
import time
import traceback
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.pvalue import TaggedOutput
from apache_beam.utils.timestamp import Timestamp


class ParsePubSubMessageToRawJson(beam.DoFn):
    """Parses Pub/Sub messages and extracts raw JSON data with metadata.

    This transform extracts:
    - Pub/Sub metadata: subscription_name, message_id, publish_time, attributes
    - Payload: The raw JSON message content stored in a JSON column
    """

    def __init__(self, subscription_name: str):
        """Initializes the transform with subscription name.

        Args:
            subscription_name: The Pub/Sub subscription name to record in BigQuery.
        """
        self.subscription_name = subscription_name

    def process(self, element: PubsubMessage) -> Any:
        """Processes a single Pub/Sub message.

        Args:
            element: PubsubMessage containing data and attributes.

        Yields:
            Dictionary with metadata and raw payload for BigQuery insertion.
            Or TaggedOutput('dlq', error_row) on failure.
        """
        try:
            # Decode the message data
            message_data = element.data.decode("utf-8")

            # Verify it's valid JSON, but keep the structure for the JSON column
            # We parse it just to ensure validity and strict error handling
            json_payload = json.loads(message_data)

            # Extract Pub/Sub metadata (for data quality checks)
            message_id = element.message_id if hasattr(
                element, "message_id") else ""

            # Convert to Beam Timestamp for Storage Write API compatibility
            if hasattr(element, "publish_time") and element.publish_time:
                publish_time = Timestamp.of(element.publish_time.timestamp())
            else:
                publish_time = Timestamp.of(time.time())

            # Extract custom attributes from the message (if any)
            attributes = element.attributes if element.attributes else {}

            # Create BigQuery row
            # Note: For BigQuery JSON type, we pass the Python dict directly
            bq_row = {
                # Pub/Sub metadata
                "subscription_name": self.subscription_name,
                "message_id": message_id,
                "publish_time": publish_time,
                "attributes": json.dumps(attributes),
                # The payload itself as a JSON string
                "payload": json.dumps(json_payload),
            }

            yield bq_row

        except Exception as e:
            logging.error(f"Error processing message: {e}")

            # Capture full stack trace
            stack_trace = traceback.format_exc()

            # Safe payload extraction
            try:
                original_payload = element.data.decode("utf-8")
            except Exception:
                # Fallback if decoding fails
                original_payload = str(element.data)

            error_row = {
                "timestamp": Timestamp.of(time.time()),
                "error_message": str(e),
                "stack_trace": stack_trace,
                "original_payload": original_payload,
                "subscription_name": self.subscription_name,
            }
            yield TaggedOutput("dlq", error_row)


def get_raw_json_bigquery_schema() -> list:
    """Returns the BigQuery schema for the taxi_events_json table.

    Returns:
        List of BigQuery schema field definitions.
    """
    return [
        {"name": "subscription_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "message_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "publish_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "attributes", "type": "STRING", "mode": "NULLABLE"},
        # Defined as STRING for Beam validation
        {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
    ]


def get_dead_letter_bigquery_schema() -> list:
    """Returns the BigQuery schema for the dead letter queue table.

    Returns:
        List of BigQuery schema field definitions.
    """
    return [
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "error_message", "type": "STRING", "mode": "NULLABLE"},
        {"name": "stack_trace", "type": "STRING", "mode": "NULLABLE"},
        {"name": "original_payload", "type": "STRING", "mode": "NULLABLE"},
        {"name": "subscription_name", "type": "STRING", "mode": "NULLABLE"},
    ]
