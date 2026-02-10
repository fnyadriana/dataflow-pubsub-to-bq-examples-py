"""Main pipeline for reading from Kafka and writing to BigQuery.

This pipeline reads taxi ride data from a Google Managed Service for
Apache Kafka topic using Managed I/O, parses the JSON messages, and
writes them to a BigQuery table with the same flattened schema as the
Pub/Sub pipeline.
"""

import logging

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_pubsub_to_bq.pipeline_kafka_options import KafkaToBigQueryOptions
from dataflow_pubsub_to_bq.transforms.json_to_tablerow import get_bigquery_schema
from dataflow_pubsub_to_bq.transforms.kafka_to_tablerow import ParseKafkaMessage


def run(argv=None):
    """Runs the Kafka to BigQuery pipeline.

    Args:
        argv: Command-line arguments.
    """
    # Parse pipeline options
    pipeline_options = PipelineOptions(argv)
    custom_options = pipeline_options.view_as(KafkaToBigQueryOptions)

    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from Kafka using Managed I/O with GMK authentication
        kafka_records = pipeline | "ReadFromKafka" >> beam.managed.Read(
            beam.managed.KAFKA,
            config={
                "bootstrap_servers": custom_options.bootstrap_server,
                "topic": custom_options.kafka_topic,
                "format": "RAW",
                "auto_offset_reset_config": "earliest",
                # GMK SASL/SSL authentication via Application Default Credentials
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "OAUTHBEARER",
                "sasl.login.callback.handler.class": "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
                "sasl.jaas.config": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;",
                # Redistribute options - increase parallelism beyond partition count.
                # Uncomment if Kafka partitions become a Dataflow bottleneck.
                # See: https://docs.cloud.google.com/dataflow/docs/guides/read-from-kafka#parallelism
                # "redistributed": True,
                # "redistribute_num_keys": 40,
                # "offset_deduplication": True,
            },
        )

        # Extract payload bytes from Beam Row and parse into BigQuery rows
        (
            kafka_records
            | "ExtractPayload" >> beam.Map(lambda row: row.payload)
            | "ParseMessages"
            >> beam.ParDo(ParseKafkaMessage(custom_options.source_name))
            | "WriteToBigQuery"
            >> bigquery.WriteToBigQuery(
                table=custom_options.output_table,
                schema={"fields": get_bigquery_schema()},
                write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=bigquery.BigQueryDisposition.CREATE_NEVER,
                method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
                triggering_frequency=1,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
