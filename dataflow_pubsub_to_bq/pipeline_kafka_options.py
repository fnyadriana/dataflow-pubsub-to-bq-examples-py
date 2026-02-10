"""Custom pipeline options for Kafka to BigQuery pipeline."""

from apache_beam.options.pipeline_options import PipelineOptions


class KafkaToBigQueryOptions(PipelineOptions):
    """Custom pipeline options for the Kafka to BigQuery pipeline.

    These options define the configuration parameters for reading from
    Google Managed Service for Apache Kafka and writing to BigQuery.
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        """Adds custom command-line arguments to the parser.

        Args:
            parser: ArgumentParser object to add arguments to.
        """
        parser.add_argument(
            "--bootstrap_server",
            required=True,
            help="Kafka bootstrap server address (e.g., bootstrap.CLUSTER.REGION.managedkafka.PROJECT.cloud.goog:9092)",
        )
        parser.add_argument(
            "--kafka_topic",
            required=True,
            help="Kafka topic name to consume from",
        )
        parser.add_argument(
            "--output_table",
            required=True,
            help="BigQuery output table (e.g., PROJECT_ID:DATASET.TABLE)",
        )
        parser.add_argument(
            "--source_name",
            required=True,
            help="Source identifier to record in BigQuery metadata (stored in subscription_name column)",
        )
