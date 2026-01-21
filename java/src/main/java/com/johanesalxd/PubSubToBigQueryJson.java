package com.johanesalxd;

import com.johanesalxd.schemas.RawJsonBigQuerySchema;
import com.johanesalxd.transforms.PubsubMessageToRawJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;

public class PubSubToBigQueryJson {

    public static void main(String[] args) {
        // Parse pipeline options
        PubSubToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Read from Pub/Sub
        pipeline
            .apply("ReadFromPubSub", PubsubIO.readMessagesWithAttributes()
                .fromSubscription(options.getSubscription()))

            // Parse messages to TableRow
            .apply("ParseMessagesToRawJson", ParDo.of(
                new PubsubMessageToRawJson(options.getSubscriptionName())))

            // Write to BigQuery using Storage Write API
            .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                .to(options.getOutputTable())
                .withSchema(RawJsonBigQuerySchema.getSchema())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withTriggeringFrequency(Duration.standardSeconds(1))
            );

        // Run the pipeline
        pipeline.run();
    }
}
