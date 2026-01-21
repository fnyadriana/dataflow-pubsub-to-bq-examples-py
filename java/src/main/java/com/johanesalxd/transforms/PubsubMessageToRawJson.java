package com.johanesalxd.transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Transform to parse Pub/Sub messages and convert to BigQuery TableRow with Raw JSON.
 */
public class PubsubMessageToRawJson extends DoFn<PubsubMessage, TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageToRawJson.class);
    private final String subscriptionName;

    public PubsubMessageToRawJson(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        PubsubMessage message = c.element();

        try {
            // Decode the message data
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

            // Verify it's valid JSON (just to be safe, like the Python version)
            // If it fails, it will go to the catch block
            new JSONObject(payload);

            // Extract Pub/Sub metadata
            String messageId = message.getMessageId();
            
            // Use processing time if publish time is missing (though Pub/Sub IO usually provides it)
            // Note: In Beam Java, context.timestamp() gives the event time assigned by the source
            Instant publishTime = c.timestamp();

            // Extract custom attributes
            Map<String, String> attributes = message.getAttributeMap();
            String attributesJson = attributes != null ? new JSONObject(attributes).toString() : "{}";

            TableRow row = new TableRow();
            row.set("subscription_name", this.subscriptionName);
            row.set("message_id", messageId);
            row.set("publish_time", publishTime.toString());
            row.set("attributes", attributesJson);
            // In Python: json.dumps(json_payload). In Java, payload is already the JSON string.
            // BQ JSON type expects a JSON string.
            row.set("payload", payload);

            c.output(row);

        } catch (Exception e) {
            LOG.error("Error processing message: " + e.getMessage());
            // In a real pipeline, we might output to a dead-letter queue here
        }
    }
}
