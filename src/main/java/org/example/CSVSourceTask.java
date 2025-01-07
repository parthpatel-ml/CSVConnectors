package org.example;

import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CSVSourceTask extends SourceTask {
    private String filePath;
    private String topic;
    private int currentOffset = 0; // Default offset in case no prior offset is found.

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        filePath = props.get("file.path");
        topic = props.get("topic");

        // Retrieve the last saved offset using the Kafka Connect offset storage mechanism
        Map<String, Object> offset = context.offsetStorageReader()
                .offset(Collections.singletonMap("file", filePath));
        if (offset != null) {
            Object offsetValue = offset.get("position");
            if (offsetValue != null) {
                currentOffset = (int) offsetValue; // Set the starting offset
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] row;
            int offset = 0;

            while ((row = reader.readNext()) != null) {
                if (offset >= currentOffset) {
                    Schema schema = SchemaBuilder.struct()
                            .field("column1", Schema.STRING_SCHEMA)
                            .field("column2", Schema.STRING_SCHEMA)
                            .build();

                    Struct recordValue = new Struct(schema)
                            .put("column1", row[0])  // Map CSV columns to your schema fields
                            .put("column2", row[1]);

                    // Create a source offset for this record
                    Map<String, Object> sourcePartition = Collections.singletonMap("file", filePath);
                    Map<String, Object> sourceOffset = Collections.singletonMap("position", offset);

                    SourceRecord record = new SourceRecord(
                            sourcePartition, // topic partition (null if default)
                            sourceOffset, // source offset (optional)
                            topic, // target Kafka topic
                            null, // schema for the key (optional)
                            schema, // schema for the value
                            recordValue
                    );

                    records.add(record);
                   /* Map<String, Object> sourcePartition = Collections.singletonMap("file", filePath);
                    Map<String, Object> sourceOffset = Collections.singletonMap("position", offset);

                    Struct value = new Struct(Schema.STRING_SCHEMA).put("value", String.join(",", row));
                    SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, value);

                    records.add(record);*/
                    currentOffset = offset + 1; // Increment offset
                }
                offset++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return records;
    }

    @Override
    public void stop() {
        // Cleanup logic if needed
    }
}
