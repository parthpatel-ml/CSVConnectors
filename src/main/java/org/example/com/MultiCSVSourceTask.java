package org.example.com;

import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.FileReader;
import java.util.*;

public class MultiCSVSourceTask extends SourceTask {
    private List<String> filePaths; // List of file paths
    private List<String> topics;    // Corresponding topics
    private Map<String, Integer> currentOffsets = new HashMap<>(); // Offsets for each file


    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println(" " + this.getClass().getSimpleName() + " Start Method");
        filePaths = Arrays.asList(props.get("file.paths_2").split(",")); // Comma-separated file paths
        topics = Arrays.asList(props.get("topics_2").split(","));        // Comma-separated topics

        for (String filePath : filePaths) {
            // Retrieve the last saved offset for each file
            Map<String, Object> offset = context.offsetStorageReader()
                    .offset(Collections.singletonMap("file", filePath));
            if (offset != null) {
                offset.forEach((k, v) -> System.out.println("## Key is " + k + " ## Value is " + v));
            }
            int startOffset = 0;
            if (offset != null && offset.get("position") != null) {
//                Object position = offset.get("position");
//                startOffset = ((Long)position).intValue();
                //startOffset = (int) offset.get("position");

                Object position = offset.get("position");
                if (position instanceof Long) {
                    startOffset = ((Long) position).intValue();
                    System.out.println("position instanceof Long");
                } else if (position instanceof Integer) {
                    System.out.println("position instanceof Integer");
                    startOffset = (Integer) position;
                }
            }
            currentOffsets.put(filePath, startOffset); // Initialize offsets
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();

        for (int i = 0; i < filePaths.size(); i++) {
            String filePath = filePaths.get(i);
            String topic = topics.get(i);
            int currentOffset = currentOffsets.get(filePath);

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
                                .put("column1", row[0])
                                .put("column2", row[1]);

                        Map<String, Object> sourcePartition = Collections.singletonMap("file", filePath);
                        Map<String, Object> sourceOffset = Collections.singletonMap("position", offset);

                        if (sourcePartition != null) {
                            sourcePartition.forEach((k, v) -> System.out.println("## Key is " + k + " ## value is " + v));
                        }
                        if (sourceOffset != null) {
                            sourceOffset.forEach((k, v) -> System.out.println("### Key is " + k + " ## value is " + v));

                        }
                        System.out.println(" " + this.getClass().getSimpleName() + " ");

                        SourceRecord record = new SourceRecord(
                                sourcePartition,
                                sourceOffset,
                                topic,
                                null,
                                schema,
                                recordValue
                        );

                        records.add(record);
                        currentOffsets.put(filePath, offset + 1); // Increment offset for this file
                        System.out.println("FilePath: " + filePath);
                        System.out.println("Offset retrieved: " + offset);
                        System.out.println("CurrentOffsets: " + currentOffsets);
                    }
                    offset++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return records;
    }

    @Override
    public void stop() {
        // Cleanup logic if needed
    }
}
