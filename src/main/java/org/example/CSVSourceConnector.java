package org.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSVSourceConnector extends SourceConnector {
    private Map<String, String> config;

    @Override
    public void start(Map<String, String> props) {
        this.config = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CSVSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Cleanup logic
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("file.path", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Path to the CSV file")
                .define("topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic to send records");
    }

    @Override
    public String version() {
        return "1.0";
    }
}
