package io.confluent.connect.jdbc.convert;

import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;

public class JsonConverter extends org.apache.kafka.connect.json.JsonConverter {

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(JsonConverterConfig.DECIMAL_FORMAT_CONFIG)) {
            super.configure(configs);
        } else {
            Map<String, Object> newConfigs = new HashMap<>(configs);
            newConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
            super.configure(newConfigs);
        }
    }
}
