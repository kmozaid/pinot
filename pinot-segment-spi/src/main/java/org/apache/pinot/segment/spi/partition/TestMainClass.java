package org.apache.pinot.segment.spi.partition;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.utils.JsonUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestMainClass {

    public static void main(String[] args) throws ConfigurationException, IOException {
        PropertiesConfiguration configuration = new PropertiesConfiguration();
        String key = "column.example.partitionFunctionConfig";
        Map<String, String> functionConfig = Map.of("columnValues", "value1|value2", "otherValue", "1");
        for (Map.Entry<String, String> entry : functionConfig.entrySet()) {
            configuration.setProperty(key + "." + entry.getKey(), entry.getValue());
        }
        configuration.setProperty("anyotherconfig", "configvalue");
        System.out.println(configuration.getProperty("column.example.partitionFunctionConfig.columnValues"));
        System.out.println(configuration.getProperty("column.example.partitionFunctionConfig.otherValue"));
        System.out.println(configuration.getProperty("anyotherconfig"));
        configuration.save(new File("/tmp/configtest/metadata.properties"));

        System.out.println("Loading from saved file...");

        Map<String, String> loadedFunctionConfig = new HashMap<>();
        PropertiesConfiguration loadedConfig = new PropertiesConfiguration();
        loadedConfig.load(new File("/tmp/configtest/metadata.properties"));
        Configuration partitionConfig = loadedConfig.subset("column.example.partitionFunctionConfig");
        System.out.println(partitionConfig.isEmpty());
        Iterator<String> partitionFunctionConfigKeysIter = partitionConfig.getKeys();
        while (partitionFunctionConfigKeysIter.hasNext()) {
            String keyye = partitionFunctionConfigKeysIter.next();
            loadedFunctionConfig.put(keyye, partitionConfig.getString(keyye));
        }
        System.out.println(loadedFunctionConfig.get("columnValues"));
        System.out.println(loadedFunctionConfig.get("otherValue"));
    }
}
