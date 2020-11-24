package com.reiser.stream.untils;

import java.util.Properties;

/**
 * @author: reiserx
 * Date:2020/11/17
 * Des:
 */
public class FlinkKafkaProducerUtils {
    public static Properties getProducerProperties(String brokers) {
        Properties properties = getCommonProperties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("metadata.broker.list", brokers);
        properties.setProperty("zookeeper.connect", "reiser001:2181,reiser002:2181");
        return properties;
    }

    public static Properties getCommonProperties() {
        Properties properties = new Properties();
        properties.setProperty("linger.ms", "100");
        properties.setProperty("retries", "100");
        properties.setProperty("retry.backoff.ms", "200");
        properties.setProperty("buffer.memory", "524288");
        properties.setProperty("batch.size", "100");
        properties.setProperty("max.request.size", "524288");
        properties.setProperty("compression.type", "snappy");
        properties.setProperty("request.timeout.ms", "180000");
        properties.setProperty("max.block.ms", "180000");
        return properties;
    }
}
