package org.apache.flink.connector.kafka.source;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public interface KafkaConsumerFactory {
    KafkaConsumer<byte[], byte[]> get(Properties properties);
}
