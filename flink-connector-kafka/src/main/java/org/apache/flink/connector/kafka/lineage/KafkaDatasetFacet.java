package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.util.Properties;

/** Facet definition to contain all Kafka specific information on Kafka sources and sinks. */
public interface KafkaDatasetFacet extends LineageDatasetFacet {
    Properties getProperties();

    KafkaDatasetIdentifier getTopicIdentifier();

    void setProperties(Properties properties);
}
