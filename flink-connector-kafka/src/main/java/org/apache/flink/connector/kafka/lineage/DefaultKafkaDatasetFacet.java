package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;

import java.util.Objects;
import java.util.Properties;

/** Default implementation of {@link KafkaDatasetFacet}. */
public class DefaultKafkaDatasetFacet implements KafkaDatasetFacet {

    public static final String KAFKA_FACET_NAME = "kafka";

    private Properties properties;

    private final KafkaDatasetIdentifier topicIdentifier;

    public DefaultKafkaDatasetFacet(KafkaDatasetIdentifier topicIdentifier, Properties properties) {
        this(topicIdentifier);

        this.properties = new Properties();
        KafkaPropertiesUtil.copyProperties(properties, this.properties);
    }

    public DefaultKafkaDatasetFacet(KafkaDatasetIdentifier topicIdentifier) {
        this.topicIdentifier = topicIdentifier;
    }

    public void setProperties(Properties properties) {
        this.properties = new Properties();
        KafkaPropertiesUtil.copyProperties(properties, this.properties);
    }

    public Properties getProperties() {
        return properties;
    }

    public KafkaDatasetIdentifier getTopicIdentifier() {
        return topicIdentifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultKafkaDatasetFacet that = (DefaultKafkaDatasetFacet) o;
        return Objects.equals(properties, that.properties)
                && Objects.equals(topicIdentifier, that.topicIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, topicIdentifier);
    }

    @Override
    public String name() {
        return KAFKA_FACET_NAME;
    }
}
