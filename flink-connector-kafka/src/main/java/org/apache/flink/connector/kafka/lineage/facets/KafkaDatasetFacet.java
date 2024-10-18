package org.apache.flink.connector.kafka.lineage.facets;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/** Facet containing all information related to sources and sinks on Kafka. */
public class KafkaDatasetFacet implements LineageDatasetFacet {

    public static final String KAFKA_FACET_NAME = "kafka";

    public final Properties properties;
    public final TypeInformation typeInformation;
    public final KafkaDatasetIdentifier topicIdentifier;

    public KafkaDatasetFacet(
            KafkaDatasetIdentifier topicIdentifier,
            Properties properties,
            TypeInformation typeInformation) {
        this.topicIdentifier = topicIdentifier;
        this.properties = properties;
        this.typeInformation = typeInformation;
    }

    public void addProperties(Properties properties) {
        this.properties.putAll(properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaDatasetFacet that = (KafkaDatasetFacet) o;
        return Objects.equals(properties, that.properties)
                && Objects.equals(typeInformation, that.typeInformation)
                && Objects.equals(topicIdentifier, that.topicIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, typeInformation, topicIdentifier);
    }

    @Override
    public String name() {
        return KAFKA_FACET_NAME;
    }

    /**
     * Record class to contain topics' identifier information which can be either a list of topics
     * or a topic pattern.
     */
    public static class KafkaDatasetIdentifier {
        public final List<String> topics;
        public final Pattern topicPattern;

        public KafkaDatasetIdentifier(List<String> fixedTopics, Pattern topicPattern) {
            this.topics = fixedTopics;
            this.topicPattern = topicPattern;
        }

        public static KafkaDatasetIdentifier of(Pattern pattern) {
            return new KafkaDatasetIdentifier(Collections.emptyList(), pattern);
        }

        public static KafkaDatasetIdentifier of(List<String> fixedTopics) {
            return new KafkaDatasetIdentifier(fixedTopics, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KafkaDatasetIdentifier that = (KafkaDatasetIdentifier) o;
            return Objects.equals(topics, that.topics)
                    && Objects.equals(topicPattern, that.topicPattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topics, topicPattern);
        }
    }
}
