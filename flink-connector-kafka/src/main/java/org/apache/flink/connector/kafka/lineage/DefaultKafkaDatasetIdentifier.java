package org.apache.flink.connector.kafka.lineage;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/** Default implementation of {@link KafkaDatasetIdentifier}. */
public class DefaultKafkaDatasetIdentifier implements KafkaDatasetIdentifier {

    @Nullable private final List<String> topics;
    @Nullable private final Pattern topicPattern;

    public DefaultKafkaDatasetIdentifier(List<String> fixedTopics, Pattern topicPattern) {
        this.topics = fixedTopics;
        this.topicPattern = topicPattern;
    }

    public static DefaultKafkaDatasetIdentifier ofPattern(Pattern pattern) {
        return new DefaultKafkaDatasetIdentifier(Collections.emptyList(), pattern);
    }

    public static DefaultKafkaDatasetIdentifier ofTopics(List<String> fixedTopics) {
        return new DefaultKafkaDatasetIdentifier(fixedTopics, null);
    }

    @Nullable
    public List<String> getTopics() {
        return topics;
    }

    @Nullable
    public Pattern getTopicPattern() {
        return topicPattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultKafkaDatasetIdentifier that = (DefaultKafkaDatasetIdentifier) o;
        return Objects.equals(topics, that.topics)
                && Objects.equals(topicPattern, that.topicPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topics, topicPattern);
    }
}
