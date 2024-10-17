package org.apache.flink.connector.kafka.lineage;

import javax.annotation.Nullable;

import java.util.List;
import java.util.regex.Pattern;

/** Kafka dataset identifier which can contain either a list of topics or a topic pattern. */
public interface KafkaDatasetIdentifier {
    @Nullable
    List<String> getTopics();

    @Nullable
    Pattern getTopicPattern();

    /**
     * Assigns lineage dataset's name which is topic pattern if it is present or comma separated
     * list of topics.
     *
     * @return
     */
    default String toLineageName() {
        if (getTopicPattern() != null) {
            return getTopicPattern().toString();
        }
        return String.join(",", getTopics());
    }
}
