package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/** Contains method which allows extracting topic identifier. */
@PublicEvolving
public interface KafkaDatasetIdentifierProvider {

    /**
     * Gets Kafka dataset identifier or empty in case a class implementing is not able to extract
     * dataset identifier.
     */
    Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier();
}
