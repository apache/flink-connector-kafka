package org.apache.flink.connector.kafka.lineage;

import java.util.Optional;

/** Contains method which allows extracting topic identifier. */
public interface KafkaDatasetIdentifierProvider {

    /**
     * Gets Kafka dataset identifier or empty in case a class implementing is not able to extract
     * dataset identifier.
     *
     * @return
     */
    Optional<DefaultKafkaDatasetIdentifier> getDatasetIdentifier();
}
