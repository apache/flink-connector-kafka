package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.connector.kafka.lineage.facets.KafkaDatasetFacet.KafkaDatasetIdentifier;

import java.util.Optional;

/** Contains method which allows extracting topic identifier. */
public interface KafkaDatasetIdentifierProvider {

    /**
     * List of lineage dataset facets.
     *
     * @return
     */
    Optional<KafkaDatasetIdentifier> getDatasetIdentifier();
}
