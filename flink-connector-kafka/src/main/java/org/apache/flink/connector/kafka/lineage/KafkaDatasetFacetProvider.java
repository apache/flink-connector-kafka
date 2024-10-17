package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.connector.kafka.lineage.facets.KafkaDatasetFacet;

import java.util.Optional;

/** Contains method to extract {@link KafkaDatasetFacet}. */
public interface KafkaDatasetFacetProvider {

    /**
     * List of lineage dataset facets.
     *
     * @return
     */
    Optional<KafkaDatasetFacet> getKafkaDatasetFacet();
}
