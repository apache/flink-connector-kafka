package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/** Contains method to extract {@link KafkaDatasetFacet}. */
@PublicEvolving
public interface KafkaDatasetFacetProvider {

    /**
     * Returns a Kafka dataset facet or empty in case an implementing class is not able to identify
     * a dataset.
     */
    Optional<KafkaDatasetFacet> getKafkaDatasetFacet();
}
