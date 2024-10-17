package org.apache.flink.connector.kafka.lineage;

import java.util.Optional;

/** Contains method to extract {@link KafkaDatasetFacet}. */
public interface KafkaDatasetFacetProvider {

    /**
     * Returns a Kafka dataset facet or `Optional.empty` in case an implementing class is not able
     * to identify a dataset.
     *
     * @return
     */
    Optional<KafkaDatasetFacet> getKafkaDatasetFacet();
}
