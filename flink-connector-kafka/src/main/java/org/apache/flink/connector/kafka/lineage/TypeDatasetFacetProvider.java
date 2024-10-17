package org.apache.flink.connector.kafka.lineage;

import java.util.Optional;

/** Contains method to extract {@link TypeDatasetFacet}. */
public interface TypeDatasetFacetProvider {

    /**
     * Returns a type dataset facet or `Optional.empty` in case an implementing class is not able to
     * resolve type.
     *
     * @return
     */
    Optional<TypeDatasetFacet> getTypeDatasetFacet();
}
