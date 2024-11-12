package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Optional;

/** Contains method to extract {@link TypeDatasetFacet}. */
@PublicEvolving
public interface TypeDatasetFacetProvider {

    /**
     * Returns a type dataset facet or `Optional.empty` in case an implementing class is not able to
     * resolve type.
     */
    Optional<TypeDatasetFacet> getTypeDatasetFacet();
}
