package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.util.Collection;

/**
 * Contains method which can be used for lineage schema facet extraction. Useful for classes like
 * topic selectors or serialization schemas to extract dataset information from.
 */
public interface LineageFacetProvider {

    /**
     * List of lineage dataset facets.
     *
     * @return
     */
    Collection<LineageDatasetFacet> getDatasetFacets();
}
