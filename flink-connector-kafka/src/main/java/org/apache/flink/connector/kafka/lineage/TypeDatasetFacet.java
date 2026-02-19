package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.util.Optional;

/** Facet definition to contain type information of source and sink. */
@PublicEvolving
public interface TypeDatasetFacet extends LineageDatasetFacet {
    TypeInformation getTypeInformation();

    /**
     * Sometimes a sink implementing {@link TypeDatasetFacetProvider} is not able to extract type.
     * This is happening for AvroSerializationSchema due to type erasure problem. In this case, it
     * makes sense to expose SerializationSchema to the lineage consumer so that it can use it to
     * extract type information.
     *
     * @return
     */
    default Optional<SerializationSchema> getSerializationSchema() {
        return Optional.empty();
    }
}
