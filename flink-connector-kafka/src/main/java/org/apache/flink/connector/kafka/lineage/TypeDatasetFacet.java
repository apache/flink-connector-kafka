package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

/** Facet definition to contain type information of source and sink. */
@PublicEvolving
public interface TypeDatasetFacet extends LineageDatasetFacet {
    TypeInformation getTypeInformation();
}
