package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

/** Facet definition to contain type information of source and sink. */
public interface TypeDatasetFacet extends LineageDatasetFacet {
    TypeInformation getTypeInformation();
}
