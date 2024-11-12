package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Objects;

/** Default implementation of {@link KafkaDatasetFacet}. */
@PublicEvolving
public class DefaultTypeDatasetFacet implements TypeDatasetFacet {

    public static final String TYPE_FACET_NAME = "type";

    private final TypeInformation typeInformation;

    public DefaultTypeDatasetFacet(TypeInformation typeInformation) {
        this.typeInformation = typeInformation;
    }

    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultTypeDatasetFacet that = (DefaultTypeDatasetFacet) o;
        return Objects.equals(typeInformation, that.typeInformation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInformation);
    }

    @Override
    public String name() {
        return TYPE_FACET_NAME;
    }
}
