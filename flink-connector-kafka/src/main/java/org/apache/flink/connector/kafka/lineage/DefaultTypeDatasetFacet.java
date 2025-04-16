package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Objects;
import java.util.Optional;

/** Default implementation of {@link KafkaDatasetFacet}. */
@PublicEvolving
public class DefaultTypeDatasetFacet implements TypeDatasetFacet {

    public static final String TYPE_FACET_NAME = "type";

    private final TypeInformation typeInformation;

    private final Optional<SerializationSchema> serializationSchema;

    public DefaultTypeDatasetFacet(TypeInformation typeInformation) {
        this.typeInformation = typeInformation;
        this.serializationSchema = Optional.empty();
    }

    public DefaultTypeDatasetFacet(SerializationSchema serializationSchema) {
        this.serializationSchema = Optional.of(serializationSchema);
        this.typeInformation = null;
    }

    public TypeInformation getTypeInformation() {
        return typeInformation;
    }

    public Optional<SerializationSchema> getSerializationSchema() {
        return serializationSchema;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultTypeDatasetFacet that = (DefaultTypeDatasetFacet) o;
        return Objects.equals(typeInformation, that.typeInformation)
                && Objects.equals(serializationSchema, that.serializationSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeInformation, serializationSchema);
    }

    @Override
    public String name() {
        return TYPE_FACET_NAME;
    }
}
