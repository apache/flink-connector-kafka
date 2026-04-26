/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
