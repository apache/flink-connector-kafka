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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/** Utility class with useful methods for managing lineage objects. */
public class LineageUtil {

    private static final String KAFKA_DATASET_PREFIX = "kafka://";
    private static final String COMMA = ",";
    private static final String SEMICOLON = ";";

    public static LineageDataset datasetOf(String namespace, KafkaDatasetFacet kafkaDatasetFacet) {
        return datasetOf(namespace, kafkaDatasetFacet, Collections.emptyList());
    }

    public static LineageDataset datasetOf(
            String namespace, KafkaDatasetFacet kafkaDatasetFacet, TypeDatasetFacet typeFacet) {
        return datasetOf(namespace, kafkaDatasetFacet, Collections.singletonList(typeFacet));
    }

    private static LineageDataset datasetOf(
            String namespace,
            KafkaDatasetFacet kafkaDatasetFacet,
            List<LineageDatasetFacet> facets) {
        return new LineageDataset() {
            @Override
            public String name() {
                return kafkaDatasetFacet.getTopicIdentifier().toLineageName();
            }

            @Override
            public String namespace() {
                return namespace;
            }

            @Override
            public Map<String, LineageDatasetFacet> facets() {
                Map<String, LineageDatasetFacet> facetMap = new HashMap<>();
                facetMap.put(DefaultKafkaDatasetFacet.KAFKA_FACET_NAME, kafkaDatasetFacet);
                facetMap.putAll(
                        facets.stream()
                                .collect(
                                        Collectors.toMap(LineageDatasetFacet::name, item -> item)));
                return facetMap;
            }
        };
    }

    public static String namespaceOf(Properties properties) {
        String bootstrapServers = properties.getProperty("bootstrap.servers");

        if (bootstrapServers == null) {
            return KAFKA_DATASET_PREFIX;
        }

        if (bootstrapServers.contains(COMMA)) {
            bootstrapServers = bootstrapServers.split(COMMA)[0];
        } else if (bootstrapServers.contains(SEMICOLON)) {
            bootstrapServers = bootstrapServers.split(SEMICOLON)[0];
        }

        return String.format(KAFKA_DATASET_PREFIX + bootstrapServers);
    }

    public static SourceLineageVertex sourceLineageVertexOf(Collection<LineageDataset> datasets) {
        return new SourceLineageVertex() {
            @Override
            public Boundedness boundedness() {
                return Boundedness.CONTINUOUS_UNBOUNDED;
            }

            @Override
            public List<LineageDataset> datasets() {
                return datasets.stream().collect(Collectors.toList());
            }
        };
    }

    public static LineageVertex lineageVertexOf(Collection<LineageDataset> datasets) {
        return new LineageVertex() {
            @Override
            public List<LineageDataset> datasets() {
                return datasets.stream().collect(Collectors.toList());
            }
        };
    }
}
