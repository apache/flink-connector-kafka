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
import org.apache.flink.connector.kafka.lineage.facets.KafkaTopicListFacet;
import org.apache.flink.connector.kafka.lineage.facets.KafkaTopicPatternFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/** Utility class with useful methods for managing dataset facets. */
public class LineageUtil {

    private static final String KAFKA_DATASET_PREFIX = "kafka://";
    private static final String COMMA = ",";
    private static final String SEMICOLON = ";";

    /**
     * Loads facet from any object implementing @link{DatasetFacetProvider} interface.
     *
     * @param object
     * @return
     */
    public static Collection<LineageDatasetFacet> facetsFrom(Object object) {
        return Optional.of(object)
                .filter(LineageFacetProvider.class::isInstance)
                .map(LineageFacetProvider.class::cast)
                .map(LineageFacetProvider::getDatasetFacets)
                .orElse(Collections.emptyList());
    }

    /**
     * Creates dataset from a list of facets. Uses {@link KafkaTopicListFacet} to extract dataset
     * name from. Dataset per each element of topic list is created
     *
     * @param facets
     * @return
     */
    public static Collection<LineageDataset> datasetsFrom(
            String namespace, Collection<LineageDatasetFacet> facets) {
        // Check if topic list facet is available -> if so explode the list of facets
        Optional<KafkaTopicListFacet> topicList =
                facets.stream()
                        .filter(KafkaTopicListFacet.class::isInstance)
                        .map(KafkaTopicListFacet.class::cast)
                        .findAny();

        List<LineageDataset> datasets = new ArrayList<>();

        // Explode list of other facets
        if (topicList.isPresent()) {
            List<LineageDatasetFacet> facetsWithoutTopicList =
                    facets.stream().filter(f -> !f.equals(topicList)).collect(Collectors.toList());

            datasets.addAll(
                    topicList.get().topics.stream()
                            .map(t -> datasetOf(namespace, t, facetsWithoutTopicList))
                            .collect(Collectors.toList()));
        }

        // Check if topic pattern is present
        // If so topic pattern will be used as a dataset name
        datasets.addAll(
                facets.stream()
                        .filter(KafkaTopicPatternFacet.class::isInstance)
                        .map(KafkaTopicPatternFacet.class::cast)
                        .map(f -> datasetOf(namespace, f.pattern.toString(), facets))
                        .collect(Collectors.toList()));
        return datasets;
    }

    private static LineageDataset datasetOf(
            String namespace, String name, Collection<LineageDatasetFacet> facets) {
        return new LineageDataset() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String namespace() {
                return namespace;
            }

            @Override
            public Map<String, LineageDatasetFacet> facets() {
                return facets.stream()
                        .distinct()
                        .collect(Collectors.toMap(LineageDatasetFacet::name, item -> item));
            }
        };
    }

    public static String datasetNamespaceOf(Properties properties) {
        String bootstrapServers = properties.getProperty("bootstrap.servers");

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
