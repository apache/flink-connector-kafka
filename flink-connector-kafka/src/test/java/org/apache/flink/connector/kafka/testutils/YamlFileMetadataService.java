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
 */

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.DumperOptions;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.TypeDescription;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.Constructor;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.Node;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.SequenceNode;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.Tag;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.representer.Representer;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads metadata from yaml file and lazily refreshes periodically. This implementation assumes that
 * specified topics exist in the clusters that are contained in the yaml metadata. Therefore, topic
 * is used as the stream name. This is designed to integrate with K8s configmap and cluster
 * migration.
 *
 * <p>Files must be of the form:
 *
 * <pre>{@code
 * - streamId: stream0
 *   clusterMetadataList:
 *     - clusterId: cluster0
 *       bootstrapServers: bootstrap-server-0:443
 *       topics:
 *         - topic0
 *         - topic1
 *     - clusterId: cluster1
 *       bootstrapServers: bootstrap-server-1:443
 *       topics:
 *         - topic2
 *         - topic3
 * - streamId: stream1
 *   clusterMetadataList:
 *     - clusterId: cluster2
 *       bootstrapServers: bootstrap-server-2:443
 *       topics:
 *         - topic4
 *         - topic5
 * }</pre>
 *
 * <p>Typically, usage will look like: first consuming from one cluster, second adding new cluster
 * and consuming from both clusters, and third consuming from only from the new cluster after all
 * data from the old cluster has been read.
 */
public class YamlFileMetadataService implements KafkaMetadataService {
    private static final Logger logger = LoggerFactory.getLogger(YamlFileMetadataService.class);
    private final String metadataFilePath;
    private final Duration refreshInterval;
    private Instant lastRefresh;
    // current metadata should be accessed from #getAllStreams()
    private transient Set<KafkaStream> streamMetadata;
    private transient Yaml yaml;

    /**
     * Constructs a metadata service based on cluster information stored in a file.
     *
     * @param metadataFilePath location of the metadata file
     * @param metadataTtl ttl of metadata that controls how often to refresh
     */
    public YamlFileMetadataService(String metadataFilePath, Duration metadataTtl) {
        this.metadataFilePath = metadataFilePath;
        this.refreshInterval = metadataTtl;
        this.lastRefresh = Instant.MIN;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This obtains the all stream metadata and enforces the ttl configuration on the metadata.
     */
    @Override
    public Set<KafkaStream> getAllStreams() {
        refreshIfNeeded();
        return streamMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        ImmutableMap.Builder<String, KafkaStream> builder = ImmutableMap.builder();
        Set<KafkaStream> streams = getAllStreams();
        for (KafkaStream stream : streams) {
            if (streamIds.contains(stream.getStreamId())) {
                builder.put(stream.getStreamId(), stream);
            }
        }

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        return getAllStreams().stream()
                .flatMap(kafkaStream -> kafkaStream.getClusterMetadataMap().keySet().stream())
                .anyMatch(cluster -> cluster.equals(kafkaClusterId));
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {}

    /**
     * A utility method for writing metadata in the expected yaml format.
     *
     * @param streamMetadata list of {@link StreamMetadata}
     * @param metadataFile the metadata {@link File}
     */
    public static void saveToYaml(List<StreamMetadata> streamMetadata, File metadataFile)
            throws IOException {
        logger.debug("Writing stream infos to file: {}", streamMetadata);
        Yaml yaml = initYamlParser();
        FileWriter fileWriter = new FileWriter(metadataFile, false);
        yaml.dump(streamMetadata, fileWriter);
        fileWriter.close();
    }

    /**
     * A utility method for writing metadata in the expected yaml format.
     *
     * @param kafkaStreams list of {@link KafkaStream}
     * @param metadataFile the metadata {@link File}
     */
    public static void saveToYamlFromKafkaStreams(List<KafkaStream> kafkaStreams, File metadataFile)
            throws IOException {
        saveToYaml(
                kafkaStreams.stream()
                        .map(YamlFileMetadataService::convertToStreamMetadata)
                        .collect(Collectors.toList()),
                metadataFile);
    }

    private static StreamMetadata convertToStreamMetadata(KafkaStream kafkaStream) {
        return new StreamMetadata(
                kafkaStream.getStreamId(),
                kafkaStream.getClusterMetadataMap().entrySet().stream()
                        .map(
                                entry ->
                                        new StreamMetadata.ClusterMetadata(
                                                entry.getKey(),
                                                entry.getValue()
                                                        .getProperties()
                                                        .getProperty(
                                                                CommonClientConfigs
                                                                        .BOOTSTRAP_SERVERS_CONFIG),
                                                new ArrayList<>(entry.getValue().getTopics())))
                        .collect(Collectors.toList()));
    }

    private void refreshIfNeeded() {
        Instant now = Instant.now();
        try {
            if (now.isAfter(lastRefresh.plus(refreshInterval.toMillis(), ChronoUnit.MILLIS))) {
                streamMetadata = parseFile();
                lastRefresh = now;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    Set<KafkaStream> parseFile() throws IOException {
        if (yaml == null) {
            yaml = initYamlParser();
        }

        List<StreamMetadata> streamMetadataList =
                yaml.load(Files.newInputStream(Paths.get(metadataFilePath)));
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Input stream of metadata file has size: {}",
                    Files.newInputStream(Paths.get(metadataFilePath)).available());
        }
        Set<KafkaStream> kafkaStreams = new HashSet<>();

        for (StreamMetadata streamMetadata : streamMetadataList) {
            Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();

            for (StreamMetadata.ClusterMetadata clusterMetadata :
                    streamMetadata.getClusterMetadataList()) {
                final String kafkaClusterId;
                if (clusterMetadata.getClusterId() != null) {
                    kafkaClusterId = clusterMetadata.getClusterId();
                } else {
                    kafkaClusterId = clusterMetadata.getBootstrapServers();
                }

                Properties properties = new Properties();
                properties.setProperty(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        clusterMetadata.getBootstrapServers());
                clusterMetadataMap.put(
                        kafkaClusterId,
                        new ClusterMetadata(
                                new HashSet<>(clusterMetadata.getTopics()), properties));
            }

            kafkaStreams.add(new KafkaStream(streamMetadata.getStreamId(), clusterMetadataMap));
        }

        logger.debug("From {} loaded metadata: {}", metadataFilePath, kafkaStreams);
        return kafkaStreams;
    }

    private static Yaml initYamlParser() {
        Representer representer = new Representer();
        representer.addClassTag(StreamMetadata.class, Tag.MAP);
        TypeDescription typeDescription = new TypeDescription(StreamMetadata.class);
        representer.addTypeDescription(typeDescription);
        representer.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        return new Yaml(new ListConstructor<>(StreamMetadata.class), representer);
    }

    /** A custom constructor is required to read yaml lists at the root. */
    private static class ListConstructor<T> extends Constructor {
        private final Class<T> clazz;

        public ListConstructor(final Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        protected Object constructObject(final Node node) {
            if (node instanceof SequenceNode && isRootNode(node)) {
                ((SequenceNode) node).setListType(clazz);
            }
            return super.constructObject(node);
        }

        private boolean isRootNode(final Node node) {
            return node.getStartMark().getIndex() == 0;
        }
    }

    /** Internal class for snake yaml parsing. A mutable, no arg, public class is required. */
    public static class StreamMetadata {

        private String streamId;
        private List<ClusterMetadata> clusterMetadataList;

        public StreamMetadata() {}

        public StreamMetadata(String streamId, List<ClusterMetadata> clusterMetadataList) {
            this.streamId = streamId;
            this.clusterMetadataList = clusterMetadataList;
        }

        public String getStreamId() {
            return streamId;
        }

        public void setStreamId(String streamId) {
            this.streamId = streamId;
        }

        public List<ClusterMetadata> getClusterMetadataList() {
            return clusterMetadataList;
        }

        public void setClusterMetadataList(List<ClusterMetadata> clusterMetadata) {
            this.clusterMetadataList = clusterMetadata;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("streamId", streamId)
                    .add("clusterMetadataList", clusterMetadataList)
                    .toString();
        }

        /** Information to connect to a particular cluster. */
        public static class ClusterMetadata {
            private String clusterId;
            private String bootstrapServers;
            private List<String> topics;

            public ClusterMetadata() {}

            public ClusterMetadata(String clusterId, String bootstrapServers, List<String> topics) {
                this.clusterId = clusterId;
                this.bootstrapServers = bootstrapServers;
                this.topics = topics;
            }

            public String getClusterId() {
                return clusterId;
            }

            public void setClusterId(String clusterId) {
                this.clusterId = clusterId;
            }

            public String getBootstrapServers() {
                return bootstrapServers;
            }

            public void setBootstrapServers(String bootstrapServers) {
                this.bootstrapServers = bootstrapServers;
            }

            public List<String> getTopics() {
                return topics;
            }

            public void setTopics(List<String> topics) {
                this.topics = topics;
            }
        }
    }
}
