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

package org.apache.flink.connector.kafka.dynamic.metadata;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connector.kafka.source.KafkaPropertiesUtil;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * A {@link KafkaMetadataService} that delegates metadata fetching to a single {@link AdminClient},
 * which is scoped to a single cluster. The stream ids are equivalent to topics.
 */
@Experimental
public class SingleClusterTopicMetadataService implements KafkaMetadataService {

    private final String kafkaClusterId;
    private final Properties properties;
    @Nullable private final OffsetsInitializer startingOffsetsInitializer;
    @Nullable private final OffsetsInitializer stoppingOffsetsInitializer;
    private transient AdminClient adminClient;

    /**
     * Create a {@link SingleClusterTopicMetadataService}.
     *
     * @param kafkaClusterId the id of the Kafka cluster.
     * @param properties the properties of the Kafka cluster.
     */
    public SingleClusterTopicMetadataService(String kafkaClusterId, Properties properties) {
        this(kafkaClusterId, properties, null, null);
    }

    /**
     * Create a {@link SingleClusterTopicMetadataService} with per-cluster offsets initializers.
     *
     * @param kafkaClusterId the id of the Kafka cluster.
     * @param properties the properties of the Kafka cluster.
     * @param startingOffsetsInitializer the starting offsets initializer for the cluster.
     * @param stoppingOffsetsInitializer the stopping offsets initializer for the cluster.
     */
    public SingleClusterTopicMetadataService(
            String kafkaClusterId,
            Properties properties,
            @Nullable OffsetsInitializer startingOffsetsInitializer,
            @Nullable OffsetsInitializer stoppingOffsetsInitializer) {
        this.kafkaClusterId = kafkaClusterId;
        this.properties = properties;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
    }

    /** {@inheritDoc} */
    @Override
    public Set<KafkaStream> getAllStreams() {
        try {
            return getAdminClient().listTopics().names().get().stream()
                    .map(this::createKafkaStream)
                    .collect(Collectors.toSet());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Fetching all streams failed", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        try {
            return getAdminClient().describeTopics(new ArrayList<>(streamIds)).allTopicNames().get()
                    .keySet().stream()
                    .collect(Collectors.toMap(topic -> topic, this::createKafkaStream));
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Fetching all streams failed", e);
        }
    }

    private KafkaStream createKafkaStream(String topic) {
        ClusterMetadata clusterMetadata =
                new ClusterMetadata(
                        Collections.singleton(topic),
                        properties,
                        startingOffsetsInitializer,
                        stoppingOffsetsInitializer);

        return new KafkaStream(topic, Collections.singletonMap(kafkaClusterId, clusterMetadata));
    }

    private AdminClient getAdminClient() {
        if (adminClient == null) {
            Properties adminClientProps = new Properties();
            KafkaPropertiesUtil.copyProperties(properties, adminClientProps);
            String clientIdPrefix =
                    adminClientProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
            adminClientProps.setProperty(
                    CommonClientConfigs.CLIENT_ID_CONFIG,
                    clientIdPrefix + "-single-cluster-topic-metadata-service");
            adminClient = AdminClient.create(adminClientProps);
        }

        return adminClient;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isClusterActive(String kafkaClusterId) {
        return this.kafkaClusterId.equals(kafkaClusterId);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
