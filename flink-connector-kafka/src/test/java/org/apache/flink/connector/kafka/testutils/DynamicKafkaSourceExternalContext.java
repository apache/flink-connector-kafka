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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.connectors.kafka.DynamicKafkaSourceTestHelper;
import org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironmentImpl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A external context for {@link DynamicKafkaSource} connector testing framework. */
public class DynamicKafkaSourceExternalContext implements DataStreamSourceExternalContext<String> {
    private static final Logger logger =
            LoggerFactory.getLogger(DynamicKafkaSourceExternalContext.class);
    private static final int NUM_TEST_RECORDS_PER_SPLIT = 10;
    private static final int NUM_PARTITIONS = 1;

    private static final Pattern STREAM_ID_PATTERN = Pattern.compile("stream-[0-9]+");
    private final List<URL> connectorJarPaths;
    private final Set<KafkaStream> kafkaStreams = new HashSet<>();
    private final Map<String, Properties> clusterPropertiesMap;
    private final List<SplitDataWriter> splitDataWriters = new ArrayList<>();

    // add random suffix to alleviate race conditions with Kafka deleting topics
    private final long randomTopicSuffix;

    public DynamicKafkaSourceExternalContext(
            List<String> bootstrapServerList, List<URL> connectorJarPaths) {
        this.connectorJarPaths = connectorJarPaths;
        Properties propertiesForCluster0 = new Properties();
        propertiesForCluster0.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerList.get(0));
        Properties propertiesForCluster1 = new Properties();
        propertiesForCluster1.setProperty(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerList.get(1));

        this.clusterPropertiesMap =
                ImmutableMap.of(
                        "cluster0", propertiesForCluster0, "cluster1", propertiesForCluster1);
        this.randomTopicSuffix = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
    }

    @Override
    public Source<String, ?, ?> createSource(TestingSourceSettings sourceSettings)
            throws UnsupportedOperationException {
        final DynamicKafkaSourceBuilder<String> builder = DynamicKafkaSource.builder();

        builder.setStreamPattern(STREAM_ID_PATTERN)
                .setKafkaMetadataService(new MockKafkaMetadataService(kafkaStreams))
                .setGroupId("DynamicKafkaSourceExternalContext")
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class));

        if (sourceSettings.getBoundedness().equals(Boundedness.BOUNDED)) {
            builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.build();
    }

    @Override
    public ExternalSystemSplitDataWriter<String> createSourceSplitDataWriter(
            TestingSourceSettings sourceSettings) {
        int suffix = splitDataWriters.size();
        List<Tuple2<String, String>> clusterTopics = setupSplits(String.valueOf(suffix));
        SplitDataWriter splitDataWriter = new SplitDataWriter(clusterPropertiesMap, clusterTopics);
        this.splitDataWriters.add(splitDataWriter);
        return splitDataWriter;
    }

    private List<Tuple2<String, String>> setupSplits(String suffix) {
        KafkaStream kafkaStream = getKafkaStream(suffix + randomTopicSuffix);
        logger.info("Setting up splits for {}", kafkaStream);
        List<Tuple2<String, String>> clusterTopics =
                kafkaStream.getClusterMetadataMap().entrySet().stream()
                        .flatMap(
                                entry ->
                                        entry.getValue().getTopics().stream()
                                                .map(topic -> Tuple2.of(entry.getKey(), topic)))
                        .collect(Collectors.toList());

        for (Tuple2<String, String> clusterTopic : clusterTopics) {
            String cluster = clusterTopic.f0;
            String topic = clusterTopic.f1;
            KafkaTestEnvironmentImpl.createNewTopic(
                    topic, NUM_PARTITIONS, 1, clusterPropertiesMap.get(cluster));
        }

        kafkaStreams.add(kafkaStream);
        return clusterTopics;
    }

    private KafkaStream getKafkaStream(String suffix) {
        return new KafkaStream(
                "stream-" + suffix,
                ImmutableMap.of(
                        "cluster0",
                        new ClusterMetadata(
                                ImmutableSet.of("topic0-" + suffix, "topic1-" + suffix),
                                clusterPropertiesMap.get("cluster0")),
                        "cluster1",
                        new ClusterMetadata(
                                ImmutableSet.of("topic2-" + suffix, "topic3-" + suffix),
                                clusterPropertiesMap.get("cluster1"))));
    }

    @Override
    public List<String> generateTestData(
            TestingSourceSettings sourceSettings, int splitIndex, long seed) {
        return IntStream.range(0, NUM_TEST_RECORDS_PER_SPLIT * NUM_PARTITIONS)
                .boxed()
                .map(num -> Integer.toString(num))
                .collect(Collectors.toList());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

    @Override
    public List<URL> getConnectorJarPaths() {
        return connectorJarPaths;
    }

    @Override
    public void close() throws Exception {
        // need to clear topics
        Map<String, List<String>> clusterTopics = new HashMap<>();
        for (SplitDataWriter splitDataWriter : splitDataWriters) {
            for (Tuple2<String, String> clusterTopic : splitDataWriter.getClusterTopics()) {
                clusterTopics
                        .computeIfAbsent(clusterTopic.f0, unused -> new ArrayList<>())
                        .add(clusterTopic.f1);
            }
        }
        for (Map.Entry<String, List<String>> entry : clusterTopics.entrySet()) {
            String cluster = entry.getKey();
            List<String> topics = entry.getValue();
            try (AdminClient adminClient = AdminClient.create(clusterPropertiesMap.get(cluster))) {
                adminClient.deleteTopics(topics).all().get();
                CommonTestUtils.waitUtil(
                        () -> {
                            try {
                                return adminClient.listTopics().listings().get().stream()
                                        .map(TopicListing::name)
                                        .noneMatch(topics::contains);
                            } catch (Exception e) {
                                logger.warn("Exception caught when listing Kafka topics", e);
                                return false;
                            }
                        },
                        Duration.ofSeconds(30),
                        String.format("Topics %s were not deleted within timeout", topics));
            }

            logger.info("topics {} are deleted from {}", topics, cluster);
        }
    }

    private static class SplitDataWriter implements ExternalSystemSplitDataWriter<String> {
        private final Map<String, Properties> clusterPropertiesMap;
        private final List<Tuple2<String, String>> clusterTopics;

        public SplitDataWriter(
                Map<String, Properties> clusterPropertiesMap,
                List<Tuple2<String, String>> clusterTopics) {
            this.clusterPropertiesMap = clusterPropertiesMap;
            this.clusterTopics = clusterTopics;
        }

        @Override
        public void writeRecords(List<String> records) {
            int counter = 0;
            try {
                for (Tuple2<String, String> clusterTopic : clusterTopics) {
                    String cluster = clusterTopic.f0;
                    String topic = clusterTopic.f1;
                    List<ProducerRecord<String, String>> producerRecords = new ArrayList<>();
                    for (int j = 0; j < NUM_PARTITIONS; j++) {
                        for (int k = 0; k < NUM_TEST_RECORDS_PER_SPLIT; k++) {
                            if (records.size() <= counter) {
                                break;
                            }

                            producerRecords.add(
                                    new ProducerRecord<>(topic, j, null, records.get(counter++)));
                        }
                    }

                    logger.info("Writing producer records: {}", producerRecords);

                    DynamicKafkaSourceTestHelper.produceToKafka(
                            clusterPropertiesMap.get(cluster),
                            producerRecords,
                            StringSerializer.class,
                            StringSerializer.class);
                }
            } catch (Throwable e) {
                throw new RuntimeException("Failed to produce test data", e);
            }
        }

        @Override
        public void close() throws Exception {}

        public List<Tuple2<String, String>> getClusterTopics() {
            return clusterTopics;
        }
    }
}
