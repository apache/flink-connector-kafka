/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.share;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.source.reader.transaction.KafkaShareAckTransactionManager;
import org.apache.flink.connector.kafka.source.reader.transaction.ShareAckTransactionClient;
import org.apache.flink.connector.kafka.source.reader.transaction.ShareAckTransactionHandle;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(180)
@ResourceLock("KafkaTestBase")
class KafkaShareAckTransactionITCase {

    private static final String BOOTSTRAP_SERVERS_PROPERTY =
            "flink.kafka.share.it.bootstrap.servers";
    private static final String KAFKA_IMAGE_PROPERTY = "flink.kafka.share.it.image";
    private static final String SHARE_ACK_MODE_CONFIG = "share.acknowledgement.mode";
    private static final String SHARE_AUTO_OFFSET_RESET_CONFIG = "share.auto.offset.reset";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final int PARALLELISM = 4;

    private TestKafkaContainer kafkaContainer;

    @AfterEach
    void tearDown() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }

    @Test
    void testShareAckCommitOnCheckpoint() throws Exception {
        ShareTestContext context = createContext();
        produce(context.bootstrapServers, context.topic, "first");

        ReflectiveShareAckTransactionClient client =
                new ReflectiveShareAckTransactionClient(
                        context.bootstrapServers, context.groupId, context.topic);
        try (KafkaShareAckTransactionManager manager =
                        new KafkaShareAckTransactionManager(client, context.groupId, 0, List.of());
                AdminClient admin = createAdmin(context.bootstrapServers)) {
            ConsumerRecord<byte[], byte[]> record = client.pollOne();
            assertThat(value(record)).isEqualTo("first");

            client.acknowledgeAccept(record);
            manager.stageAcknowledgements();
            assertThat(client.pollCount()).isZero();

            List<ShareAckCommittable> committables = manager.snapshotState(42L);
            assertThat(committables).hasSize(1);

            client.commit(committables.get(0));
            manager.markCommittedUpTo(42L);

            waitForShareLag(admin, context.groupId, context.topicPartition, 0L);
            assertThat(client.pollCount()).isZero();
            assertThat(manager.snapshotState(43L)).isEmpty();
        }
    }

    @Test
    void testShareAckAbortAfterFailedCheckpointRedelivers() throws Exception {
        ShareTestContext context = createContext();
        produce(context.bootstrapServers, context.topic, "redeliver");

        ReflectiveShareAckTransactionClient client =
                new ReflectiveShareAckTransactionClient(
                        context.bootstrapServers, context.groupId, context.topic);
        try (KafkaShareAckTransactionManager manager =
                        new KafkaShareAckTransactionManager(client, context.groupId, 1, List.of());
                AdminClient admin = createAdmin(context.bootstrapServers)) {
            ConsumerRecord<byte[], byte[]> record = client.pollOne();
            assertThat(value(record)).isEqualTo("redeliver");

            client.acknowledgeAccept(record);
            manager.stageAcknowledgements();

            ShareAckCommittable committable = manager.snapshotState(42L).get(0);
            client.abort(committable);

            waitForShareLag(admin, context.groupId, context.topicPartition, 1L);
            ConsumerRecord<byte[], byte[]> redelivered = client.pollOne();
            assertThat(redelivered.offset()).isEqualTo(record.offset());
            assertThat(value(redelivered)).isEqualTo("redeliver");
        }
    }

    @Test
    void testMultiplePollAcksCommitInOneCheckpointTransaction() throws Exception {
        ShareTestContext context = createContext();
        produce(context.bootstrapServers, context.topic, "first");

        ReflectiveShareAckTransactionClient client =
                new ReflectiveShareAckTransactionClient(
                        context.bootstrapServers, context.groupId, context.topic);
        try (KafkaShareAckTransactionManager manager =
                        new KafkaShareAckTransactionManager(client, context.groupId, 2, List.of());
                AdminClient admin = createAdmin(context.bootstrapServers)) {
            ConsumerRecord<byte[], byte[]> first = client.pollOne();
            assertThat(value(first)).isEqualTo("first");
            client.acknowledgeAccept(first);
            manager.stageAcknowledgements();

            produce(context.bootstrapServers, context.topic, "second");
            ConsumerRecord<byte[], byte[]> second = client.pollOne();
            assertThat(value(second)).isEqualTo("second");
            client.acknowledgeAccept(second);
            manager.stageAcknowledgements();

            List<ShareAckCommittable> committables = manager.snapshotState(42L);
            assertThat(committables).hasSize(1);

            client.commit(committables.get(0));
            manager.markCommittedUpTo(42L);

            waitForShareLag(admin, context.groupId, context.topicPartition, 0L);
            assertThat(client.pollCount()).isZero();
        }
    }

    @Test
    void testParallelSubtasksCommitMultiPartitionShareAcks() throws Exception {
        int partitionCount = 6;
        int recordsPerPartition = 4;
        int expectedRecords = partitionCount * recordsPerPartition;
        ShareTestContext context = createContext(partitionCount);
        produceToPartitions(
                context.bootstrapServers, context.topic, partitionCount, recordsPerPartition);

        MiniClusterWithClientResource miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .setConfiguration(new Configuration())
                                .build());
        miniCluster.before();
        try (AdminClient admin = createAdmin(context.bootstrapServers)) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);

            List<ShareReadRecord> results = new ArrayList<>();
            try (CloseableIterator<ShareReadRecord> iterator =
                    env.addSource(
                                    new ParallelTransactionalShareSource(
                                            context.bootstrapServers,
                                            context.groupId,
                                            context.topic))
                            .name("parallel-share-source")
                            .setParallelism(PARALLELISM)
                            .rebalance()
                            .map(new RecordingMapFunction())
                            .name("downstream-map")
                            .setParallelism(PARALLELISM)
                            .executeAndCollect()) {
                iterator.forEachRemaining(results::add);
            }

            List<ShareReadRecord> dataRecords =
                    results.stream()
                            .filter(record -> !record.sourceStarted)
                            .collect(Collectors.toList());
            assertThat(dataRecords).hasSize(expectedRecords);
            assertThat(
                            dataRecords.stream()
                                    .map(record -> record.partition)
                                    .collect(Collectors.toSet()))
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, partitionCount)
                                    .boxed()
                                    .collect(Collectors.toSet()));
            assertThat(
                            results.stream()
                                    .filter(record -> record.sourceStarted)
                                    .map(record -> record.sourceSubtaskId)
                                    .collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder(0, 1, 2, 3);
            assertThat(
                            results.stream()
                                    .map(record -> record.mapSubtaskId)
                                    .collect(Collectors.toSet()))
                    .hasSizeGreaterThan(1);
            Set<String> topicPartitionOffsets =
                    dataRecords.stream()
                            .map(record -> record.partition + "-" + record.offset)
                            .collect(Collectors.toCollection(HashSet::new));
            assertThat(topicPartitionOffsets).hasSize(expectedRecords);

            for (TopicPartition topicPartition : context.topicPartitions) {
                waitForShareLag(admin, context.groupId, topicPartition, 0L);
            }
        } finally {
            miniCluster.after();
        }
    }

    private ShareTestContext createContext() throws Exception {
        return createContext(1);
    }

    private ShareTestContext createContext(int partitionCount) throws Exception {
        assumeTransactionalShareAckApis();
        String bootstrapServers = bootstrapServers();
        String suffix = UUID.randomUUID().toString();
        String topic = "flink-share-ack-it-" + suffix;
        String groupId = "flink-share-ack-group-" + suffix;
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        List<TopicPartition> topicPartitions =
                IntStream.range(0, partitionCount)
                        .mapToObj(partition -> new TopicPartition(topic, partition))
                        .collect(Collectors.toList());

        try (AdminClient admin = createAdmin(bootstrapServers)) {
            admin.createTopics(Set.of(new NewTopic(topic, partitionCount, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
            alterShareGroupOffsetReset(admin, groupId);
        }
        return new ShareTestContext(
                bootstrapServers, topic, groupId, topicPartition, topicPartitions);
    }

    private String bootstrapServers() {
        String configuredBootstrapServers = System.getProperty(BOOTSTRAP_SERVERS_PROPERTY);
        if (configuredBootstrapServers != null && !configuredBootstrapServers.isBlank()) {
            return configuredBootstrapServers;
        }

        String kafkaImage = System.getProperty(KAFKA_IMAGE_PROPERTY);
        Assumptions.assumeTrue(
                kafkaImage != null && !kafkaImage.isBlank(),
                "Set "
                        + BOOTSTRAP_SERVERS_PROPERTY
                        + " or "
                        + KAFKA_IMAGE_PROPERTY
                        + " to run Kafka share-group ITs.");
        kafkaContainer =
                new TestKafkaContainer(DockerImageName.parse(kafkaImage))
                        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                        .withEnv("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "15000")
                        .withEnv("KAFKA_GROUP_SHARE_PARTITION_MAX_RECORD_LOCKS", "10000")
                        .withEnv("KAFKA_GROUP_SHARE_MAX_PARTITION_MAX_RECORD_LOCKS", "10000")
                        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
                        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS", "3")
                        .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1");
        kafkaContainer.start();
        return kafkaContainer.getBootstrapServers();
    }

    private static void assumeTransactionalShareAckApis() {
        Assumptions.assumeTrue(
                transactionalShareAckApisAvailable(),
                "Kafka client on the test classpath does not expose KIP-1289 transactional share ACK APIs.");
    }

    private static boolean transactionalShareAckApisAvailable() {
        try {
            Class<?> acknowledgementsClass =
                    Class.forName("org.apache.kafka.clients.consumer.ShareAcknowledgements");
            Class<?> metadataClass =
                    Class.forName("org.apache.kafka.clients.consumer.ShareGroupMetadata");
            KafkaShareConsumer.class.getMethod("shareGroupMetadata");
            KafkaShareConsumer.class.getMethod("acknowledgementsForTransaction");
            KafkaProducer.class.getMethod(
                    "sendShareAcknowledgementsToTransaction",
                    acknowledgementsClass,
                    metadataClass);
            return true;
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    private static AdminClient createAdmin(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(properties);
    }

    private static void alterShareGroupOffsetReset(AdminClient admin, String groupId)
            throws Exception {
        ConfigResource groupResource = new ConfigResource(Type.GROUP, groupId);
        admin.incrementalAlterConfigs(
                        Map.of(
                                groupResource,
                                List.of(
                                        new AlterConfigOp(
                                                new ConfigEntry(
                                                        SHARE_AUTO_OFFSET_RESET_CONFIG,
                                                        "earliest"),
                                                AlterConfigOp.OpType.SET))))
                .all()
                .get(30, TimeUnit.SECONDS);
    }

    private static void produce(String bootstrapServers, String topic, String value)
            throws Exception {
        Properties properties = producerProperties(bootstrapServers);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            producer.send(new ProducerRecord<>(topic, 0, null, bytes, bytes)).get();
            producer.flush();
        }
    }

    private static void produceToPartitions(
            String bootstrapServers, String topic, int partitionCount, int recordsPerPartition)
            throws Exception {
        Properties properties = producerProperties(bootstrapServers);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            for (int partition = 0; partition < partitionCount; partition++) {
                for (int index = 0; index < recordsPerPartition; index++) {
                    String value = "partition-" + partition + "-record-" + index;
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    producer.send(new ProducerRecord<>(topic, partition, null, bytes, bytes))
                            .get();
                }
            }
            producer.flush();
        }
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");
        return properties;
    }

    private static Properties shareConsumerProperties(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(SHARE_ACK_MODE_CONFIG, "explicit");
        return properties;
    }

    private static void waitForShareLag(
            AdminClient admin, String groupId, TopicPartition topicPartition, long expectedLag)
            throws Exception {
        CommonTestUtils.waitUtil(
                () -> {
                    try {
                        SharePartitionOffsetInfo info =
                                sharePartitionOffsetInfo(admin, groupId, topicPartition);
                        return info != null
                                && info.lag().isPresent()
                                && info.lag().get() == expectedLag;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                WAIT_TIMEOUT,
                "Share partition lag did not reach " + expectedLag + " for " + topicPartition);
    }

    private static SharePartitionOffsetInfo sharePartitionOffsetInfo(
            AdminClient admin, String groupId, TopicPartition topicPartition) throws Exception {
        ListShareGroupOffsetsResult result =
                admin.listShareGroupOffsets(
                        Map.of(
                                groupId,
                                new ListShareGroupOffsetsSpec()
                                        .topicPartitions(List.of(topicPartition))),
                        new ListShareGroupOffsetsOptions().timeoutMs(30000));
        return result.partitionsToOffsetInfo(groupId).get(30, TimeUnit.SECONDS).get(topicPartition);
    }

    private static String value(ConsumerRecord<byte[], byte[]> record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }

    private static Object invoke(
            Object target, String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = target.getClass().getMethod(methodName, parameterTypes);
        return method.invoke(target, args);
    }

    private static Object invoke(Object target, String methodName) throws Exception {
        return invoke(target, methodName, new Class<?>[0]);
    }

    private static final class ShareTestContext {
        private final String bootstrapServers;
        private final String topic;
        private final String groupId;
        private final TopicPartition topicPartition;
        private final List<TopicPartition> topicPartitions;

        private ShareTestContext(
                String bootstrapServers,
                String topic,
                String groupId,
                TopicPartition topicPartition,
                List<TopicPartition> topicPartitions) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.groupId = groupId;
            this.topicPartition = topicPartition;
            this.topicPartitions = topicPartitions;
        }
    }

    private static final class ReflectiveShareAckTransactionClient
            implements ShareAckTransactionClient {

        private final String topic;
        private final KafkaShareConsumer<byte[], byte[]> consumer;
        private final Properties producerProperties;

        private FlinkKafkaInternalProducer<byte[], byte[]> producer;
        private ShareAckTransactionHandle activeHandle;
        private boolean transactionOpen;

        private ReflectiveShareAckTransactionClient(
                String bootstrapServers, String groupId, String topic) {
            this.topic = topic;
            this.consumer =
                    new KafkaShareConsumer<>(
                            shareConsumerProperties(bootstrapServers, groupId),
                            new ByteArrayDeserializer(),
                            new ByteArrayDeserializer());
            this.consumer.subscribe(List.of(topic));
            this.producerProperties = producerProperties(bootstrapServers);
        }

        @Override
        public ShareAckTransactionHandle beginTransaction() {
            String transactionalId = "flink-share-ack-it-txn-" + UUID.randomUUID();
            producer = new FlinkKafkaInternalProducer<>(producerProperties, transactionalId);
            producer.initTransactions();
            producer.partitionsFor(topic);
            producer.beginTransaction();
            transactionOpen = true;
            activeHandle =
                    new ShareAckTransactionHandle(
                            transactionalId, producer.getProducerId(), producer.getEpoch());
            return activeHandle;
        }

        @Override
        public void stageAcknowledgements(ShareAckTransactionHandle transaction) throws IOException {
            try {
                assertThat(transaction).isEqualTo(activeHandle);
                Object acknowledgements = invoke(consumer, "acknowledgementsForTransaction");
                assertThat((Boolean) invoke(acknowledgements, "isEmpty")).isFalse();
                Object groupMetadata = invoke(consumer, "shareGroupMetadata");
                invoke(
                        producer,
                        "sendShareAcknowledgementsToTransaction",
                        new Class<?>[] {acknowledgements.getClass(), groupMetadata.getClass()},
                        acknowledgements,
                        groupMetadata);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void preCommit(ShareAckTransactionHandle transaction) {
            assertThat(transaction).isEqualTo(activeHandle);
            producer.flush();
        }

        private ConsumerRecord<byte[], byte[]> pollOne() throws Exception {
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            CommonTestUtils.waitUtil(
                    () -> {
                        ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_TIMEOUT);
                        polled.forEach(records::add);
                        return !records.isEmpty();
                    },
                    WAIT_TIMEOUT,
                    "Timed out waiting for one share-group record.");
            return records.get(0);
        }

        private int pollCount() {
            return consumer.poll(POLL_TIMEOUT).count();
        }

        private ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            return consumer.poll(timeout);
        }

        private void acknowledgeAccept(ConsumerRecord<byte[], byte[]> record) {
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        }

        private void commit(ShareAckCommittable committable) {
            assertThat(committable.getTransactionalId()).isEqualTo(activeHandle.getTransactionalId());
            assertThat(committable.getTransactionOwnerId())
                    .isEqualTo(activeHandle.getTransactionOwnerId());
            assertThat(committable.getTransactionOwnerEpoch())
                    .isEqualTo(activeHandle.getTransactionOwnerEpoch());
            producer.commitTransaction();
            transactionOpen = false;
        }

        private void abort(ShareAckCommittable committable) {
            assertThat(committable.getTransactionalId()).isEqualTo(activeHandle.getTransactionalId());
            producer.abortTransaction();
            transactionOpen = false;
        }

        @Override
        public void close() {
            if (producer != null) {
                if (transactionOpen) {
                    producer.abortTransaction();
                    transactionOpen = false;
                }
                producer.close();
            }
            consumer.close(Duration.ZERO);
        }
    }

    private static final class ParallelTransactionalShareSource
            extends RichParallelSourceFunction<ShareReadRecord> {

        private static final int MAX_EMPTY_POLLS = 12;

        private final String bootstrapServers;
        private final String groupId;
        private final String topic;

        private volatile boolean running = true;

        private ParallelTransactionalShareSource(
                String bootstrapServers, String groupId, String topic) {
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.topic = topic;
        }

        @Override
        public void run(SourceFunction.SourceContext<ShareReadRecord> context) throws Exception {
            int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            synchronized (context.getCheckpointLock()) {
                context.collect(ShareReadRecord.sourceStarted(subtaskId));
            }

            ReflectiveShareAckTransactionClient client =
                    new ReflectiveShareAckTransactionClient(bootstrapServers, groupId, topic);
            try (KafkaShareAckTransactionManager manager =
                    new KafkaShareAckTransactionManager(client, groupId, subtaskId, List.of())) {
                boolean stagedAcknowledgements = false;
                int emptyPolls = 0;
                while (running && emptyPolls < MAX_EMPTY_POLLS) {
                    ConsumerRecords<byte[], byte[]> records = client.poll(POLL_TIMEOUT);
                    if (records.isEmpty()) {
                        emptyPolls++;
                        continue;
                    }

                    emptyPolls = 0;
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        client.acknowledgeAccept(record);
                        synchronized (context.getCheckpointLock()) {
                            context.collect(ShareReadRecord.data(subtaskId, record));
                        }
                    }
                    manager.stageAcknowledgements();
                    stagedAcknowledgements = true;
                }

                if (stagedAcknowledgements) {
                    for (ShareAckCommittable committable : manager.snapshotState(1L)) {
                        client.commit(committable);
                    }
                    manager.markCommittedUpTo(1L);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static final class RecordingMapFunction
            extends RichMapFunction<ShareReadRecord, ShareReadRecord> {

        @Override
        public ShareReadRecord map(ShareReadRecord value) {
            return value.withMapSubtaskId(
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    private static final class ShareReadRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean sourceStarted;
        private final int sourceSubtaskId;
        private final int mapSubtaskId;
        private final int partition;
        private final long offset;
        private final String value;

        private ShareReadRecord(
                boolean sourceStarted,
                int sourceSubtaskId,
                int mapSubtaskId,
                int partition,
                long offset,
                String value) {
            this.sourceStarted = sourceStarted;
            this.sourceSubtaskId = sourceSubtaskId;
            this.mapSubtaskId = mapSubtaskId;
            this.partition = partition;
            this.offset = offset;
            this.value = value;
        }

        private static ShareReadRecord sourceStarted(int subtaskId) {
            return new ShareReadRecord(true, subtaskId, -1, -1, -1L, "");
        }

        private static ShareReadRecord data(
                int subtaskId, ConsumerRecord<byte[], byte[]> record) {
            return new ShareReadRecord(
                    false,
                    subtaskId,
                    -1,
                    record.partition(),
                    record.offset(),
                    value(record));
        }

        private ShareReadRecord withMapSubtaskId(int mapSubtaskId) {
            return new ShareReadRecord(
                    sourceStarted,
                    sourceSubtaskId,
                    mapSubtaskId,
                    partition,
                    offset,
                    value);
        }
    }
}
