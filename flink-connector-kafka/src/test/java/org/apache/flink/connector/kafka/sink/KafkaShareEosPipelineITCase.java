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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.share.ShareAckCommittable;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;
import org.apache.flink.connector.kafka.source.reader.transaction.KafkaShareAckTransactionManager;
import org.apache.flink.connector.kafka.source.reader.transaction.ShareAckTransactionClient;
import org.apache.flink.connector.kafka.source.reader.transaction.ShareAckTransactionHandle;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

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
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(240)
@ResourceLock("KafkaTestBase")
class KafkaShareEosPipelineITCase {

    private static final String BOOTSTRAP_SERVERS_PROPERTY =
            "flink.kafka.share.it.bootstrap.servers";
    private static final String KAFKA_IMAGE_PROPERTY = "flink.kafka.share.it.image";
    private static final String SHARE_ACK_MODE_CONFIG = "share.acknowledgement.mode";
    private static final String SHARE_AUTO_OFFSET_RESET_CONFIG = "share.auto.offset.reset";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(45);
    private static final int PARALLELISM = 4;
    private static final int NO_CHECKPOINT = -1;

    private static final Map<String, FlinkKafkaInternalProducer<byte[], byte[]>>
            SOURCE_ACK_PRODUCERS = new ConcurrentHashMap<>();
    private static final Set<String> SINK_COMMITTED_SHARE_ACKS = ConcurrentHashMap.newKeySet();
    private static final Set<String> COMMITTED_SHARE_ACKS = ConcurrentHashMap.newKeySet();
    private static final Queue<String> COMMIT_EVENTS = new ConcurrentLinkedQueue<>();

    private TestKafkaContainer kafkaContainer;

    @AfterEach
    void tearDown() {
        SOURCE_ACK_PRODUCERS.values()
                .forEach(
                        producer -> {
                            try {
                                producer.close();
                            } catch (Exception ignored) {
                            }
                        });
        SOURCE_ACK_PRODUCERS.clear();
        SINK_COMMITTED_SHARE_ACKS.clear();
        COMMITTED_SHARE_ACKS.clear();
        COMMIT_EVENTS.clear();
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }

    @Test
    void testShareSourceOperatorToKafkaSinkExactlyOnceCommitsSinkBeforeShareAcks()
            throws Exception {
        int partitionCount = 6;
        int recordsPerPartition = 5;
        int expectedRecords = partitionCount * recordsPerPartition;
        SharePipelineContext context = createContext(partitionCount);
        produceToPartitions(
                context.bootstrapServers, context.inputTopic, partitionCount, recordsPerPartition);

        Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        MiniClusterWithClientResource miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .setConfiguration(flinkConfiguration)
                                .build());
        miniCluster.before();
        try (AdminClient admin = createAdmin(context.bootstrapServers)) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.configure(flinkConfiguration);
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100L);

            env.addSource(
                            new CheckpointedTransactionalShareSource(
                                    context.bootstrapServers,
                                    context.shareGroupId,
                                    context.inputTopic))
                    .name("kafka-share-source")
                    .setParallelism(PARALLELISM)
                    .rebalance()
                    .map(new RecordingTransform())
                    .name("flink-operator")
                    .setParallelism(PARALLELISM)
                    .sinkTo(
                            new ShareAwareExactlyOnceKafkaSink(
                                    context.bootstrapServers,
                                    context.outputTopic,
                                    "flink-share-eos-sink-" + context.suffix))
                    .name("kafka-eos-sink")
                    .setParallelism(PARALLELISM);

            env.execute("share-source-to-kafka-sink-eos-it");

            List<ShareOutputRecord> outputRecords =
                    readCommittedOutput(
                            context.bootstrapServers, context.outputTopic, expectedRecords);
            assertThat(outputRecords).hasSize(expectedRecords);
            assertThat(
                            outputRecords.stream()
                                    .map(record -> record.inputPartition + "-" + record.inputOffset)
                                    .collect(Collectors.toSet()))
                    .hasSize(expectedRecords);
            assertThat(
                            outputRecords.stream()
                                    .map(record -> record.inputPartition)
                                    .collect(Collectors.toSet()))
                    .containsExactlyInAnyOrderElementsOf(
                            IntStream.range(0, partitionCount)
                                    .boxed()
                                    .collect(Collectors.toSet()));
            assertThat(
                            outputRecords.stream()
                                    .map(record -> record.mapSubtaskId)
                                    .collect(Collectors.toSet()))
                    .hasSizeGreaterThan(1);

            for (TopicPartition topicPartition : context.inputTopicPartitions) {
                waitForShareLag(admin, context.shareGroupId, topicPartition, 0L);
            }

            List<String> commitEvents = new ArrayList<>(COMMIT_EVENTS);
            List<String> sinkCommits =
                    commitEvents.stream()
                            .filter(event -> event.startsWith("sink:"))
                            .collect(Collectors.toList());
            List<String> shareCommits =
                    commitEvents.stream()
                            .filter(event -> event.startsWith("share:"))
                            .collect(Collectors.toList());
            assertThat(sinkCommits).isNotEmpty();
            assertThat(shareCommits).isNotEmpty();
            assertThat(shareCommits).doesNotHaveDuplicates();
            assertThat(COMMITTED_SHARE_ACKS).hasSameSizeAs(shareCommits);
            assertThat(SINK_COMMITTED_SHARE_ACKS).containsAll(COMMITTED_SHARE_ACKS);
            for (String shareAckCommitKey : COMMITTED_SHARE_ACKS) {
                int sinkCommitIndex = commitEvents.indexOf("sink-share:" + shareAckCommitKey);
                int shareCommitIndex = commitEvents.indexOf("share:" + shareAckCommitKey);
                assertThat(sinkCommitIndex).isGreaterThanOrEqualTo(0);
                assertThat(shareCommitIndex).isGreaterThan(sinkCommitIndex);
            }
        } finally {
            miniCluster.after();
        }
    }

    private SharePipelineContext createContext(int partitionCount) throws Exception {
        assumeTransactionalShareAckApis();
        String bootstrapServers = bootstrapServers();
        String suffix = UUID.randomUUID().toString();
        String inputTopic = "flink-share-eos-input-" + suffix;
        String outputTopic = "flink-share-eos-output-" + suffix;
        String shareGroupId = "flink-share-eos-group-" + suffix;
        List<TopicPartition> inputTopicPartitions =
                IntStream.range(0, partitionCount)
                        .mapToObj(partition -> new TopicPartition(inputTopic, partition))
                        .collect(Collectors.toList());

        try (AdminClient admin = createAdmin(bootstrapServers)) {
            admin.createTopics(
                            Set.of(
                                    new NewTopic(inputTopic, partitionCount, (short) 1),
                                    new NewTopic(outputTopic, partitionCount, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
            alterShareGroupOffsetReset(admin, shareGroupId);
        }
        return new SharePipelineContext(
                bootstrapServers, suffix, inputTopic, outputTopic, shareGroupId, inputTopicPartitions);
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
                        + " to run Kafka share-group EOS pipeline ITs.");
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

    private static void produceToPartitions(
            String bootstrapServers, String topic, int partitionCount, int recordsPerPartition)
            throws Exception {
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProperties(bootstrapServers))) {
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

    private static List<ShareOutputRecord> readCommittedOutput(
            String bootstrapServers, String topic, int expectedRecords) throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "share-eos-output-reader-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        List<ShareOutputRecord> records = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            CommonTestUtils.waitUtil(
                    () -> {
                        ConsumerRecords<byte[], byte[]> polled = consumer.poll(POLL_TIMEOUT);
                        polled.forEach(record -> records.add(ShareOutputRecord.parse(record)));
                        return records.size() >= expectedRecords;
                    },
                    WAIT_TIMEOUT,
                    "Timed out waiting for committed sink output.");
        }
        return records;
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

    private static Object invoke(
            Object target, String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = target.getClass().getMethod(methodName, parameterTypes);
        return method.invoke(target, args);
    }

    private static Object invoke(Object target, String methodName) throws Exception {
        return invoke(target, methodName, new Class<?>[0]);
    }

    private static String shareAckCommitKey(ShareAckCommittable committable) {
        return committable.getTransactionalId()
                + "-"
                + committable.getTransactionOwnerId()
                + "-"
                + committable.getTransactionOwnerEpoch();
    }

    private static final class SharePipelineContext {
        private final String bootstrapServers;
        private final String suffix;
        private final String inputTopic;
        private final String outputTopic;
        private final String shareGroupId;
        private final List<TopicPartition> inputTopicPartitions;

        private SharePipelineContext(
                String bootstrapServers,
                String suffix,
                String inputTopic,
                String outputTopic,
                String shareGroupId,
                List<TopicPartition> inputTopicPartitions) {
            this.bootstrapServers = bootstrapServers;
            this.suffix = suffix;
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
            this.shareGroupId = shareGroupId;
            this.inputTopicPartitions = inputTopicPartitions;
        }
    }

    private static final class CheckpointedTransactionalShareSource
            extends RichParallelSourceFunction<ShareSourceRecord>
            implements CheckpointedFunction, CheckpointListener {

        private static final int MAX_EMPTY_POLLS = 12;

        private final String bootstrapServers;
        private final String groupId;
        private final String topic;

        private transient ListState<ShareAckCommittable> pendingState;
        private transient KafkaShareAckTransactionManager transactionManager;
        private transient List<ShareAckCommittable> restoredPendingCommittables;

        private volatile boolean running = true;
        private volatile boolean hasUncheckpointedAcks;
        private volatile long lastSnapshotCheckpointId = NO_CHECKPOINT;
        private volatile long completedCheckpointId = NO_CHECKPOINT;
        private int emittedRecords;

        private CheckpointedTransactionalShareSource(
                String bootstrapServers, String groupId, String topic) {
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.topic = topic;
        }

        @Override
        public void initializeState(
                org.apache.flink.runtime.state.FunctionInitializationContext context)
                throws Exception {
            pendingState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "pending-share-ack-committables",
                                            ShareAckCommittable.class));
            restoredPendingCommittables = new ArrayList<>();
            for (ShareAckCommittable committable : pendingState.get()) {
                restoredPendingCommittables.add(committable);
            }
        }

        @Override
        public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context)
                throws Exception {
            if (transactionManager == null) {
                return;
            }
            List<ShareAckCommittable> pending =
                    transactionManager.snapshotState(context.getCheckpointId());
            pendingState.update(pending);
            hasUncheckpointedAcks = false;
            if (!pending.isEmpty()) {
                lastSnapshotCheckpointId = context.getCheckpointId();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            completedCheckpointId = checkpointId;
            if (transactionManager != null) {
                transactionManager.markCommittedUpTo(checkpointId);
            }
        }

        @Override
        public void run(SourceFunction.SourceContext<ShareSourceRecord> context) throws Exception {
            int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            ReflectiveShareAckTransactionClient client =
                    new ReflectiveShareAckTransactionClient(
                            bootstrapServers, groupId, topic, subtaskId);
            transactionManager =
                    new KafkaShareAckTransactionManager(
                            client, groupId, subtaskId, restoredPendingCommittables);
            try (KafkaShareAckTransactionManager ignored = transactionManager) {
                int emptyPolls = 0;
                while (running) {
                    ConsumerRecords<byte[], byte[]> records = client.poll(POLL_TIMEOUT);
                    if (records.isEmpty()) {
                        emptyPolls++;
                        if (emptyPolls >= MAX_EMPTY_POLLS && canFinish()) {
                            return;
                        }
                        Thread.sleep(50L);
                        continue;
                    }

                    emptyPolls = 0;
                    List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
                    records.forEach(batch::add);
                    synchronized (context.getCheckpointLock()) {
                        for (ConsumerRecord<byte[], byte[]> record : batch) {
                            client.acknowledgeAccept(record);
                        }
                        ShareAckTransactionHandle transaction =
                                transactionManager.stageAcknowledgementsForTransaction();
                        ShareAckCommittable shareAck =
                                new ShareAckCommittable(
                                        NO_CHECKPOINT,
                                        transaction.getTransactionalId(),
                                        transaction.getTransactionOwnerId(),
                                        transaction.getTransactionOwnerEpoch(),
                                        groupId,
                                        subtaskId);
                        for (ConsumerRecord<byte[], byte[]> record : batch) {
                            context.collect(ShareSourceRecord.from(subtaskId, record, shareAck));
                        }
                        emittedRecords += batch.size();
                        hasUncheckpointedAcks = true;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private boolean canFinish() {
            return emittedRecords == 0
                    || (!hasUncheckpointedAcks
                            && lastSnapshotCheckpointId != NO_CHECKPOINT
                            && completedCheckpointId >= lastSnapshotCheckpointId);
        }
    }

    private static final class ReflectiveShareAckTransactionClient
            implements ShareAckTransactionClient {

        private final String topic;
        private final int sourceSubtaskId;
        private final KafkaShareConsumer<byte[], byte[]> consumer;
        private final Properties producerProperties;

        @Nullable private FlinkKafkaInternalProducer<byte[], byte[]> producer;
        @Nullable private ShareAckTransactionHandle activeHandle;
        private boolean transactionOpen;

        private ReflectiveShareAckTransactionClient(
                String bootstrapServers, String groupId, String topic, int sourceSubtaskId) {
            this.topic = topic;
            this.sourceSubtaskId = sourceSubtaskId;
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
            String transactionalId =
                    "flink-share-eos-source-"
                            + sourceSubtaskId
                            + "-"
                            + UUID.randomUUID();
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
            SOURCE_ACK_PRODUCERS.put(transaction.getTransactionalId(), producer);
            producer = null;
            activeHandle = null;
            transactionOpen = false;
        }

        private ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            return consumer.poll(timeout);
        }

        private void acknowledgeAccept(ConsumerRecord<byte[], byte[]> record) {
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        }

        @Override
        public void close() {
            if (producer != null) {
                if (transactionOpen) {
                    producer.abortTransaction();
                }
                producer.close();
            }
            consumer.close(Duration.ZERO);
        }
    }

    private static final class RecordingTransform
            extends RichMapFunction<ShareSourceRecord, ShareSourceRecord> {

        @Override
        public ShareSourceRecord map(ShareSourceRecord value) {
            return value.withMapSubtaskId(
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    private static final class ShareAwareExactlyOnceKafkaSink
            implements TwoPhaseCommittingStatefulSink<
                            ShareSourceRecord, KafkaWriterState, KafkaShareEosCommittable>,
                    org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology<
                            KafkaShareEosCommittable> {

        private final Properties kafkaProducerConfig;
        private final String transactionalIdPrefix;
        private final KafkaRecordSerializationSchema<ShareSourceRecord> recordSerializer;

        private ShareAwareExactlyOnceKafkaSink(
                String bootstrapServers, String outputTopic, String transactionalIdPrefix) {
            this.kafkaProducerConfig = producerProperties(bootstrapServers);
            this.transactionalIdPrefix = transactionalIdPrefix;
            this.recordSerializer = new ShareRecordSerializationSchema(outputTopic);
        }

        @Override
        public PrecommittingStatefulSinkWriter<
                        ShareSourceRecord, KafkaWriterState, KafkaShareEosCommittable>
                createWriter(WriterInitContext context) throws IOException {
            return restoreWriter(context, Collections.emptyList());
        }

        @Override
        public PrecommittingStatefulSinkWriter<
                        ShareSourceRecord, KafkaWriterState, KafkaShareEosCommittable>
                restoreWriter(WriterInitContext context, Collection<KafkaWriterState> recoveredState)
                        throws IOException {
            ExactlyOnceKafkaWriter<ShareSourceRecord> writer =
                    new ExactlyOnceKafkaWriter<>(
                            DeliveryGuarantee.EXACTLY_ONCE,
                            kafkaProducerConfig,
                            transactionalIdPrefix,
                            context,
                            recordSerializer,
                            context.asSerializationSchemaInitializationContext(),
                            TransactionNamingStrategy.DEFAULT.getAbortImpl(),
                            TransactionNamingStrategy.DEFAULT.getImpl(),
                            recoveredState);
            ShareAwareKafkaWriter shareAwareWriter = new ShareAwareKafkaWriter(writer);
            shareAwareWriter.initialize();
            return shareAwareWriter;
        }

        @Override
        public Committer<KafkaShareEosCommittable> createCommitter(CommitterInitContext context) {
            return new SinkOnlyKafkaCommitter(
                    new KafkaCommitter(
                            kafkaProducerConfig,
                            transactionalIdPrefix,
                            context.getTaskInfo().getIndexOfThisSubtask(),
                            context.getTaskInfo().getAttemptNumber(),
                            false,
                            FlinkKafkaInternalProducer::new));
        }

        @Override
        public SimpleVersionedSerializer<KafkaShareEosCommittable> getCommittableSerializer() {
            return new KafkaShareEosCommittableSerializer();
        }

        @Override
        public SimpleVersionedSerializer<KafkaWriterState> getWriterStateSerializer() {
            return new KafkaWriterStateSerializer();
        }

        @Override
        public void addPostCommitTopology(
                org.apache.flink.streaming.api.datastream.DataStream<
                                CommittableMessage<KafkaShareEosCommittable>>
                        committables) {
            StandardSinkTopologies.addGlobalCommitter(
                    committables,
                    context -> new ShareAckPostCommitter(),
                    KafkaShareEosCommittableSerializer::new);
        }
    }

    private static final class ShareAwareKafkaWriter
            implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                    ShareSourceRecord, KafkaWriterState, KafkaShareEosCommittable> {

        private final ExactlyOnceKafkaWriter<ShareSourceRecord> delegate;
        private final Set<ShareAckCommittable> currentShareAckCommittables =
                new LinkedHashSet<>();

        private ShareAwareKafkaWriter(ExactlyOnceKafkaWriter<ShareSourceRecord> delegate) {
            this.delegate = delegate;
        }

        private void initialize() {
            delegate.initialize();
        }

        @Override
        public void write(ShareSourceRecord element, Context context)
                throws IOException, InterruptedException {
            delegate.write(element, context);
            currentShareAckCommittables.add(element.shareAckCommittable);
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            delegate.flush(endOfInput);
        }

        @Override
        public Collection<KafkaShareEosCommittable> prepareCommit()
                throws IOException, InterruptedException {
            Collection<KafkaCommittable> kafkaCommittables = delegate.prepareCommit();
            if (kafkaCommittables.isEmpty()) {
                currentShareAckCommittables.clear();
                return Collections.emptyList();
            }
            KafkaShareEosCommittable committable =
                    KafkaShareEosCommittable.ready(
                            NO_CHECKPOINT, kafkaCommittables, currentShareAckCommittables);
            currentShareAckCommittables.clear();
            return List.of(committable);
        }

        @Override
        public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
            return delegate.snapshotState(checkpointId);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    private static final class SinkOnlyKafkaCommitter
            implements Committer<KafkaShareEosCommittable> {

        private final KafkaCommitter kafkaCommitter;

        private SinkOnlyKafkaCommitter(KafkaCommitter kafkaCommitter) {
            this.kafkaCommitter = kafkaCommitter;
        }

        @Override
        public void commit(Collection<CommitRequest<KafkaShareEosCommittable>> requests)
                throws IOException, InterruptedException {
            for (CommitRequest<KafkaShareEosCommittable> request : requests) {
                KafkaShareEosCommittable committable = request.getCommittable();
                List<ForwardingKafkaCommitRequest> kafkaRequests =
                        committable.getKafkaCommittables().stream()
                                .map(ForwardingKafkaCommitRequest::new)
                                .collect(Collectors.toList());
                List<Committer.CommitRequest<KafkaCommittable>> kafkaCommitRequests =
                        new ArrayList<>(kafkaRequests);
                kafkaCommitter.commit(kafkaCommitRequests);
                Optional<ForwardingKafkaCommitRequest> retry =
                        kafkaRequests.stream()
                                .filter(kafkaRequest -> kafkaRequest.retry)
                                .findFirst();
                if (retry.isPresent()) {
                    request.retryLater();
                    continue;
                }
                Optional<ForwardingKafkaCommitRequest> failed =
                        kafkaRequests.stream()
                                .filter(kafkaRequest -> kafkaRequest.failure.get() != null)
                                .findFirst();
                if (failed.isPresent()) {
                    request.signalFailedWithUnknownReason(failed.get().failure.get());
                    continue;
                }
                committable.getKafkaCommittables().stream()
                        .map(KafkaCommittable::getTransactionalId)
                        .forEach(transactionalId -> COMMIT_EVENTS.add("sink:" + transactionalId));
                committable.getShareAckCommittables().stream()
                        .map(KafkaShareEosPipelineITCase::shareAckCommitKey)
                        .forEach(
                                shareAckCommitKey -> {
                                    SINK_COMMITTED_SHARE_ACKS.add(shareAckCommitKey);
                                    COMMIT_EVENTS.add("sink-share:" + shareAckCommitKey);
                                });
            }
        }

        @Override
        public void close() throws Exception {
            kafkaCommitter.close();
        }
    }

    private static final class ForwardingKafkaCommitRequest
            implements Committer.CommitRequest<KafkaCommittable> {

        private final KafkaCommittable committable;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private boolean retry;
        private int retries;

        private ForwardingKafkaCommitRequest(KafkaCommittable committable) {
            this.committable = committable;
        }

        @Override
        public KafkaCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return retries;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            failure.set(t);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            failure.set(t);
        }

        @Override
        public void retryLater() {
            retry = true;
            retries++;
        }

        @Override
        public void updateAndRetryLater(KafkaCommittable committable) {
            retryLater();
        }

        @Override
        public void signalAlreadyCommitted() {}
    }

    private static final class ShareAckPostCommitter
            implements Committer<KafkaShareEosCommittable> {

        @Override
        public void commit(Collection<CommitRequest<KafkaShareEosCommittable>> requests) {
            Set<ShareAckCommittable> shareAckCommittables =
                    requests.stream()
                            .flatMap(
                                    request ->
                                            request.getCommittable()
                                                    .getShareAckCommittables()
                                                    .stream())
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            for (ShareAckCommittable shareAckCommittable : shareAckCommittables) {
                commitShareAck(shareAckCommittable);
            }
        }

        private void commitShareAck(ShareAckCommittable committable) {
            String commitKey = shareAckCommitKey(committable);
            assertThat(SINK_COMMITTED_SHARE_ACKS).contains(commitKey);
            if (!COMMITTED_SHARE_ACKS.add(commitKey)) {
                return;
            }

            FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    SOURCE_ACK_PRODUCERS.remove(committable.getTransactionalId());
            assertThat(producer).isNotNull();
            producer.commitTransaction();
            producer.close();
            COMMIT_EVENTS.add("share:" + commitKey);
        }

        @Override
        public void close() {}
    }

    private static final class ShareRecordSerializationSchema
            implements KafkaRecordSerializationSchema<ShareSourceRecord> {

        private static final long serialVersionUID = 1L;

        private final String outputTopic;

        private ShareRecordSerializationSchema(String outputTopic) {
            this.outputTopic = outputTopic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                ShareSourceRecord element, KafkaSinkContext context, Long timestamp) {
            byte[] key =
                    (element.inputPartition + "-" + element.inputOffset)
                            .getBytes(StandardCharsets.UTF_8);
            byte[] value = element.outputValue().getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(outputTopic, null, timestamp, key, value);
        }
    }

    private static final class ShareSourceRecord implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int sourceSubtaskId;
        private final int mapSubtaskId;
        private final int inputPartition;
        private final long inputOffset;
        private final String inputValue;
        private final ShareAckCommittable shareAckCommittable;

        private ShareSourceRecord(
                int sourceSubtaskId,
                int mapSubtaskId,
                int inputPartition,
                long inputOffset,
                String inputValue,
                ShareAckCommittable shareAckCommittable) {
            this.sourceSubtaskId = sourceSubtaskId;
            this.mapSubtaskId = mapSubtaskId;
            this.inputPartition = inputPartition;
            this.inputOffset = inputOffset;
            this.inputValue = inputValue;
            this.shareAckCommittable = shareAckCommittable;
        }

        private static ShareSourceRecord from(
                int sourceSubtaskId,
                ConsumerRecord<byte[], byte[]> record,
                ShareAckCommittable shareAckCommittable) {
            return new ShareSourceRecord(
                    sourceSubtaskId,
                    NO_CHECKPOINT,
                    record.partition(),
                    record.offset(),
                    new String(record.value(), StandardCharsets.UTF_8),
                    shareAckCommittable);
        }

        private ShareSourceRecord withMapSubtaskId(int mapSubtaskId) {
            return new ShareSourceRecord(
                    sourceSubtaskId,
                    mapSubtaskId,
                    inputPartition,
                    inputOffset,
                    inputValue,
                    shareAckCommittable);
        }

        private String outputValue() {
            return inputPartition
                    + "|"
                    + inputOffset
                    + "|"
                    + sourceSubtaskId
                    + "|"
                    + mapSubtaskId
                    + "|"
                    + inputValue;
        }
    }

    private static final class ShareOutputRecord {

        private final int inputPartition;
        private final long inputOffset;
        private final int mapSubtaskId;

        private ShareOutputRecord(int inputPartition, long inputOffset, int mapSubtaskId) {
            this.inputPartition = inputPartition;
            this.inputOffset = inputOffset;
            this.mapSubtaskId = mapSubtaskId;
        }

        private static ShareOutputRecord parse(ConsumerRecord<byte[], byte[]> record) {
            String[] parts = new String(record.value(), StandardCharsets.UTF_8).split("\\|", 5);
            return new ShareOutputRecord(
                    Integer.parseInt(parts[0]),
                    Long.parseLong(parts[1]),
                    Integer.parseInt(parts[3]));
        }
    }
}
