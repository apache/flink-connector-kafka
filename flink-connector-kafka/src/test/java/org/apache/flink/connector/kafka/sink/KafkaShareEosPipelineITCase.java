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
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;
import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
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

    private static final Queue<String> COMMIT_EVENTS = new ConcurrentLinkedQueue<>();

    private TestKafkaContainer kafkaContainer;

    @AfterEach
    void tearDown() {
        COMMIT_EVENTS.clear();
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }

    @Test
    void testShareSourceOperatorToKafkaSinkExactlyOnceCommitsShareAcksInSinkTransaction()
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
                            new CheckpointedShareSource(
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
            assertThat(commitEvents).isNotEmpty();
            assertThat(commitEvents).allMatch(event -> event.startsWith("sink:"));
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

    private static final class CheckpointedShareSource
            extends RichParallelSourceFunction<ShareSourceRecord>
            implements CheckpointedFunction, CheckpointListener {

        private static final int MAX_EMPTY_POLLS = 12;

        private final String bootstrapServers;
        private final String groupId;
        private final String topic;

        private volatile boolean running = true;
        private volatile boolean hasUncheckpointedAcks;
        private volatile long lastSnapshotCheckpointId = NO_CHECKPOINT;
        private volatile long completedCheckpointId = NO_CHECKPOINT;
        private int emittedRecords;
        private int ackPayloadSequence;

        private CheckpointedShareSource(String bootstrapServers, String groupId, String topic) {
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.topic = topic;
        }

        @Override
        public void initializeState(
                org.apache.flink.runtime.state.FunctionInitializationContext context) {}

        @Override
        public void snapshotState(org.apache.flink.runtime.state.FunctionSnapshotContext context)
                throws Exception {
            if (hasUncheckpointedAcks) {
                lastSnapshotCheckpointId = context.getCheckpointId();
                hasUncheckpointedAcks = false;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            completedCheckpointId = checkpointId;
        }

        @Override
        public void run(SourceFunction.SourceContext<ShareSourceRecord> context) throws Exception {
            int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            ReflectiveShareConsumerClient client =
                    new ReflectiveShareConsumerClient(bootstrapServers, groupId, topic);
            try (ReflectiveShareConsumerClient ignored = client) {
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
                        ShareAckPayload shareAckPayload =
                                client.shareAckPayload(
                                        subtaskId + "-" + ackPayloadSequence++);
                        for (ConsumerRecord<byte[], byte[]> record : batch) {
                            context.collect(
                                    ShareSourceRecord.from(
                                            subtaskId, record, shareAckPayload));
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

    private static final class ReflectiveShareConsumerClient implements AutoCloseable {

        private final KafkaShareConsumer<byte[], byte[]> consumer;

        private ReflectiveShareConsumerClient(String bootstrapServers, String groupId, String topic) {
            this.consumer =
                    new KafkaShareConsumer<>(
                            shareConsumerProperties(bootstrapServers, groupId),
                            new ByteArrayDeserializer(),
                            new ByteArrayDeserializer());
            this.consumer.subscribe(List.of(topic));
        }

        private ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            return consumer.poll(timeout);
        }

        private void acknowledgeAccept(ConsumerRecord<byte[], byte[]> record) {
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
        }

        private ShareAckPayload shareAckPayload(String payloadId) throws IOException {
            try {
                Object acknowledgements = invoke(consumer, "acknowledgementsForTransaction");
                assertThat((Boolean) invoke(acknowledgements, "isEmpty")).isFalse();
                Object groupMetadata = invoke(consumer, "shareGroupMetadata");
                return ShareAckPayload.fromKafkaObjects(payloadId, acknowledgements, groupMetadata);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
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
                    ShareSourceRecord, KafkaWriterState, KafkaCommittable> {

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
        public PrecommittingStatefulSinkWriter<ShareSourceRecord, KafkaWriterState, KafkaCommittable>
                createWriter(WriterInitContext context) throws IOException {
            return restoreWriter(context, Collections.emptyList());
        }

        @Override
        public PrecommittingStatefulSinkWriter<ShareSourceRecord, KafkaWriterState, KafkaCommittable>
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
            SameTransactionShareAckKafkaWriter<ShareSourceRecord> shareAwareWriter =
                    new SameTransactionShareAckKafkaWriter<>(
                            writer, record -> List.of(record.shareAckPayload));
            shareAwareWriter.initialize();
            return shareAwareWriter;
        }

        @Override
        public Committer<KafkaCommittable> createCommitter(CommitterInitContext context) {
            return new RecordingKafkaCommitter(
                    new KafkaCommitter(
                            kafkaProducerConfig,
                            transactionalIdPrefix,
                            context.getTaskInfo().getIndexOfThisSubtask(),
                            context.getTaskInfo().getAttemptNumber(),
                            false,
                            FlinkKafkaInternalProducer::new));
        }

        @Override
        public SimpleVersionedSerializer<KafkaCommittable> getCommittableSerializer() {
            return new KafkaCommittableSerializer();
        }

        @Override
        public SimpleVersionedSerializer<KafkaWriterState> getWriterStateSerializer() {
            return new KafkaWriterStateSerializer();
        }
    }

    private static final class RecordingKafkaCommitter implements Committer<KafkaCommittable> {

        private final KafkaCommitter kafkaCommitter;

        private RecordingKafkaCommitter(KafkaCommitter kafkaCommitter) {
            this.kafkaCommitter = kafkaCommitter;
        }

        @Override
        public void commit(Collection<CommitRequest<KafkaCommittable>> requests)
                throws IOException, InterruptedException {
            for (CommitRequest<KafkaCommittable> request : requests) {
                ForwardingKafkaCommitRequest kafkaRequest =
                        new ForwardingKafkaCommitRequest(request.getCommittable());
                kafkaCommitter.commit(List.of(kafkaRequest));
                if (kafkaRequest.retry) {
                    request.retryLater();
                    continue;
                }
                Throwable failure = kafkaRequest.failure.get();
                if (failure != null) {
                    request.signalFailedWithUnknownReason(failure);
                    continue;
                }
                COMMIT_EVENTS.add("sink:" + request.getCommittable().getTransactionalId());
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
        private final ShareAckPayload shareAckPayload;

        private ShareSourceRecord(
                int sourceSubtaskId,
                int mapSubtaskId,
                int inputPartition,
                long inputOffset,
                String inputValue,
                ShareAckPayload shareAckPayload) {
            this.sourceSubtaskId = sourceSubtaskId;
            this.mapSubtaskId = mapSubtaskId;
            this.inputPartition = inputPartition;
            this.inputOffset = inputOffset;
            this.inputValue = inputValue;
            this.shareAckPayload = shareAckPayload;
        }

        private static ShareSourceRecord from(
                int sourceSubtaskId,
                ConsumerRecord<byte[], byte[]> record,
                ShareAckPayload shareAckPayload) {
            return new ShareSourceRecord(
                    sourceSubtaskId,
                    NO_CHECKPOINT,
                    record.partition(),
                    record.offset(),
                    new String(record.value(), StandardCharsets.UTF_8),
                    shareAckPayload);
        }

        private ShareSourceRecord withMapSubtaskId(int mapSubtaskId) {
            return new ShareSourceRecord(
                    sourceSubtaskId,
                    mapSubtaskId,
                    inputPartition,
                    inputOffset,
                    inputValue,
                    shareAckPayload);
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
