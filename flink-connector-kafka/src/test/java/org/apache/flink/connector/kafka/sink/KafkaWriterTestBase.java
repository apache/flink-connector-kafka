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

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.kafka.sink.internal.BackchannelFactory;
import org.apache.flink.connector.kafka.sink.internal.TransactionFinished;
import org.apache.flink.connector.kafka.sink.internal.WritableBackchannel;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.checkProducerLeak;
import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

/** Test base for KafkaWriter. */
@Testcontainers
public abstract class KafkaWriterTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaWriterTestBase.class);
    protected static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    protected static final Network NETWORK = Network.newNetwork();
    protected static final String KAFKA_METRIC_WITH_GROUP_NAME =
            "KafkaProducer.incoming-byte-total";
    protected static final SinkWriter.Context SINK_WRITER_CONTEXT = new DummySinkWriterContext();
    private static final String TEST_PREFIX = "test-prefix";
    private int writerIndex;
    private static final int SUB_ID = 0;
    private static final int ATTEMPT = 0;
    protected static String topic;

    protected MetricListener metricListener;
    protected TriggerTimeService timeService;

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KafkaWriterTestBase.class)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        metricListener = new MetricListener();
        timeService = new TriggerTimeService();
        topic = testInfo.getDisplayName().replaceAll("\\W", "");
        Map<String, Object> properties = new java.util.HashMap<>();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        try (Admin admin = AdminClient.create(properties)) {
            admin.createTopics(Collections.singleton(new NewTopic(topic, 10, (short) 1)));
        }
    }

    @AfterEach
    public void teardown() {
        checkProducerLeak();
    }

    <T extends KafkaWriter<?>> T createWriter(DeliveryGuarantee guarantee) throws IOException {
        return createWriter(guarantee, createInitContext());
    }

    <T extends KafkaWriter<?>> T createWriter(
            DeliveryGuarantee guarantee, SinkInitContext sinkInitContext) throws IOException {
        return createWriter(builder -> builder.setDeliveryGuarantee(guarantee), sinkInitContext);
    }

    @SuppressWarnings("unchecked")
    <T extends KafkaWriter<?>> T createWriter(
            Consumer<KafkaSinkBuilder<?>> sinkBuilderAdjuster, SinkInitContext sinkInitContext)
            throws IOException {
        return (T) createSink(sinkBuilderAdjuster).createWriter(sinkInitContext);
    }

    @SuppressWarnings("unchecked")
    <T extends KafkaWriter<?>> T restoreWriter(
            Consumer<KafkaSinkBuilder<?>> sinkBuilderAdjuster,
            Collection<KafkaWriterState> recoveredState,
            SinkInitContext initContext) {
        return (T) createSink(sinkBuilderAdjuster).restoreWriter(initContext, recoveredState);
    }

    public String getTransactionalPrefix() {
        return TEST_PREFIX + writerIndex;
    }

    KafkaSink<Integer> createSink(Consumer<KafkaSinkBuilder<?>> sinkBuilderAdjuster) {
        KafkaSinkBuilder<Integer> builder =
                KafkaSink.<Integer>builder()
                        .setKafkaProducerConfig(getKafkaClientConfiguration())
                        .setTransactionalIdPrefix(TEST_PREFIX + writerIndex++)
                        .setRecordSerializer(new IntegerRecordSerializer(topic));
        sinkBuilderAdjuster.accept(builder);
        return builder.build();
    }

    SinkInitContext createInitContext() {
        return new SinkInitContext(createSinkWriterMetricGroup(), timeService, null);
    }

    WritableBackchannel<TransactionFinished> getBackchannel(ExactlyOnceKafkaWriter<?> writer) {
        return BackchannelFactory.getInstance()
                .getWritableBackchannel(SUB_ID, ATTEMPT, writer.getTransactionalIdPrefix());
    }

    protected SinkWriterMetricGroup createSinkWriterMetricGroup() {
        DummyOperatorMetricGroup operatorMetricGroup =
                new DummyOperatorMetricGroup(metricListener.getMetricGroup());
        return InternalSinkWriterMetricGroup.wrap(operatorMetricGroup);
    }

    protected Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "kafkaWriter-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        return standardProps;
    }

    /** mock sink context for initializing KafkaWriter. */
    protected static class SinkInitContext extends TestSinkInitContext {

        protected final SinkWriterMetricGroup metricGroup;
        protected final ProcessingTimeService timeService;
        @Nullable protected final Consumer<RecordMetadata> metadataConsumer;
        private Long checkpointId;

        SinkInitContext(
                SinkWriterMetricGroup metricGroup,
                ProcessingTimeService timeService,
                @Nullable Consumer<RecordMetadata> metadataConsumer) {
            this.metricGroup = metricGroup;
            this.timeService = timeService;
            this.metadataConsumer = metadataConsumer;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return null;
        }

        @Override
        public <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
            return Optional.ofNullable((Consumer<MetaT>) metadataConsumer);
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return checkpointId == null ? OptionalLong.empty() : OptionalLong.of(checkpointId);
        }

        public void setRestoredCheckpointId(long checkpointId) {
            this.checkpointId = checkpointId;
        }
    }

    /**
     * mock context for KafkaWriter#write(java.lang.Object,
     * org.apache.flink.api.connector.sink2.SinkWriter.Context).
     */
    protected static class DummySinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return ATTEMPT;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    /** mock metrics group for initializing KafkaWriter. */
    protected static class DummyOperatorMetricGroup extends ProxyMetricGroup<MetricGroup>
            implements OperatorMetricGroup {

        private final OperatorIOMetricGroup operatorIOMetricGroup;

        public DummyOperatorMetricGroup(MetricGroup parentMetricGroup) {
            super(parentMetricGroup);
            this.operatorIOMetricGroup =
                    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()
                            .getIOMetricGroup();
        }

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            return operatorIOMetricGroup;
        }
    }

    /** mock time service for KafkaWriter. */
    protected static class TriggerTimeService implements ProcessingTimeService {

        private final PriorityQueue<Tuple2<Long, ProcessingTimeCallback>> registeredCallbacks =
                new PriorityQueue<>(Comparator.comparingLong(o -> o.f0));

        @Override
        public long getCurrentProcessingTime() {
            return ATTEMPT;
        }

        @Override
        public ScheduledFuture<?> registerTimer(
                long time, ProcessingTimeCallback processingTimerCallback) {
            registeredCallbacks.add(new Tuple2<>(time, processingTimerCallback));
            return null;
        }

        public void trigger() throws Exception {
            final Tuple2<Long, ProcessingTimeCallback> registered = registeredCallbacks.poll();
            if (registered == null) {
                LOG.warn("Triggered time service but no callback was registered.");
                return;
            }
            registered.f1.onProcessingTime(registered.f0);
        }
    }
}
