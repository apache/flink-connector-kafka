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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
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
public abstract class KafkaWriterTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaWriterTestBase.class);
    protected static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    protected static final Network NETWORK = Network.newNetwork();
    protected static final String KAFKA_METRIC_WITH_GROUP_NAME =
            "KafkaProducer.incoming-byte-total";
    protected static final SinkWriter.Context SINK_WRITER_CONTEXT = new DummySinkWriterContext();
    public static final String TEST_PREFIX = "test-prefix";
    protected static String topic;

    protected MetricListener metricListener;
    protected TriggerTimeService timeService;

    protected static final KafkaContainer KAFKA_CONTAINER =
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
    public void check() {
        checkProducerLeak();
    }

    KafkaWriter<Integer> createWriter(DeliveryGuarantee guarantee) throws IOException {
        return createWriter(guarantee, createInitContext());
    }

    KafkaWriter<Integer> createWriter(DeliveryGuarantee guarantee, SinkInitContext sinkInitContext)
            throws IOException {
        return createWriter(builder -> builder.setDeliveryGuarantee(guarantee), sinkInitContext);
    }

    KafkaWriter<Integer> createWriter(
            Consumer<KafkaSinkBuilder<?>> sinkBuilderAdjuster, SinkInitContext sinkInitContext)
            throws IOException {
        return (KafkaWriter<Integer>) createSink(sinkBuilderAdjuster).createWriter(sinkInitContext);
    }

    KafkaSink<Integer> createSink(Consumer<KafkaSinkBuilder<?>> sinkBuilderAdjuster) {
        KafkaSinkBuilder<Integer> builder =
                KafkaSink.<Integer>builder()
                        .setKafkaProducerConfig(getKafkaClientConfiguration())
                        .setTransactionalIdPrefix(TEST_PREFIX)
                        .setRecordSerializer(new DummyRecordSerializer());
        sinkBuilderAdjuster.accept(builder);
        return builder.build();
    }

    SinkInitContext createInitContext() {
        return new SinkInitContext(createSinkWriterMetricGroup(), timeService, null);
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
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
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
    }

    /** mock recordSerializer for KafkaSink. */
    protected static class DummyRecordSerializer
            implements KafkaRecordSerializationSchema<Integer> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Integer element, KafkaSinkContext context, Long timestamp) {
            if (element == null) {
                // in general, serializers should be allowed to skip invalid elements
                return null;
            }
            return new ProducerRecord<>(topic, ByteBuffer.allocate(4).putInt(element).array());
        }
    }

    /**
     * mock context for KafkaWriter#write(java.lang.Object,
     * org.apache.flink.api.connector.sink2.SinkWriter.Context).
     */
    protected static class DummySinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
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
            return 0;
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
