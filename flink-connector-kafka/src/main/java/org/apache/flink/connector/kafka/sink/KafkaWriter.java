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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.MetricUtil;
import org.apache.flink.connector.kafka.sink.internal.FlinkKafkaInternalProducer;
import org.apache.flink.connector.kafka.sink.internal.KafkaCommitter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricMutableWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible to write records in a Kafka topic without transactions.
 *
 * @param <IN> The type of the input elements.
 */
class KafkaWriter<IN>
        implements TwoPhaseCommittingStatefulSink.PrecommittingStatefulSinkWriter<
                IN, KafkaWriterState, KafkaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);
    private static final String KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";
    private static final String KAFKA_PRODUCER_METRICS = "producer-metrics";

    private final DeliveryGuarantee deliveryGuarantee;
    protected final Properties kafkaProducerConfig;
    protected final KafkaRecordSerializationSchema<IN> recordSerializer;
    protected final Callback deliveryCallback;
    protected final KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext;
    private volatile Exception asyncProducerException;
    private final Map<String, KafkaMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();
    private final SinkWriterMetricGroup metricGroup;
    private final boolean disabledMetrics;
    private final Counter numRecordsOutCounter;
    private final Counter numBytesOutCounter;
    private final Counter numRecordsOutErrorsCounter;
    private final ProcessingTimeService timeService;

    // Number of outgoing bytes at the latest metric sync
    private long latestOutgoingByteTotal;
    private Metric byteOutMetric;
    protected FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;

    private volatile boolean closed = false;
    private long lastSync = System.currentTimeMillis();

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext,
     * KafkaRecordSerializationSchema.KafkaSinkContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     */
    KafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            WriterInitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        this.kafkaProducerConfig = checkNotNull(kafkaProducerConfig, "kafkaProducerConfig");
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        checkNotNull(sinkInitContext, "sinkInitContext");
        this.deliveryCallback =
                new WriterCallback(
                        sinkInitContext.getMailboxExecutor(),
                        sinkInitContext.<RecordMetadata>metadataConsumer().orElse(null));
        this.disabledMetrics =
                kafkaProducerConfig.containsKey(KEY_DISABLE_METRICS)
                                && Boolean.parseBoolean(
                                        kafkaProducerConfig.get(KEY_DISABLE_METRICS).toString())
                        || kafkaProducerConfig.containsKey(KEY_REGISTER_METRICS)
                                && !Boolean.parseBoolean(
                                        kafkaProducerConfig.get(KEY_REGISTER_METRICS).toString());
        this.timeService = sinkInitContext.getProcessingTimeService();
        this.metricGroup = sinkInitContext.metricGroup();
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter = metricGroup.getIOMetricGroup().getNumRecordsOutCounter();
        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.kafkaSinkContext =
                new DefaultKafkaSinkContext(
                        sinkInitContext.getTaskInfo().getIndexOfThisSubtask(),
                        sinkInitContext.getTaskInfo().getNumberOfParallelSubtasks(),
                        kafkaProducerConfig);
        try {
            recordSerializer.open(schemaContext, kafkaSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        initFlinkMetrics();
    }

    public void initialize() {
        // Workaround for FLINK-37612: ensure that we are not leaking producers
        try {
            this.currentProducer = new FlinkKafkaInternalProducer<>(this.kafkaProducerConfig);
            initKafkaMetrics(this.currentProducer);
        } catch (Throwable t) {
            try {
                close();
            } catch (Exception e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    @Override
    public void write(@Nullable IN element, Context context) throws IOException {
        checkAsyncException();
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        if (record != null) {
            currentProducer.send(record, deliveryCallback);
            numRecordsOutCounter.inc();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.NONE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
            currentProducer.flush();
        }

        checkAsyncException();
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() {
        return Collections.emptyList();
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        LOG.debug("Closing writer with {}", currentProducer);
        closeAll(currentProducer);
        checkState(
                currentProducer.isClosed(), "Could not close current producer " + currentProducer);
        currentProducer = null;

        // Rethrow exception for the case in which close is called before writer() and flush().
        checkAsyncException();
    }

    @VisibleForTesting
    FlinkKafkaInternalProducer<byte[], byte[]> getCurrentProducer() {
        return currentProducer;
    }

    protected void initFlinkMetrics() {
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
        registerMetricSync();
    }

    protected void initKafkaMetrics(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        byteOutMetric =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "outgoing-byte-total");
        if (disabledMetrics) {
            return;
        }
        final MetricGroup kafkaMetricGroup = metricGroup.addGroup(KAFKA_PRODUCER_METRIC_NAME);
        producer.metrics().entrySet().forEach(initMetric(kafkaMetricGroup));
    }

    private Consumer<Map.Entry<MetricName, ? extends Metric>> initMetric(
            MetricGroup kafkaMetricGroup) {
        return (entry) -> {
            final String name = entry.getKey().name();
            final Metric metric = entry.getValue();
            if (previouslyCreatedMetrics.containsKey(name)) {
                final KafkaMetricMutableWrapper wrapper = previouslyCreatedMetrics.get(name);
                wrapper.setKafkaMetric(metric);
            } else {
                final KafkaMetricMutableWrapper wrapper = new KafkaMetricMutableWrapper(metric);
                previouslyCreatedMetrics.put(name, wrapper);
                kafkaMetricGroup.gauge(name, wrapper);
            }
        };
    }

    private long computeSendTime() {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = this.currentProducer;
        if (producer == null) {
            return -1L;
        }
        final Metric sendTime =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "request-latency-avg");
        final Metric queueTime =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "record-queue-time-avg");
        return ((Number) sendTime.metricValue()).longValue()
                + ((Number) queueTime.metricValue()).longValue();
    }

    private void registerMetricSync() {
        timeService.registerTimer(
                lastSync + METRIC_UPDATE_INTERVAL_MILLIS,
                (time) -> {
                    if (closed) {
                        return;
                    }
                    long outgoingBytesUntilNow = ((Number) byteOutMetric.metricValue()).longValue();
                    long outgoingBytesSinceLastUpdate =
                            outgoingBytesUntilNow - latestOutgoingByteTotal;
                    numBytesOutCounter.inc(outgoingBytesSinceLastUpdate);
                    latestOutgoingByteTotal = outgoingBytesUntilNow;
                    lastSync = time;
                    registerMetricSync();
                });
    }

    /**
     * This method should only be invoked in the mailbox thread since the counter is not volatile.
     * Logic needs to be invoked by write AND flush since we support various semantics.
     */
    private void checkAsyncException() throws IOException {
        // reset this exception since we could close the writer later on
        Exception e = asyncProducerException;
        if (e != null) {

            asyncProducerException = null;
            numRecordsOutErrorsCounter.inc();
            throw new IOException(
                    "One or more Kafka Producer send requests have encountered exception", e);
        }
    }

    private class WriterCallback implements Callback {
        private final MailboxExecutor mailboxExecutor;
        @Nullable private final Consumer<RecordMetadata> metadataConsumer;

        public WriterCallback(
                MailboxExecutor mailboxExecutor,
                @Nullable Consumer<RecordMetadata> metadataConsumer) {
            this.mailboxExecutor = mailboxExecutor;
            this.metadataConsumer = metadataConsumer;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                if (closed) {
                    LOG.debug(
                            "Completed exceptionally, but shutdown was already initiated",
                            exception);
                    return;
                }
                FlinkKafkaInternalProducer<byte[], byte[]> producer =
                        KafkaWriter.this.currentProducer;

                // Propagate the first exception since amount of exceptions could be large. Need to
                // do this in Producer IO thread since flush() guarantees that the future will
                // complete. The same guarantee does not hold for tasks executed in separate
                // executor e.g. mailbox executor. flush() needs to have the exception immediately
                // available to fail the checkpoint.
                if (asyncProducerException == null) {
                    asyncProducerException = decorateException(metadata, exception, producer);
                }

                // Checking for exceptions from previous writes
                // Notice: throwing exception in mailboxExecutor thread is not safe enough for
                // triggering global fail over, which has been fixed in [FLINK-31305].
                mailboxExecutor.execute(
                        () -> {
                            // Checking for exceptions from previous writes
                            checkAsyncException();
                        },
                        "Update error metric");
            }

            if (metadataConsumer != null) {
                metadataConsumer.accept(metadata);
            }
        }

        private FlinkRuntimeException decorateException(
                RecordMetadata metadata,
                Exception exception,
                FlinkKafkaInternalProducer<byte[], byte[]> producer) {
            String message =
                    String.format("Failed to send data to Kafka %s with %s ", metadata, producer);
            if (exception instanceof UnknownProducerIdException) {
                message += KafkaCommitter.UNKNOWN_PRODUCER_ID_ERROR_MESSAGE;
            }
            return new FlinkRuntimeException(message, exception);
        }
    }
}
