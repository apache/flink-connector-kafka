/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.share.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

final class KafkaShareSourceReader<T>
        implements SourceReader<KafkaShareRecord<T>, KafkaShareSource.SubscriptionSplit> {
    private final SourceReaderContext context;
    private final KafkaRecordDeserializationSchema<T> deserializer;
    private final Supplier<Client> factory;
    private final Object monitor = new Object();
    private CompletableFuture<Void> available = new CompletableFuture<>();
    private AcquiredRecord queued;
    private Throwable error;
    private boolean subscribed;
    private volatile boolean running = true;
    private volatile Client client;
    private Thread worker;

    KafkaShareSourceReader(
            SourceReaderContext context,
            KafkaRecordDeserializationSchema<T> deserializer,
            Supplier<Client> factory) {
        this.context = context;
        this.deserializer = deserializer;
        this.factory = factory;
    }

    @Override
    public void start() {
        try {
            deserializer.open(
                    new DeserializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return context.metricGroup().addGroup("deserializer");
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return context.getUserCodeClassLoader();
                        }
                    });
        } catch (Exception e) {
            throw new IllegalStateException("Cannot open share deserializer", e);
        }
        context.metricGroup()
                .addGroup("KafkaShareSourceReader")
                .gauge(
                        "prefetchedRecords",
                        () -> {
                            synchronized (monitor) {
                                return queued == null ? 0 : 1;
                            }
                        });
    }

    @Override
    public InputStatus pollNext(ReaderOutput<KafkaShareRecord<T>> output) throws Exception {
        final AcquiredRecord record;
        synchronized (monitor) {
            if (error != null) {
                throw new IOException("Share fetcher failed", error);
            }
            record = queued;
            if (record == null) {
                return InputStatus.NOTHING_AVAILABLE;
            }
            queued = null;
            available = new CompletableFuture<>();
            monitor.notifyAll();
        }
        final List<T> values = new ArrayList<>(1);
        deserializer.deserialize(
                record.record,
                new Collector<T>() {
                    @Override
                    public void collect(T value) {
                        values.add(value);
                    }

                    @Override
                    public void close() {}
                });
        if (values.size() != 1 || values.get(0) == null) {
            throw new IOException(
                    "Share source requires exactly one deserialized output per input");
        }
        output.collect(
                new KafkaShareRecord<>(values.get(0), record.payload, record.record.timestamp()),
                record.record.timestamp());
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        synchronized (monitor) {
            return available;
        }
    }

    @Override
    public void addSplits(List<KafkaShareSource.SubscriptionSplit> splits) {
        synchronized (monitor) {
            if (splits.isEmpty() || subscribed) {
                return;
            }
            subscribed = true;
            worker = new Thread(this::fetch, "kafka-share-fetcher-" + context.getIndexOfSubtask());
            worker.setDaemon(true);
            worker.start();
        }
    }

    private void fetch() {
        try (Client created = factory.get()) {
            client = created;
            while (running) {
                synchronized (monitor) {
                    while (running && queued != null) {
                        monitor.wait();
                    }
                }
                if (!running) {
                    break;
                }
                AcquiredRecord record = created.poll(Duration.ofMillis(200));
                if (record == null) {
                    continue;
                }
                final CompletableFuture<Void> signal;
                synchronized (monitor) {
                    queued = record;
                    signal = available;
                }
                signal.complete(null);
            }
        } catch (Throwable t) {
            if (running) {
                final CompletableFuture<Void> signal;
                synchronized (monitor) {
                    error = t;
                    signal = available;
                }
                signal.complete(null);
            }
        } finally {
            client = null;
        }
    }

    @Override
    public List<KafkaShareSource.SubscriptionSplit> snapshotState(long checkpointId) {
        return subscribed
                ? Collections.singletonList(
                        new KafkaShareSource.SubscriptionSplit(context.getIndexOfSubtask()))
                : Collections.emptyList();
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {
        running = false;
        Client current = client;
        if (current != null) {
            current.wakeup();
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
        if (worker != null) {
            worker.interrupt();
            worker.join(10000);
            if (worker.isAlive()) {
                throw new IOException("Share fetcher did not close");
            }
        }
    }

    interface Client extends AutoCloseable {
        AcquiredRecord poll(Duration timeout) throws Exception;

        void wakeup();

        @Override
        void close();
    }

    static final class AcquiredRecord {
        final ConsumerRecord<byte[], byte[]> record;
        final ShareAckPayload payload;

        AcquiredRecord(ConsumerRecord<byte[], byte[]> record, ShareAckPayload payload) {
            this.record = record;
            this.payload = payload;
        }
    }

    static final class KafkaClient implements Client {
        private final KafkaShareConsumer<byte[], byte[]> consumer;
        private final Method drain;
        private final Method metadata;
        private final String incarnation = UUID.randomUUID().toString();
        private long sequence;

        KafkaClient(Properties properties, List<String> topics) {
            try {
                drain = KafkaShareConsumer.class.getMethod("acknowledgementsForTransaction");
                metadata = KafkaShareConsumer.class.getMethod("shareGroupMetadata");
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(
                        "Kafka client must support transactional share acknowledgements", e);
            }
            consumer =
                    new KafkaShareConsumer<>(
                            properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            consumer.subscribe(topics);
        }

        @Override
        public AcquiredRecord poll(Duration timeout) throws Exception {
            final var records = consumer.poll(timeout);
            if (records.isEmpty()) {
                return null;
            }
            if (records.count() != 1) {
                throw new IOException("Expected record-limit acquisition of one record");
            }
            final ConsumerRecord<byte[], byte[]> record = records.iterator().next();
            consumer.acknowledge(record, AcknowledgeType.ACCEPT);
            final ShareAckPayload payload =
                    ShareAckPayload.fromKafkaObjects(
                            incarnation + "-" + sequence++,
                            drain.invoke(consumer),
                            metadata.invoke(consumer));
            return new AcquiredRecord(record, payload);
        }

        @Override
        public void wakeup() {
            consumer.wakeup();
        }

        @Override
        public void close() {
            Thread.interrupted();
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
