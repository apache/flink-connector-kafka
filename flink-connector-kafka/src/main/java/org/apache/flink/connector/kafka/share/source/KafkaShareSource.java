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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/** Experimental stateless map-only source; durable share state owns replay, not offsets. */
@Experimental
public final class KafkaShareSource<T>
        implements Source<KafkaShareRecord<T>, KafkaShareSource.SubscriptionSplit, Integer>,
                ResultTypeQueryable<KafkaShareRecord<T>> {
    private static final long serialVersionUID = 1L;
    private final Properties properties;
    private final List<String> topics;
    private final KafkaRecordDeserializationSchema<T> deserializer;
    private final TypeInformation<KafkaShareRecord<T>> type;

    public KafkaShareSource(
            Properties properties,
            List<String> topics,
            KafkaRecordDeserializationSchema<T> deserializer,
            TypeInformation<KafkaShareRecord<T>> type) {
        this.properties = new Properties();
        this.properties.putAll(properties);
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }
        this.topics = List.copyOf(topics);
        this.deserializer = Objects.requireNonNull(deserializer);
        this.type = Objects.requireNonNull(type);
        this.properties.setProperty("share.acknowledgement.mode", "explicit");
        this.properties.setProperty("share.acquire.mode", "record_limit");
        this.properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<KafkaShareRecord<T>, SubscriptionSplit> createReader(
            SourceReaderContext context) {
        return new KafkaShareSourceReader<>(
                context,
                deserializer,
                () -> new KafkaShareSourceReader.KafkaClient(properties, topics));
    }

    @Override
    public SplitEnumerator<SubscriptionSplit, Integer> createEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context) {
        return new Enumerator(context);
    }

    @Override
    public SplitEnumerator<SubscriptionSplit, Integer> restoreEnumerator(
            SplitEnumeratorContext<SubscriptionSplit> context, Integer state) throws IOException {
        if (state != 1) {
            throw new IOException("Unsupported share source checkpoint");
        }
        return new Enumerator(context);
    }

    @Override
    public SimpleVersionedSerializer<SubscriptionSplit> getSplitSerializer() {
        return new SimpleVersionedSerializer<SubscriptionSplit>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(SubscriptionSplit split) {
                return ByteBuffer.allocate(4).putInt(split.readerId).array();
            }

            @Override
            public SubscriptionSplit deserialize(int version, byte[] bytes) throws IOException {
                if (version != 1 || bytes.length != 4) {
                    throw new IOException("Unsupported subscription split");
                }
                return new SubscriptionSplit(ByteBuffer.wrap(bytes).getInt());
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
        return new SimpleVersionedSerializer<Integer>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(Integer state) throws IOException {
                if (state != 1) {
                    throw new IOException("Unsupported share source checkpoint");
                }
                return new byte[] {1};
            }

            @Override
            public Integer deserialize(int version, byte[] bytes) throws IOException {
                validateState(version, bytes);
                return 1;
            }
        };
    }

    private static void validateState(int version, byte[] bytes) throws IOException {
        if (version != 1 || bytes.length != 1 || bytes[0] != 1) {
            throw new IOException("Unsupported share source checkpoint");
        }
    }

    @Override
    public TypeInformation<KafkaShareRecord<T>> getProducedType() {
        return type;
    }

    @Experimental
    public static final class SubscriptionSplit implements SourceSplit {
        private final int readerId;

        public SubscriptionSplit(int readerId) {
            this.readerId = readerId;
        }

        @Override
        public String splitId() {
            return "share-subscription-" + readerId;
        }
    }

    private static final class Enumerator implements SplitEnumerator<SubscriptionSplit, Integer> {
        private final SplitEnumeratorContext<SubscriptionSplit> context;

        private Enumerator(SplitEnumeratorContext<SubscriptionSplit> context) {
            this.context = context;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtask, String hostname) {
            context.assignSplit(new SubscriptionSplit(subtask), subtask);
        }

        @Override
        public void addReader(int subtask) {
            context.assignSplit(new SubscriptionSplit(subtask), subtask);
        }

        @Override
        public void addSplitsBack(List<SubscriptionSplit> splits, int subtask) {}

        @Override
        public Integer snapshotState(long checkpointId) {
            return 1;
        }

        @Override
        public void close() {}
    }
}
