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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaShareSourceReaderTest {
    @Test
    void testAcquisitionIsBoundedAndSnapshotContainsSubscriptionNotOffsets() throws Exception {
        final FakeClient client = new FakeClient();
        client.records.add(record(0));
        client.records.add(record(1));
        try (KafkaShareSourceReader<String> reader = reader(client)) {
            reader.start();
            reader.addSplits(List.of(new KafkaShareSource.SubscriptionSplit(0)));
            reader.addSplits(List.of(new KafkaShareSource.SubscriptionSplit(0)));
            reader.isAvailable().get(5, TimeUnit.SECONDS);
            assertThat(client.polls.get()).isEqualTo(1);
            assertThat(reader.snapshotState(1))
                    .extracting(KafkaShareSource.SubscriptionSplit::splitId)
                    .containsExactly("share-subscription-0");
            final TestingReaderOutput<KafkaShareRecord<String>> output =
                    new TestingReaderOutput<>();
            assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
            assertThat(output.getEmittedRecords()).hasSize(1);
            assertThat(output.getEmittedRecords().get(0).value).isEqualTo("event-0");
            assertThat(output.getEmittedRecords().get(0).acknowledgement.getId())
                    .isEqualTo("payload-0");
            reader.isAvailable().get(5, TimeUnit.SECONDS);
            reader.pollNext(output);
            assertThat(output.getEmittedRecords()).hasSize(2);
        }
        assertThat(client.closed.get()).isEqualTo(1);
    }

    @Test
    void testFetcherFailureWakesMailboxAndIsNotSwallowed() throws Exception {
        final FakeClient client = new FakeClient();
        client.failure = new IOException("broker unavailable");
        try (KafkaShareSourceReader<String> reader = reader(client)) {
            reader.start();
            reader.addSplits(List.of(new KafkaShareSource.SubscriptionSplit(0)));
            reader.isAvailable().get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> reader.pollNext(new TestingReaderOutput<>()))
                    .isInstanceOf(IOException.class)
                    .hasCause(client.failure);
        }
    }

    @Test
    void testEmptyDeserializerDoesNotEmitAnAcknowledgement() throws Exception {
        final FakeClient client = new FakeClient();
        client.records.add(record(0));
        final KafkaRecordDeserializationSchema<String> empty =
                new KafkaRecordDeserializationSchema<String>() {
                    @Override
                    public void deserialize(
                            ConsumerRecord<byte[], byte[]> record,
                            org.apache.flink.util.Collector<String> output) {}

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                };
        try (KafkaShareSourceReader<String> reader =
                new KafkaShareSourceReader<>(new TestingReaderContext(), empty, () -> client)) {
            reader.start();
            reader.addSplits(List.of(new KafkaShareSource.SubscriptionSplit(0)));
            reader.isAvailable().get(5, TimeUnit.SECONDS);
            final TestingReaderOutput<KafkaShareRecord<String>> output =
                    new TestingReaderOutput<>();
            assertThatThrownBy(() -> reader.pollNext(output)).isInstanceOf(IOException.class);
            assertThat(output.getEmittedRecords()).isEmpty();
        }
    }

    @Test
    void testSubscriptionAndEnumeratorSerializersRejectUnknownVersions() throws Exception {
        final KafkaShareSource<String> source =
                new KafkaShareSource<>(
                        new Properties(),
                        List.of("source"),
                        KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()),
                        TypeInformation.of(new TypeHint<KafkaShareRecord<String>>() {}));
        final var serializer = source.getSplitSerializer();
        final var split = new KafkaShareSource.SubscriptionSplit(7);
        assertThat(serializer.deserialize(1, serializer.serialize(split)).splitId())
                .isEqualTo(split.splitId());
        assertThatThrownBy(() -> serializer.deserialize(2, new byte[4]))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(1, new byte[1]))
                .isInstanceOf(IOException.class);
        final var enumSerializer = source.getEnumeratorCheckpointSerializer();
        assertThat(enumSerializer.deserialize(1, enumSerializer.serialize(1))).isEqualTo(1);
        assertThatThrownBy(() -> enumSerializer.deserialize(2, new byte[] {1}))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> enumSerializer.deserialize(1, new byte[] {2}))
                .isInstanceOf(IOException.class);
    }

    private KafkaShareSourceReader<String> reader(FakeClient client) {
        return new KafkaShareSourceReader<>(
                new TestingReaderContext(),
                KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()),
                () -> client);
    }

    private static KafkaShareSourceReader.AcquiredRecord record(long offset) {
        return new KafkaShareSourceReader.AcquiredRecord(
                new ConsumerRecord<>(
                        "source",
                        0,
                        offset,
                        null,
                        ("event-" + offset).getBytes(StandardCharsets.UTF_8)),
                new ShareAckPayload(
                        "payload-" + offset,
                        "group",
                        "member",
                        1,
                        List.of(
                                new ShareAckPayload.TopicPartitionAcknowledgements(
                                        "topic-id",
                                        "source",
                                        0,
                                        List.of(
                                                new ShareAckPayload.AcknowledgementBatch(
                                                        offset, offset, List.of((byte) 1)))))));
    }

    private static final class FakeClient implements KafkaShareSourceReader.Client {
        final LinkedBlockingQueue<KafkaShareSourceReader.AcquiredRecord> records =
                new LinkedBlockingQueue<>();
        final AtomicInteger polls = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        IOException failure;

        @Override
        public KafkaShareSourceReader.AcquiredRecord poll(Duration timeout) throws Exception {
            if (failure != null) {
                throw failure;
            }
            polls.incrementAndGet();
            return records.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }
}
