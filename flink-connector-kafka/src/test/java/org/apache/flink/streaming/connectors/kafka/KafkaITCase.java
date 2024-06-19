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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** IT cases for Kafka. */
class KafkaITCase extends KafkaConsumerTestBase {

    @BeforeAll
    protected static void prepare() throws Exception {
        KafkaProducerTestBase.prepare();
        ((KafkaTestEnvironmentImpl) kafkaServer)
                .setProducerSemantic(FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
    }

    // ------------------------------------------------------------------------
    //  Suite of Tests
    // ------------------------------------------------------------------------

    @Test
    @Timeout(value = 120L, unit = TimeUnit.SECONDS)
    void testFailOnNoBroker() throws Exception {
        runFailOnNoBrokerTest();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testConcurrentProducerConsumerTopology() throws Exception {
        runSimpleConcurrentProducerConsumerTopology();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testKeyValueSupport() throws Exception {
        runKeyValueTest();
    }

    // --- canceling / failures ---

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testCancelingEmptyTopic() throws Exception {
        runCancelingOnEmptyInputTest();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testCancelingFullTopic() throws Exception {
        runCancelingOnFullInputTest();
    }

    // --- source to partition mappings and exactly once ---

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testOneToOneSources() throws Exception {
        runOneToOneExactlyOnceTest();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testOneSourceMultiplePartitions() throws Exception {
        runOneSourceMultiplePartitionsExactlyOnceTest();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testMultipleSourcesOnePartition() throws Exception {
        runMultipleSourcesOnePartitionExactlyOnceTest();
    }

    // --- broker failure ---

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testBrokerFailure() throws Exception {
        runBrokerFailureTest();
    }

    // --- special executions ---

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testBigRecordJob() throws Exception {
        runBigRecordTestTopology();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testMultipleTopicsWithLegacySerializer() throws Exception {
        runProduceConsumeMultipleTopics(true);
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testMultipleTopicsWithKafkaSerializer() throws Exception {
        runProduceConsumeMultipleTopics(false);
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testAllDeletes() throws Exception {
        runAllDeletesTest();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testMetricsAndEndOfStream() throws Exception {
        runEndOfStreamTest();
    }

    // --- startup mode ---

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testStartFromEarliestOffsets() throws Exception {
        runStartFromEarliestOffsets();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testStartFromLatestOffsets() throws Exception {
        runStartFromLatestOffsets();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testStartFromGroupOffsets() throws Exception {
        runStartFromGroupOffsets();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testStartFromSpecificOffsets() throws Exception {
        runStartFromSpecificOffsets();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testStartFromTimestamp() throws Exception {
        runStartFromTimestamp();
    }

    // --- offset committing ---

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testCommitOffsetsToKafka() throws Exception {
        runCommitOffsetsToKafka();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testAutoOffsetRetrievalAndCommitToKafka() throws Exception {
        runAutoOffsetRetrievalAndCommitToKafka();
    }

    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testCollectingSchema() throws Exception {
        runCollectingSchemaTest();
    }

    /** Kafka 20 specific test, ensuring Timestamps are properly written to and read from Kafka. */
    @Test
    @Timeout(value = 60L, unit = TimeUnit.SECONDS)
    void testTimestamps() throws Exception {

        final String topic = "tstopic-" + UUID.randomUUID();
        createTestTopic(topic, 3, 1);

        // ---------- Produce an event time stream into Kafka -------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        DataStream<Long> streamWithTimestamps =
                env.addSource(
                        new SourceFunction<Long>() {
                            private static final long serialVersionUID = -2255115836471289626L;
                            boolean running = true;

                            @Override
                            public void run(SourceContext<Long> ctx) {
                                long i = 0;
                                while (running) {
                                    ctx.collectWithTimestamp(i, i * 2);
                                    if (i++ == 1110L) {
                                        running = false;
                                    }
                                }
                            }

                            @Override
                            public void cancel() {
                                running = false;
                            }
                        });

        final TypeInformationSerializationSchema<Long> longSer =
                new TypeInformationSerializationSchema<>(Types.LONG, env.getConfig());
        FlinkKafkaProducer<Long> prod =
                new FlinkKafkaProducer<>(
                        topic,
                        new KeyedSerializationSchemaWrapper<>(longSer),
                        standardProps,
                        Optional.of(
                                new FlinkKafkaPartitioner<Long>() {
                                    private static final long serialVersionUID =
                                            -6730989584364230617L;

                                    @Override
                                    public int partition(
                                            Long next,
                                            byte[] key,
                                            byte[] value,
                                            String targetTopic,
                                            int[] partitions) {
                                        return (int) (next % 3);
                                    }
                                }));
        prod.setWriteTimestampToKafka(true);

        streamWithTimestamps.addSink(prod).setParallelism(3);

        env.execute("Produce some");

        // ---------- Consume stream from Kafka -------------------

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        FlinkKafkaConsumer<Long> kafkaSource =
                new FlinkKafkaConsumer<>(
                        topic, new KafkaITCase.LimitedLongDeserializer(), standardProps);
        kafkaSource.assignTimestampsAndWatermarks(
                new AssignerWithPunctuatedWatermarks<Long>() {
                    private static final long serialVersionUID = -4834111173247835189L;

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(
                            Long lastElement, long extractedTimestamp) {
                        if (lastElement % 11 == 0) {
                            return new Watermark(lastElement);
                        }
                        return null;
                    }

                    @Override
                    public long extractTimestamp(Long element, long previousElementTimestamp) {
                        return previousElementTimestamp;
                    }
                });

        DataStream<Long> stream = env.addSource(kafkaSource);
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        stream.transform(
                        "timestamp validating operator",
                        objectTypeInfo,
                        new TimestampValidatingOperator())
                .setParallelism(1);

        env.execute("Consume again");

        deleteTestTopic(topic);
    }

    private static class TimestampValidatingOperator extends StreamSink<Long> {

        private static final long serialVersionUID = 1353168781235526806L;

        public TimestampValidatingOperator() {
            super(
                    new SinkFunction<Long>() {
                        private static final long serialVersionUID = -6676565693361786524L;

                        @Override
                        public void invoke(Long value) throws Exception {
                            throw new RuntimeException("Unexpected");
                        }
                    });
        }

        long elCount = 0;
        long wmCount = 0;
        long lastWM = Long.MIN_VALUE;

        @Override
        public void processElement(StreamRecord<Long> element) throws Exception {
            elCount++;
            if (element.getValue() * 2 != element.getTimestamp()) {
                throw new RuntimeException("Invalid timestamp: " + element);
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            wmCount++;

            if (lastWM <= mark.getTimestamp()) {
                lastWM = mark.getTimestamp();
            } else {
                throw new RuntimeException("Received watermark higher than the last one");
            }

            if (mark.getTimestamp() % 11 != 0 && mark.getTimestamp() != Long.MAX_VALUE) {
                throw new RuntimeException("Invalid watermark: " + mark.getTimestamp());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (elCount != 1110L) {
                throw new RuntimeException("Wrong final element count " + elCount);
            }

            if (wmCount <= 2) {
                throw new RuntimeException("Almost no watermarks have been sent " + wmCount);
            }
        }
    }

    private static class LimitedLongDeserializer implements KafkaDeserializationSchema<Long> {

        private static final long serialVersionUID = 6966177118923713521L;
        private final TypeInformation<Long> ti;
        private final TypeSerializer<Long> ser;
        long cnt = 0;

        public LimitedLongDeserializer() {
            this.ti = Types.LONG;
            this.ser = ti.createSerializer(new ExecutionConfig());
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return ti;
        }

        @Override
        public Long deserialize(ConsumerRecord<byte[], byte[]> record) throws IOException {
            cnt++;
            DataInputView in =
                    new DataInputViewStreamWrapper(new ByteArrayInputStream(record.value()));
            return ser.deserialize(in);
        }

        @Override
        public boolean isEndOfStream(Long nextElement) {
            return cnt > 1110L;
        }
    }
}
