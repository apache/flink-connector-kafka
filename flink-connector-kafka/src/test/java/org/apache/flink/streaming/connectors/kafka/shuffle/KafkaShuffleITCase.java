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

package org.apache.flink.streaming.connectors.kafka.shuffle;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaShuffleFetcher.KafkaShuffleElement;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaShuffleFetcher.KafkaShuffleElementDeserializer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaShuffleFetcher.KafkaShuffleRecord;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaShuffleFetcher.KafkaShuffleWatermark;
import org.apache.flink.util.PropertiesUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.streaming.api.TimeCharacteristic.EventTime;
import static org.apache.flink.streaming.api.TimeCharacteristic.IngestionTime;
import static org.apache.flink.streaming.api.TimeCharacteristic.ProcessingTime;
import static org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffle.PARTITION_NUMBER;
import static org.apache.flink.streaming.connectors.kafka.shuffle.FlinkKafkaShuffle.PRODUCER_PARALLELISM;
import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Simple End to End Test for Kafka. */
public class KafkaShuffleITCase extends KafkaShuffleTestBase {

    @Rule public final Timeout timeout = Timeout.millis(600000L);

    /**
     * To test no data is lost or duplicated end-2-end with the default time characteristic:
     * ProcessingTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testSimpleProcessingTime() throws Exception {
        testKafkaShuffle(200000, ProcessingTime);
    }

    /**
     * To test no data is lost or duplicated end-2-end with time characteristic: IngestionTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testSimpleIngestionTime() throws Exception {
        testKafkaShuffle(200000, IngestionTime);
    }

    /**
     * To test no data is lost or duplicated end-2-end with time characteristic: EventTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testSimpleEventTime() throws Exception {
        testKafkaShuffle(100000, EventTime);
    }

    /**
     * To test data is partitioned to the right partition with time characteristic: ProcessingTime.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testAssignedToPartitionProcessingTime() throws Exception {
        testAssignedToPartition(300000, ProcessingTime);
    }

    /**
     * To test data is partitioned to the right partition with time characteristic: IngestionTime.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testAssignedToPartitionIngestionTime() throws Exception {
        testAssignedToPartition(300000, IngestionTime);
    }

    /**
     * To test data is partitioned to the right partition with time characteristic: EventTime.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testAssignedToPartitionEventTime() throws Exception {
        testAssignedToPartition(100000, EventTime);
    }

    /**
     * To test watermark is monotonically incremental with randomized watermark.
     *
     * <p>Producer Parallelism = 2; Kafka Partition # = 3; Consumer Parallelism = 3.
     */
    @Test
    public void testWatermarkIncremental() throws Exception {
        testWatermarkIncremental(100000);
    }

    /**
     * To test value serialization and deserialization with time characteristic: ProcessingTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testSerDeProcessingTime() throws Exception {
        testRecordSerDe(ProcessingTime);
    }

    /**
     * To test value and watermark serialization and deserialization with time characteristic:
     * IngestionTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testSerDeIngestionTime() throws Exception {
        testRecordSerDe(IngestionTime);
    }

    /**
     * To test value and watermark serialization and deserialization with time characteristic:
     * EventTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testSerDeEventTime() throws Exception {
        testRecordSerDe(EventTime);
    }

    /**
     * To test value and watermark serialization and deserialization with time characteristic:
     * EventTime.
     *
     * <p>Producer Parallelism = 1; Kafka Partition # = 1; Consumer Parallelism = 1.
     */
    @Test
    public void testWatermarkBroadcasting() throws Exception {
        final int numberOfPartitions = 3;
        final int producerParallelism = 2;
        final int numElementsPerProducer = 1000;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<Integer, Collection<ConsumerRecord<byte[], byte[]>>> results =
                testKafkaShuffleProducer(
                        topic("test_watermark_broadcast-" + UUID.randomUUID(), EventTime),
                        env,
                        numberOfPartitions,
                        producerParallelism,
                        numElementsPerProducer,
                        EventTime);
        TypeSerializer<Tuple3<Integer, Long, Integer>> typeSerializer = createTypeSerializer(env);
        KafkaShuffleElementDeserializer deserializer =
                new KafkaShuffleElementDeserializer<>(typeSerializer);

        // Records in a single partition are kept in order
        for (int p = 0; p < numberOfPartitions; p++) {
            Collection<ConsumerRecord<byte[], byte[]>> records = results.get(p);
            Map<Integer, List<KafkaShuffleWatermark>> watermarks = new HashMap<>();

            for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
                assertThat(consumerRecord.key()).isNull();
                KafkaShuffleElement element = deserializer.deserialize(consumerRecord);
                if (element.isRecord()) {
                    KafkaShuffleRecord<Tuple3<Integer, Long, Integer>> record = element.asRecord();
                    assertThat(INIT_TIMESTAMP + record.getValue().f0)
                            .isEqualTo(record.getValue().f1.longValue());
                    assertThat(record.getValue().f1.longValue())
                            .isEqualTo(record.getTimestamp().longValue());
                } else if (element.isWatermark()) {
                    KafkaShuffleWatermark watermark = element.asWatermark();
                    watermarks.computeIfAbsent(watermark.getSubtask(), k -> new ArrayList<>());
                    watermarks.get(watermark.getSubtask()).add(watermark);
                } else {
                    fail("KafkaShuffleElement is either record or watermark");
                }
            }

            // According to the setting how watermarks are generated in this ITTest,
            // every producer task emits a watermark corresponding to each record + the
            // end-of-event-time watermark.
            // Hence each producer sub task generates `numElementsPerProducer + 1` watermarks.
            // Each producer sub task broadcasts these `numElementsPerProducer + 1` watermarks to
            // all partitions.
            // Thus in total, each producer sub task emits `(numElementsPerProducer + 1) *
            // numberOfPartitions` watermarks.
            // From the consumer side, each partition receives `(numElementsPerProducer + 1) *
            // producerParallelism` watermarks,
            // with each producer sub task produces `numElementsPerProducer + 1` watermarks.
            // Besides, watermarks from the same producer sub task should keep in order.
            for (List<KafkaShuffleWatermark> subTaskWatermarks : watermarks.values()) {
                int index = 0;
                assertThat(subTaskWatermarks).hasSize(numElementsPerProducer + 1);
                for (KafkaShuffleWatermark watermark : subTaskWatermarks) {
                    if (index == numElementsPerProducer) {
                        // the last element is the watermark that signifies end-of-event-time
                        assertThat(Watermark.MAX_WATERMARK.getTimestamp())
                                .isEqualTo(watermark.getWatermark());
                    } else {
                        assertThat(INIT_TIMESTAMP + index++).isEqualTo(watermark.getWatermark());
                    }
                }
            }
        }
    }

    /**
     * To test no data is lost or duplicated end-2-end.
     *
     * <p>Schema: (key, timestamp, source instance Id). Producer Parallelism = 1; Kafka Partition #
     * = 1; Consumer Parallelism = 1
     */
    private void testKafkaShuffle(int numElementsPerProducer, TimeCharacteristic timeCharacteristic)
            throws Exception {
        String topic = topic("test_simple-" + UUID.randomUUID(), timeCharacteristic);
        final int numberOfPartitions = 1;
        final int producerParallelism = 1;

        createTestTopic(topic, numberOfPartitions, 1);

        final StreamExecutionEnvironment env =
                createEnvironment(producerParallelism, timeCharacteristic);
        createKafkaShuffle(
                        env,
                        topic,
                        numElementsPerProducer,
                        producerParallelism,
                        timeCharacteristic,
                        numberOfPartitions)
                .map(
                        new ElementCountNoMoreThanValidator(
                                numElementsPerProducer * producerParallelism))
                .setParallelism(1)
                .map(
                        new ElementCountNoLessThanValidator(
                                numElementsPerProducer * producerParallelism))
                .setParallelism(1);

        tryExecute(env, topic);

        deleteTestTopic(topic);
    }

    /**
     * To test data is partitioned to the right partition.
     *
     * <p>Schema: (key, timestamp, source instance Id). Producer Parallelism = 2; Kafka Partition #
     * = 3; Consumer Parallelism = 3
     */
    private void testAssignedToPartition(
            int numElementsPerProducer, TimeCharacteristic timeCharacteristic) throws Exception {
        String topic = topic("test_assigned_to_partition-" + UUID.randomUUID(), timeCharacteristic);
        final int numberOfPartitions = 3;
        final int producerParallelism = 2;

        createTestTopic(topic, numberOfPartitions, 1);

        final StreamExecutionEnvironment env =
                createEnvironment(producerParallelism, timeCharacteristic);

        KeyedStream<Tuple3<Integer, Long, Integer>, Tuple> keyedStream =
                createKafkaShuffle(
                        env,
                        topic,
                        numElementsPerProducer,
                        producerParallelism,
                        timeCharacteristic,
                        numberOfPartitions);
        keyedStream
                .process(
                        new PartitionValidator(
                                keyedStream.getKeySelector(), numberOfPartitions, topic))
                .setParallelism(numberOfPartitions)
                .map(
                        new ElementCountNoMoreThanValidator(
                                numElementsPerProducer * producerParallelism))
                .setParallelism(1)
                .map(
                        new ElementCountNoLessThanValidator(
                                numElementsPerProducer * producerParallelism))
                .setParallelism(1);

        tryExecute(env, topic);

        deleteTestTopic(topic);
    }

    /**
     * To watermark from the consumer side always increase.
     *
     * <p>Schema: (key, timestamp, source instance Id). Producer Parallelism = 2; Kafka Partition #
     * = 3; Consumer Parallelism = 3
     */
    private void testWatermarkIncremental(int numElementsPerProducer) throws Exception {
        TimeCharacteristic timeCharacteristic = EventTime;
        String topic = topic("test_watermark_incremental-" + UUID.randomUUID(), timeCharacteristic);
        final int numberOfPartitions = 3;
        final int producerParallelism = 2;

        createTestTopic(topic, numberOfPartitions, 1);

        final StreamExecutionEnvironment env =
                createEnvironment(producerParallelism, timeCharacteristic);

        KeyedStream<Tuple3<Integer, Long, Integer>, Tuple> keyedStream =
                createKafkaShuffle(
                        env,
                        topic,
                        numElementsPerProducer,
                        producerParallelism,
                        timeCharacteristic,
                        numberOfPartitions,
                        true);
        keyedStream
                .process(new WatermarkValidator())
                .setParallelism(numberOfPartitions)
                .map(
                        new ElementCountNoMoreThanValidator(
                                numElementsPerProducer * producerParallelism))
                .setParallelism(1)
                .map(
                        new ElementCountNoLessThanValidator(
                                numElementsPerProducer * producerParallelism))
                .setParallelism(1);

        tryExecute(env, topic);

        deleteTestTopic(topic);
    }

    private void testRecordSerDe(TimeCharacteristic timeCharacteristic) throws Exception {
        final int numElementsPerProducer = 2000;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Records in a single partition are kept in order
        Collection<ConsumerRecord<byte[], byte[]>> records =
                testKafkaShuffleProducer(
                                topic("test_serde-" + UUID.randomUUID(), timeCharacteristic),
                                env,
                                1,
                                1,
                                numElementsPerProducer,
                                timeCharacteristic)
                        .values()
                        .iterator()
                        .next();

        switch (timeCharacteristic) {
            case ProcessingTime:
                // NonTimestampContext, no intermediate watermarks, and one end-of-event-time
                // watermark
                assertThat(records.size()).isEqualTo(numElementsPerProducer + 1);
                break;
            case IngestionTime:
                // IngestionTime uses AutomaticWatermarkContext and it emits a watermark after every
                // `watermarkInterval`
                // with default interval 200, hence difficult to control the number of watermarks
                break;
            case EventTime:
                // ManualWatermarkContext
                // `numElementsPerProducer` records, `numElementsPerProducer` watermarks, and one
                // end-of-event-time watermark
                assertThat(records.size()).isEqualTo(numElementsPerProducer * 2 + 1);
                break;
            default:
                fail("unknown TimeCharacteristic type");
        }

        TypeSerializer<Tuple3<Integer, Long, Integer>> typeSerializer = createTypeSerializer(env);

        KafkaShuffleElementDeserializer deserializer =
                new KafkaShuffleElementDeserializer<>(typeSerializer);

        int recordIndex = 0;
        int watermarkIndex = 0;
        for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
            assertThat(consumerRecord.key()).isNull();
            KafkaShuffleElement element = deserializer.deserialize(consumerRecord);
            if (element.isRecord()) {
                KafkaShuffleRecord<Tuple3<Integer, Long, Integer>> record = element.asRecord();
                switch (timeCharacteristic) {
                    case ProcessingTime:
                        assertThat(record.getTimestamp()).isNull();
                        break;
                    case IngestionTime:
                        assertThat(record.getTimestamp()).isNotNull();
                        break;
                    case EventTime:
                        assertThat(record.getValue().f1.longValue())
                                .isEqualTo(record.getTimestamp().longValue());
                        break;
                    default:
                        fail("unknown TimeCharacteristic type");
                }
                assertThat(recordIndex).isEqualTo(record.getValue().f0.intValue());
                assertThat(INIT_TIMESTAMP + recordIndex)
                        .isEqualTo(record.getValue().f1.longValue());
                assertThat(0).isEqualTo(record.getValue().f2.intValue());
                recordIndex++;
            } else if (element.isWatermark()) {
                KafkaShuffleWatermark watermark = element.asWatermark();
                switch (timeCharacteristic) {
                    case ProcessingTime:
                        assertThat(watermark.getSubtask()).isEqualTo(0);
                        // the last element is the watermark that signifies end-of-event-time
                        assertThat(recordIndex).isEqualTo(numElementsPerProducer);
                        assertThat(Watermark.MAX_WATERMARK.getTimestamp())
                                .isEqualTo(watermark.getWatermark());
                        break;
                    case IngestionTime:
                        break;
                    case EventTime:
                        assertThat(watermark.getSubtask()).isEqualTo(0);
                        if (watermarkIndex == recordIndex) {
                            // the last element is the watermark that signifies end-of-event-time
                            assertThat(Watermark.MAX_WATERMARK.getTimestamp())
                                    .isEqualTo(watermark.getWatermark());
                        } else {
                            assertThat(INIT_TIMESTAMP + watermarkIndex)
                                    .isEqualTo(watermark.getWatermark());
                        }
                        break;
                    default:
                        fail("unknown TimeCharacteristic type");
                }
                watermarkIndex++;
            } else {
                fail("KafkaShuffleElement is either record or watermark");
            }
        }
    }

    private Map<Integer, Collection<ConsumerRecord<byte[], byte[]>>> testKafkaShuffleProducer(
            String topic,
            StreamExecutionEnvironment env,
            int numberOfPartitions,
            int producerParallelism,
            int numElementsPerProducer,
            TimeCharacteristic timeCharacteristic)
            throws Exception {
        createTestTopic(topic, numberOfPartitions, 1);

        env.setParallelism(producerParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStreamTimeCharacteristic(timeCharacteristic);

        DataStream<Tuple3<Integer, Long, Integer>> source =
                env.addSource(new KafkaSourceFunction(numElementsPerProducer, false))
                        .setParallelism(producerParallelism);
        DataStream<Tuple3<Integer, Long, Integer>> input =
                (timeCharacteristic == EventTime)
                        ? source.assignTimestampsAndWatermarks(new PunctuatedExtractor())
                                .setParallelism(producerParallelism)
                        : source;

        Properties properties = kafkaServer.getStandardProperties();
        Properties kafkaProperties = PropertiesUtil.flatten(properties);

        kafkaProperties.setProperty(PRODUCER_PARALLELISM, String.valueOf(producerParallelism));
        kafkaProperties.setProperty(PARTITION_NUMBER, String.valueOf(numberOfPartitions));
        kafkaProperties.setProperty(
                "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProperties.setProperty(
                "value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        FlinkKafkaShuffle.writeKeyBy(input, topic, kafkaProperties, 0);

        env.execute("Write to " + topic);

        Map<Integer, Collection<ConsumerRecord<byte[], byte[]>>> results = new HashMap<>();

        kafkaServer
                .<byte[], byte[]>getAllRecordsFromTopic(kafkaProperties, topic)
                .forEach(
                        r -> {
                            final int partition = r.partition();
                            if (!results.containsKey(partition)) {
                                results.put(partition, new ArrayList<>());
                            }
                            results.get(partition).add(r);
                        });

        deleteTestTopic(topic);

        return results;
    }

    private StreamExecutionEnvironment createEnvironment(
            int producerParallelism, TimeCharacteristic timeCharacteristic) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(producerParallelism);
        env.setStreamTimeCharacteristic(timeCharacteristic);
        env.setRestartStrategy(RestartStrategies.noRestart());

        return env;
    }

    private TypeSerializer<Tuple3<Integer, Long, Integer>> createTypeSerializer(
            StreamExecutionEnvironment env) {
        return new TupleTypeInfo<Tuple3<Integer, Long, Integer>>(
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO)
                .createSerializer(env.getConfig());
    }
}
