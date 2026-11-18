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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.share.source.KafkaShareRecord;
import org.apache.flink.connector.kafka.share.source.KafkaShareSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(120)
class KafkaShareSourcePipelineITCase {
    private static final AtomicBoolean FAILED = new AtomicBoolean();

    @RegisterExtension
    static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(5)
                            .build());

    @Test
    void testThreePartitionsTenReaders() throws Exception {
        runPipeline(false);
    }

    @Test
    void testRecoverAfterMapFailureWithoutCommittedDuplicates() throws Exception {
        runPipeline(true);
    }

    private void runPipeline(boolean fail) throws Exception {
        final String brokers = System.getProperty("flink.kafka.share.it.bootstrap.servers");
        Assumptions.assumeTrue(brokers != null, "Requires experimental Kafka broker");
        final String suffix = UUID.randomUUID().toString();
        final String sourceTopic = "share-source-" + suffix;
        final String sinkTopic = "share-output-" + suffix;
        final String group = "share-group-" + suffix;
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", group);
        properties.setProperty("transaction.timeout.ms", "120000");
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(
                            List.of(
                                    new NewTopic(sourceTopic, 3, (short) 1),
                                    new NewTopic(sinkTopic, 3, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
            admin.incrementalAlterConfigs(
                            Map.of(
                                    new ConfigResource(ConfigResource.Type.GROUP, group),
                                    List.of(
                                            new AlterConfigOp(
                                                    new ConfigEntry(
                                                            "share.auto.offset.reset", "earliest"),
                                                    AlterConfigOp.OpType.SET))))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(
                        properties, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (int partition = 0; partition < 3; partition++) {
                for (int offset = 0; offset < 10; offset++) {
                    producer.send(
                                    new ProducerRecord<>(
                                            sourceTopic,
                                            partition,
                                            null,
                                            (partition + "-" + offset)
                                                    .getBytes(StandardCharsets.UTF_8)))
                            .get();
                }
            }
        }
        FAILED.set(false);
        final Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(configuration);
        env.setParallelism(10);
        env.enableCheckpointing(500);
        final var source =
                new KafkaShareSource<>(
                        properties,
                        List.of(sourceTopic),
                        KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()),
                        TypeInformation.of(new TypeHint<KafkaShareRecord<String>>() {}));
        final var sink =
                new KafkaShareTransactionalSink<KafkaShareRecord<String>>(
                        properties,
                        "share-pipeline-" + suffix,
                        (record, context, timestamp) ->
                                new ProducerRecord<>(
                                        sinkTopic,
                                        null,
                                        record.value.getBytes(StandardCharsets.UTF_8)),
                        record -> List.of(record.acknowledgement));
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "share-source")
                .map(new FailOnce(fail))
                .sinkTo(sink);
        final var job = env.executeAsync("FLIP-27 share source / three partitions / ten readers");
        try {
            final Properties read = new Properties();
            read.putAll(properties);
            read.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + suffix);
            read.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            read.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            read.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            final List<String> output = new ArrayList<>();
            try (KafkaConsumer<byte[], byte[]> consumer =
                    new KafkaConsumer<>(
                            read, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                consumer.subscribe(List.of(sinkTopic));
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(75);
                while (output.size() < 30 && System.nanoTime() < deadline) {
                    consumer.poll(Duration.ofMillis(200))
                            .forEach(
                                    record ->
                                            output.add(
                                                    new String(
                                                            record.value(),
                                                            StandardCharsets.UTF_8)));
                }
                consumer.poll(Duration.ofSeconds(2))
                        .forEach(
                                record ->
                                        output.add(
                                                new String(
                                                        record.value(), StandardCharsets.UTF_8)));
            }
            assertThat(output).hasSize(30);
            assertThat(new HashSet<>(output)).hasSize(30);
            assertThat(FAILED.get()).isEqualTo(fail);
        } finally {
            job.cancel().get(20, TimeUnit.SECONDS);
        }
    }

    private static final class FailOnce
            extends RichMapFunction<KafkaShareRecord<String>, KafkaShareRecord<String>> {
        private final boolean fail;

        private FailOnce(boolean fail) {
            this.fail = fail;
        }

        @Override
        public KafkaShareRecord<String> map(KafkaShareRecord<String> record) {
            if (fail && record.value.endsWith("-2") && FAILED.compareAndSet(false, true)) {
                throw new IllegalStateException("Injected map failure before checkpoint");
            }
            return record;
        }
    }
}
