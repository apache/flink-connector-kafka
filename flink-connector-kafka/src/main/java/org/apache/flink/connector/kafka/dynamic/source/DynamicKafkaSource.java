/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.dynamic.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumState;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumStateSerializer;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.DynamicKafkaSourceEnumerator;
import org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber.KafkaStreamSubscriber;
import org.apache.flink.connector.kafka.dynamic.source.reader.DynamicKafkaSourceReader;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplitSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;

/**
 * Factory class for the DynamicKafkaSource components. <a
 * href="https://cwiki.apache.org/confluence/x/CBn1D">FLIP-246: DynamicKafkaSource</a>
 *
 * <p>This source's key difference from {@link KafkaSource} is that it enables users to read
 * dynamically, which does not require job restart, from streams (topics that belong to one or more
 * clusters). If using {@link KafkaSource}, users need to restart the job by deleting the job and
 * reconfiguring the topics and clusters.
 *
 * <p>This example shows how to configure a {@link DynamicKafkaSource} that emits Integer records:
 *
 * <pre>{@code
 * DynamicKafkaSource<Integer> dynamicKafkaSource =
 *                     DynamicKafkaSource.<Integer>builder()
 *                             .setStreamIds(Collections.singleton("MY_STREAM_ID"))
 *                             // custom metadata service that resolves `MY_STREAM_ID` to the associated clusters and topics
 *                             .setKafkaMetadataService(kafkaMetadataService)
 *                             .setDeserializer(
 *                                     KafkaRecordDeserializationSchema.valueOnly(
 *                                             IntegerDeserializer.class))
 *                             .setStartingOffsets(OffsetsInitializer.earliest())
 *                             // common properties for all Kafka clusters
 *                             .setProperties(properties)
 *                             .build();
 * }</pre>
 *
 * <p>See more configuration options in {@link DynamicKafkaSourceBuilder} and {@link
 * DynamicKafkaSourceOptions}.
 *
 * @param <T> Record type
 */
@Experimental
public class DynamicKafkaSource<T>
        implements Source<T, DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState>,
                ResultTypeQueryable<T> {

    private final KafkaStreamSubscriber kafkaStreamSubscriber;
    private final KafkaMetadataService kafkaMetadataService;
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final OffsetsInitializer startingOffsetsInitializer;
    private final OffsetsInitializer stoppingOffsetsInitializer;
    private final Properties properties;
    private final Boundedness boundedness;

    DynamicKafkaSource(
            KafkaStreamSubscriber kafkaStreamSubscriber,
            KafkaMetadataService kafkaMetadataService,
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            OffsetsInitializer startingOffsetsInitializer,
            OffsetsInitializer stoppingOffsetsInitializer,
            Properties properties,
            Boundedness boundedness) {
        this.kafkaStreamSubscriber = kafkaStreamSubscriber;
        this.deserializationSchema = deserializationSchema;
        this.properties = properties;
        this.kafkaMetadataService = kafkaMetadataService;
        this.startingOffsetsInitializer = startingOffsetsInitializer;
        this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
        this.boundedness = boundedness;
    }

    /**
     * Get a builder for this source.
     *
     * @return a {@link DynamicKafkaSourceBuilder}.
     */
    public static <T> DynamicKafkaSourceBuilder<T> builder() {
        return new DynamicKafkaSourceBuilder<>();
    }

    /**
     * Get the {@link Boundedness}.
     *
     * @return the {@link Boundedness}.
     */
    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    /**
     * Create the {@link DynamicKafkaSourceReader}.
     *
     * @param readerContext The {@link SourceReaderContext context} for the source reader.
     * @return the {@link DynamicKafkaSourceReader}.
     */
    @Internal
    @Override
    public SourceReader<T, DynamicKafkaSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new DynamicKafkaSourceReader<>(readerContext, deserializationSchema, properties);
    }

    /**
     * Create the configured split enumerator implementation.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
     * @return the split enumerator.
     */
    @Internal
    @Override
    public SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState> createEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext) {
        return new DynamicKafkaSourceEnumerator(
                kafkaStreamSubscriber,
                kafkaMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                properties,
                boundedness,
                new DynamicKafkaSourceEnumState());
    }

    /**
     * Restore the configured split enumerator implementation.
     *
     * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
     *     enumerator.
     * @param checkpoint The checkpoint to restore the SplitEnumerator from.
     * @return the split enumerator.
     */
    @Internal
    @Override
    public SplitEnumerator<DynamicKafkaSourceSplit, DynamicKafkaSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<DynamicKafkaSourceSplit> enumContext,
            DynamicKafkaSourceEnumState checkpoint) {
        return new DynamicKafkaSourceEnumerator(
                kafkaStreamSubscriber,
                kafkaMetadataService,
                enumContext,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                properties,
                boundedness,
                checkpoint);
    }

    /**
     * Get the {@link DynamicKafkaSourceSplitSerializer}.
     *
     * @return the {@link DynamicKafkaSourceSplitSerializer}.
     */
    @Internal
    @Override
    public SimpleVersionedSerializer<DynamicKafkaSourceSplit> getSplitSerializer() {
        return new DynamicKafkaSourceSplitSerializer();
    }

    /**
     * Get the {@link DynamicKafkaSourceEnumStateSerializer}.
     *
     * @return the {@link DynamicKafkaSourceEnumStateSerializer}.
     */
    @Internal
    @Override
    public SimpleVersionedSerializer<DynamicKafkaSourceEnumState>
            getEnumeratorCheckpointSerializer() {
        return new DynamicKafkaSourceEnumStateSerializer();
    }

    /**
     * Get the {@link TypeInformation} of the source.
     *
     * @return the {@link TypeInformation}.
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @VisibleForTesting
    public KafkaStreamSubscriber getKafkaStreamSubscriber() {
        return kafkaStreamSubscriber;
    }
}
