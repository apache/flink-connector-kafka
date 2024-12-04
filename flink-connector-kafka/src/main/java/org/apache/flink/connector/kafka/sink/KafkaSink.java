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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacet;
import org.apache.flink.connector.kafka.lineage.KafkaDatasetFacetProvider;
import org.apache.flink.connector.kafka.lineage.LineageUtil;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacetProvider;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

/**
 * Flink Sink to produce data into a Kafka topic. The sink supports all delivery guarantees
 * described by {@link DeliveryGuarantee}.
 * <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: messages may be lost in case
 *     of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.
 * <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} the sink will wait for all outstanding records in the
 *     Kafka buffers to be acknowledged by the Kafka producer on a checkpoint. No messages will be
 *     lost in case of any issue with the Kafka brokers but messages may be duplicated when Flink
 *     restarts.
 * <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: In this mode the KafkaSink will write all messages in
 *     a Kafka transaction that will be committed to Kafka on a checkpoint. Thus, if the consumer
 *     reads only committed data (see Kafka consumer config isolation.level), no duplicates will be
 *     seen in case of a Flink restart. However, this delays record writing effectively until a
 *     checkpoint is written, so adjust the checkpoint duration accordingly. Please ensure that you
 *     use unique {@link #transactionalIdPrefix}s across your applications running on the same Kafka
 *     cluster such that multiple running jobs do not interfere in their transactions! Additionally,
 *     it is highly recommended to tweak Kafka transaction timeout (link) >> maximum checkpoint
 *     duration + maximum restart duration or data loss may happen when Kafka expires an uncommitted
 *     transaction.
 *
 * @param <IN> type of the records written to Kafka
 * @see KafkaSinkBuilder on how to construct a KafkaSink
 */
@PublicEvolving
public class KafkaSink<IN>
        implements LineageVertexProvider,
                TwoPhaseCommittingStatefulSink<IN, KafkaWriterState, KafkaCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private final DeliveryGuarantee deliveryGuarantee;

    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Properties kafkaProducerConfig;
    private final String transactionalIdPrefix;

    KafkaSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.recordSerializer = recordSerializer;
    }

    /**
     * Create a {@link KafkaSinkBuilder} to construct a new {@link KafkaSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link KafkaSinkBuilder}
     */
    public static <IN> KafkaSinkBuilder<IN> builder() {
        return new KafkaSinkBuilder<>();
    }

    @Internal
    @Override
    public Committer<KafkaCommittable> createCommitter(CommitterInitContext committerInitContext)
            throws IOException {
        return new KafkaCommitter(kafkaProducerConfig);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<KafkaCommittable> getCommittableSerializer() {
        return new KafkaCommittableSerializer();
    }

    @Internal
    @Override
    public KafkaWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return new KafkaWriter<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                Collections.emptyList());
    }

    @Internal
    @Override
    public KafkaWriter<IN> restoreWriter(
            WriterInitContext context, Collection<KafkaWriterState> recoveredState)
            throws IOException {
        return new KafkaWriter<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                context.asSerializationSchemaInitializationContext(),
                recoveredState);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<KafkaWriterState> getWriterStateSerializer() {
        return new KafkaWriterStateSerializer();
    }

    @VisibleForTesting
    protected Properties getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }

    @Override
    public LineageVertex getLineageVertex() {
        // enrich dataset facet with properties
        Optional<KafkaDatasetFacet> kafkaDatasetFacet;
        if (recordSerializer instanceof KafkaDatasetFacetProvider) {
            kafkaDatasetFacet =
                    ((KafkaDatasetFacetProvider) recordSerializer).getKafkaDatasetFacet();

            if (!kafkaDatasetFacet.isPresent()) {
                LOG.info("Provider did not return kafka dataset facet");
                return LineageUtil.sourceLineageVertexOf(Collections.emptyList());
            }
            kafkaDatasetFacet.get().setProperties(this.kafkaProducerConfig);
        } else {
            LOG.info(
                    "recordSerializer does not implement KafkaDatasetFacetProvider: {}",
                    recordSerializer);
            return LineageUtil.sourceLineageVertexOf(Collections.emptyList());
        }

        String namespace = LineageUtil.namespaceOf(kafkaProducerConfig);

        Optional<TypeDatasetFacet> typeDatasetFacet = Optional.empty();
        if (recordSerializer instanceof TypeDatasetFacetProvider) {
            typeDatasetFacet = ((TypeDatasetFacetProvider) recordSerializer).getTypeDatasetFacet();
        }

        if (typeDatasetFacet.isPresent()) {
            return LineageUtil.sourceLineageVertexOf(
                    Collections.singleton(
                            LineageUtil.datasetOf(
                                    namespace, kafkaDatasetFacet.get(), typeDatasetFacet.get())));
        }

        return LineageUtil.sourceLineageVertexOf(
                Collections.singleton(LineageUtil.datasetOf(namespace, kafkaDatasetFacet.get())));
    }
}
