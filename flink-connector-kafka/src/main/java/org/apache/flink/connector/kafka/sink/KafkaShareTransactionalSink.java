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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.share.ShareAckPayload;
import org.apache.flink.util.function.SerializableFunction;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;

/** Experimental one-output map sink that stages input ACCEPT in the output transaction. */
@Experimental
public final class KafkaShareTransactionalSink<IN> extends KafkaSink<IN> {
    private static final long serialVersionUID = 1L;
    private final SerializableFunction<IN, Collection<ShareAckPayload>> extractor;

    public KafkaShareTransactionalSink(
            Properties properties,
            String transactionalIdPrefix,
            KafkaRecordSerializationSchema<IN> serializer,
            SerializableFunction<IN, Collection<ShareAckPayload>> extractor) {
        super(
                KafkaSink.<IN>builder()
                        .setKafkaProducerConfig(properties)
                        .setTransactionalIdPrefix(transactionalIdPrefix)
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setRecordSerializer(serializer)
                        .build());
        this.extractor = Objects.requireNonNull(extractor, "extractor");
    }

    @Internal
    @Override
    public KafkaWriter<IN> restoreWriter(
            WriterInitContext context, Collection<KafkaWriterState> state) {
        ExactlyOnceKafkaWriter<IN> writer =
                (ExactlyOnceKafkaWriter<IN>) super.restoreWriter(context, state);
        writer.setShareAckPayloadExtractor(extractor);
        return writer;
    }
}
