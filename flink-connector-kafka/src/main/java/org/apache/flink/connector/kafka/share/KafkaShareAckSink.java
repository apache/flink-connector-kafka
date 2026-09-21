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

package org.apache.flink.connector.kafka.share;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Properties;

/** Sink for Kafka share-group acknowledgement control records. */
@PublicEvolving
public class KafkaShareAckSink implements Sink<ShareAckRecord>, SupportsCommitter<ShareAckCommittable> {

    public static final String SHARE_EXACTLY_ONCE_ID_CONFIG = "share.exactly-once.id";

    private final DeliveryGuarantee deliveryGuarantee;
    private final Properties kafkaProducerConfig;
    private final String shareExactlyOnceId;

    KafkaShareAckSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String shareExactlyOnceId) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProducerConfig = copyOf(kafkaProducerConfig);
        this.shareExactlyOnceId = shareExactlyOnceId;
    }

    public static KafkaShareAckSinkBuilder builder() {
        return new KafkaShareAckSinkBuilder();
    }

    @Override
    public SinkWriter<ShareAckRecord> createWriter(WriterInitContext context) throws IOException {
        return new KafkaShareAckSinkWriter(shareExactlyOnceId, kafkaProducerConfig, context);
    }

    @Override
    public Committer<ShareAckCommittable> createCommitter(CommitterInitContext context) {
        switch (deliveryGuarantee) {
            case AT_LEAST_ONCE:
            case EXACTLY_ONCE:
                return new AtLeastOnceShareAckCommitter(
                        new FlinkKafkaShareAckTransactionCommitter(kafkaProducerConfig));
            default:
                throw new IllegalStateException("Unsupported delivery guarantee: " + deliveryGuarantee);
        }
    }

    @Override
    public SimpleVersionedSerializer<ShareAckCommittable> getCommittableSerializer() {
        return new ShareAckCommittableSerializer();
    }

    @VisibleForTesting
    DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    @VisibleForTesting
    Properties getKafkaProducerConfig() {
        return copyOf(kafkaProducerConfig);
    }

    @VisibleForTesting
    String getShareExactlyOnceId() {
        return shareExactlyOnceId;
    }

    private static Properties copyOf(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(properties);
        return copy;
    }
}
