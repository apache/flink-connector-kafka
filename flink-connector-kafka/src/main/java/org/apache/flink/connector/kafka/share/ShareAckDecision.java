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

/** Final acknowledgement decision for a Kafka share-group record. */
@PublicEvolving
public enum ShareAckDecision {
    ACCEPT((byte) 1),
    REJECT((byte) 3);

    private final byte kafkaTypeId;

    ShareAckDecision(byte kafkaTypeId) {
        this.kafkaTypeId = kafkaTypeId;
    }

    public byte kafkaTypeId() {
        return kafkaTypeId;
    }

    public static ShareAckDecision fromKafkaTypeId(byte kafkaTypeId) {
        switch (kafkaTypeId) {
            case 1:
                return ACCEPT;
            case 3:
                return REJECT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported transactional share acknowledgement type id: "
                                + kafkaTypeId);
        }
    }
}
