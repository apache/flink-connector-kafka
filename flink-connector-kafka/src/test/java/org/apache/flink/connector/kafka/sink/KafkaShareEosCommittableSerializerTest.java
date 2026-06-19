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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaShareEosCommittableSerializerTest {

    private static final KafkaShareEosCommittableSerializer SERIALIZER =
            new KafkaShareEosCommittableSerializer();

    @Test
    void testCommittableSerDe() throws IOException {
        KafkaShareEosCommittable committable =
                new KafkaShareEosCommittable(
                        42L,
                        List.of(new KafkaCommittable(1L, (short) 2, "sink-txn", null)),
                        List.of(
                                new ShareAckCommittable(
                                        42L, "share-txn", 3L, (short) 4, "share-group", 5)),
                        KafkaShareEosCommittable.CommitPhase.SINK_COMMITTED);

        byte[] serialized = SERIALIZER.serialize(committable);

        assertThat(SERIALIZER.deserialize(SERIALIZER.getVersion(), serialized))
                .isEqualTo(committable);
    }
}
