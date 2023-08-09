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

package org.apache.flink.connector.kafka.dynamic.source.split;

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A test for {@link
 * org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplitSerializer}.
 */
public class DynamicKafkaSourceSplitSerializerTest {

    @Test
    public void testSerde() throws IOException {
        DynamicKafkaSourceSplitSerializer serializer = new DynamicKafkaSourceSplitSerializer();
        DynamicKafkaSourceSplit dynamicKafkaSourceSplit =
                new DynamicKafkaSourceSplit(
                        "test-cluster",
                        new KafkaPartitionSplit(new TopicPartition("test-topic", 3), 1));
        DynamicKafkaSourceSplit dynamicKafkaSourceSplitAfterSerde =
                serializer.deserialize(1, serializer.serialize(dynamicKafkaSourceSplit));
        assertEquals(dynamicKafkaSourceSplit, dynamicKafkaSourceSplitAfterSerde);
    }
}
