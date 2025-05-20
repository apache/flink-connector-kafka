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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import java.util.Collection;

/**
 * The committer to be used for non exactly-once delivery guarantees.
 *
 * <p>This committer does not commit any records. It is needed because the current {@link
 * org.apache.flink.api.connector.sink2.Sink} design supports only either transactional or
 * non-transactional operation and the {@link org.apache.flink.connector.kafka.sink.KafkaSink} is
 * doing both through {@link org.apache.flink.connector.base.DeliveryGuarantee}s.
 */
@Internal
public class NoopCommitter implements Committer<KafkaCommittable> {
    @Override
    public void commit(Collection<CommitRequest<KafkaCommittable>> committables) {}

    @Override
    public void close() {}
}
