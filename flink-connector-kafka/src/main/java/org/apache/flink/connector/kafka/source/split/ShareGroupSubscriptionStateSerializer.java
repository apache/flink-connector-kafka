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

package org.apache.flink.connector.kafka.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Serializer for {@link ShareGroupSubscriptionState}.
 *
 * <p>This serializer stores the minimal state needed to restore a share group subscription after a
 * failure. Unlike traditional Kafka split serializers that store partition assignments and offsets,
 * this only stores the share group ID and subscribed topics.
 *
 * <h2>Serialization Format</h2>
 *
 * <pre>
 * Version 1 Format:
 * +------------------+
 * | share_group_id   | (UTF string)
 * | topic_count      | (int)
 * | topic_1          | (UTF string)
 * | topic_2          | (UTF string)
 * | ...              |
 * +------------------+
 * </pre>
 *
 * <h2>Version Compatibility</h2>
 *
 * Version 1 is the initial version. Future versions should maintain backwards compatibility by
 * checking the version number during deserialization.
 *
 * @see ShareGroupSubscriptionState
 */
@Internal
public class ShareGroupSubscriptionStateSerializer
        implements SimpleVersionedSerializer<ShareGroupSubscriptionState> {

    /** Current serialization version. */
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(ShareGroupSubscriptionState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            // Write share group ID
            out.writeUTF(state.getShareGroupId());

            // Write subscribed topics
            Set<String> topics = state.getSubscribedTopics();
            out.writeInt(topics.size());
            for (String topic : topics) {
                out.writeUTF(topic);
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public ShareGroupSubscriptionState deserialize(int version, byte[] serialized)
            throws IOException {

        if (version != CURRENT_VERSION) {
            throw new IOException(
                    String.format(
                            "Unsupported serialization version %d. Current version is %d",
                            version, CURRENT_VERSION));
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            // Read share group ID
            String shareGroupId = in.readUTF();

            // Read subscribed topics
            int topicCount = in.readInt();
            Set<String> topics = new HashSet<>(topicCount);
            for (int i = 0; i < topicCount; i++) {
                topics.add(in.readUTF());
            }

            return new ShareGroupSubscriptionState(shareGroupId, topics);
        }
    }
}
