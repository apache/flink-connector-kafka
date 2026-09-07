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

package org.apache.flink.connector.kafka.source.enumerator.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.kafka.util.AdminUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Interface for providing topic integrity mapping to subscribers that are aware of topic integrity.
 */
@Internal
public interface TopicMetadataProvider {

    Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Collection<String> subscribedTopicNames);

    Map<String, TopicDescription> getTopicMetadata(AdminClient adminClient, Pattern pattern);

    static TopicMetadataProvider createDefault() {
        return new TopicMetadataProvider() {
            @Override
            public Map<String, TopicDescription> getTopicMetadata(
                    AdminClient adminClient, Collection<String> subscribedTopicNames)
                    throws RuntimeException {
                return AdminUtils.getTopicMetadata(adminClient, subscribedTopicNames);
            }

            @Override
            public Map<String, TopicDescription> getTopicMetadata(
                    AdminClient adminClient, Pattern pattern) throws RuntimeException {
                return AdminUtils.getTopicMetadata(adminClient, pattern);
            }
        };
    }
}
