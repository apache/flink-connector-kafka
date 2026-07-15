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

package org.apache.flink.connector.kafka.integrity;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface for subscribers that are aware of topic integrity. And can provide topic integrity
 * mapping if the feature is enabled.
 */
public interface TopicIntegrityAware extends Serializable {

    /**
     * Opens the instance and should be called before any other method.
     *
     * @param initializationContext initialization context for the subscriber.
     */
    void open(InitializationContext initializationContext);

    /**
     * Get the topic integrity state for checkpointing.
     *
     * @return The topic integrity mapping to checkpoint.
     */
    Map<String, String> getTopicIntegrityMapping();

    /** Initialization context for the {@link TopicIntegrityAware}. */
    interface InitializationContext {

        /**
         * Returns whether to check topic integrity during runtime.
         *
         * <p>If enabled, new topics will be checked against their tracked id
         */
        boolean topicIntegrityCheckEnabled();

        /** Returns the topic integrity mapping, if any was restored from state. */
        Map<String, String> topicIntegrityMapping();
    }
}
