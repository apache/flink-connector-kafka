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

package org.apache.flink.connector.kafka.sink.internal;

import org.apache.flink.annotation.Internal;

/** Utility class for constructing transactionalIds for Kafka transactions. */
@Internal
public class TransactionalIdFactory {
    private static final String TRANSACTIONAL_ID_DELIMITER = "-";

    /**
     * Constructs a transactionalId with the following format {@code
     * transactionalIdPrefix-subtaskId-checkpointOffset}.
     *
     * @param transactionalIdPrefix prefix for the id
     * @param subtaskId describing the subtask which is opening the transaction
     * @param checkpointOffset an always incrementing number usually capturing the number of
     *     checkpoints taken by the subtask
     * @return transactionalId
     */
    public static String buildTransactionalId(
            String transactionalIdPrefix, int subtaskId, long checkpointOffset) {
        return transactionalIdPrefix
                + TRANSACTIONAL_ID_DELIMITER
                + subtaskId
                + TRANSACTIONAL_ID_DELIMITER
                + checkpointOffset;
    }

    public static long extractSubtaskId(String name) {
        int lastSep = name.lastIndexOf("-");
        int secondLastSep = name.lastIndexOf("-", lastSep - 1);
        String subtaskString = name.substring(secondLastSep + 1, lastSep);
        return Long.parseLong(subtaskString);
    }

    public static String extractPrefix(String name) {
        int lastSep = name.lastIndexOf("-");
        int secondLastSep = name.lastIndexOf("-", lastSep - 1);
        return name.substring(0, secondLastSep);
    }
}
