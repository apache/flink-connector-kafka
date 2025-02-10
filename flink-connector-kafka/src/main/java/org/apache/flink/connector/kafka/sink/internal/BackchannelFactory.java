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
import org.apache.flink.api.java.tuple.Tuple3;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Creates and manages backchannels for the Kafka sink. The backchannels are used to communicate
 * between the Kafka committer and writer.
 *
 * <p>Each backchannel is uniquely identified by the subtask id, attempt number, and transactional
 * id prefix. The subtask id prevents concurrent subtasks of the same sink from conflicting. The
 * attempt number prevents conflicts for task-local recovery. The transactional id prefix prevents
 * conflicts between multiple Kafka sinks.
 */
@Internal
@ThreadSafe
public class BackchannelFactory {
    private static final BackchannelFactory INSTANCE = new BackchannelFactory();

    /** Gets the singleton instance of the {@link BackchannelFactory}. */
    public static BackchannelFactory getInstance() {
        return INSTANCE;
    }

    private BackchannelFactory() {}

    /**
     * The map of backchannels, keyed by the subtask id and transactional id prefix to uniquely
     * identify the backchannel while establishing the connection.
     */
    private final Map<Tuple3<Integer, Integer, String>, BackchannelImpl<?>> backchannels =
            new ConcurrentHashMap<>();

    /**
     * Gets a {@link ReadableBackchannel} for the given subtask, attempt, and transactional id
     * prefix.
     *
     * <p>If this method is called twice with the same arguments, it will throw an exception as it
     * indicates that the transactional id prefix is being reused for multiple Kafka sinks.
     *
     * <p>If the corresponding {@link #getWritableBackchannel(int, int, String)} is called, the
     * {@link ReadableBackchannel#isEstablished()} will return true.
     */
    @SuppressWarnings("unchecked")
    public <T> ReadableBackchannel<T> getReadableBackchannel(
            int subtaskId, int attemptNumber, String transactionalIdPrefix) {
        return (ReadableBackchannel<T>)
                getBackchannel(
                        subtaskId,
                        attemptNumber,
                        transactionalIdPrefix,
                        BackchannelImpl::createReadableBackchannel);
    }

    /**
     * Gets a {@link WritableBackchannel} for the given subtask, attempt, and transactional id
     * prefix.
     *
     * <p>If this method is called twice with the same arguments, it will throw an exception as it
     * indicates that the transactional id prefix is being reused for multiple Kafka sinks.
     *
     * <p>If the corresponding {@link #getReadableBackchannel(int, int, String)} is called, the
     * {@link WritableBackchannel#isEstablished()} will return true.
     */
    @SuppressWarnings("unchecked")
    public <T> WritableBackchannel<T> getWritableBackchannel(
            int subtaskId, int attemptNumber, String transactionalIdPrefix) {
        return (WritableBackchannel<T>)
                getBackchannel(
                        subtaskId,
                        attemptNumber,
                        transactionalIdPrefix,
                        BackchannelImpl::createWritableBackchannel);
    }

    private <R> R getBackchannel(
            int subtaskId,
            int attemptNumber,
            String transactionalIdPrefix,
            Function<BackchannelImpl<?>, R> subchannelCreator) {
        Tuple3<Integer, Integer, String> id =
                new Tuple3<>(subtaskId, attemptNumber, transactionalIdPrefix);
        BackchannelImpl<?> backchannel =
                backchannels.computeIfAbsent(id, k -> new BackchannelImpl<>(() -> unregister(id)));
        try {
            return subchannelCreator.apply(backchannel);
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format(
                            "Found duplicate transactionalIdPrefix for multiple Kafka sinks: %s. Transactional id prefixes need to be unique. You may experience memory leaks without fixing this.",
                            transactionalIdPrefix),
                    e);
        }
    }

    private void unregister(Tuple3<Integer, Integer, String> id) {
        backchannels.remove(id);
    }
}
