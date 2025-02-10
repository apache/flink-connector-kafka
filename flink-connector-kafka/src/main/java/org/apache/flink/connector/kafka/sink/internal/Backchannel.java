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

import java.io.Closeable;

/**
 * A backchannel for communication between the commiter -> writer. It's used to signal that certain
 * transactions have been committed and respective producers are good to be reused.
 *
 * <p>The model closely follows the idea of statefun except that there is no need to checkpoint the
 * state since the backchannel will fully recover on restart from the committer state.
 *
 * <p>Establishing a backchannel for Kafka sink works because there is only writer and committer and
 * nothing in between these two operators. In most cases, these two are chained in live inside the
 * same task thread. In rare cases, committer and writer are not chained, so writer and committer
 * are in different tasks and threads. However, because of colocations of tasks, we still know that
 * both instances will run inside the same JVM and we can establish a backchannel between them. The
 * latter case requires some synchronization in the buffer.
 */
public interface Backchannel extends Closeable {
    /** Check if the backchannel is fully established. */
    boolean isEstablished();

    @Override
    void close();
}
