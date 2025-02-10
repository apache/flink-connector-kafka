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

import javax.annotation.Nullable;

/**
 * The readable portion of a backchannel for communication between the commiter -> writer. It's used
 * to signal that certain transactions have been committed and respective producers are good to be
 * reused.
 */
public interface ReadableBackchannel<T> extends Backchannel {
    /**
     * Poll the next message from the backchannel. This method is non-blocking and returns {@code
     * null} if there is no message available.
     */
    @Nullable
    T poll();
}
