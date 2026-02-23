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
 *
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.Collection;

/**
 * A combination of {@link SupportsCommitter} and {@link SupportsWriterState}.
 *
 * <p>The purpose of this interface is to be able to pass an interface rather than a {@link
 * KafkaSink} implementation into the reducing sink which simplifies unit testing.
 *
 * @param <InputT> The type of the sink's input
 * @param <WriterStateT> The type of the sink writer's state
 * @param <CommT> The type of the committables.
 */
@Internal
public interface TwoPhaseCommittingStatefulSink<InputT, WriterStateT, CommT>
        extends SupportsCommitter<CommT>, SupportsWriterState<InputT, WriterStateT>, Sink<InputT> {

    PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT> createWriter(
            WriterInitContext context) throws IOException;

    PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT> restoreWriter(
            WriterInitContext context, Collection<WriterStateT> recoveredState) throws IOException;

    /** A combination of {@link StatefulSinkWriter}. */
    interface PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT>
            extends StatefulSinkWriter<InputT, WriterStateT>, CommittingSinkWriter<InputT, CommT> {}
}
