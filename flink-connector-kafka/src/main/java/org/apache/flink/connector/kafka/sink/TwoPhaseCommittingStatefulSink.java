package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;

import java.io.IOException;
import java.util.Collection;

/**
 * A combination of {@link TwoPhaseCommittingSink} and {@link StatefulSink}.
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
        extends TwoPhaseCommittingSink<InputT, CommT>, StatefulSink<InputT, WriterStateT> {

    PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT> createWriter(InitContext context)
            throws IOException;

    PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT> restoreWriter(
            InitContext context, Collection<WriterStateT> recoveredState) throws IOException;

    /** A combination of {@link PrecommittingSinkWriter} and {@link StatefulSinkWriter}. */
    interface PrecommittingStatefulSinkWriter<InputT, WriterStateT, CommT>
            extends PrecommittingSinkWriter<InputT, CommT>,
                    StatefulSinkWriter<InputT, WriterStateT> {}
}
