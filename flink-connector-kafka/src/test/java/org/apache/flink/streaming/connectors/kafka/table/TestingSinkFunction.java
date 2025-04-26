package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.SuccessException;

import java.util.ArrayList;
import java.util.List;

final class TestingSinkFunction implements SinkFunction<RowData> {

    private static final long serialVersionUID = 455430015321124493L;

    private static final List<String> rows = new ArrayList<>();

    private final int expectedSize;

    TestingSinkFunction(int expectedSize) {
        this.expectedSize = expectedSize;
        rows.clear();
    }

    @Override
    public void invoke(RowData value, Context context) {
        rows.add(value.toString());
        if (rows.size() >= expectedSize) {
            // job finish
            throw new SuccessException();
        }
    }

    public static List<String> getRows() {
        return rows;
    }
}
