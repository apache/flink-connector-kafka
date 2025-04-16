package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;

/** Be silent. **/
public class TempITCase extends KafkaTableTestBase {

    @Before
    public void before() {
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    private void createKafkaTable(final String topic, Schema schema) {
        tEnv.createTable("kafka", TableDescriptor
                .forConnector("kafka")
                .schema(schema)
                .format("json")
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, getBootstrapServers())
                .option(KafkaConnectorOptions.TOPIC.key(), topic)
                .option(
                        KafkaConnectorOptions.PROPS_GROUP_ID.key(),
                        getStandardProps().getProperty("group.id"))
                .option(KafkaConnectorOptions.SCAN_STARTUP_MODE.key(), "earliest-offset")
                .build());
    }

    private void execSql(final String sql) {
        try {
            tEnv.executeSql(sql).await();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSafeSQL() throws Exception {
        // Setup data

        final String topic = UUID.randomUUID().toString();
        createTestTopic(topic, 1, 1);

        createKafkaTable(
                topic,
                Schema.newBuilder()
                        .column("a", DataTypes.STRING())
                        // b is originally an INT
                        .column("b", DataTypes.INT())
                        .build());
        execSql("INSERT INTO kafka SELECT a, b FROM (VALUES ('a', 1)) AS orders (a, b)");
        tEnv.dropTable("kafka");

        createKafkaTable(
                topic,
                Schema.newBuilder()
                        .column("a", DataTypes.STRING())
                        // b is now a STRING i.e. breaking schema change
                        .column("b", DataTypes.STRING())
                        .build());
        execSql("INSERT INTO kafka SELECT a, b FROM (VALUES ('a', 'b')) AS orders (a, b)");
        tEnv.dropTable("kafka");

        // Read only "a" column back

        createKafkaTable(
                topic,
                Schema.newBuilder()
                        // Notice, we're using the original schema when creating the table
                        .column("a", DataTypes.STRING())
                        .column("b", DataTypes.INT())
                        .build());
        String query = "SELECT a FROM kafka";
        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query), RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(2);
        result.addSink(sink).setParallelism(1);

        try {
            env.execute("Job_2");
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                throw e;
            }
        }

        List<String> expected = Arrays.asList("+I(a)", "+I(a)");
        assertThat(TestingSinkFunction.rows).isEqualTo(expected);

        cleanupTopic(topic);
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }

    private void cleanupTopic(String topic) {
        ignoreExceptions(
                () -> deleteTestTopic(topic),
                anyCauseMatches(UnknownTopicOrPartitionException.class));
    }

    @SafeVarargs
    private static void ignoreExceptions(
            RunnableWithException runnable, ThrowingConsumer<? super Throwable>... ignoreIf) {
        try {
            runnable.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            // check if the exception is one of the ignored ones
            assertThat(ex).satisfiesAnyOf(ignoreIf);
        }
    }

    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
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
    }
}
