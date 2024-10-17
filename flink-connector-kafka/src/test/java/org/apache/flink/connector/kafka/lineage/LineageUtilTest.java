package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LineageUtil}. */
public class LineageUtilTest {
    @Test
    public void testSourceLineageVertexOf() {
        LineageDataset dataset = new TestingLineageDataset();
        SourceLineageVertex sourceLineageVertex =
                LineageUtil.sourceLineageVertexOf(Collections.singletonList(dataset));

        assertThat(sourceLineageVertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(sourceLineageVertex.datasets()).containsExactly(dataset);
    }

    @Test
    public void testDatasetNamespaceOf() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "my-kafka-host");

        assertThat(LineageUtil.namespaceOf(properties)).isEqualTo("kafka://my-kafka-host");
    }

    @Test
    public void testDatasetNamespaceOfWithSemicolon() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "my-kafka-host1;my-kafka-host2");

        assertThat(LineageUtil.namespaceOf(properties)).isEqualTo("kafka://my-kafka-host1");
    }

    @Test
    public void testDatasetNamespaceOfWithComma() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "my-kafka-host1,my-kafka-host2");

        assertThat(LineageUtil.namespaceOf(properties)).isEqualTo("kafka://my-kafka-host1");
    }

    @Test
    public void testDatasetNamespaceWhenNoBootstrapServersProperty() {
        Properties properties = new Properties();
        assertThat(LineageUtil.namespaceOf(properties)).isEqualTo("kafka://");
    }

    private static class TestingLineageDataset implements LineageDataset {
        @Override
        public String name() {
            return null;
        }

        @Override
        public String namespace() {
            return null;
        }

        @Override
        public Map<String, LineageDatasetFacet> facets() {
            return null;
        }
    }
}
