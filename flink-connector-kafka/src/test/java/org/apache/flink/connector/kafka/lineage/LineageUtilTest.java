package org.apache.flink.connector.kafka.lineage;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.lineage.facets.KafkaTopicListFacet;
import org.apache.flink.connector.kafka.lineage.facets.KafkaTopicPatternFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link LineageUtil}. */
public class LineageUtilTest {

    @Test
    public void testFromFacetsForNonDatasetFacetProvider() {
        assertThat(LineageUtil.facetsFrom(new Object())).isEmpty();
    }

    @Test
    public void testFromFacetsWhenNoFacetsReturned() {
        LineageFacetProvider facetProvider = mock(LineageFacetProvider.class);
        when(facetProvider.getDatasetFacets()).thenReturn(Collections.emptyList());

        assertThat(LineageUtil.facetsFrom(facetProvider)).isEmpty();
    }

    @Test
    public void testFromFacets() {
        LineageDatasetFacet facet1 = mock(LineageDatasetFacet.class);
        LineageDatasetFacet facet2 = mock(LineageDatasetFacet.class);

        LineageFacetProvider facetProvider = mock(LineageFacetProvider.class);
        when(facetProvider.getDatasetFacets()).thenReturn(Arrays.asList(facet1, facet2));

        assertThat(LineageUtil.facetsFrom(facetProvider)).containsExactly(facet1, facet2);
    }

    @Test
    public void testDatasetsFromWithTopicList() {
        LineageDatasetFacet facet1 = new KafkaTopicListFacet(Arrays.asList("topic1", "topic2"));
        LineageDatasetFacet facet2 = mock(LineageDatasetFacet.class);
        LineageDatasetFacet facet3 = mock(LineageDatasetFacet.class);

        when(facet2.name()).thenReturn("facetName2");
        when(facet3.name()).thenReturn("facetName3");
        String namespace = "kafka://host";

        List<LineageDatasetFacet> facets = Arrays.asList(facet1, facet2, facet3);

        Collection<LineageDataset> datasets = LineageUtil.datasetsFrom(namespace, facets);

        assertThat(datasets).hasSize(2);

        Optional<LineageDataset> topic1 =
                datasets.stream().filter(e -> "topic1".equals(e.name())).findAny();
        assertThat(topic1).isPresent();
        assertThat(topic1.get().namespace()).isEqualTo(namespace);
        assertThat(topic1.get().facets().get("facetName2")).isEqualTo(facet2);
        assertThat(topic1.get().facets().get("facetName3")).isEqualTo(facet3);

        Optional<LineageDataset> topic2 =
                datasets.stream().filter(e -> "topic2".equals(e.name())).findAny();
        assertThat(topic2).isPresent();
        assertThat(topic2.get().name()).isEqualTo("topic2");
        assertThat(topic2.get().namespace()).isEqualTo(namespace);
        assertThat(topic2.get().facets().get("facetName2")).isEqualTo(facet2);
        assertThat(topic2.get().facets().get("facetName3")).isEqualTo(facet3);
    }

    @Test
    public void testDatasetsFromWithTopicPattern() {
        Pattern pattern = Pattern.compile("some-pattern");

        LineageDatasetFacet facet1 = new KafkaTopicPatternFacet(pattern);
        LineageDatasetFacet facet2 = mock(LineageDatasetFacet.class);
        LineageDatasetFacet facet3 = mock(LineageDatasetFacet.class);

        when(facet2.name()).thenReturn("facetName2");
        when(facet3.name()).thenReturn("facetName3");
        String namespace = "kafka://host";

        List<LineageDatasetFacet> facets = Arrays.asList(facet1, facet2, facet3);

        Collection<LineageDataset> datasets = LineageUtil.datasetsFrom(namespace, facets);
        assertThat(datasets).hasSize(1);

        LineageDataset dataset = datasets.iterator().next();

        assertThat(dataset.name()).isEqualTo("some-pattern");
        assertThat(dataset.namespace()).isEqualTo(namespace);
        assertThat(dataset.facets().get("facetName2")).isEqualTo(facet2);
        assertThat(dataset.facets().get("facetName3")).isEqualTo(facet3);
    }

    @Test
    public void testDatasetsWithNoTopicListNorPattern() {
        LineageDatasetFacet facet2 = mock(LineageDatasetFacet.class);
        LineageDatasetFacet facet3 = mock(LineageDatasetFacet.class);

        List<LineageDatasetFacet> facets = Arrays.asList(facet2, facet3);

        assertThat(LineageUtil.datasetsFrom("some-namespace", facets)).isEmpty();
    }

    @Test
    public void testSourceLineageVertexOf() {
        List<LineageDataset> datasets = Arrays.asList(Mockito.mock(LineageDataset.class));

        SourceLineageVertex sourceLineageVertex = LineageUtil.sourceLineageVertexOf(datasets);

        assertThat(sourceLineageVertex.boundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(sourceLineageVertex.datasets()).isEqualTo(datasets);
    }

    @Test
    public void testDatasetNamespaceOf() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "my-kafka-host");

        assertThat(LineageUtil.datasetNamespaceOf(properties)).isEqualTo("kafka://my-kafka-host");
    }

    @Test
    public void testDatasetNamespaceOfWithSemicolon() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "my-kafka-host1;my-kafka-host2");

        assertThat(LineageUtil.datasetNamespaceOf(properties)).isEqualTo("kafka://my-kafka-host1");
    }

    @Test
    public void testDatasetNamespaceOfWithComma() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "my-kafka-host1,my-kafka-host2");

        assertThat(LineageUtil.datasetNamespaceOf(properties)).isEqualTo("kafka://my-kafka-host1");
    }
}
