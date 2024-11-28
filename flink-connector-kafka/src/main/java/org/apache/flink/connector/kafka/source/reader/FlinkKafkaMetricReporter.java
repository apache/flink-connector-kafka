package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.metrics.MetricGroup;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.KAFKA_CONSUMER_METRIC_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP;

/**
 * Handy metric reporter to hook into kafka metric reporters interface.
 * This reports kafka metrics using flink metric group for the source operator
 * and further scope KafkaSourceReader.KafkaConsumer.Native
 * <br/>
 * This metric reporter is enabled if `register.consumer.metrics` is true
 * and if a metricGroup was found in config (kafka consumer props).
 */
public class FlinkKafkaMetricReporter implements MetricsReporter {
    public static final String METRIC_GROUP_CONFIG = "flink_metric_group";
    private static final String NATIVE_TAG = "Native";
    private MetricGroup metricGroup;

    @Override
    public void init(List<KafkaMetric> metrics) {
        if (metricGroup == null) {
            return;
        }
        metrics.forEach(this::addGauge);
    }

    private void addGauge(KafkaMetric metric) {
        if (metricGroup == null) {
            return;
        }
        final var group = new AtomicReference<>(metricGroup);
        final var name = metric.metricName();
        name.tags().keySet().stream().sorted().forEach(
                key -> group.set(group.get().addGroup(key, name.tags().get(key))));
        group.get().gauge(name.name(), metric::metricValue);
    }

    @Override
    public void metricChange(KafkaMetric kafkaMetric) {
        if (metricGroup != null) {
            this.addGauge(kafkaMetric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric kafkaMetric) {
        // flink metric doesn't support removing metrics
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> config) {
        Object value = config.get(KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key());
        // kafka consumer metrics reporting is enabled by default
        boolean enabled = true;
        if (value instanceof Boolean) {
            enabled = (Boolean) value;
        } else if (value instanceof String) {
            enabled = Boolean.parseBoolean((String) value);
        }
        Object metricGroup = config.get(METRIC_GROUP_CONFIG);
        if (enabled && metricGroup instanceof MetricGroup) {
            this.metricGroup = ((MetricGroup) metricGroup)
                    .addGroup(KAFKA_SOURCE_READER_METRIC_GROUP)
                    .addGroup(KAFKA_CONSUMER_METRIC_GROUP)
                    .addGroup(NATIVE_TAG);
        }
    }
}
