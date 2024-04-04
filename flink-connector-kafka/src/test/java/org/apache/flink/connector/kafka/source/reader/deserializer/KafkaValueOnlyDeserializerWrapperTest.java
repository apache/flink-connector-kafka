package org.apache.flink.connector.kafka.source.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/** Tests for {@link KafkaValueOnlyDeserializerWrapper}. */
public class KafkaValueOnlyDeserializerWrapperTest {
    @Test
    public void testUserCodeClassLoaderIsUsed() throws Exception {
        final KafkaValueOnlyDeserializerWrapperCaptureForTest wrapper =
                new KafkaValueOnlyDeserializerWrapperCaptureForTest();
        final ClassLoader classLoader =
                FlinkUserCodeClassLoaders.childFirst(
                        new URL[0],
                        getClass().getClassLoader(),
                        new String[0],
                        throwable -> {},
                        true);
        wrapper.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return new UnregisteredMetricsGroup();
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return SimpleUserCodeClassLoader.create(classLoader);
                    }
                });

        assertEquals(classLoader, wrapper.getClassLoaderUsed());
    }

    static class KafkaValueOnlyDeserializerWrapperCaptureForTest
            extends KafkaValueOnlyDeserializerWrapper<String> {
        private ClassLoader classLoaderUsed;

        KafkaValueOnlyDeserializerWrapperCaptureForTest() {
            super(StringDeserializer.class, new HashMap<>());
        }

        public ClassLoader getClassLoaderUsed() {
            return classLoaderUsed;
        }

        @Override
        protected void initializeDeserializer(ClassLoader classLoader) throws Exception {
            classLoaderUsed = classLoader;
            super.initializeDeserializer(classLoader);
        }
    }
}
