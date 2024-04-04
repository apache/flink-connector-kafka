package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertEquals;

/** Tests for {@link KafkaSerializerWrapper}. */
public class KafkaSerializerWrapperTest {
    @Test
    public void testUserCodeClassLoaderIsUsed() throws Exception {
        final KafkaSerializerWrapperCaptureForTest wrapper =
                new KafkaSerializerWrapperCaptureForTest();
        final ClassLoader classLoader =
                FlinkUserCodeClassLoaders.childFirst(
                        new URL[0],
                        getClass().getClassLoader(),
                        new String[0],
                        throwable -> {},
                        true);
        wrapper.open(
                new SerializationSchema.InitializationContext() {
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

    static class KafkaSerializerWrapperCaptureForTest extends KafkaSerializerWrapper<String> {
        private ClassLoader classLoaderUsed;

        KafkaSerializerWrapperCaptureForTest() {
            super(StringSerializer.class, true, (value) -> "topic");
        }

        public ClassLoader getClassLoaderUsed() {
            return classLoaderUsed;
        }

        @Override
        protected void initializeSerializer(ClassLoader classLoader) throws Exception {
            classLoaderUsed = classLoader;
            super.initializeSerializer(classLoader);
        }
    }
}
