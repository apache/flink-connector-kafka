package org.apache.flink.connector.kafka.source.reader.deserializer;

import org.apache.flink.streaming.connectors.kafka.testutils.SerializationTestBase;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaValueOnlyDeserializerWrapperTest extends SerializationTestBase {
    @Override
    protected void setupContext() {
        when(deserializationContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
    }

    @Test
    public void testUserCodeClassLoaderIsUsed() throws Exception {
        final Map<String, String> config = new HashMap<>();
        final KafkaValueOnlyDeserializerWrapper<String> wrapper =
                new KafkaValueOnlyDeserializerWrapper<>(StringDeserializer.class, config);

        testUserClassLoaderIsUsedWhen(() -> {
            wrapper.open(deserializationContext);
            return null;
        }, new StringDeserializer());
    }
}
