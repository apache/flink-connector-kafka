package org.apache.flink.connector.kafka.sink;

import org.apache.flink.streaming.connectors.kafka.testutils.SerializationTestBase;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSerializerWrapperTest extends SerializationTestBase {
    @Override
    protected void setupContext() {
        when(serializationContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);
    }

    @Test
    public void testUserCodeClassLoaderIsUsed() throws Exception {
        final KafkaSerializerWrapper<String> wrapper =
                new KafkaSerializerWrapper<>(StringSerializer.class, true, (value) -> "topic");

        testUserClassLoaderIsUsedWhen(() -> {
            wrapper.open(serializationContext);
            return null;
        }, new StringSerializer());
    }
}
