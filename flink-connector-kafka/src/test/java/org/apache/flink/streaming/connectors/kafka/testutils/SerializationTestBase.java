package org.apache.flink.streaming.connectors.kafka.testutils;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.Before;
import org.mockito.*;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

public class SerializationTestBase {
    @Mock
    protected DeserializationSchema.InitializationContext deserializationContext;
    @Mock
    protected SerializationSchema.InitializationContext serializationContext;
    @Mock
    protected UserCodeClassLoader userCodeClassLoader;
    @Mock
    protected ClassLoader classLoader;
    @Captor
    private ArgumentCaptor<ClassLoader> classLoaderCaptor;

    @Before
    public void setUp() {
        when(userCodeClassLoader.asClassLoader()).thenReturn(classLoader);
        setupContext();
    }

    protected void setupContext() {
    }

    protected void testUserClassLoaderIsUsedWhen(Callable<Object> callable, Object instance) throws Exception {
        try (MockedStatic<InstantiationUtil> mocked = Mockito.mockStatic(InstantiationUtil.class)) {

            mocked.when(() -> InstantiationUtil.instantiate(
                    anyString(),
                    notNull(),
                    any(ClassLoader.class)
            )).thenReturn(instance);

            callable.call();

            mocked.verify(() -> InstantiationUtil.instantiate(
                    anyString(),
                    notNull(),
                    classLoaderCaptor.capture()
            ));

            assertEquals(classLoader, classLoaderCaptor.getValue());
        }
    }
}
