/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.streaming.connectors.kafka.internals.FlinkKafkaInternalProducer;
import org.apache.flink.streaming.connectors.kafka.testutils.TypeSerializerConditions;
import org.apache.flink.streaming.connectors.kafka.testutils.TypeSerializerUpgradeTestBase;

import org.assertj.core.api.Condition;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link FlinkKafkaProducer.TransactionStateSerializer}
 * and {@link FlinkKafkaProducer.ContextStateSerializer}.
 */
class KafkaSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

    public Collection<TestSpecification<?, ?>> createTestSpecifications(FlinkVersion currentVersion)
            throws Exception {
        Set<FlinkVersion> migrationVersions =
                FlinkVersion.rangeOf(FlinkVersion.v1_11, FlinkVersion.v1_17);
        ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
        for (FlinkVersion flinkVersion : migrationVersions) {
            testSpecifications.add(
                    new TestSpecification<>(
                            "transaction-state-serializer",
                            flinkVersion,
                            TransactionStateSerializerSetup.class,
                            TransactionStateSerializerVerifier.class));
            testSpecifications.add(
                    new TestSpecification<>(
                            "context-state-serializer",
                            flinkVersion,
                            ContextStateSerializerSetup.class,
                            ContextStateSerializerVerifier.class));
        }
        return testSpecifications;
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "transaction-state-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TransactionStateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    FlinkKafkaProducer.KafkaTransactionState> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionState> createPriorSerializer() {
            return new FlinkKafkaProducer.TransactionStateSerializer();
        }

        @Override
        public FlinkKafkaProducer.KafkaTransactionState createTestData() {
            @SuppressWarnings("unchecked")
            FlinkKafkaInternalProducer<byte[], byte[]> mock =
                    Mockito.mock(FlinkKafkaInternalProducer.class);
            return new FlinkKafkaProducer.KafkaTransactionState("1234", 3456, (short) 789, mock);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class TransactionStateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    FlinkKafkaProducer.KafkaTransactionState> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionState> createUpgradedSerializer() {
            return new FlinkKafkaProducer.TransactionStateSerializer();
        }

        @Override
        public Condition<FlinkKafkaProducer.KafkaTransactionState> testDataCondition() {
            @SuppressWarnings("unchecked")
            FlinkKafkaInternalProducer<byte[], byte[]> mock =
                    Mockito.mock(FlinkKafkaInternalProducer.class);
            FlinkKafkaProducer.KafkaTransactionState state =
                    new FlinkKafkaProducer.KafkaTransactionState("1234", 3456, (short) 789, mock);
            return new Condition<>(state::equals, "state is " + state);
        }

        @Override
        public Condition<
                        TypeSerializerSchemaCompatibility<FlinkKafkaProducer.KafkaTransactionState>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }

    // ----------------------------------------------------------------------------------------------
    //  Specification for "context-state-serializer"
    // ----------------------------------------------------------------------------------------------

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ContextStateSerializerSetup
            implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<
                    FlinkKafkaProducer.KafkaTransactionContext> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionContext> createPriorSerializer() {
            return new FlinkKafkaProducer.ContextStateSerializer();
        }

        @Override
        public FlinkKafkaProducer.KafkaTransactionContext createTestData() {
            Set<String> transactionIds = new HashSet<>();
            transactionIds.add("123");
            transactionIds.add("456");
            transactionIds.add("789");
            return new FlinkKafkaProducer.KafkaTransactionContext(transactionIds);
        }
    }

    /**
     * This class is only public to work with {@link
     * org.apache.flink.api.common.typeutils.ClassRelocator}.
     */
    public static final class ContextStateSerializerVerifier
            implements TypeSerializerUpgradeTestBase.UpgradeVerifier<
                    FlinkKafkaProducer.KafkaTransactionContext> {
        @Override
        public TypeSerializer<FlinkKafkaProducer.KafkaTransactionContext>
                createUpgradedSerializer() {
            return new FlinkKafkaProducer.ContextStateSerializer();
        }

        @Override
        public Condition<FlinkKafkaProducer.KafkaTransactionContext> testDataCondition() {
            Set<String> transactionIds = new HashSet<>();
            transactionIds.add("123");
            transactionIds.add("456");
            transactionIds.add("789");
            FlinkKafkaProducer.KafkaTransactionContext context =
                    new FlinkKafkaProducer.KafkaTransactionContext(transactionIds);
            return new Condition<>(context::equals, "context is " + context);
        }

        @Override
        public Condition<
                        TypeSerializerSchemaCompatibility<
                                FlinkKafkaProducer.KafkaTransactionContext>>
                schemaCompatibilityCondition(FlinkVersion version) {
            return TypeSerializerConditions.isCompatibleAsIs();
        }
    }
}
