################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.dynamic_kafka import DynamicKafkaSource, \
    KafkaRecordDeserializationSchema, SingleClusterTopicMetadataService
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_field_value, is_instance_of


class DynamicKafkaSourceTests(PyFlinkStreamingTestCase):

    def _build_metadata_service(self):
        return SingleClusterTopicMetadataService(
            'test-cluster', {'bootstrap.servers': 'localhost:9092'})

    @staticmethod
    def _build_deserializer():
        return KafkaRecordDeserializationSchema.value_only(SimpleStringSchema())

    def test_set_stream_ids(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a', 'stream-b'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_deserializer(self._build_deserializer()) \
            .build()

        subscriber = get_field_value(source.get_java_function(), 'kafkaStreamSubscriber')
        self.assertEqual(
            subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber'
            '.KafkaStreamSetSubscriber'
        )
        stream_ids = get_field_value(subscriber, 'streamIds')
        self.assertTrue(is_instance_of(stream_ids, get_gateway().jvm.java.util.Set))
        self.assertEqual(stream_ids.size(), 2)
        self.assertTrue(stream_ids.contains('stream-a'))
        self.assertTrue(stream_ids.contains('stream-b'))

    def test_set_stream_pattern(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_pattern('stream-*') \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_deserializer(self._build_deserializer()) \
            .build()

        subscriber = get_field_value(source.get_java_function(), 'kafkaStreamSubscriber')
        self.assertEqual(
            subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber'
            '.StreamPatternSubscriber'
        )
        pattern = get_field_value(subscriber, 'streamPattern')
        self.assertTrue(is_instance_of(pattern, get_gateway().jvm.java.util.regex.Pattern))
        self.assertEqual(pattern.toString(), 'stream-*')

    def test_set_properties_and_metadata(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_deserializer(self._build_deserializer()) \
            .set_group_id('test-group') \
            .set_client_id_prefix('client-prefix') \
            .set_property('custom.property', 'custom-value') \
            .build()

        j_source = source.get_java_function()
        props = get_field_value(j_source, 'properties')
        self.assertEqual(props.getProperty('group.id'), 'test-group')
        self.assertEqual(props.getProperty('client.id.prefix'), 'client-prefix')
        self.assertEqual(props.getProperty('custom.property'), 'custom-value')

        metadata_service = get_field_value(j_source, 'kafkaMetadataService')
        self.assertEqual(
            metadata_service.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.metadata.SingleClusterTopicMetadataService'
        )

        deserialization_schema = get_field_value(j_source, 'deserializationSchema')
        self.assertEqual(
            deserialization_schema.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.reader.deserializer'
            '.KafkaValueOnlyDeserializationSchemaWrapper'
        )

    def test_bounded_with_offsets(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_deserializer(self._build_deserializer()) \
            .set_starting_offsets(KafkaOffsetsInitializer.timestamp(123)) \
            .set_bounded(KafkaOffsetsInitializer.latest()) \
            .build()

        j_source = source.get_java_function()
        self.assertEqual(get_field_value(j_source, 'boundedness').toString(), 'BOUNDED')

        starting_offsets_initializer = get_field_value(j_source, 'startingOffsetsInitializer')
        self.assertEqual(
            starting_offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.TimestampOffsetsInitializer'
        )
        stopping_offsets_initializer = get_field_value(j_source, 'stoppingOffsetsInitializer')
        self.assertEqual(
            stopping_offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.LatestOffsetsInitializer'
        )
