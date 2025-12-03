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
from typing import Dict, Optional, Set, Union, TYPE_CHECKING

from py4j.java_gateway import JavaObject, get_java_class

from pyflink.common import DeserializationSchema
from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway

if TYPE_CHECKING:
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer

__all__ = [
    'DynamicKafkaSource',
    'DynamicKafkaSourceBuilder',
    'KafkaMetadataService',
    'KafkaRecordDeserializationSchema',
    'SingleClusterTopicMetadataService'
]


class KafkaMetadataService(object):
    """
    Base class for Kafka metadata service implementations.
    """

    def __init__(self, j_kafka_metadata_service: JavaObject):
        self._j_kafka_metadata_service = j_kafka_metadata_service


class SingleClusterTopicMetadataService(KafkaMetadataService):
    """
    Metadata service that delegates to a single Kafka cluster.
    """

    def __init__(self, kafka_cluster_id: str, properties: Dict[str, str]):
        gateway = get_gateway()
        j_props = gateway.jvm.java.util.Properties()
        for key, value in properties.items():
            j_props.setProperty(key, value)
        j_metadata_service = gateway.jvm.org.apache.flink.connector.kafka.dynamic.metadata \
            .SingleClusterTopicMetadataService(kafka_cluster_id, j_props)
        super().__init__(j_metadata_service)


class KafkaRecordDeserializationSchema(object):
    """
    An interface for deserializing Kafka records.
    """

    def __init__(self, j_deserialization_schema: JavaObject):
        self._j_deserialization_schema = j_deserialization_schema

    @staticmethod
    def value_only(value_deserialization_schema: DeserializationSchema) \
            -> 'KafkaRecordDeserializationSchema':
        """
        Wraps a :class:`~pyflink.common.serialization.DeserializationSchema` to only deserialize the
        record value.
        """
        JKafkaRecordDeserializationSchema = get_gateway().jvm \
            .org.apache.flink.connector.kafka.source.reader.deserializer \
            .KafkaRecordDeserializationSchema
        return KafkaRecordDeserializationSchema(
            JKafkaRecordDeserializationSchema.valueOnly(
                value_deserialization_schema._j_deserialization_schema))

    @staticmethod
    def value_only_deserializer(value_deserializer_class: Union[str, JavaObject],
                                config: Optional[Dict[str, str]] = None) \
            -> 'KafkaRecordDeserializationSchema':
        """
        Wraps a Kafka deserializer class to only deserialize the record value.

        :param value_deserializer_class: The full qualified class name or Java class for the Kafka
            deserializer.
        :param config: Optional configuration map passed to the deserializer.
        """
        gateway = get_gateway()
        jvm = gateway.jvm
        if isinstance(value_deserializer_class, str):
            j_class = jvm.java.lang.Class.forName(value_deserializer_class)
        else:
            j_class = get_java_class(value_deserializer_class)

        JKafkaRecordDeserializationSchema = jvm.org.apache.flink.connector.kafka.source.reader \
            .deserializer.KafkaRecordDeserializationSchema
        if config is None:
            return KafkaRecordDeserializationSchema(
                JKafkaRecordDeserializationSchema.valueOnly(j_class))

        j_map = jvm.java.util.HashMap()
        for key, value in config.items():
            j_map.put(key, value)
        return KafkaRecordDeserializationSchema(
            JKafkaRecordDeserializationSchema.valueOnly(j_class, j_map))


class DynamicKafkaSource(Source):
    """
    The dynamic Kafka source that discovers streams across Kafka clusters without job restarts.
    """

    def __init__(self, j_dynamic_kafka_source: JavaObject):
        super().__init__(j_dynamic_kafka_source)

    @staticmethod
    def builder() -> 'DynamicKafkaSourceBuilder':
        """
        Get a builder for constructing a :class:`DynamicKafkaSource`.
        """
        return DynamicKafkaSourceBuilder()


class DynamicKafkaSourceBuilder(object):
    """
    Builder for :class:`DynamicKafkaSource`.
    """

    def __init__(self):
        self._j_builder = get_gateway().jvm \
            .org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource.builder()

    def set_stream_ids(self, stream_ids: Set[str]) -> 'DynamicKafkaSourceBuilder':
        """
        Subscribe to an explicit set of stream ids.
        """
        j_set = get_gateway().jvm.java.util.HashSet()
        for stream_id in stream_ids:
            j_set.add(stream_id)
        self._j_builder.setStreamIds(j_set)
        return self

    def set_stream_pattern(self, stream_pattern: str) -> 'DynamicKafkaSourceBuilder':
        """
        Subscribe to streams that match the provided regex pattern.
        """
        self._j_builder.setStreamPattern(
            get_gateway().jvm.java.util.regex.Pattern.compile(stream_pattern))
        return self

    def set_kafka_stream_subscriber(self, kafka_stream_subscriber: JavaObject) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set a custom Kafka stream subscriber implementation.
        """
        self._j_builder.setKafkaStreamSubscriber(kafka_stream_subscriber)
        return self

    def set_bounded(self, stopping_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'DynamicKafkaSourceBuilder':
        """
        Configure bounded mode with a stopping offsets initializer.
        """
        self._j_builder.setBounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_kafka_metadata_service(self, kafka_metadata_service: 'KafkaMetadataService') \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the Kafka metadata service responsible for resolving streams.
        """
        j_metadata_service = kafka_metadata_service._j_kafka_metadata_service \
            if isinstance(kafka_metadata_service, KafkaMetadataService) \
            else kafka_metadata_service
        self._j_builder.setKafkaMetadataService(j_metadata_service)
        return self

    def set_deserializer(self, record_deserializer: 'KafkaRecordDeserializationSchema') \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the record deserializer for the source.
        """
        self._j_builder.setDeserializer(record_deserializer._j_deserialization_schema)
        return self

    def set_starting_offsets(self, starting_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'DynamicKafkaSourceBuilder':
        """
        Configure starting offsets across all clusters.
        """
        self._j_builder.setStartingOffsets(starting_offsets_initializer._j_initializer)
        return self

    def set_properties(self, props: Dict[str, str]) -> 'DynamicKafkaSourceBuilder':
        """
        Set properties applied to all Kafka clusters.
        """
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in props.items():
            j_properties.setProperty(key, value)
        self._j_builder.setProperties(j_properties)
        return self

    def set_property(self, key: str, value: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set a property applied to all Kafka clusters.
        """
        self._j_builder.setProperty(key, value)
        return self

    def set_group_id(self, group_id: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the consumer group id applied to all clusters.
        """
        self._j_builder.setGroupId(group_id)
        return self

    def set_client_id_prefix(self, prefix: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the client id prefix applied to all clusters.
        """
        self._j_builder.setClientIdPrefix(prefix)
        return self

    def build(self) -> 'DynamicKafkaSource':
        return DynamicKafkaSource(self._j_builder.build())
