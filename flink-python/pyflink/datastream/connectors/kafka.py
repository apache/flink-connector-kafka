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
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Union, Set, Callable, Any, Optional

from py4j.java_gateway import JavaObject, get_java_class
from pyflink.common import DeserializationSchema, SerializationSchema, \
    Types, Row
from pyflink.datastream.connectors import Source, Sink
from pyflink.datastream.connectors.base import DeliveryGuarantee, SupportsPreprocessing, \
    StreamTransformer
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray, get_field, get_field_value

__all__ = [
    'DynamicKafkaSource',
    'DynamicKafkaSourceBuilder',
    'KafkaSource',
    'KafkaSourceBuilder',
    'KafkaSink',
    'KafkaSinkBuilder',
    'KafkaTopicPartition',
    'KafkaOffsetsInitializer',
    'KafkaOffsetResetStrategy',
    'KafkaRecordDeserializationSchema',
    'KafkaRecordSerializationSchema',
    'KafkaRecordSerializationSchemaBuilder',
    'KafkaTopicSelector'
]


# ---- DynamicKafkaSource ----


class DynamicKafkaSource(Source):
    """
    The Dynamic Source implementation of Kafka. Please use a
    :class:`DynamicKafkaSourceBuilder` to construct a :class:`DynamicKafkaSource`.
    """

    def __init__(self, j_dynamic_kafka_source: JavaObject):
        super().__init__(j_dynamic_kafka_source)

    @staticmethod
    def builder() -> 'DynamicKafkaSourceBuilder':
        """
        Get a DynamicKafkaSourceBuilder to build a :class:`DynamicKafkaSource`.

        :return: a Dynamic Kafka source builder.
        """
        return DynamicKafkaSourceBuilder()


class DynamicKafkaSourceBuilder(object):
    """
    The builder class for :class:`DynamicKafkaSource` to make it easier for users to construct a
    :class:`DynamicKafkaSource`.

    The bootstrap servers are derived from the configured :class:`KafkaMetadataService`. Users
    should provide stream identifiers or a stream pattern together with the metadata service and a
    record deserializer.
    """

    def __init__(self):
        self._j_builder = get_gateway().jvm.org.apache.flink.connector.kafka.dynamic.source \
            .DynamicKafkaSource.builder()

    def build(self) -> 'DynamicKafkaSource':
        return DynamicKafkaSource(self._j_builder.build())

    def set_stream_ids(self, stream_ids: Set[str]) -> 'DynamicKafkaSourceBuilder':
        """
        Set the stream ids belonging to the :class:`KafkaMetadataService`.

        :param stream_ids: the stream ids to read.
        :return: this DynamicKafkaSourceBuilder.
        """
        j_set = get_gateway().jvm.java.util.HashSet()
        for stream_id in stream_ids:
            j_set.add(stream_id)
        self._j_builder.setStreamIds(j_set)
        return self

    def set_stream_pattern(self, stream_pattern: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the stream pattern to determine stream ids belonging to the
        :class:`KafkaMetadataService`.

        :param stream_pattern: the stream name pattern to match.
        :return: this DynamicKafkaSourceBuilder.
        """
        j_pattern = get_gateway().jvm.java.util.regex.Pattern.compile(stream_pattern)
        self._j_builder.setStreamPattern(j_pattern)
        return self

    def set_kafka_metadata_service(self, kafka_metadata_service: JavaObject) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the :class:`KafkaMetadataService` that resolves streams to clusters/topics.

        :param kafka_metadata_service: Java implementation of KafkaMetadataService.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setKafkaMetadataService(kafka_metadata_service)
        return self

    def set_kafka_stream_subscriber(self, kafka_stream_subscriber: JavaObject) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set a custom Kafka stream subscriber.

        :param kafka_stream_subscriber: Java implementation of KafkaStreamSubscriber.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setKafkaStreamSubscriber(kafka_stream_subscriber)
        return self

    def set_deserializer(self, record_deserializer: Union['KafkaRecordDeserializationSchema',
                                                         DeserializationSchema]) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the record deserializer.

        :param record_deserializer: a :class:`KafkaRecordDeserializationSchema` or a
            :class:`~pyflink.common.serialization.DeserializationSchema` which will be wrapped
            as a value-only deserializer.
        :return: this DynamicKafkaSourceBuilder.
        """
        if isinstance(record_deserializer, DeserializationSchema):
            j_deserializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.reader. \
                deserializer.KafkaRecordDeserializationSchema.valueOnly(
                    record_deserializer._j_deserialization_schema)
        elif isinstance(record_deserializer, KafkaRecordDeserializationSchema):
            j_deserializer = record_deserializer._j_record_deserialization_schema
        else:
            raise TypeError(
                "record_deserializer should be either KafkaRecordDeserializationSchema or "
                "DeserializationSchema.")
        self._j_builder.setDeserializer(j_deserializer)
        return self

    def set_starting_offsets(self, starting_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'DynamicKafkaSourceBuilder':
        """
        Specify the starting offsets by providing a :class:`KafkaOffsetsInitializer`.

        :param starting_offsets_initializer: starting offsets initializer applied to all clusters.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setStartingOffsets(starting_offsets_initializer._j_initializer)
        return self

    def set_bounded(self, stopping_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the source bounded and specify stopping offsets.

        :param stopping_offsets_initializer: stopping offsets initializer applied to all clusters.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setBounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_property(self, key: str, value: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set an arbitrary property for the consumer. Values apply to all clusters and may be
        overridden by the metadata service.

        :param key: the key of the property.
        :param value: the value of the property.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setProperty(key, value)
        return self

    def set_properties(self, props: Dict) -> 'DynamicKafkaSourceBuilder':
        """
        Set arbitrary properties for the consumer. Values apply to all clusters and may be
        overridden by the metadata service.

        :param props: the properties to set.
        :return: this DynamicKafkaSourceBuilder.
        """
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in props.items():
            j_properties.setProperty(key, value)
        self._j_builder.setProperties(j_properties)
        return self

    def set_group_id(self, group_id: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the consumer group id for all clusters.

        :param group_id: the group id of the DynamicKafkaSource.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setGroupId(group_id)
        return self

    def set_client_id_prefix(self, prefix: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the client id prefix of the consumers for all clusters.

        :param prefix: the client id prefix to use.
        :return: this DynamicKafkaSourceBuilder.
        """
        self._j_builder.setClientIdPrefix(prefix)
        return self


# ---- KafkaSource ----


class KafkaSource(Source):
    """
    The Source implementation of Kafka. Please use a :class:`KafkaSourceBuilder` to construct a
    :class:`KafkaSource`. The following example shows how to create a KafkaSource emitting records
    of String type.

    ::

        >>> source = KafkaSource \\
        ...     .builder() \\
        ...     .set_bootstrap_servers('MY_BOOTSTRAP_SERVERS') \\
        ...     .set_group_id('MY_GROUP') \\
        ...     .set_topics('TOPIC1', 'TOPIC2') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \\
        ...     .build()

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_kafka_source: JavaObject):
        super().__init__(j_kafka_source)

    @staticmethod
    def builder() -> 'KafkaSourceBuilder':
        """
        Get a kafkaSourceBuilder to build a :class:`KafkaSource`.

        :return: a Kafka source builder.
        """
        return KafkaSourceBuilder()


class KafkaSourceBuilder(object):
    """
    The builder class for :class:`KafkaSource` to make it easier for the users to construct a
    :class:`KafkaSource`.

    The following example shows the minimum setup to create a KafkaSource that reads the String
    values from a Kafka topic.

    ::

        >>> source = KafkaSource.builder() \\
        ...     .set_bootstrap_servers('MY_BOOTSTRAP_SERVERS') \\
        ...     .set_topics('TOPIC1', 'TOPIC2') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .build()

    The bootstrap servers, topics/partitions to consume, and the record deserializer are required
    fields that must be set.

    To specify the starting offsets of the KafkaSource, one can call :meth:`set_starting_offsets`.

    By default, the KafkaSource runs in an CONTINUOUS_UNBOUNDED mode and never stops until the Flink
    job is canceled or fails. To let the KafkaSource run in CONTINUOUS_UNBOUNDED but stops at some
    given offsets, one can call :meth:`set_stopping_offsets`. For example the following KafkaSource
    stops after it consumes up to the latest partition offsets at the point when the Flink started.

    ::

        >>> source = KafkaSource.builder() \\
        ...     .set_bootstrap_servers('MY_BOOTSTRAP_SERVERS') \\
        ...     .set_topics('TOPIC1', 'TOPIC2') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .set_unbounded(KafkaOffsetsInitializer.latest()) \\
        ...     .build()

    .. versionadded:: 1.16.0
    """

    def __init__(self):
        self._j_builder = get_gateway().jvm.org.apache.flink.connector.kafka.source \
            .KafkaSource.builder()

    def build(self) -> 'KafkaSource':
        return KafkaSource(self._j_builder.build())

    def set_bootstrap_servers(self, bootstrap_servers: str) -> 'KafkaSourceBuilder':
        """
        Sets the bootstrap servers for the KafkaConsumer of the KafkaSource.

        :param bootstrap_servers: the bootstrap servers of the Kafka cluster.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setBootstrapServers(bootstrap_servers)
        return self

    def set_group_id(self, group_id: str) -> 'KafkaSourceBuilder':
        """
        Sets the consumer group id of the KafkaSource.

        :param group_id: the group id of the KafkaSource.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setGroupId(group_id)
        return self

    def set_topics(self, *topics: str) -> 'KafkaSourceBuilder':
        """
        Set a list of topics the KafkaSource should consume from. All the topics in the list should
        have existed in the Kafka cluster. Otherwise, an exception will be thrown. To allow some
        topics to be created lazily, please use :meth:`set_topic_pattern` instead.

        :param topics: the list of topics to consume from.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setTopics(to_jarray(get_gateway().jvm.java.lang.String, topics))
        return self

    def set_topic_pattern(self, topic_pattern: str) -> 'KafkaSourceBuilder':
        """
        Set a topic pattern to consume from use the java Pattern. For grammar, check out
        `JavaDoc <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`_ .

        :param topic_pattern: the pattern of the topic name to consume from.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setTopicPattern(get_gateway().jvm.java.util.regex
                                        .Pattern.compile(topic_pattern))
        return self

    def set_partitions(self, partitions: Set['KafkaTopicPartition']) -> 'KafkaSourceBuilder':
        """
        Set a set of partitions to consume from.

        Example:
        ::

            >>> KafkaSource.builder().set_partitions({
            ...     KafkaTopicPartition('TOPIC1', 0),
            ...     KafkaTopicPartition('TOPIC1', 1),
            ... })

        :param partitions: the set of partitions to consume from.
        :return: this KafkaSourceBuilder.
        """
        j_set = get_gateway().jvm.java.util.HashSet()
        for tp in partitions:
            j_set.add(tp._to_j_topic_partition())
        self._j_builder.setPartitions(j_set)
        return self

    def set_starting_offsets(self, starting_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'KafkaSourceBuilder':
        """
        Specify from which offsets the KafkaSource should start consume from by providing an
        :class:`KafkaOffsetsInitializer`.

        The following :class:`KafkaOffsetsInitializer` s are commonly used and provided out of the
        box. Currently, customized offset initializer is not supported in PyFlink.

        * :meth:`KafkaOffsetsInitializer.earliest` - starting from the earliest offsets. This is
          also the default offset initializer of the KafkaSource for starting offsets.
        * :meth:`KafkaOffsetsInitializer.latest` - starting from the latest offsets.
        * :meth:`KafkaOffsetsInitializer.committedOffsets` - starting from the committed offsets of
          the consumer group. If there is no committed offsets, starting from the offsets
          specified by the :class:`KafkaOffsetResetStrategy`.
        * :meth:`KafkaOffsetsInitializer.offsets` - starting from the specified offsets for each
          partition.
        * :meth:`KafkaOffsetsInitializer.timestamp` - starting from the specified timestamp for each
          partition. Note that the guarantee here is that all the records in Kafka whose timestamp
          is greater than the given starting timestamp will be consumed. However, it is possible
          that some consumer records whose timestamp is smaller than the given starting timestamp
          are also consumed.

        :param starting_offsets_initializer: the :class:`KafkaOffsetsInitializer` setting the
            starting offsets for the Source.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setStartingOffsets(starting_offsets_initializer._j_initializer)
        return self

    def set_unbounded(self, stopping_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'KafkaSourceBuilder':
        """
        By default, the KafkaSource is set to run in CONTINUOUS_UNBOUNDED manner and thus never
        stops until the Flink job fails or is canceled. To let the KafkaSource run as a streaming
        source but still stops at some point, one can set an :class:`KafkaOffsetsInitializer`
        to specify the stopping offsets for each partition. When all the partitions have reached
        their stopping offsets, the KafkaSource will then exit.

        This method is different from :meth:`set_bounded` that after setting the stopping offsets
        with this method, KafkaSource will still be CONTINUOUS_UNBOUNDED even though it will stop at
        the stopping offsets specified by the stopping offset initializer.

        The following :class:`KafkaOffsetsInitializer` s are commonly used and provided out of the
        box. Currently, customized offset initializer is not supported in PyFlink.

        * :meth:`KafkaOffsetsInitializer.latest` - starting from the latest offsets.
        * :meth:`KafkaOffsetsInitializer.committedOffsets` - starting from the committed offsets of
          the consumer group. If there is no committed offsets, starting from the offsets
          specified by the :class:`KafkaOffsetResetStrategy`.
        * :meth:`KafkaOffsetsInitializer.offsets` - starting from the specified offsets for each
          partition.
        * :meth:`KafkaOffsetsInitializer.timestamp` - starting from the specified timestamp for each
          partition. Note that the guarantee here is that all the records in Kafka whose timestamp
          is greater than the given starting timestamp will be consumed. However, it is possible
          that some consumer records whose timestamp is smaller than the given starting timestamp
          are also consumed.

        :param stopping_offsets_initializer: the :class:`KafkaOffsetsInitializer` to specify the
            stopping offsets.
        :return: this KafkaSourceBuilder
        """
        self._j_builder.setUnbounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_bounded(self, stopping_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'KafkaSourceBuilder':
        """
        By default, the KafkaSource is set to run in CONTINUOUS_UNBOUNDED manner and thus never
        stops until the Flink job fails or is canceled. To let the KafkaSource run in BOUNDED manner
        and stop at some point, one can set an :class:`KafkaOffsetsInitializer` to specify the
        stopping offsets for each partition. When all the partitions have reached their stopping
        offsets, the KafkaSource will then exit.

        This method is different from :meth:`set_unbounded` that after setting the stopping offsets
        with this method, :meth:`KafkaSource.get_boundedness` will return BOUNDED instead of
        CONTINUOUS_UNBOUNDED.

        The following :class:`KafkaOffsetsInitializer` s are commonly used and provided out of the
        box. Currently, customized offset initializer is not supported in PyFlink.

        * :meth:`KafkaOffsetsInitializer.latest` - starting from the latest offsets.
        * :meth:`KafkaOffsetsInitializer.committedOffsets` - starting from the committed offsets of
          the consumer group. If there is no committed offsets, starting from the offsets
          specified by the :class:`KafkaOffsetResetStrategy`.
        * :meth:`KafkaOffsetsInitializer.offsets` - starting from the specified offsets for each
          partition.
        * :meth:`KafkaOffsetsInitializer.timestamp` - starting from the specified timestamp for each
          partition. Note that the guarantee here is that all the records in Kafka whose timestamp
          is greater than the given starting timestamp will be consumed. However, it is possible
          that some consumer records whose timestamp is smaller than the given starting timestamp
          are also consumed.

        :param stopping_offsets_initializer: the :class:`KafkaOffsetsInitializer` to specify the
            stopping offsets.
        :return: this KafkaSourceBuilder
        """
        self._j_builder.setBounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_value_only_deserializer(self, deserialization_schema: DeserializationSchema) \
            -> 'KafkaSourceBuilder':
        """
        Sets the :class:`~pyflink.common.serialization.DeserializationSchema` for deserializing the
        value of Kafka's ConsumerRecord. The other information (e.g. key) in a ConsumerRecord will
        be ignored.

        :param deserialization_schema: the :class:`DeserializationSchema` to use for
            deserialization.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setValueOnlyDeserializer(deserialization_schema._j_deserialization_schema)
        return self

    def set_client_id_prefix(self, prefix: str) -> 'KafkaSourceBuilder':
        """
        Sets the client id prefix of this KafkaSource.

        :param prefix: the client id prefix to use for this KafkaSource.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setClientIdPrefix(prefix)
        return self

    def set_property(self, key: str, value: str) -> 'KafkaSourceBuilder':
        """
        Set an arbitrary property for the KafkaSource and KafkaConsumer. The valid keys can be found
        in ConsumerConfig and KafkaSourceOptions.

        Note that the following keys will be overridden by the builder when the KafkaSource is
        created.

        * ``auto.offset.reset.strategy`` is overridden by AutoOffsetResetStrategy returned by
          :class:`KafkaOffsetsInitializer` for the starting offsets, which is by default
          :meth:`KafkaOffsetsInitializer.earliest`.
        * ``partition.discovery.interval.ms`` is overridden to -1 when :meth:`set_bounded` has been
          invoked.

        :param key: the key of the property.
        :param value: the value of the property.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setProperty(key, value)
        return self

    def set_properties(self, props: Dict) -> 'KafkaSourceBuilder':
        """
        Set arbitrary properties for the KafkaSource and KafkaConsumer. The valid keys can be found
        in ConsumerConfig and KafkaSourceOptions.

        Note that the following keys will be overridden by the builder when the KafkaSource is
        created.

        * ``auto.offset.reset.strategy`` is overridden by AutoOffsetResetStrategy returned by
          :class:`KafkaOffsetsInitializer` for the starting offsets, which is by default
          :meth:`KafkaOffsetsInitializer.earliest`.
        * ``partition.discovery.interval.ms`` is overridden to -1 when :meth:`set_bounded` has been
          invoked.
        * ``client.id`` is overridden to "client.id.prefix-RANDOM_LONG", or "group.id-RANDOM_LONG"
          if the client id prefix is not set.

        :param props: the properties to set for the KafkaSource.
        :return: this KafkaSourceBuilder.
        """
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in props.items():
            j_properties.setProperty(key, value)
        self._j_builder.setProperties(j_properties)
        return self


class KafkaTopicPartition(object):
    """
    Corresponding to Java ``org.apache.kafka.common.TopicPartition`` class.

    Example:
    ::

        >>> topic_partition = KafkaTopicPartition('TOPIC1', 0)

    .. versionadded:: 1.16.0
    """

    def __init__(self, topic: str, partition: int):
        self._topic = topic
        self._partition = partition

    def _to_j_topic_partition(self):
        jvm = get_gateway().jvm
        return jvm.org.apache.flink.kafka.shaded.org.apache.kafka.common.TopicPartition(
            self._topic, self._partition)

    def __eq__(self, other):
        if not isinstance(other, KafkaTopicPartition):
            return False
        return self._topic == other._topic and self._partition == other._partition

    def __hash__(self):
        return 31 * (31 + self._partition) + hash(self._topic)


class KafkaOffsetResetStrategy(Enum):
    """
    Corresponding to Java ``org.apache.kafka.client.consumer.OffsetResetStrategy`` class.

    .. versionadded:: 1.16.0
    """

    LATEST = 0
    EARLIEST = 1
    NONE = 2

    def _to_j_offset_reset_strategy(self):
        JOffsetResetStrategy = get_gateway().jvm.org.apache.flink.kafka.shaded.org.apache.kafka.\
            clients.consumer.OffsetResetStrategy
        return getattr(JOffsetResetStrategy, self.name)


class KafkaOffsetsInitializer(object):
    """
    An interface for users to specify the starting / stopping offset of a KafkaPartitionSplit.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_initializer: JavaObject):
        self._j_initializer = j_initializer

    @staticmethod
    def committed_offsets(
            offset_reset_strategy: 'KafkaOffsetResetStrategy' = KafkaOffsetResetStrategy.NONE) -> \
            'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the committed
        offsets. An exception will be thrown at runtime if there is no committed offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets if
        the committed offsets does not exist.

        :param offset_reset_strategy: the offset reset strategy to use when the committed offsets do
            not exist.
        :return: an offset initializer which initialize the offsets to the committed offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.\
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.committedOffsets(
            offset_reset_strategy._to_j_offset_reset_strategy()))

    @staticmethod
    def timestamp(timestamp: int) -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets in each partition so
        that the initialized offset is the offset of the first record whose record timestamp is
        greater than or equals the give timestamp.

        :param timestamp: the timestamp to start the consumption.
        :return: an :class:`OffsetsInitializer` which initializes the offsets based on the given
            timestamp.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.timestamp(timestamp))

    @staticmethod
    def earliest() -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
        available offsets of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
            available offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.earliest())

    @staticmethod
    def latest() -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest offsets
        of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest
            offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.latest())

    @staticmethod
    def offsets(offsets: Dict['KafkaTopicPartition', int],
                offset_reset_strategy: 'KafkaOffsetResetStrategy' =
                KafkaOffsetResetStrategy.EARLIEST) -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
        offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets in
        case the specified offset is out of range.

        Example:
        ::

            >>> KafkaOffsetsInitializer.offsets({
            ...     KafkaTopicPartition('TOPIC1', 0): 0,
            ...     KafkaTopicPartition('TOPIC1', 1): 10000
            ... }, KafkaOffsetResetStrategy.EARLIEST)

        :param offsets: the specified offsets for each partition.
        :param offset_reset_strategy: the :class:`KafkaOffsetResetStrategy` to use when the
            specified offset is out of range.
        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
            offsets.
        """
        jvm = get_gateway().jvm
        j_map_wrapper = jvm.org.apache.flink.python.util.HashMapWrapper(
            None, get_java_class(jvm.Long))
        for tp, offset in offsets.items():
            j_map_wrapper.put(tp._to_j_topic_partition(), offset)

        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.offsets(
            j_map_wrapper.asMap(), offset_reset_strategy._to_j_offset_reset_strategy()))


class KafkaRecordDeserializationSchema(object):
    """
    Wrapper around Java ``KafkaRecordDeserializationSchema``.
    """

    def __init__(self, j_record_deserialization_schema: JavaObject):
        self._j_record_deserialization_schema = j_record_deserialization_schema

    @staticmethod
    def value_only(value_deserialization_schema: DeserializationSchema) \
            -> 'KafkaRecordDeserializationSchema':
        """
        Wrap the provided :class:`~pyflink.common.serialization.DeserializationSchema` so that only
        the Kafka record value is deserialized.

        :param value_deserialization_schema: the value deserialization schema.
        :return: a :class:`KafkaRecordDeserializationSchema`.
        """
        j_deserializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.reader. \
            deserializer.KafkaRecordDeserializationSchema.valueOnly(
                value_deserialization_schema._j_deserialization_schema)
        return KafkaRecordDeserializationSchema(j_deserializer)


class KafkaSink(Sink, SupportsPreprocessing):
    """
    Flink Sink to produce data into a Kafka topic. The sink supports all delivery guarantees
    described by :class:`DeliveryGuarantee`.

    * :attr:`DeliveryGuarantee.NONE` does not provide any guarantees: messages may be lost in case
      of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.
    * :attr:`DeliveryGuarantee.AT_LEAST_ONCE` the sink will wait for all outstanding records in the
      Kafka buffers to be acknowledged by the Kafka producer on a checkpoint. No messages will be
      lost in case of any issue with the Kafka brokers but messages may be duplicated when Flink
      restarts.
    * :attr:`DeliveryGuarantee.EXACTLY_ONCE`: In this mode the KafkaSink will write all messages in
      a Kafka transaction that will be committed to Kafka on a checkpoint. Thus, if the consumer
      reads only committed data (see Kafka consumer config ``isolation.level``), no duplicates
      will be seen in case of a Flink restart. However, this delays record writing effectively
      until a checkpoint is written, so adjust the checkpoint duration accordingly. Please ensure
      that you use unique transactional id prefixes across your applications running on the same
      Kafka cluster such that multiple running jobs do not interfere in their transactions!
      Additionally, it is highly recommended to tweak Kafka transaction timeout (link) >> maximum
      checkpoint duration + maximum restart duration or data loss may happen when Kafka expires an
      uncommitted transaction.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_kafka_sink, transformer: Optional[StreamTransformer] = None):
        super().__init__(j_kafka_sink)
        self._transformer = transformer

    @staticmethod
    def builder() -> 'KafkaSinkBuilder':
        """
        Create a :class:`KafkaSinkBuilder` to construct :class:`KafkaSink`.
        """
        return KafkaSinkBuilder()

    def get_transformer(self) -> Optional[StreamTransformer]:
        return self._transformer


class KafkaSinkBuilder(object):
    """
    Builder to construct :class:`KafkaSink`.

    The following example shows the minimum setup to create a KafkaSink that writes String values
    to a Kafka topic.

    ::

        >>> record_serializer = KafkaRecordSerializationSchema.builder() \\
        ...     .set_topic(MY_SINK_TOPIC) \\
        ...     .set_value_serialization_schema(SimpleStringSchema()) \\
        ...     .build()
        >>> sink = KafkaSink.builder() \\
        ...     .set_bootstrap_servers(MY_BOOTSTRAP_SERVERS) \\
        ...     .set_record_serializer(record_serializer) \\
        ...     .build()

    One can also configure different :class:`DeliveryGuarantee` by using
    :meth:`set_delivery_guarantee` but keep in mind when using
    :attr:`DeliveryGuarantee.EXACTLY_ONCE`, one must set the transactional id prefix
    :meth:`set_transactional_id_prefix`.

    .. versionadded:: 1.16.0
    """

    def __init__(self):
        jvm = get_gateway().jvm
        self._j_builder = jvm.org.apache.flink.connector.kafka.sink.KafkaSink.builder()
        self._preprocessing = None

    def build(self) -> 'KafkaSink':
        """
        Constructs the :class:`KafkaSink` with the configured properties.
        """
        return KafkaSink(self._j_builder.build(), self._preprocessing)

    def set_bootstrap_servers(self, bootstrap_servers: str) -> 'KafkaSinkBuilder':
        """
        Sets the Kafka bootstrap servers.

        :param bootstrap_servers: A comma separated list of valid URIs to reach the Kafka broker.
        """
        self._j_builder.setBootstrapServers(bootstrap_servers)
        return self

    def set_delivery_guarantee(self, delivery_guarantee: DeliveryGuarantee) -> 'KafkaSinkBuilder':
        """
        Sets the wanted :class:`DeliveryGuarantee`. The default delivery guarantee is
        :attr:`DeliveryGuarantee.NONE`.

        :param delivery_guarantee: The wanted :class:`DeliveryGuarantee`.
        """
        self._j_builder.setDeliveryGuarantee(delivery_guarantee._to_j_delivery_guarantee())
        return self

    def set_transactional_id_prefix(self, transactional_id_prefix: str) -> 'KafkaSinkBuilder':
        """
        Sets the prefix for all created transactionalIds if :attr:`DeliveryGuarantee.EXACTLY_ONCE`
        is configured.

        It is mandatory to always set this value with :attr:`DeliveryGuarantee.EXACTLY_ONCE` to
        prevent corrupted transactions if multiple jobs using the KafkaSink run against the same
        Kafka Cluster. The default prefix is ``"kafka-sink"``.

        The size of the prefix is capped by MAXIMUM_PREFIX_BYTES (6400) formatted with UTF-8.

        It is important to keep the prefix stable across application restarts. If the prefix changes
        it might happen that lingering transactions are not correctly aborted and newly written
        messages are not immediately consumable until transactions timeout.

        :param transactional_id_prefix: The transactional id prefix.
        """
        self._j_builder.setTransactionalIdPrefix(transactional_id_prefix)
        return self

    def set_record_serializer(self, record_serializer: 'KafkaRecordSerializationSchema') \
            -> 'KafkaSinkBuilder':
        """
        Sets the :class:`KafkaRecordSerializationSchema` that transforms incoming records to kafka
        producer records.

        :param record_serializer: The :class:`KafkaRecordSerializationSchema`.
        """
        # NOTE: If topic selector is a generated first-column selector, do extra preprocessing
        j_topic_selector = get_field_value(record_serializer._j_serialization_schema,
                                           'topicSelector')
        if (
            j_topic_selector.getClass().getCanonicalName() ==
            'org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder.'
            'CachingTopicSelector'
        ) and (
            get_field_value(j_topic_selector, 'topicSelector').getClass().getCanonicalName()
            is not None and
            (get_field_value(j_topic_selector, 'topicSelector').getClass().getCanonicalName()
             .startswith('com.sun.proxy') or
             get_field_value(j_topic_selector, 'topicSelector').getClass().getCanonicalName()
             .startswith('jdk.proxy'))
        ):
            record_serializer._wrap_serialization_schema()
            self._preprocessing = record_serializer._build_preprocessing()

        self._j_builder.setRecordSerializer(record_serializer._j_serialization_schema)
        return self

    def set_property(self, key: str, value: str) -> 'KafkaSinkBuilder':
        """
        Sets kafka producer config.

        :param key: Kafka producer config key.
        :param value: Kafka producer config value.
        """
        self._j_builder.setProperty(key, value)
        return self


class KafkaRecordSerializationSchema(SerializationSchema):
    """
    A serialization schema which defines how to convert the stream record to kafka producer record.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_serialization_schema,
                 topic_selector: Optional['KafkaTopicSelector'] = None):
        super().__init__(j_serialization_schema)
        self._topic_selector = topic_selector

    @staticmethod
    def builder() -> 'KafkaRecordSerializationSchemaBuilder':
        """
        Creates a default schema builder to provide common building blocks i.e. key serialization,
        value serialization, topic selection.
        """
        return KafkaRecordSerializationSchemaBuilder()

    def _wrap_serialization_schema(self):
        jvm = get_gateway().jvm

        def _wrap_schema(field_name):
            j_schema_field = get_field(self._j_serialization_schema.getClass(), field_name)
            if j_schema_field.get(self._j_serialization_schema) is not None:
                j_schema_field.set(
                    self._j_serialization_schema,
                    jvm.org.apache.flink.python.util.PythonConnectorUtils
                    .SecondColumnSerializationSchema(
                        j_schema_field.get(self._j_serialization_schema)
                    )
                )

        _wrap_schema('keySerializationSchema')
        _wrap_schema('valueSerializationSchema')

    def _build_preprocessing(self) -> StreamTransformer:
        class SelectTopicTransformer(StreamTransformer):

            def __init__(self, topic_selector: KafkaTopicSelector):
                self._topic_selector = topic_selector

            def apply(self, ds):
                output_type = Types.ROW([Types.STRING(), ds.get_type()])
                return ds.map(lambda v: Row(self._topic_selector.apply(v), v),
                              output_type=output_type)

        return SelectTopicTransformer(self._topic_selector)


class KafkaRecordSerializationSchemaBuilder(object):
    """
    Builder to construct :class:`KafkaRecordSerializationSchema`.

    Example:
    ::

        >>> KafkaRecordSerializationSchema.builder() \\
        ...     .set_topic('topic') \\
        ...     .set_key_serialization_schema(SimpleStringSchema()) \\
        ...     .set_value_serialization_schema(SimpleStringSchema()) \\
        ...     .build()

    And the sink topic can be calculated dynamically from each record:
    ::

        >>> KafkaRecordSerializationSchema.builder() \\
        ...     .set_topic_selector(lambda row: 'topic-' + row['category']) \\
        ...     .set_value_serialization_schema(
        ...         JsonRowSerializationSchema.builder().with_type_info(ROW_TYPE).build()) \\
        ...     .build()

    It is necessary to configure exactly one serialization method for the value and a topic.

    .. versionadded:: 1.16.0
    """

    def __init__(self):
        jvm = get_gateway().jvm
        self._j_builder = jvm.org.apache.flink.connector.kafka.sink \
            .KafkaRecordSerializationSchemaBuilder()
        self._fixed_topic = True  # type: bool
        self._topic_selector = None  # type: Optional[KafkaTopicSelector]
        self._key_serialization_schema = None  # type: Optional[SerializationSchema]
        self._value_serialization_schema = None  # type: Optional[SerializationSchema]

    def build(self) -> 'KafkaRecordSerializationSchema':
        """
        Constructs the :class:`KafkaRecordSerializationSchemaBuilder` with the configured
        properties.
        """
        if self._fixed_topic:
            return KafkaRecordSerializationSchema(self._j_builder.build())
        else:
            return KafkaRecordSerializationSchema(self._j_builder.build(), self._topic_selector)

    def set_topic(self, topic: str) -> 'KafkaRecordSerializationSchemaBuilder':
        """
        Sets a fixed topic which used as destination for all records.

        :param topic: The fixed topic.
        """
        self._j_builder.setTopic(topic)
        self._fixed_topic = True
        return self

    def set_topic_selector(self, topic_selector: Union[Callable[[Any], str], 'KafkaTopicSelector'])\
            -> 'KafkaRecordSerializationSchemaBuilder':
        """
        Sets a topic selector which computes the target topic for every incoming record.

        :param topic_selector: A :class:`KafkaTopicSelector` implementation or a function that
            consumes each incoming record and return the topic string.
        """
        if not isinstance(topic_selector, KafkaTopicSelector) and not callable(topic_selector):
            raise TypeError('topic_selector must be KafkaTopicSelector or a callable')
        if not isinstance(topic_selector, KafkaTopicSelector):
            class TopicSelectorFunctionAdapter(KafkaTopicSelector):

                def __init__(self, f: Callable[[Any], str]):
                    self._f = f

                def apply(self, data) -> str:
                    return self._f(data)

            topic_selector = TopicSelectorFunctionAdapter(topic_selector)

        jvm = get_gateway().jvm
        self._j_builder.setTopicSelector(
            jvm.org.apache.flink.python.util.PythonConnectorUtils.createFirstColumnTopicSelector(
                get_java_class(jvm.org.apache.flink.connector.kafka.sink.TopicSelector)
            )
        )
        self._fixed_topic = False
        self._topic_selector = topic_selector
        return self

    def set_key_serialization_schema(self, key_serialization_schema: SerializationSchema) \
            -> 'KafkaRecordSerializationSchemaBuilder':
        """
        Sets a :class:`SerializationSchema` which is used to serialize the incoming element to the
        key of the producer record. The key serialization is optional, if not set, the key of the
        producer record will be null.

        :param key_serialization_schema: The :class:`SerializationSchema` to serialize each incoming
            record as the key of producer record.
        """
        self._key_serialization_schema = key_serialization_schema
        self._j_builder.setKeySerializationSchema(key_serialization_schema._j_serialization_schema)
        return self

    def set_value_serialization_schema(self, value_serialization_schema: SerializationSchema) \
            -> 'KafkaRecordSerializationSchemaBuilder':
        """
        Sets a :class:`SerializationSchema` which is used to serialize the incoming element to the
        value of the producer record. The value serialization is required.

        :param value_serialization_schema: The :class:`SerializationSchema` to serialize each data
            record as the key of producer record.
        """
        self._value_serialization_schema = value_serialization_schema
        self._j_builder.setValueSerializationSchema(
            value_serialization_schema._j_serialization_schema)
        return self


class KafkaTopicSelector(ABC):
    """
    Select topic for an incoming record

    .. versionadded:: 1.16.0
    """

    @abstractmethod
    def apply(self, data) -> str:
        pass
