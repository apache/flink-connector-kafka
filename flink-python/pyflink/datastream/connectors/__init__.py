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
from pyflink.datastream.connectors.base import Sink, Source, DeliveryGuarantee


__all__ = [
    'Sink',
    'Source',
    'DeliveryGuarantee'
]


def _install():
    from pyflink.datastream import connectors

    # kafka
    from pyflink.datastream.connectors import kafka
    setattr(connectors, 'KafkaSource', kafka.KafkaSource)
    setattr(connectors, 'FlinkKafkaConsumer', kafka.FlinkKafkaConsumer)
    setattr(connectors, 'FlinkKafkaProducer', kafka.FlinkKafkaProducer)
    setattr(connectors, 'Semantic', kafka.Semantic)

    # dynamic kafka
    from pyflink.datastream.connectors import dynamic_kafka
    setattr(connectors, 'DynamicKafkaSource', dynamic_kafka.DynamicKafkaSource)
    setattr(connectors, 'DynamicKafkaSourceBuilder', dynamic_kafka.DynamicKafkaSourceBuilder)
    setattr(connectors, 'KafkaMetadataService', dynamic_kafka.KafkaMetadataService)
    setattr(connectors, 'KafkaRecordDeserializationSchema',
            dynamic_kafka.KafkaRecordDeserializationSchema)
    setattr(connectors, 'SingleClusterTopicMetadataService',
            dynamic_kafka.SingleClusterTopicMetadataService)


# for backward compatibility
_install()
del _install
