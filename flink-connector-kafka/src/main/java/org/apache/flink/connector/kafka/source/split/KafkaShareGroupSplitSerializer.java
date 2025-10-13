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

package org.apache.flink.connector.kafka.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializer for KafkaShareGroupSplit.
 * 
 * <p>This serializer handles the serialization and deserialization of share group splits
 * for checkpointing and recovery purposes.
 */
public class KafkaShareGroupSplitSerializer implements SimpleVersionedSerializer<KafkaShareGroupSplit> {
    
    private static final int CURRENT_VERSION = 1;
    
    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }
    
    @Override
    public byte[] serialize(KafkaShareGroupSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            
            // Serialize topic name
            out.writeUTF(split.getTopicName());
            
            // Serialize share group ID  
            out.writeUTF(split.getShareGroupId());
            
            // Serialize reader ID
            out.writeInt(split.getReaderId());
            
            return baos.toByteArray();
        }
    }
    
    @Override
    public KafkaShareGroupSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            
            // Deserialize topic name
            String topicName = in.readUTF();
            
            // Deserialize share group ID
            String shareGroupId = in.readUTF();
            
            // Deserialize reader ID
            int readerId = in.readInt();
            
            return new KafkaShareGroupSplit(topicName, shareGroupId, readerId);
        }
    }
}