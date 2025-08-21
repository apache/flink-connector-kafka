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

package org.apache.flink.connector.kafka.source.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Utility class to check Kafka version compatibility and share group feature availability.
 * This ensures proper fallback behavior when share group features are not available.
 */
public final class KafkaVersionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaVersionUtils.class);
    
    // Cached results to avoid repeated reflection calls
    private static Boolean shareGroupSupported = null;
    private static String kafkaVersion = null;
    
    private KafkaVersionUtils() {
        // Utility class
    }
    
    /**
     * Checks if the current Kafka client version supports share groups (KIP-932).
     * Share groups were introduced in Kafka 4.0.0 (experimental) and became stable in 4.1.0.
     *
     * @return true if share groups are supported, false otherwise
     */
    public static boolean isShareGroupSupported() {
        if (shareGroupSupported == null) {
            shareGroupSupported = detectShareGroupSupport();
        }
        return shareGroupSupported;
    }
    
    /**
     * Gets the Kafka client version string.
     *
     * @return the Kafka version or "unknown" if detection fails
     */
    public static String getKafkaVersion() {
        if (kafkaVersion == null) {
            kafkaVersion = detectKafkaVersion();
        }
        return kafkaVersion;
    }
    
    /**
     * Validates that the provided properties are compatible with the current Kafka version
     * for share group usage.
     *
     * @param props the consumer properties to validate
     * @throws UnsupportedOperationException if share groups are requested but not supported
     */
    public static void validateShareGroupProperties(Properties props) {
        String groupType = props.getProperty("group.type");
        
        if ("share".equals(groupType)) {
            if (!isShareGroupSupported()) {
                throw new UnsupportedOperationException(
                    String.format(
                        "Share groups (group.type=share) require Kafka 4.1.0+ but detected version: %s. " +
                        "Please upgrade to Kafka 4.1.0+ or use traditional consumer groups.",
                        getKafkaVersion()));
            }
            LOG.info("Share group support detected and enabled for Kafka version: {}", getKafkaVersion());
        }
    }
    
    /**
     * Checks if this is a share group configuration by examining properties.
     *
     * @param props the consumer properties to check
     * @return true if this appears to be a share group configuration
     */
    public static boolean isShareGroupConfiguration(Properties props) {
        return "share".equals(props.getProperty("group.type"));
    }
    
    /**
     * Creates a warning message for when share group features are requested but not available.
     *
     * @return a descriptive warning message
     */
    public static String getShareGroupUnsupportedMessage() {
        return String.format(
            "Share groups are not supported in Kafka client version %s. " +
            "Share groups require Kafka 4.1.0+. Falling back to traditional consumer groups.",
            getKafkaVersion());
    }
    
    private static boolean detectShareGroupSupport() {
        try {
            // Method 1: Check for KafkaShareConsumer class (most reliable)
            try {
                Class.forName("org.apache.kafka.clients.consumer.KafkaShareConsumer");
                LOG.info("Share group support detected via KafkaShareConsumer class");
                return true;
            } catch (ClassNotFoundException e) {
                LOG.debug("KafkaShareConsumer class not found: {}", e.getMessage());
            }
            
            // Method 2: Check for share group specific config constants
            try {
                Class<?> consumerConfigClass = Class.forName("org.apache.kafka.clients.consumer.ConsumerConfig");
                consumerConfigClass.getDeclaredField("GROUP_TYPE_CONFIG");
                LOG.info("Share group support detected via ConsumerConfig.GROUP_TYPE_CONFIG");
                return true;
            } catch (NoSuchFieldException | ClassNotFoundException e) {
                LOG.debug("GROUP_TYPE_CONFIG not found: {}", e.getMessage());
            }
            
            // Method 3: Check version through AppInfoParser (fallback)
            String version = detectKafkaVersion();
            boolean versionSupported = isVersionAtLeast(version, "4.1.0");
            if (versionSupported) {
                LOG.info("Share group support detected via version check: {}", version);
            } else {
                LOG.info("Share group not supported in version: {}", version);
            }
            return versionSupported;
            
        } catch (Exception e) {
            LOG.warn("Failed to detect share group support: {}", e.getMessage());
            return false;
        }
    }
    
    private static String detectKafkaVersion() {
        try {
            // Try to get version from AppInfoParser
            Class<?> appInfoClass = Class.forName("org.apache.kafka.common.utils.AppInfoParser");
            Method getVersionMethod = appInfoClass.getDeclaredMethod("getVersion");
            String version = (String) getVersionMethod.invoke(null);
            
            LOG.info("Detected Kafka version: {}", version);
            return version != null ? version : "unknown";
            
        } catch (Exception e) {
            LOG.warn("Failed to detect Kafka version: {}", e.getMessage());
            
            // Fallback: try to read from manifest or properties
            try {
                Package kafkaPackage = org.apache.kafka.clients.consumer.KafkaConsumer.class.getPackage();
                String implVersion = kafkaPackage.getImplementationVersion();
                if (implVersion != null) {
                    LOG.info("Detected Kafka version from package: {}", implVersion);
                    return implVersion;
                }
            } catch (Exception ex) {
                LOG.debug("Package version detection failed", ex);
            }
            
            return "unknown";
        }
    }
    
    private static boolean isVersionAtLeast(String currentVersion, String requiredVersion) {
        if ("unknown".equals(currentVersion)) {
            // Conservative approach: assume older version if we can't detect
            return false;
        }
        
        try {
            // Simple version comparison for major.minor.patch format
            String[] current = currentVersion.split("\\.");
            String[] required = requiredVersion.split("\\.");
            
            for (int i = 0; i < Math.min(current.length, required.length); i++) {
                int currentPart = Integer.parseInt(current[i].replaceAll("[^0-9]", ""));
                int requiredPart = Integer.parseInt(required[i].replaceAll("[^0-9]", ""));
                
                if (currentPart > requiredPart) {
                    return true;
                } else if (currentPart < requiredPart) {
                    return false;
                }
                // Equal, continue to next part
            }
            
            // All compared parts are equal, version is at least the required version
            return true;
            
        } catch (Exception e) {
            LOG.warn("Failed to compare versions {} and {}: {}", currentVersion, requiredVersion, e.getMessage());
            return false;
        }
    }
}