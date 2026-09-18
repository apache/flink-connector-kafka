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

package org.apache.flink.connector.kafka.tool;

import org.junit.jupiter.api.Test;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the standalone artifact without borrowing dependencies from the test classpath. */
class KafkaTransactionToolPackagingITCase {

    @Test
    void testOAuthAuthenticationWithPackagedDependencies() throws Exception {
        final String oauthPackage = "org.apache.kafka.common.security.oauthbearer.";
        try (URLClassLoader loader =
                new URLClassLoader(
                        new URL[] {toolJar().toUri().toURL()},
                        ClassLoader.getPlatformClassLoader())) {
            final Class<?> handlerClass =
                    loader.loadClass(
                            oauthPackage
                                    + "internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler");
            final Object handler = handlerClass.getConstructor().newInstance();
            handlerClass
                    .getMethod("configure", Map.class, String.class, List.class)
                    .invoke(
                            handler,
                            Map.of(),
                            "OAUTHBEARER",
                            List.of(
                                    new AppConfigurationEntry(
                                            oauthPackage + "OAuthBearerLoginModule",
                                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                            Map.of(
                                                    "unsecuredLoginStringClaim_sub",
                                                    "packaging-test"))));
            final Class<?> callbackClass =
                    loader.loadClass(oauthPackage + "OAuthBearerTokenCallback");
            final Callback callback = (Callback) callbackClass.getConstructor().newInstance();
            handlerClass
                    .getMethod("handle", Callback[].class)
                    .invoke(handler, (Object) new Callback[] {callback});

            final Object token = callbackClass.getMethod("token").invoke(callback);
            assertThat(token).isNotNull();
            assertThat(
                            loader.loadClass(oauthPackage + "OAuthBearerToken")
                                    .getMethod("principalName")
                                    .invoke(token))
                    .isEqualTo("packaging-test");
        }
    }

    @Test
    void testBundledLicenseAndNoticeResources() throws Exception {
        try (JarFile jar = new JarFile(toolJar().toFile())) {
            assertThat(readEntry(jar, "META-INF/LICENSE"))
                    .contains(
                            "Apache License",
                            "QOS.ch",
                            "Luben Karavelov",
                            "Facebook, Inc.",
                            "Yann Collet",
                            "Google Inc.",
                            "Kiyoshi Masui",
                            "Redistribution and use in source and binary forms",
                            "Permission is hereby granted");
            assertThat(readEntry(jar, "META-INF/NOTICE"))
                    .contains(
                            "org.apache.kafka:kafka-clients:",
                            "com.fasterxml.jackson.core:jackson-databind:",
                            "org.slf4j:slf4j-simple:",
                            "com.github.luben:zstd-jni:",
                            "at.yawk.lz4:lz4-java:",
                            "org.xerial.snappy:snappy-java:");
            assertThat(readEntry(jar, "META-INF/FastDoubleParser-LICENSE"))
                    .contains("Werner Randelshofer");
            assertThat(readEntry(jar, "META-INF/FastDoubleParser-ThirdParty-LICENSE"))
                    .contains("Boost Software License", "Tim Buktu");
            assertThat(readEntry(jar, "META-INF/Schubfach-LICENSE"))
                    .contains("Permission is hereby granted");
        }
    }

    @Test
    void testExcludedDependencyMetadataAndClasses() throws Exception {
        try (JarFile jar = new JarFile(toolJar().toFile())) {
            assertThat(jar.stream().map(entry -> entry.getName()))
                    .noneMatch(
                            name ->
                                    name.equals("module-info.class")
                                            || name.endsWith("/module-info.class")
                                            || name.equals("META-INF/DEPENDENCIES")
                                            || name.startsWith("org/apache/commons/io/"));
        }
    }

    private static Path toolJar() {
        final String jarPath = System.getProperty("transaction.tool.jar");
        assertThat(jarPath)
                .as("Set -Dtransaction.tool.jar to the packaged uber-jar path, or run Maven verify")
                .isNotBlank();
        final Path jar = Path.of(jarPath);
        assertThat(jar).as("Packaged transaction tool; run Maven package first").isRegularFile();
        return jar;
    }

    private static String readEntry(JarFile jar, String name) throws Exception {
        assertThat(jar.getJarEntry(name)).as("Packaged resource %s", name).isNotNull();
        try (InputStream input = jar.getInputStream(jar.getJarEntry(name))) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
