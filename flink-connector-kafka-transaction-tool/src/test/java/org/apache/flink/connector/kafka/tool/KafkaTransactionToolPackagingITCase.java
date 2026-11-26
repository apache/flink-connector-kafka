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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the standalone artifact without borrowing dependencies from the test classpath. */
class KafkaTransactionToolPackagingITCase {

    @TempDir private Path temporaryDirectory;

    @Test
    void testExecutableHelp() throws Exception {
        assertThat(runExecutable("--help", 0))
                .contains("usage: KafkaTransactionTool", "job is stopped");
    }

    @Test
    void testExecutableRejectsUnknownOption() throws Exception {
        assertThat(runExecutable("--unknown-option", 1))
                .contains("Error parsing command line arguments", "--unknown-option");
    }

    private String runExecutable(String argument, int expectedExitCode) throws Exception {
        return runJava(List.of("-jar", toolJar().toString(), argument), expectedExitCode);
    }

    private String runJava(List<String> arguments, int expectedExitCode) throws Exception {
        final List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(arguments);
        final Path output = temporaryDirectory.resolve("tool-output.txt");
        final Process process =
                new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("CLI process completed").isTrue();
            final String result = Files.readString(output, StandardCharsets.UTF_8);
            assertThat(process.exitValue())
                    .as("CLI output: %s", result)
                    .isEqualTo(expectedExitCode);
            return result;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

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
    void testSecuredOAuthAuthenticationWithPackagedDependencies() throws Exception {
        final Path token = temporaryDirectory.resolve("token.jwt");
        final Base64.Encoder base64 = Base64.getUrlEncoder().withoutPadding();
        final String header =
                base64.encodeToString("{\"alg\":\"RS256\"}".getBytes(StandardCharsets.UTF_8));
        final String claims =
                base64.encodeToString(
                        "{\"sub\":\"packaging-test\",\"exp\":4102444800}"
                                .getBytes(StandardCharsets.UTF_8));
        // The login handler reads claims on the client. Signature verification belongs to the
        // broker.
        Files.writeString(token, header + "." + claims + ".unused");
        final String tokenUrl = token.toUri().toString();
        final Path testClasses =
                Path.of(
                        SecuredOAuthLogin.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        assertThat(
                        runJava(
                                List.of(
                                        "-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls="
                                                + tokenUrl,
                                        "-cp",
                                        toolJar() + File.pathSeparator + testClasses,
                                        SecuredOAuthLogin.class.getName(),
                                        tokenUrl),
                                0))
                .contains("packaging-test");
    }

    /** Invoked with only the executable jar and this test's classes on the classpath. */
    public static class SecuredOAuthLogin {
        public static void main(String[] args) throws Exception {
            final AdminClientConfig config =
                    new AdminClientConfig(
                            Map.of(
                                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    "localhost:1",
                                    SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
                                    args[0]));
            final AppConfigurationEntry jaas =
                    new AppConfigurationEntry(
                            OAuthBearerLoginModule.class.getName(),
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            Map.of());
            final OAuthBearerLoginCallbackHandler handler = new OAuthBearerLoginCallbackHandler();
            try {
                handler.configure(config.values(), "OAUTHBEARER", List.of(jaas));
                final OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
                handler.handle(new Callback[] {callback});
                System.out.println(callback.token().principalName());
            } finally {
                handler.close();
            }
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
                            "org.bitbucket.b_c:jose4j:0.9.6",
                            "Brian Campbell",
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
    void testThinJarContainsOnlyProjectLicenseAndNotice() throws Exception {
        final Path thinJar = Path.of(System.getProperty("transaction.tool.thin.jar"));
        try (JarFile jar = new JarFile(thinJar.toFile())) {
            assertThat(readEntry(jar, "META-INF/LICENSE"))
                    .contains("Apache License")
                    .doesNotContain("QOS.ch", "Luben Karavelov");
            assertThat(readEntry(jar, "META-INF/NOTICE"))
                    .contains("The Apache Software Foundation")
                    .doesNotContain("This project bundles", "org.apache.kafka:kafka-clients:");
        }
    }

    @Test
    void testExcludedDependencyMetadata() throws Exception {
        try (JarFile jar = new JarFile(toolJar().toFile())) {
            assertThat(jar.stream().map(entry -> entry.getName()))
                    .noneMatch(
                            name ->
                                    name.equals("module-info.class")
                                            || name.endsWith("/module-info.class")
                                            || name.equals("META-INF/DEPENDENCIES"));
        }
    }

    @Test
    void testBundledClassPackages() throws Exception {
        final List<String> expectedPackages =
                List.of(
                        "com/fasterxml/jackson/annotation/",
                        "com/fasterxml/jackson/core/",
                        "com/fasterxml/jackson/databind/",
                        "com/github/luben/zstd/",
                        "javax/annotation/",
                        "net/jpountz/",
                        "org/apache/commons/cli/",
                        "org/apache/commons/collections/",
                        "org/apache/commons/lang3/",
                        "org/apache/commons/text/",
                        "org/apache/flink/",
                        "org/apache/kafka/",
                        "org/jose4j/",
                        "org/slf4j/",
                        "org/xerial/snappy/");
        try (JarFile jar = new JarFile(toolJar().toFile())) {
            assertThat(
                            jar.stream()
                                    .map(entry -> entry.getName())
                                    .filter(name -> name.endsWith(".class"))
                                    .map(
                                            name ->
                                                    name.replaceFirst(
                                                            "^META-INF/versions/[0-9]+/", ""))
                                    .filter(
                                            name ->
                                                    expectedPackages.stream()
                                                            .noneMatch(name::startsWith)))
                    .as("Classes outside the expected bundled packages")
                    .isEmpty();
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
