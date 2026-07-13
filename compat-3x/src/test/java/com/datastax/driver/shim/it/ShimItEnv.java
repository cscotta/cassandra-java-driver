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
package com.datastax.driver.shim.it;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testng.SkipException;

/**
 * Shared environment resolution + gating for the Layer 2 integration tests.
 *
 * <p>Both ITs wrap the existing (working) differential/upstream harness shell scripts, parameterized
 * via {@code SHIM_*} environment variables derived from Maven system properties (see the {@code -Pit}
 * profile in {@code pom.xml}). Nothing here hardcodes a {@code /tmp} path; the real-3.x and shim
 * classpaths are resolved into the module's {@code target/} by the scripts.
 *
 * <p>Gating: an IT SKIPs (TestNG {@link SkipException}) when the live Cassandra contact point is
 * unreachable or the real 3.12.1 reference jars are absent, so {@code mvn verify -Pit} degrades
 * cleanly off-sandbox.
 */
final class ShimItEnv {

  final String contactPoint; // host:port
  final String host;
  final int port;
  final File moduleDir;
  final File shimJar;
  final File workDir;
  final File scriptsDir;
  final String m2;
  final String cjd;
  final String jdk8;

  private ShimItEnv() {
    this.contactPoint = System.getProperty("shim.contactPoint", "127.0.0.1:9042");
    int colon = contactPoint.lastIndexOf(':');
    if (colon > 0) {
      this.host = contactPoint.substring(0, colon);
      this.port = Integer.parseInt(contactPoint.substring(colon + 1).trim());
    } else {
      this.host = contactPoint;
      this.port = 9042;
    }
    // shim.moduleDir defaults to CWD (which is the module dir when run by Maven).
    this.moduleDir = new File(System.getProperty("shim.moduleDir", ".")).getAbsoluteFile();
    this.workDir =
        new File(System.getProperty("shim.workDir", new File(moduleDir, "target").getPath()))
            .getAbsoluteFile();
    this.shimJar =
        new File(
                System.getProperty(
                    "shim.jar", new File(workDir, "cassandra-driver-shim.jar").getPath()))
            .getAbsoluteFile();
    this.scriptsDir = new File(moduleDir, "src/test/scripts");
    this.m2 =
        System.getProperty(
            "shim.m2", new File(System.getProperty("user.home"), ".m2/repository").getPath());
    this.cjd = System.getProperty("shim.cjd", "/home/agent/projects/cjd-3.12.1");
    this.jdk8 = System.getProperty("shim.jdk8", "/usr/lib/jvm/jdk8u492-b09");
  }

  static ShimItEnv resolve() {
    return new ShimItEnv();
  }

  /** Applies the SHIM_* env vars the harness scripts read, on top of an inherited environment. */
  void applyEnv(Map<String, String> env) {
    env.put("SHIM_HOST", host);
    env.put("SHIM_M2", m2);
    env.put("SHIM_CJD", cjd);
    env.put("SHIM_JDK8", jdk8);
    env.put("SHIM_JAR", shimJar.getPath());
    env.put("SHIM_WORKDIR", workDir.getPath());
  }

  boolean cassandraReachable() {
    try (Socket s = new Socket()) {
      s.connect(new InetSocketAddress(host, port), 3000);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  boolean realJarsPresent() {
    String base = m2 + "/org/apache/cassandra/";
    return new File(base + "cassandra-driver-core/3.12.1/cassandra-driver-core-3.12.1.jar").isFile()
        && new File(base + "cassandra-driver-mapping/3.12.1/cassandra-driver-mapping-3.12.1.jar")
            .isFile()
        && new File(base + "cassandra-driver-extras/3.12.1/cassandra-driver-extras-3.12.1.jar")
            .isFile();
  }

  boolean shimJarPresent() {
    return shimJar.isFile();
  }

  boolean cjdTestClassesPresent() {
    return new File(cjd, "driver-core/target/test-classes").isDirectory();
  }

  boolean jdk8Present() {
    return new File(jdk8, "bin/javac").isFile();
  }

  /** Throws {@link SkipException} with a clear reason if any Layer-2 prerequisite is missing. */
  void requireDifferentialPrereqs() {
    List<String> missing = new ArrayList<>();
    if (!jdk8Present()) missing.add("JDK 8 (shim.jdk8=" + jdk8 + ")");
    if (!shimJarPresent()) missing.add("shim jar (shim.jar=" + shimJar + ")");
    if (!realJarsPresent()) missing.add("real cassandra-driver-3.12.1 jars in " + m2);
    if (!new File(cjd).isDirectory()) missing.add("3.12.1 source checkout (shim.cjd=" + cjd + ")");
    if (!cassandraReachable()) missing.add("live Cassandra at " + contactPoint);
    if (!missing.isEmpty()) {
      throw new SkipException(
          "Layer 2 (differential parity) prerequisites unavailable: " + String.join(", ", missing));
    }
  }

  void requireUpstreamPrereqs() {
    List<String> missing = new ArrayList<>();
    if (!jdk8Present()) missing.add("JDK 8 (shim.jdk8=" + jdk8 + ")");
    if (!shimJarPresent()) missing.add("shim jar (shim.jar=" + shimJar + ")");
    if (!realJarsPresent()) missing.add("real cassandra-driver-3.12.1 jars in " + m2);
    if (!cjdTestClassesPresent()) {
      missing.add("3.12.1 driver test-classes (build them: mvn -f " + cjd + " test-compile)");
    }
    if (!cassandraReachable()) missing.add("live Cassandra at " + contactPoint);
    if (!missing.isEmpty()) {
      throw new SkipException(
          "Layer 2 (upstream suite) prerequisites unavailable: " + String.join(", ", missing));
    }
  }
}
