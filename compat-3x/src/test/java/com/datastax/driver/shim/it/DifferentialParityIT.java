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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Layer 2 integration test — differential real-vs-shim parity.
 *
 * <p>In {@link #runParity()} this compiles {@code ParityMain} once against the real 3.12.1 API and
 * runs it under (A) the real 3.12.1 driver and (B) the shim + 4.x, both against the same live
 * Cassandra node (reusing the parameterized {@code run_parity.sh}). The two KEY=VALUE outputs are
 * parsed and, per scenario key (via a {@link DataProvider}), asserted identical — giving per-scenario
 * TestNG reporting. Identical output proves behavioral parity and, implicitly, binary compatibility:
 * the 3.x-compiled bytecode links against the shim at runtime.
 *
 * <p>SKIPs gracefully (TestNG {@code SkipException}) when the live Cassandra or the real 3.12.1 jars
 * are unavailable.
 */
public class DifferentialParityIT {

  /** Number of KEY=VALUE observation lines (some values span multiple physical lines). */
  private static final int EXPECTED_LINES = 149;

  private static final Pattern KEY_LINE = Pattern.compile("^[A-Za-z_][A-Za-z0-9_.]*=.*");

  private ShimItEnv env;
  private Map<String, String> real;
  private Map<String, String> shim;
  private int realLineCount;
  private int shimLineCount;

  @BeforeClass(alwaysRun = true)
  public void runParity() throws Exception {
    env = ShimItEnv.resolve();
    env.requireDifferentialPrereqs();

    File script = new File(env.scriptsDir, "run_parity.sh");
    assertThat(script).as("parity runner script").isFile();

    int rc = runScript(script, "both");
    File realOut = new File(env.workDir, "parity/parity_real.txt");
    File shimOut = new File(env.workDir, "parity/parity_shim.txt");
    assertThat(realOut).as("real parity output (run_parity.sh rc=" + rc + ")").isFile();
    assertThat(shimOut).as("shim parity output (run_parity.sh rc=" + rc + ")").isFile();

    List<String> realLines = Files.readAllLines(realOut.toPath(), StandardCharsets.UTF_8);
    List<String> shimLines = Files.readAllLines(shimOut.toPath(), StandardCharsets.UTF_8);
    realLineCount = realLines.size();
    shimLineCount = shimLines.size();
    real = parse(realLines);
    shim = parse(shimLines);
  }

  /** One row per scenario key present in EITHER run, so a missing key on one side is reported. */
  @DataProvider(name = "scenarioKeys")
  public Object[][] scenarioKeys() {
    // BeforeClass runs first; if it SKIPped, TestNG will not invoke this.
    java.util.Set<String> keys = new java.util.TreeSet<>();
    if (real != null) keys.addAll(real.keySet());
    if (shim != null) keys.addAll(shim.keySet());
    Object[][] data = new Object[keys.size()][1];
    int i = 0;
    for (String k : keys) {
      data[i++][0] = k;
    }
    return data;
  }

  @Test(groups = "integration", dataProvider = "scenarioKeys")
  public void scenario_should_match_real(String key) {
    String r = real.get(key);
    String s = shim.get(key);
    assertThat(s).as("shim[" + key + "] must equal real[" + key + "]").isEqualTo(r);
  }

  @Test(groups = "integration")
  public void outputs_should_be_line_for_line_identical() {
    // The authoritative guarantee: the two full outputs are byte-identical.
    assertThat(shimLineCount).as("shim output line count").isEqualTo(realLineCount);
    assertThat(realLineCount).as("total KEY=VALUE observation lines").isEqualTo(EXPECTED_LINES);
    assertThat(shim).as("full observation map (shim == real)").isEqualTo(real);
  }

  private int runScript(File script, String arg) throws IOException, InterruptedException {
    List<String> cmd = new ArrayList<>();
    cmd.add("bash");
    cmd.add(script.getAbsolutePath());
    cmd.add(arg);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(env.moduleDir);
    env.applyEnv(pb.environment());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    // Drain output to the test log (helps diagnose failures / skips).
    try (java.io.BufferedReader br =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println("[run_parity] " + line);
      }
    }
    return p.waitFor();
  }

  /**
   * Parses the KEY=VALUE observation stream. A new record begins on a line matching {@code KEY=...};
   * continuation lines (they start with a tab in the multi-line SchemaBuilder DDL values) are
   * appended to the current record's value, preserving the newline so the reconstructed value is
   * byte-faithful.
   */
  private static Map<String, String> parse(List<String> lines) {
    Map<String, String> map = new LinkedHashMap<>();
    String currentKey = null;
    StringBuilder currentVal = null;
    for (String line : lines) {
      if (KEY_LINE.matcher(line).matches()) {
        if (currentKey != null) {
          map.put(currentKey, currentVal.toString());
        }
        int eq = line.indexOf('=');
        currentKey = line.substring(0, eq);
        currentVal = new StringBuilder(line.substring(eq + 1));
      } else if (currentKey != null) {
        currentVal.append('\n').append(line);
      }
    }
    if (currentKey != null) {
      map.put(currentKey, currentVal.toString());
    }
    return map;
  }
}
