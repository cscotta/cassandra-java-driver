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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Layer 2 integration test — the DataStax/Apache Cassandra Java driver 3.12.1's OWN unit suite run
 * against the shim.
 *
 * <p>In {@link #runUpstreamSuite()} this runs each curated candidate {@code *Test} class (TestNG
 * group {@code unit}) under (A) the real 3.12.1 jars and (B) the shim (with NO real {@code
 * com.datastax.driver.*} on the classpath — the 3.x-compiled test bytecode links against the shim by
 * binary compatibility), reusing the parameterized {@code run-upstream.sh}. Per candidate class (via
 * a {@link DataProvider}) it asserts the shim's pass-set ⊇ the real's: every class the real driver
 * executes-and-passes, the shim also passes; classes listed in {@code exclusions.txt} are
 * expected-non-pass and are reported as such.
 *
 * <p>SKIPs gracefully when the live Cassandra, the real 3.12.1 jars, or the 3.12.1 test-classes are
 * unavailable.
 */
public class UpstreamSuiteIT {

  private ShimItEnv env;
  private Map<String, Row> real;
  private Map<String, Row> shim;
  private Set<String> excluded;

  /** One parsed tally row: total/fail/skip, raw status, and annotated verdict. */
  private static final class Row {
    final String fqcn;
    final String total;
    final String status; // PASS / FAIL / NOTESTS / LOADFAIL:... / TIMEOUT
    final String verdict; // PASS / EXCLUDED:<cat> / no-unit-tests / UNEXPECTED

    Row(String fqcn, String total, String status, String verdict) {
      this.fqcn = fqcn;
      this.total = total;
      this.status = status;
      this.verdict = verdict;
    }

    boolean isPass() {
      return "PASS".equals(status);
    }
  }

  @BeforeClass(alwaysRun = true)
  public void runUpstreamSuite() throws Exception {
    env = ShimItEnv.resolve();
    env.requireUpstreamPrereqs();

    File script = new File(env.scriptsDir, "run-upstream.sh");
    assertThat(script).as("upstream runner script").isFile();

    runScript(script, "real");
    runScript(script, "shim");

    File realTsv = new File(env.workDir, "upstream-tests/results-real.tsv");
    File shimTsv = new File(env.workDir, "upstream-tests/results-shim.tsv");
    assertThat(realTsv).as("real results tsv").isFile();
    assertThat(shimTsv).as("shim results tsv").isFile();

    real = parse(realTsv);
    shim = parse(shimTsv);
    excluded =
        loadExclusions(
            new File(env.moduleDir, "src/test/resources/parity/upstream-tests/exclusions.txt"));
  }

  /** One row per curated candidate class. */
  @DataProvider(name = "candidates")
  public Object[][] candidates() {
    Set<String> keys = new LinkedHashSet<>();
    if (real != null) keys.addAll(real.keySet());
    if (shim != null) keys.addAll(shim.keySet());
    Object[][] data = new Object[keys.size()][1];
    int i = 0;
    for (String k : keys) {
      data[i++][0] = k;
    }
    return data;
  }

  @Test(groups = "integration", dataProvider = "candidates")
  public void shim_pass_set_should_contain_real(String fqcn) {
    Row r = real.get(fqcn);
    Row s = shim.get(fqcn);
    assertThat(r).as("real tally for " + fqcn).isNotNull();
    assertThat(s).as("shim tally for " + fqcn).isNotNull();

    if (r.isPass() && !excluded.contains(fqcn)) {
      // The core guarantee: any class the real driver executes-and-passes, the shim must also pass,
      // with the same number of executed methods.
      assertThat(s.isPass())
          .as(
              "shim must PASS "
                  + fqcn
                  + " (real PASS, not excluded); shim status="
                  + s.status
                  + " verdict="
                  + s.verdict)
          .isTrue();
      assertThat(s.total)
          .as("shim executed-method count for " + fqcn)
          .isEqualTo(r.total);
    } else {
      // Excluded / non-pass-on-real classes must not be silently flagged as a genuine regression.
      assertThat(s.verdict)
          .as("verdict for non-guaranteed class " + fqcn + " (should not be UNEXPECTED)")
          .isNotEqualTo("UNEXPECTED");
    }
  }

  @Test(groups = "integration")
  public void aggregate_guarantee_should_hold() {
    int classes = 0;
    int methods = 0;
    for (Map.Entry<String, Row> e : real.entrySet()) {
      String fqcn = e.getKey();
      Row r = e.getValue();
      if (r.isPass() && !excluded.contains(fqcn)) {
        Row s = shim.get(fqcn);
        assertThat(s).as("shim tally for guaranteed class " + fqcn).isNotNull();
        assertThat(s.isPass()).as("shim PASS for " + fqcn).isTrue();
        assertThat(s.total).as("shim method count for " + fqcn).isEqualTo(r.total);
        classes++;
        methods += Integer.parseInt(r.total);
      }
    }
    System.out.println(
        "[upstream] shim pass-set == real pass-set: " + classes + " classes / " + methods + " methods");
    // The documented guarantee (RESULTS.md): 64 public-API classes / 695 methods.
    assertThat(classes).as("guaranteed public-API unit classes").isEqualTo(64);
    assertThat(methods).as("guaranteed public-API unit methods").isEqualTo(695);
  }

  private void runScript(File script, String mode) throws IOException, InterruptedException {
    List<String> cmd = new ArrayList<>();
    cmd.add("bash");
    cmd.add(script.getAbsolutePath());
    cmd.add(mode);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(env.moduleDir);
    env.applyEnv(pb.environment());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println("[run-upstream " + mode + "] " + line);
      }
    }
    p.waitFor();
  }

  /** Parses an annotated results-*.tsv: fqcn TAB total TAB fail TAB skip TAB status TAB verdict. */
  private static Map<String, Row> parse(File tsv) throws IOException {
    Map<String, Row> map = new LinkedHashMap<>();
    for (String line : Files.readAllLines(tsv.toPath(), StandardCharsets.UTF_8)) {
      if (line.isEmpty() || line.startsWith("#")) continue;
      String[] c = line.split("\t", -1);
      if (c.length < 5) continue;
      String verdict = c.length >= 6 ? c[5] : "";
      map.put(c[0], new Row(c[0], c[1], c[4], verdict));
    }
    return map;
  }

  private static Set<String> loadExclusions(File f) throws IOException {
    Set<String> set = new LinkedHashSet<>();
    if (!f.isFile()) return set;
    for (String line : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
      if (line.isEmpty() || line.startsWith("#")) continue;
      String[] c = line.split("\t", -1);
      if (c.length >= 1 && !c[0].trim().isEmpty()) set.add(c[0].trim());
    }
    return set;
  }
}
