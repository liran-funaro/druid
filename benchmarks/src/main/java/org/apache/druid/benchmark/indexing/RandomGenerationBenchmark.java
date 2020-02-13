/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.benchmark.indexing;

import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class RandomGenerationBenchmark
{
  @Param({"2000000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"none", "moderate", "high"})
  private String rollupOpportunity;

  private static final int RNG_SEED = 9999;

  static {
    NullHandling.initializeForTests();
  }

  private BenchmarkSchemaInfo schemaInfo;
  private BenchmarkDataGenerator gen;

  @Setup
  public void setup()
  {
    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);
  }

  public static int getValuesPerTimestamp(String rollupOpportunity)
  {
    switch (rollupOpportunity) {
      case "high":
        return 10000;
      case "moderate":
        return 1000;
      case "small":
        return 1;
      default:
        return 0;
    }
  }

  @Setup(Level.Invocation)
  public void setup2()
  {
    gen = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval().getStartMillis(),
        getValuesPerTimestamp(rollupOpportunity),
        1000.0
    );
  }

  @TearDown(Level.Invocation)
  public void tearDown()
  {
    gen = null;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void generate(Blackhole blackhole)
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = gen.nextRow();
      blackhole.consume(row);
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(RandomGenerationBenchmark.class.getSimpleName() + ".generate$")
        .warmupIterations(3)
        .measurementIterations(10)
        .forks(0)
        .threads(1)
        .param("rollupOpportunity", "none")
        .param("rowsPerSegment", "1000000")
        // .param("rowsPerSegment", "1000000")
        .build();

    new Runner(opt).run();
  }
}
