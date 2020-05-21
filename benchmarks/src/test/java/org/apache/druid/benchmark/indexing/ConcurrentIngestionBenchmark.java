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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class ConcurrentIngestionBenchmark
{
  @Param({"20000000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"0", "1", "10", "100", "1000", "10000"})
  private int rollupOpportunity;

  @Param({"onheap", "offheap", "oak"})
  private String indexType;

  private static final int RNG_SEED = 9999;

  static {
    NullHandling.initializeForTests();
  }

  private GeneratorSchemaInfo schemaInfo;

  @Setup
  public void setup() throws IOException
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schema);
  }

  @State(Scope.Thread)
  public static class ThreadState
  {
    IncrementalIndex incIndex;

    DataGenerator gen;

    @Setup(Level.Invocation)
    public void setup2(ConcurrentIngestionBenchmark globalState)
    {
      incIndex = null;

      gen = new DataGenerator(
          globalState.schemaInfo.getColumnSchemas(),
          RNG_SEED,
          globalState.schemaInfo.getDataInterval().getStartMillis(),
          globalState.rollupOpportunity,
          1000.0
      );

      incIndex = globalState.makeIncIndex();
    }

    @TearDown(Level.Invocation)
    public void tearDown()
    {
      incIndex.close();
    }
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(rollup)
                .build()
        )
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment * 2)
        .build(indexType);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addRows(Blackhole blackhole, ThreadState threadState) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = threadState.gen.nextRow();
      int rv = threadState.incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ConcurrentIngestionBenchmark.class.getSimpleName() + ".addRows$")
        .warmupIterations(3)
        .measurementIterations(10)
        .forks(0)
        .threads(4)
        .param("indexType", "onheap")
        .param("rollup", "false")
        .param("rollupOpportunity", "0")
        .param("rowsPerSegment", "1000000")
        // .param("rowsPerSegment", "1000000")
        .build();

    new Runner(opt).run();
  }
}
