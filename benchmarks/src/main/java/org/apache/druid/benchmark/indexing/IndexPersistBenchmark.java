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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IndexPersistBenchmark
{
  public static final ObjectMapper JSON_MAPPER;
  private static final Logger log = new Logger(IndexPersistBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;

  static {
    NullHandling.initializeForTests();
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        () -> 0
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"rollo"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"none", "small", "moderate", "high"})
  private String rollupOpportunity;

  @Param({"onheap", "oak"})
  private String indexType;

  private IncrementalIndex incIndex;
  private ArrayList<InputRow> rows;
  private BenchmarkSchemaInfo schemaInfo;
  private File tmpDir;

  @Setup
  public void setup()
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    rows = new ArrayList<InputRow>();
    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval().getStartMillis(),
        RandomGenerationBenchmark.getValuesPerTimestamp(rollupOpportunity),
        1000.0
    );

    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = gen.nextRow();
      if (i % 10000 == 0) {
        log.info(i + " rows generated.");
      }
      rows.add(row);
    }
  }

  @Setup(Level.Invocation)
  public void setup2() throws IOException
  {
    incIndex = makeIncIndex();
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = rows.get(i);
      incIndex.add(row);
    }

    tmpDir = FileUtils.createTempDir();
    // log.info("Using temp dir: " + tmpDir.getAbsolutePath());
  }

  @TearDown(Level.Invocation)
  public void teardown() throws IOException
  {
    incIndex.close();
    incIndex = null;
    FileUtils.deleteDirectory(tmpDir);
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
        .setMaxRowCount(rowsPerSegment)
        .build(indexType);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void persistV9(Blackhole blackhole) throws Exception
  {
    File indexFile = INDEX_MERGER_V9.persist(
        incIndex,
        tmpDir,
        new IndexSpec(),
        null
    );

    blackhole.consume(indexFile);
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(IndexPersistBenchmark.class.getSimpleName() + ".persistV9$")
        .warmupIterations(3)
        .measurementIterations(10)
        // .measurementTime(TimeValue.NONE)
        .forks(0)
        .threads(1)
        .param("indexType", "oak")
        .param("rollup", "true")
        .param("rollupOpportunity", "none")
         .param("rowsPerSegment", "1000000")
        .build();

    new Runner(opt).run();
  }
}
