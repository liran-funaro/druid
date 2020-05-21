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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class ConcurrentFullScaleIngestionBenchmark
{
  @Param({"20000000"})
  private int rowsPerSegment;

  @Param({"1000000"})
  private int maxRowsBeforePersist;

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

    ObjectMapper jsonMapper;
    IndexIO indexIO;
    IndexMergerV9 indexMergerV9;

    DataGenerator gen;
    File mergeTmpFile;
    List<File> indexesToMerge;

    @Setup
    public void setup()
    {
      jsonMapper = new DefaultObjectMapper();
      indexIO = new IndexIO(
          jsonMapper,
          () -> 0
      );
      indexMergerV9 = new IndexMergerV9(jsonMapper, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
    }

    @Setup(Level.Invocation)
    public void setup2(ConcurrentFullScaleIngestionBenchmark globalState)
    {
      incIndex = null;

      gen = new DataGenerator(
          globalState.schemaInfo.getColumnSchemas(),
          RNG_SEED,
          globalState.schemaInfo.getDataInterval().getStartMillis(),
          globalState.rollupOpportunity,
          1000.0
      );

      mergeTmpFile = null;
      indexesToMerge = new ArrayList<>();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException
    {
      if (incIndex != null) {
        incIndex.close();
        incIndex = null;
      }

      for (File f : indexesToMerge) {
        FileUtils.deleteDirectory(f);
      }

      if (mergeTmpFile != null) {
        mergeTmpFile.delete();
      }
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
  public void addPersistMerge(Blackhole blackhole, ThreadState threadState) throws Exception
  {
    addPersist(blackhole, threadState);
    threadState.incIndex = null;

    threadState.mergeTmpFile = File.createTempFile("IndexMergeBenchmark-MERGEDFILE-V9-" + System.currentTimeMillis(), ".TEMPFILE");
    threadState.mergeTmpFile.delete();
    threadState.mergeTmpFile.mkdirs();

    List<QueryableIndex> qIndexesToMerge = new ArrayList<>();
    for (File f : threadState.indexesToMerge) {
      QueryableIndex qIndex = threadState.indexIO.loadIndex(f);
      qIndexesToMerge.add(qIndex);
    }

    File mergedFile = threadState.indexMergerV9.mergeQueryableIndex(
        qIndexesToMerge,
        rollup,
        schemaInfo.getAggsArray(),
        threadState.mergeTmpFile,
        new IndexSpec(),
        null
    );

    blackhole.consume(mergedFile);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addPersist(Blackhole blackhole, ThreadState threadState) throws Exception
  {
    threadState.incIndex = makeIncIndex();

    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = threadState.gen.nextRow();
      int rv = threadState.incIndex.add(row).getRowCount();
      blackhole.consume(rv);

      if (threadState.incIndex.size() >= maxRowsBeforePersist || i == rowsPerSegment - 1) {
        persistV9(blackhole, threadState);
        threadState.incIndex.close();
        if (i != rowsPerSegment - 1) {
          threadState.incIndex = makeIncIndex();
        }
      }
    }

    blackhole.consume(threadState.indexesToMerge);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void add(Blackhole blackhole, ThreadState threadState) throws Exception
  {
    threadState.incIndex = makeIncIndex();
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = threadState.gen.nextRow();
      int rv = threadState.incIndex.add(row).getRowCount();
      blackhole.consume(rv);

      if (threadState.incIndex.size() >= maxRowsBeforePersist || i == rowsPerSegment - 1) {
        threadState.incIndex.close();
        if (i != rowsPerSegment - 1) {
          threadState.incIndex = makeIncIndex();
        }
      }
    }
  }

  public void persistV9(Blackhole blackhole, ThreadState threadState) throws Exception
  {
    File indexFile = FileUtils.createTempDir();
    threadState.indexesToMerge.add(indexFile);
    indexFile = threadState.indexMergerV9.persist(
        threadState.incIndex,
        indexFile,
        new IndexSpec(),
        null
    );
    blackhole.consume(indexFile);
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(ConcurrentFullScaleIngestionBenchmark.class.getSimpleName() + ".add$")
        .warmupIterations(3)
        .measurementIterations(10)
        .forks(0)
        .threads(4)
        .param("indexType", "onheap")
        .param("rollup", "false")
        .param("rollupOpportunity", "0")
        .param("maxRowsBeforePersist", "1000000")
        .param("rowsPerSegment", "1000000")
        // .param("rowsPerSegment", "1000000")
        .build();

    new Runner(opt).run();
  }
}
