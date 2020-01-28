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
import org.apache.druid.segment.QueryableIndex;
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
public class FullScaleIngestionBenchmark
{
  @Param({"2000000"})
  private int rowsPerSegment;

  @Param({"100000", "500000", "1000000", "1500000", "2000000"})
  private int maxRowsBeforePersist;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"none", "small", "moderate", "high"})
  private String rollupOpportunity;

  @Param({"onheap", "oak"})
  private String indexType;

  // @Param({"/Users/lfunaro/workspace-data/flurry/flurry-data-1561000000000-4.csv"})
  // private String fakeDataPath;

  private static final Logger log = new Logger(FullScaleIngestionBenchmark.class);
  public static final ObjectMapper JSON_MAPPER;
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

  private IncrementalIndex incIndex;
  private BenchmarkSchemaInfo schemaInfo;
  private BenchmarkDataGenerator gen;
  private File persistTmpDir;
  private File mergeTmpFile;
  private List<File> indexesToMerge;
  // RandomAccessFile fakeFile;

  @Setup
  public void setup() throws IOException
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    // fakeFile = new RandomAccessFile(fakeDataPath, "r");
  }

  @Setup(Level.Invocation)
  public void setup2() throws IOException
  {
    incIndex = makeIncIndex();

    gen = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval().getStartMillis(),
        RandomGenerationBenchmark.getValuesPerTimestamp(rollupOpportunity),
        1000.0
    );

    persistTmpDir = FileUtils.createTempDir();
    mergeTmpFile = File.createTempFile("IndexMergeBenchmark-MERGEDFILE-V9-" + System.currentTimeMillis(), ".TEMPFILE");
    mergeTmpFile.delete();
    mergeTmpFile.mkdirs();
    indexesToMerge = new ArrayList<>();
  }

  @TearDown(Level.Invocation)
  public void tearDown() throws IOException
  {
    incIndex.close();
    incIndex = null;
    FileUtils.deleteDirectory(persistTmpDir);
    mergeTmpFile.delete();
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
  public void addPersistMerge(Blackhole blackhole) throws Exception
  {
    addRowsAndPersist(blackhole);

    List<QueryableIndex> qIndexesToMerge = new ArrayList<>();
    for (File f : indexesToMerge) {
      QueryableIndex qIndex = INDEX_IO.loadIndex(f);
      qIndexesToMerge.add(qIndex);
    }

    File mergedFile = INDEX_MERGER_V9.mergeQueryableIndex(
        qIndexesToMerge,
        rollup,
        schemaInfo.getAggsArray(),
        mergeTmpFile,
        new IndexSpec(),
        null
    );

    blackhole.consume(mergedFile);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addRowsAndPersist(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = gen.nextRow();
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);

      if (incIndex.size() >= maxRowsBeforePersist || i == rowsPerSegment - 1) {
        persistV9(blackhole);
        incIndex.close();
        incIndex = makeIncIndex();
      }
    }

    blackhole.consume(indexesToMerge);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void addRows(Blackhole blackhole) throws Exception
  {
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = gen.nextRow();
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);

      if (incIndex.size() >= maxRowsBeforePersist || i == rowsPerSegment - 1) {
        incIndex.close();
        incIndex = makeIncIndex();
      }
    }
  }

  public void persistV9(Blackhole blackhole) throws Exception
  {
    File indexFile = INDEX_MERGER_V9.persist(
        incIndex,
        persistTmpDir,
        new IndexSpec(),
        null
    );
    indexesToMerge.add(indexFile);

    blackhole.consume(indexFile);
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(FullScaleIngestionBenchmark.class.getSimpleName() + ".addRows$")
        .warmupIterations(3)
        .measurementIterations(10)
        .forks(0)
        .threads(1)
        .param("indexType", "oak")
        .param("rollup", "true")
        .param("rollupOpportunity", "moderate")
        .param("maxRowsBeforePersist", "2000000")
        .param("rowsPerSegment", "2000000")
        // .param("rowsPerSegment", "1000000")
        .build();

    new Runner(opt).run();
  }
}
