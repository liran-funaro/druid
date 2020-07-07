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

package org.apache.druid.segment.incremental;


import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.yahoo.oak.NativeMemoryAllocator;
import com.yahoo.oak.OakMap;
import com.yahoo.oak.OakMapBuilder;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.OakUnscopedBuffer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import javax.xml.ws.Holder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


/**
 * OakIncrementalIndex has two main attributes that are different from the other IncrementalIndex implementations:
 * 1. It stores both **keys** and **values** off-heap (as opposed to the off-heap implementation that stores only
 *    the **values** off-heap).
 * 2. It is based on OakMap (https://github.com/yahoo/Oak) instead of Java's ConcurrentSkipList.
 * These two changes significantly reduce the number of heap-objects and thus decrease dramatically the GC's memory
 * and performance overhead.
 */
public class OakIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  public static final long DEFAULT_MAX_MEMORY_IN_BYTES = 1L << 35; // 32 GB
  public static final int OAK_CUNK_MAX_ITEMS = 256;

  private final OakFactsHolder facts;
  @Nullable
  private AggsManager aggsManager;
  protected final int maxRowCount;
  protected final long maxBytesInMemory;

  private static final Logger log = new Logger(OakIncrementalIndex.class);

  @Nullable
  private String outOfRowsReason = null;

  public OakIncrementalIndex(IncrementalIndexSchema incrementalIndexSchema,
                             boolean deserializeComplexMetrics,
                             boolean reportParseExceptions,
                             boolean concurrentEventAdd,
                             int maxRowCount,
                             long maxBytesInMemory)
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory == 0 ? DEFAULT_MAX_MEMORY_IN_BYTES : maxBytesInMemory;
    this.facts = new OakFactsHolder(incrementalIndexSchema, dimensionDescsList, aggsManager, this.maxBytesInMemory);
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean countCheck = facts.size() < maxRowCount;
    // if maxBytesInMemory = -1, then ignore sizeCheck
    final boolean sizeCheck = maxBytesInMemory <= 0 || facts.memorySize() < maxBytesInMemory;
    final boolean canAdd = countCheck && sizeCheck;
    if (!countCheck && !sizeCheck) {
      outOfRowsReason = StringUtils.format(
          "Maximum number of rows [%d] and maximum size in bytes [%d] reached",
          maxRowCount,
          maxBytesInMemory
      );
    } else if (!countCheck) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
    } else if (!sizeCheck) {
      outOfRowsReason = StringUtils.format("Maximum size in bytes [%d] reached", maxBytesInMemory);
    }

    return canAdd;
  }

  public void validateSize() throws IndexSizeExceededException
  {
    if (facts.size() > maxRowCount || (maxBytesInMemory > 0 && facts.memorySize() > maxBytesInMemory)) {
      throw new IndexSizeExceededException(
          "Maximum number of rows [%d out of %d] or max size in bytes [%d out of %d] reached",
          facts.size(), maxRowCount,
          facts.memorySize(), maxBytesInMemory
      );
    }
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected BufferAggregator[] initAggs(AggregatorFactory[] metrics,
                                        Supplier<InputRow> rowSupplier,
                                        boolean deserializeComplexMetrics,
                                        boolean concurrentEventAdd)
  {
    this.aggsManager = new AggsManager(metrics, reportParseExceptions);

    for (AggregatorFactory agg : metrics) {
      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
              agg,
              rowSupplier,
              deserializeComplexMetrics
      );

      this.aggsManager.selectors.put(
              agg.getName(),
              new OnheapIncrementalIndex.CachingColumnSelectorFactory(columnSelectorFactory, concurrentEventAdd)
      );
    }

    return aggsManager.aggs;
  }

  @Override
  public void close()
  {
    super.close();
    if (aggsManager != null) {
      aggsManager.close();
    }
    facts.clear();
  }

  @Override
  protected AddToFactsResult addToFacts(InputRow row,
                                        IncrementalIndexRow key,
                                        ThreadLocal<InputRow> rowContainer,
                                        Supplier<InputRow> rowSupplier,
                                        boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    if (!skipMaxRowsInMemoryCheck) {
      // We validate here that we did not exceed the row and memory limitations in previous insertions.
      validateSize();
    }
    boolean added = facts.putIfAbsentAggIfPresent(row, key, rowContainer);

    int rowCount = facts.size();
    long memorySize = facts.memorySize();

    if (added) {
      getNumEntries().set(rowCount);
      getBytesInMemory().set(memorySize);
    }

    return new AddToFactsResult(rowCount, memorySize, new ArrayList<>());
  }

  @Override
  public int getLastRowIndex()
  {
    return this.facts.getLastRowIndex();
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    // We should never get here because we override iterableWithPostAggregations
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    // We should never get here because we override iterableWithPostAggregations
    // This implementation does not need an additional structure to keep rowOffset
    throw new UnsupportedOperationException();
  }

  @Override
  protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggsManager.aggs[aggOffset].getFloat(oakRow.getAggregationsBuffer(),
            oakRow.getAggregationsOffset() + aggsManager.aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggsManager.aggs[aggOffset].getLong(oakRow.getAggregationsBuffer(),
            oakRow.getAggregationsOffset() + aggsManager.aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggsManager.aggs[aggOffset].get(oakRow.getAggregationsBuffer(),
            oakRow.getAggregationsOffset() + aggsManager.aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggsManager.aggs[aggOffset].getDouble(oakRow.getAggregationsBuffer(),
            oakRow.getAggregationsOffset() + aggsManager.aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggsManager.aggs[aggOffset].isNull(oakRow.getAggregationsBuffer(),
            oakRow.getAggregationsOffset() + aggsManager.aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs, final boolean descending)
  {
    // It might be possible to rewrite this function to return a serialized row.
    Function<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>, Row> transformer = entry -> {
      OakUnsafeDirectBuffer keyOakBuff = (OakUnsafeDirectBuffer) entry.getKey();
      OakUnsafeDirectBuffer valueOakBuff = (OakUnsafeDirectBuffer) entry.getValue();
      long serializedKeyAddress = keyOakBuff.getAddress();

      long timeStamp = OakKey.getTimestamp(serializedKeyAddress);
      int dimsLength = OakKey.getDimsLength(serializedKeyAddress);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < dimsLength; ++i) {
        Object dim = OakKey.getDim(serializedKeyAddress, i);
        DimensionDesc dimensionDesc = dimensionDescsList.get(i);
        if (dimensionDesc == null) {
          continue;
        }
        String dimensionName = dimensionDesc.getName();
        DimensionHandler handler = dimensionDesc.getHandler();
        if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
          theVals.put(dimensionName, null);
          continue;
        }
        final DimensionIndexer indexer = dimensionDesc.getIndexer();
        Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualList(dim);
        theVals.put(dimensionName, rowVals);
      }

      BufferAggregator[] aggs = aggsManager.aggs;
      int[] aggsOffsetInBuffer = aggsManager.aggOffsetInBuffer;
      ByteBuffer valueBuff = valueOakBuff.getByteBuffer();
      int valueOffset = valueOakBuff.getOffset();
      for (int i = 0; i < aggs.length; ++i) {
        Object theVal = aggs[i].get(valueBuff, valueOffset + aggsOffsetInBuffer[i]);
        theVals.put(aggsManager.metrics[i].getName(), theVal);
      }

      return new MapBasedRow(timeStamp, theVals);
    };

    return () -> facts.transformIterator(descending, transformer);
  }

  static class AggsManager
  {
    private final AggregatorFactory[] metrics;
    private final Map<String, ColumnSelectorFactory> selectors;
    private final boolean reportParseExceptions;

    /*
    Given a ByteBuffer and an offset inside the buffer, offset + aggOffsetInBuffer[i]
    would give a position in the buffer where the i^th aggregator's value is stored.
     */
    private final int[] aggOffsetInBuffer;
    private final BufferAggregator[] aggs;
    private final int aggsTotalSize;

    public AggsManager(AggregatorFactory[] metrics, boolean reportParseExceptions)
    {
      this.metrics = metrics;
      this.selectors = new HashMap<>();
      this.reportParseExceptions = reportParseExceptions;
      this.aggOffsetInBuffer = new int[metrics.length];
      this.aggs = new BufferAggregator[metrics.length];

      int curAggOffset = 0;
      for (int i = 0; i < metrics.length; i++) {
        aggOffsetInBuffer[i] = curAggOffset;
        curAggOffset += metrics[i].getMaxIntermediateSizeWithNulls();
      }
      this.aggsTotalSize = curAggOffset;
    }

    public void initValue(ByteBuffer aggBuffer, int aggOffset,
                          InputRow row,
                          ThreadLocal<InputRow> rowContainer)
    {
      if (metrics.length > 0 && aggs[aggs.length - 1] == null) {
        synchronized (this) {
          if (aggs[aggs.length - 1] == null) {
            // note: creation of Aggregators is done lazily when at least one row from input is available
            // so that FilteredAggregators could be initialized correctly.
            rowContainer.set(row);
            for (int i = 0; i < metrics.length; i++) {
              final AggregatorFactory agg = metrics[i];
              if (aggs[i] == null) {
                aggs[i] = agg.factorizeBuffered(selectors.get(agg.getName()));
              }
            }
            rowContainer.set(null);
          }
        }
      }

      for (int i = 0; i < metrics.length; i++) {
        aggs[i].init(aggBuffer, aggOffset + aggOffsetInBuffer[i]);
      }
      aggregate(row, rowContainer, aggBuffer, aggOffset);
    }

    public void aggregate(
            InputRow row,
            ThreadLocal<InputRow> rowContainer,
            ByteBuffer aggBuffer,
            int aggOffset
    )
    {
      rowContainer.set(row);

      for (int i = 0; i < metrics.length; i++) {
        final BufferAggregator agg = aggs[i];

        try {
          agg.aggregate(aggBuffer, aggOffset + aggOffsetInBuffer[i]);
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          // This logic is taken from OffheapIncrementalIndex, but OnHeapIncrementalIndex never throws this exception,
          // while MapIncrementalIndex only throws an exception, but never log it.
          // Which behaviour is correct?
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", metrics[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          }
        }
      }

      rowContainer.set(null);
    }

    public int aggsTotalSize()
    {
      return aggsTotalSize;
    }

    public void close()
    {
      selectors.clear();
    }
  }


  private static class OakFactsHolder implements FactsHolder
  {
    @Nullable
    private static NativeMemoryAllocator allocator;
    private final OakMap<IncrementalIndexRow, Row> oak;

    private final long minTimestamp;
    private final List<DimensionDesc> dimensionDescsList;

    private final AtomicInteger rowIndexGenerator;

    private final AggsManager aggsManager;

    private final OakValueSerializer valueSerializer;

    private final boolean rollup;

    public OakFactsHolder(IncrementalIndexSchema incrementalIndexSchema,
                          List<DimensionDesc> dimensionDescsList,
                          AggsManager aggsManager,
                          long maxBytesInMemory)
    {
      this.rowIndexGenerator = new AtomicInteger(0);
      this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
      this.dimensionDescsList = dimensionDescsList;
      this.aggsManager = aggsManager;
      this.rollup = incrementalIndexSchema.isRollup();
      this.valueSerializer = new OakValueSerializer(aggsManager);

      if (allocator == null || allocator.isClosed()) {
        allocator = new NativeMemoryAllocator(JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes());
      }

      OakMapBuilder<IncrementalIndexRow, Row> builder = new OakMapBuilder<>(
          new OakKey.Comparator(dimensionDescsList, this.rollup),
          new OakKey.Serializer(dimensionDescsList, this.rowIndexGenerator),
          valueSerializer,
          getMinIncrementalIndexRow()
      ).setMemoryAllocator(allocator)
          .setMemoryCapacity(maxBytesInMemory)
          .setChunkMaxItems(OAK_CUNK_MAX_ITEMS);
      this.oak = builder.build();
    }

    public Iterator<Row> transformIterator(boolean descending,
                                           Function<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>, Row> transformer)
    {
      OakMap<IncrementalIndexRow, Row> tmpOakMap = descending ? oak.descendingMap() : oak;
      return tmpOakMap.zc().entrySet().stream().map(transformer).iterator();
    }


    private IncrementalIndexRow getMinIncrementalIndexRow()
    {
      return new IncrementalIndexRow(minTimestamp, OakIncrementalIndexRow.NO_DIMS, dimensionDescsList,
              IncrementalIndexRow.EMPTY_ROW_INDEX);
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      return 0;
    }

    @Override
    public long getMinTimeMillis()
    {
      return oak.firstKey().getTimestamp();
    }

    @Override
    public long getMaxTimeMillis()
    {
      return oak.lastKey().getTimestamp();
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      // We should never get here because we override iterableWithPostAggregations
      throw new UnsupportedOperationException();
    }

    /**
     * Generate a new row object for each iterated item.
     */
    private Iterator<IncrementalIndexRow> transformNonStreamIterator(
            Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator)
    {
      return Iterators.transform(iterator, entry ->
              new OakIncrementalIndexRow(entry.getKey(), dimensionDescsList, entry.getValue()));
    }

    /**
     * Since the buffers in the stream iterators are reused, we don't need to create
     * a new row object for each next() call.
     * See {@code OakIncrementalIndexRow.reset()} for more information.
     */
    private Iterator<IncrementalIndexRow> transformStreamIterator(
            Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator)
    {
      Holder<OakIncrementalIndexRow> row = new Holder<>();

      return Iterators.transform(iterator, entry -> {
        if (row.value == null) {
          row.value = new OakIncrementalIndexRow(entry.getKey(), dimensionDescsList, entry.getValue());
        } else {
          row.value.reset();
        }
        return row.value;
      });
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      return () -> {
        IncrementalIndexRow from = null;
        IncrementalIndexRow to = null;
        if (timeStart > getMinTimeMillis()) {
          from = new IncrementalIndexRow(timeStart, OakIncrementalIndexRow.NO_DIMS, dimensionDescsList,
                  IncrementalIndexRow.EMPTY_ROW_INDEX);
        }

        if (timeEnd < getMaxTimeMillis()) {
          to = new IncrementalIndexRow(timeEnd, OakIncrementalIndexRow.NO_DIMS, dimensionDescsList,
                  IncrementalIndexRow.EMPTY_ROW_INDEX);
        }

        OakMap<IncrementalIndexRow, Row> subMap = oak.subMap(from, true, to, false, descending);
        return transformStreamIterator(subMap.zc().entryStreamSet().iterator());
      };
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return () -> transformNonStreamIterator(oak.zc().entrySet().iterator());
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      return () -> transformStreamIterator(oak.zc().entryStreamSet().iterator());
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      throw new UnsupportedOperationException();
    }

    /**
     * This method is different from FactsHolder.putIfAbsent() because it also handles the aggregation
     * in case the key already exits.
     * @return true of a new row is added, or false if we aggregated
     */
    public boolean putIfAbsentAggIfPresent(InputRow row, IncrementalIndexRow key, ThreadLocal<InputRow> rowContainer)
    {
      if (rollup) {
        // If rollup is enabled, we let the serializer assign the row index.
        // The serializer is only called on insertion, so it will not increment the index if the key already exits.
        key.setRowIndex(OakKey.Serializer.ASSIGN_ROW_INDEX_IF_ABSENT);
      } else {
        key.setRowIndex(rowIndexGenerator.getAndIncrement());
      }

      valueSerializer.setRowContainer(rowContainer);

      return oak.zc().putIfAbsentComputeIfPresent(key, row, buffer -> {
        OakUnsafeDirectBuffer oakBuff = (OakUnsafeDirectBuffer) buffer;
        aggsManager.aggregate(row, rowContainer, oakBuff.getByteBuffer(), oakBuff.getOffset());
      });
    }

    @Override
    public void clear()
    {
      oak.close();
    }

    public int size()
    {
      return oak.size();
    }

    public long memorySize()
    {
      return oak.memorySize();
    }

    public int getLastRowIndex()
    {
      return rowIndexGenerator.get() - 1;
    }
  }
}
