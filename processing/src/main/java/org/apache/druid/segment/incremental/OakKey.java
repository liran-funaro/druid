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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yahoo.oak.OakComparator;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Responsible for the serialization, deserialization, and comparison of keys.
 *
 * It stores the key in an off-heap buffer in the following structure:
 *  - Global metadata (buffer offset + _):
 *      +0: timestamp (long)
 *      +8: dims length (int)
 *     +12: row index (int)
 *    (total of 16 bytes)
 *  - Followed by the dimensions one after the other (buffer offset + 16 + dimIdx*12 + _):
 *     +0: value type (int)
 *     +4: data (int/long/float/double)
 *    (12 bytes per dimension)
 *    Note: For string dimension (int array), the data includes:
 *           +4: the offset of the int array in the buffer (int)
 *           +8: the length of the int array (int)
 *  - The string dimensions arrays are stored after all the dims' data (buffer offset + 16 + dimsLength*12 + _).
 *
 *  Note: the specified offsets are true in most cases, but other JVM implementations may have
 *        different offsets, depending on the size of the primitives in bytes.
 *        The offset calculation below is robust to JVM implementation changes.
 */
public final class OakKey
{
  // The off-heap buffer offsets (buffer offset + _)
  static final int TIME_STAMP_OFFSET = 0;
  static final int DIMS_LENGTH_OFFSET = TIME_STAMP_OFFSET + Long.BYTES;
  static final int ROW_INDEX_OFFSET = DIMS_LENGTH_OFFSET + Integer.BYTES;
  static final int DIMS_OFFSET = ROW_INDEX_OFFSET + Integer.BYTES;
  static final int DIM_VALUE_TYPE_OFFSET = 0;
  static final int DIM_DATA_OFFSET = DIM_VALUE_TYPE_OFFSET + Integer.BYTES;
  static final int STRING_DIM_ARRAY_POS_OFFSET = DIM_DATA_OFFSET;
  static final int STRING_DIM_ARRAY_LENGTH_OFFSET = STRING_DIM_ARRAY_POS_OFFSET + Integer.BYTES;
  static final int SIZE_PER_DIM = DIM_DATA_OFFSET + Collections.max(Arrays.asList(
          // Common data types
          Integer.BYTES,
          Long.BYTES,
          Float.BYTES,
          Double.BYTES,
          // String dimension data type
          Integer.BYTES * 2
  ));

  // Dimension types
  static final ValueType[] VALUE_ORDINAL_TYPES = ValueType.values();
  // Marks a null dimension
  static final int NULL_DIM = -1;

  private OakKey()
  {
  }

  static long getTimestamp(ByteBuffer buffer, int offset)
  {
    return buffer.getLong(offset + TIME_STAMP_OFFSET);
  }

  static int getRowIndex(ByteBuffer buffer, int offset)
  {
    return buffer.getInt(offset + ROW_INDEX_OFFSET);
  }

  static int getDimsLength(ByteBuffer buffer, int offset)
  {
    return buffer.getInt(offset + DIMS_LENGTH_OFFSET);
  }

  static int getDimOffsetInBuffer(int dimIndex)
  {
    return DIMS_OFFSET + (dimIndex * SIZE_PER_DIM);
  }

  static boolean isDimNull(ByteBuffer buffer, int offset, int dimIndex)
  {
    int dimIndexInBuffer = getDimOffsetInBuffer(dimIndex);
    return buffer.getInt(offset + dimIndexInBuffer) == NULL_DIM;
  }

  @Nullable
  static Object getDim(ByteBuffer buffer, int offset, int dimIndex)
  {
    int dimIndexInBuffer = getDimOffsetInBuffer(dimIndex);
    int dimOffset = offset + dimIndexInBuffer;
    int dimValueTypeID = buffer.getInt(dimOffset + DIM_VALUE_TYPE_OFFSET);

    if (dimValueTypeID < 0 || dimValueTypeID >= VALUE_ORDINAL_TYPES.length) {
      return null;
    }

    switch (VALUE_ORDINAL_TYPES[dimValueTypeID]) {
      case DOUBLE:
        return buffer.getDouble(dimOffset + DIM_DATA_OFFSET);
      case FLOAT:
        return buffer.getFloat(dimOffset + DIM_DATA_OFFSET);
      case LONG:
        return buffer.getLong(dimOffset + DIM_DATA_OFFSET);
      case STRING:
        int arrayPos = buffer.getInt(dimOffset + STRING_DIM_ARRAY_POS_OFFSET);
        int arraySize = buffer.getInt(dimOffset + STRING_DIM_ARRAY_LENGTH_OFFSET);
        int[] array = new int[arraySize];
        for (int i = 0; i < arraySize; i++) {
          array[i] = buffer.getInt(offset + arrayPos + i * Integer.BYTES);
        }
        return array;
      default:
        return null;
    }
  }

  static Object[] getAllDims(ByteBuffer buffer, int offset)
  {
    int dimsLength = getDimsLength(buffer, offset);
    return IntStream.range(0, dimsLength).mapToObj(dimIndex -> getDim(buffer, offset, dimIndex)).toArray();
  }

  /**
   * Estimates the size of the serialized key.
   *
   * @return long estimated bytes in memory of the key
   */
  static long getTotalDimSize(ByteBuffer buffer, int offset)
  {
    int dimsLength = getDimsLength(buffer, offset);
    long sizeInBytes = getDimOffsetInBuffer(dimsLength);

    // String dimentions take additional space to store the int array.
    // So we look for such dimentions and add up their array sizes.
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      int dimOffset = offset + getDimOffsetInBuffer(dimIndex);
      int dimType = buffer.getInt(dimOffset + DIM_VALUE_TYPE_OFFSET);
      if (dimType == ValueType.STRING.ordinal()) {
        int arraySize = buffer.getInt(dimOffset + STRING_DIM_ARRAY_LENGTH_OFFSET);
        sizeInBytes += ((long) arraySize) * Integer.BYTES;
      }
    }

    return sizeInBytes;
  }

  /**
   * StringDim purpose is to generate a lazy-evaluation version of a string dimension instead of the array of integers
   * that is returned by getDim(int index).
   */
  public static class StringDim implements IndexedInts
  {
    ByteBuffer dimensionsBuffer;
    int dimensionsOffset;
    int dimIndex;
    boolean initialized;
    int arraySize;
    int arrayOffset;

    public StringDim(ByteBuffer buffer, int offset)
    {
      this.dimensionsBuffer = buffer;
      this.dimensionsOffset = offset;
      dimIndex = -1;
      initialized = false;
    }

    public void reset(ByteBuffer buffer, int offset)
    {
      this.dimensionsBuffer = buffer;
      this.dimensionsOffset = offset;
      dimIndex = -1;
      initialized = false;
    }

    public void setDimIndex(final int dimIndex)
    {
      this.dimIndex = dimIndex;
      initialized = false;
    }

    private void init()
    {
      if (initialized) {
        return;
      }

      int dimOffset = this.dimensionsOffset + getDimOffsetInBuffer(dimIndex);
      arrayOffset = dimensionsOffset + dimensionsBuffer.getInt(dimOffset + STRING_DIM_ARRAY_POS_OFFSET);
      arraySize = dimensionsBuffer.getInt(dimOffset + STRING_DIM_ARRAY_LENGTH_OFFSET);
      initialized = true;
    }

    public int getDimIndex()
    {
      init();
      return dimIndex;
    }

    @Override
    public int size()
    {
      init();
      return arraySize;
    }

    @Override
    public int get(int index)
    {
      init();
      return dimensionsBuffer.getInt(arrayOffset + (index * Integer.BYTES));
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // nothing to inspect
    }
  }

  public static class Serializer implements OakSerializer<IncrementalIndexRow>
  {
    public static final int ASSIGN_ROW_INDEX_IF_ABSENT = Integer.MIN_VALUE;

    private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;
    private final AtomicInteger rowIndexGenerator;

    public Serializer(List<IncrementalIndex.DimensionDesc> dimensionDescsList, AtomicInteger rowIndexGenerator)
    {
      this.dimensionDescsList = dimensionDescsList;
      this.rowIndexGenerator = rowIndexGenerator;
    }

    @Nullable
    private ValueType getDimValueType(int dimIndex)
    {
      IncrementalIndex.DimensionDesc dimensionDesc = dimensionDescsList.get(dimIndex);
      if (dimensionDesc == null) {
        return null;
      }
      ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
      return capabilities.getType();
    }

    @Override
    public void serialize(IncrementalIndexRow incrementalIndexRow, OakScopedWriteBuffer oakBuffer)
    {
      final OakUnsafeDirectBuffer oakDirectBuffer = (OakUnsafeDirectBuffer) oakBuffer;
      final ByteBuffer buffer = oakDirectBuffer.getByteBuffer();
      final int offset = oakDirectBuffer.getOffset();

      long timestamp = incrementalIndexRow.getTimestamp();
      int dimsLength = incrementalIndexRow.getDimsLength();
      int rowIndex = incrementalIndexRow.getRowIndex();
      if (rowIndex == ASSIGN_ROW_INDEX_IF_ABSENT) {
        rowIndex = rowIndexGenerator.getAndIncrement();
        incrementalIndexRow.setRowIndex(rowIndex);
      }
      buffer.putLong(offset + TIME_STAMP_OFFSET, timestamp);
      buffer.putInt(offset + DIMS_LENGTH_OFFSET, dimsLength);
      buffer.putInt(offset + ROW_INDEX_OFFSET, rowIndex);

      int dimsOffset = offset + DIMS_OFFSET;
      // the index for writing the int arrays of the string-dim (after all the dims' data)
      int stringDimArraysPos = getDimOffsetInBuffer(dimsLength);

      for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
        Object dim = incrementalIndexRow.getDim(dimIndex);
        ValueType dimValueType = dim != null ? getDimValueType(dimIndex) : null;
        boolean isDimHaveValue = dimValueType != null;

        int dimValueTypeID = isDimHaveValue ? dimValueType.ordinal() : NULL_DIM;
        buffer.putInt(dimsOffset + DIM_VALUE_TYPE_OFFSET, dimValueTypeID);

        if (isDimHaveValue) {
          switch (dimValueType) {
            case FLOAT:
              buffer.putFloat(dimsOffset + DIM_DATA_OFFSET, (Float) dim);
              break;
            case DOUBLE:
              buffer.putDouble(dimsOffset + DIM_DATA_OFFSET, (Double) dim);
              break;
            case LONG:
              buffer.putLong(dimsOffset + DIM_DATA_OFFSET, (Long) dim);
              break;
            case STRING:
              int[] arr = (int[]) dim;
              int length = arr.length;
              buffer.putInt(dimsOffset + STRING_DIM_ARRAY_POS_OFFSET, stringDimArraysPos);
              buffer.putInt(dimsOffset + STRING_DIM_ARRAY_LENGTH_OFFSET, length);

              for (int i = 0; i < length; i++) {
                buffer.putInt(offset + stringDimArraysPos + i * Integer.BYTES, arr[i]);
              }

              stringDimArraysPos += length * Integer.BYTES;
              break;
          }
        }

        dimsOffset += SIZE_PER_DIM;
      }
    }

    @Override
    public IncrementalIndexRow deserialize(OakScopedReadBuffer oakBuffer)
    {
      final OakUnsafeDirectBuffer oakDirectBuffer = (OakUnsafeDirectBuffer) oakBuffer;
      final ByteBuffer buffer = oakDirectBuffer.getByteBuffer();
      final int offset = oakDirectBuffer.getOffset();

      return new IncrementalIndexRow(
              getTimestamp(buffer, offset),
              getAllDims(buffer, offset),
              dimensionDescsList,
              getRowIndex(buffer, offset)
      );
    }

    @Override
    public int calculateSize(IncrementalIndexRow incrementalIndexRow)
    {
      int dimsLength = incrementalIndexRow.getDimsLength();
      int sizeInBytes = getDimOffsetInBuffer(dimsLength);

      // When the dimensionDesc's capabilities are of type ValueType.STRING,
      // the object in timeAndDims.dims is of type int[].
      // In this case, we need to know the array size before allocating the ByteBuffer.
      for (int i = 0; i < dimsLength; i++) {
        if (getDimValueType(i) != ValueType.STRING) {
          continue;
        }

        Object dim = incrementalIndexRow.getDim(i);
        if (dim != null) {
          sizeInBytes += Integer.BYTES * ((int[]) dim).length;
        }
      }

      return sizeInBytes;
    }
  }

  public static class Comparator implements OakComparator<IncrementalIndexRow>
  {
    private final List<IncrementalIndex.DimensionDesc> dimensionDescs;
    private final boolean rollup;

    public Comparator(List<IncrementalIndex.DimensionDesc> dimensionDescs, boolean rollup)
    {
      this.dimensionDescs = dimensionDescs;
      this.rollup = rollup;
    }

    @Override
    public int compareKeys(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
    {
      int retVal = Longs.compare(lhs.getTimestamp(), rhs.getTimestamp());
      if (retVal != 0) {
        return retVal;
      }

      int lhsDimsLength = lhs.getDimsLength();
      int rhsDimsLength = rhs.getDimsLength();
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.getDim(index);
        final Object rhsIdxs = rhs.getDim(index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff != 0) {
          IncrementalIndexRow largerRow = lengthDiff > 0 ? lhs : rhs;
          retVal = allNull(largerRow, numComparisons) ? 0 : lengthDiff;
        }
      }

      return retVal == 0 ? rowIndexCompare(lhs.getRowIndex(), rhs.getRowIndex()) : retVal;
    }

    @Override
    public int compareSerializedKeys(OakScopedReadBuffer lhsOakBuffer, OakScopedReadBuffer rhsOakBuffer)
    {
      final OakUnsafeDirectBuffer lhsOakDirectBuffer = (OakUnsafeDirectBuffer) lhsOakBuffer;
      final ByteBuffer lhsBuffer = lhsOakDirectBuffer.getByteBuffer();
      final int lhsOffset = lhsOakDirectBuffer.getOffset();

      final OakUnsafeDirectBuffer rhsOakDirectBuffer = (OakUnsafeDirectBuffer) rhsOakBuffer;
      final ByteBuffer rhsBuffer = rhsOakDirectBuffer.getByteBuffer();
      final int rhsOffset = rhsOakDirectBuffer.getOffset();

      int retVal = Longs.compare(getTimestamp(lhsBuffer, lhsOffset), getTimestamp(rhsBuffer, rhsOffset));
      if (retVal != 0) {
        return retVal;
      }

      int lhsDimsLength = getDimsLength(lhsBuffer, lhsOffset);
      int rhsDimsLength = getDimsLength(rhsBuffer, rhsOffset);
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = getDim(lhsBuffer, lhsOffset, index);
        final Object rhsIdxs = getDim(rhsBuffer, rhsOffset, index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff != 0) {
          boolean isAllNull = lengthDiff > 0 ?
              allNull(lhsBuffer, lhsOffset, numComparisons) : allNull(rhsBuffer, rhsOffset, numComparisons);
          retVal = isAllNull ? 0 : lengthDiff;
        }
      }

      return retVal == 0 ?
          rowIndexCompare(getRowIndex(lhsBuffer, lhsOffset), getRowIndex(rhsBuffer, rhsOffset)) : retVal;
    }

    @Override
    public int compareKeyAndSerializedKey(IncrementalIndexRow lhs, OakScopedReadBuffer rhsOakBuffer)
    {
      final OakUnsafeDirectBuffer rhsOakDirectBuffer = (OakUnsafeDirectBuffer) rhsOakBuffer;
      final ByteBuffer rhsBuffer = rhsOakDirectBuffer.getByteBuffer();
      final int rhsOffset = rhsOakDirectBuffer.getOffset();

      int retVal = Longs.compare(lhs.getTimestamp(), getTimestamp(rhsBuffer, rhsOffset));
      if (retVal != 0) {
        return retVal;
      }

      int lhsDimsLength = lhs.getDimsLength();
      int rhsDimsLength = getDimsLength(rhsBuffer, rhsOffset);
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.getDim(index);
        final Object rhsIdxs = getDim(rhsBuffer, rhsOffset, index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff != 0) {
          boolean isAllNull =
              lengthDiff > 0 ? allNull(lhs, numComparisons) : allNull(rhsBuffer, rhsOffset, numComparisons);
          retVal = isAllNull ? 0 : lengthDiff;
        }
      }

      return retVal == 0 ? rowIndexCompare(lhs.getRowIndex(), getRowIndex(rhsBuffer, rhsOffset)) : retVal;
    }

    private int rowIndexCompare(int lsIndex, int rsIndex)
    {
      if (!rollup || lsIndex == IncrementalIndexRow.EMPTY_ROW_INDEX || rsIndex == IncrementalIndexRow.EMPTY_ROW_INDEX) {
        // If we are not in a rollup mode (plain mode), then keys should never be equal.
        // In addition, if one of the keys has no index row (EMPTY_ROW_INDEX) it means it is a lower or upper bound key,
        // so we must compared it.
        return Integer.compare(lsIndex, rsIndex);
      } else {
        return 0;
      }
    }

    private static boolean allNull(IncrementalIndexRow row, int startPosition)
    {
      int dimLength = row.getDimsLength();
      for (int i = startPosition; i < dimLength; i++) {
        if (!row.isDimNull(i)) {
          return false;
        }
      }
      return true;
    }

    private static boolean allNull(ByteBuffer buffer, int offset, int startPosition)
    {
      int dimLength = getDimsLength(buffer, offset);
      for (int i = startPosition; i < dimLength; i++) {
        if (!isDimNull(buffer, offset, i)) {
          return false;
        }
      }
      return true;
    }
  }
}
