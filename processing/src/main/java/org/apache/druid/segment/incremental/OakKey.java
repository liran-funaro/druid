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

import com.oath.oak.OakSerializer;
import com.oath.oak.UnsafeUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;

public final class OakKey
{
  static final int NO_DIM = -1;
  static final int ALLOC_PER_DIM = 12;
  static final int TIME_STAMP_INDEX = 0;
  static final int DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final int ROW_INDEX_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;
  static final int DIMS_INDEX = ROW_INDEX_INDEX + Integer.BYTES;
  // Serialization and deserialization offsets
  static final int VALUE_TYPE_OFFSET = 0;
  static final int DATA_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final int ARRAY_INDEX_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final int ARRAY_LENGTH_OFFSET = ARRAY_INDEX_OFFSET + Integer.BYTES;

  static private final Unsafe unsafe;
  static private final long INT_ARRAY_OFFSET;

  // static constructor - access and create a new instance of Unsafe
  static {
    try {
      Constructor<Unsafe> unsafeConstructor = Unsafe.class.getDeclaredConstructor();
      unsafeConstructor.setAccessible(true);
      unsafe = unsafeConstructor.newInstance();
      INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private OakKey()
  {
  }

  static long getKeyAddress(ByteBuffer buff) {
    return ((DirectBuffer) buff).address() + buff.position();
  }

  static long getTimestamp(long address)
  {
    return unsafe.getLong(address + TIME_STAMP_INDEX);
  }

  static int getRowIndex(long address)
  {
    return unsafe.getInt(address + ROW_INDEX_INDEX);
  }

  static int getDimsLength(long address)
  {
    return unsafe.getInt(address + DIMS_LENGTH_INDEX);
  }

  static int getDimIndexInBuffer(int dimIndex)
  {
    return DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
  }

  static int getDimType(long address, int dimIndex)
  {
    int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
    return unsafe.getInt(address + dimIndexInBuffer + VALUE_TYPE_OFFSET);
  }

  static boolean isDimNull(long address, int dimIndex)
  {
    int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
    return unsafe.getInt(address + dimIndexInBuffer) == NO_DIM;
  }

  static final ValueType[] VALUE_ORDINAL_TYPES = ValueType.values();

  static Object getDimValue(long address, int dimIndex)
  {
    int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
    long dimAddres = address + dimIndexInBuffer;
    int dimType = unsafe.getInt(dimAddres + VALUE_TYPE_OFFSET);

    if (dimType == NO_DIM || dimType < 0 || dimType >= VALUE_ORDINAL_TYPES.length) {
      return null;
    }

    switch (VALUE_ORDINAL_TYPES[dimType]) {
      case DOUBLE:
        return unsafe.getDouble(dimAddres + DATA_OFFSET);
      case FLOAT:
        return unsafe.getFloat(dimAddres + DATA_OFFSET);
      case LONG:
        return unsafe.getLong(dimAddres + DATA_OFFSET);
      case STRING:
        int arrayOffset = unsafe.getInt(dimAddres + ARRAY_INDEX_OFFSET);
        int arraySize = unsafe.getInt(dimAddres + ARRAY_LENGTH_OFFSET);
        int[] array = new int[arraySize];
        unsafe.copyMemory(null, address + arrayOffset, array, INT_ARRAY_OFFSET, arraySize * Integer.BYTES);
        return array;
      default:
        return null;
    }
  }

  /**
   * Estimates the size of the serialized key.
   * Each serialized key contains:
   * 1. a timeStamp
   * 2. the dims array length
   * 3. the rowIndex
   * 4. the serialization of each dim
   * 5. the array (for dims with capabilities of a String ValueType)
   *
   * @return long estimated bytes in memory of the key
   */
  static long getTotalDimSize(long address)
  {
    long sizeInBytes = Long.BYTES + 2 * Integer.BYTES;
    int dimLength = getDimsLength(address);

    for (int dimIndex = 0; dimIndex < dimLength; dimIndex++) {
      sizeInBytes += ALLOC_PER_DIM;
      long dimAddress = address + getDimIndexInBuffer(dimIndex);
      int dimType = unsafe.getInt(dimAddress + VALUE_TYPE_OFFSET);;
      if (dimType == ValueType.STRING.ordinal()) {
        int arraySize = unsafe.getInt(dimAddress + ARRAY_LENGTH_OFFSET);
        sizeInBytes += arraySize * Integer.BYTES;
      }
    }
    return sizeInBytes;
  }

  public static class StringDim implements IndexedInts
  {
    long dimensions;
    int dimIndex;
    int arraySize;
    long arrayAddress;

    public StringDim(long dimensions)
    {
      this.dimensions = dimensions;
      dimIndex = -1;
    }

    public void reset(long dimensions)
    {
      this.dimensions = dimensions;
      dimIndex = -1;
    }

    public void setDimIndex(final int dimIndex)
    {
      this.dimIndex = dimIndex;
      long dimAddress = this.dimensions + getDimIndexInBuffer(dimIndex);
      arrayAddress = dimensions + unsafe.getInt(dimAddress + ARRAY_INDEX_OFFSET);
      arraySize = unsafe.getInt(dimAddress + ARRAY_LENGTH_OFFSET);
    }

    public int getDimIndex()
    {
      return dimIndex;
    }

    @Override
    public int size()
    {
      return arraySize;
    }

    @Override
    public int get(int index)
    {
      return unsafe.getInt(arrayAddress + index * Integer.BYTES);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // nothing to inspect
    }
  }

  public static class Serializer implements OakSerializer<IncrementalIndexRow>
  {
    private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;

    public Serializer(List<IncrementalIndex.DimensionDesc> dimensionDescsList)
    {
      this.dimensionDescsList = dimensionDescsList;
    }

    private ValueType getDimValueType(int dimIndex)
    {
      IncrementalIndex.DimensionDesc dimensionDesc = dimensionDescsList.get(dimIndex);
      if (dimensionDesc == null) {
        return null;
      }
      ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
      if (capabilities == null) {
        return null;
      }
      return capabilities.getType();
    }

    @Override
    public void serialize(IncrementalIndexRow incrementalIndexRow, ByteBuffer byteBuffer)
    {
      long address = getKeyAddress(byteBuffer);

      int dimsLength = incrementalIndexRow.getDimsLength();
      unsafe.putLong(address + TIME_STAMP_INDEX, incrementalIndexRow.getTimestamp());
      unsafe.putInt(address + DIMS_LENGTH_INDEX, dimsLength);
      unsafe.putInt(address + ROW_INDEX_INDEX, incrementalIndexRow.getRowIndex());

      long dimsAddress = address + DIMS_INDEX;
      // the index for writing the int arrays of dims with a STRING type (after all the dim's data)
      long dimsArraysAddresss = dimsAddress + ALLOC_PER_DIM * dimsLength;

      for (int i = 0; i < dimsLength; i++) {
        Object dim = incrementalIndexRow.getDim(i);
        ValueType valueType = null;
        if (dim != null) {
          valueType = getDimValueType(i);
        }

        if (valueType == null) {
          unsafe.putInt(dimsAddress + VALUE_TYPE_OFFSET, NO_DIM);
        } else {
          unsafe.putInt(dimsAddress + VALUE_TYPE_OFFSET, valueType.ordinal());
          switch (valueType) {
            case FLOAT:
              unsafe.putFloat(dimsAddress + DATA_OFFSET, (Float) dim);
              break;
            case DOUBLE:
              unsafe.putDouble(dimsAddress + DATA_OFFSET, (Double) dim);
              break;
            case LONG:
              unsafe.putLong(dimsAddress + DATA_OFFSET, (Long) dim);
              break;
            case STRING:
              int[] arr = (int[]) dim;
              int length = arr.length;
              unsafe.putInt(dimsAddress + ARRAY_INDEX_OFFSET, (int) (dimsArraysAddresss - address));
              unsafe.putInt(dimsAddress + ARRAY_LENGTH_OFFSET, length);

              int lengthBytes = length * Integer.BYTES;
              unsafe.copyMemory(arr, INT_ARRAY_OFFSET, null, dimsArraysAddresss, lengthBytes);
              dimsArraysAddresss += lengthBytes;
              break;
            default:
              unsafe.putInt(dimsAddress + VALUE_TYPE_OFFSET, NO_DIM);
          }
        }

        dimsAddress += ALLOC_PER_DIM;
      }
    }

    @Override
    public IncrementalIndexRow deserialize(ByteBuffer byteBuffer)
    {
      long address = getKeyAddress(byteBuffer);

      long timeStamp = getTimestamp(address);
      int dimsLength = getDimsLength(address);
      int rowIndex = getRowIndex(address);
      Object[] dims = new Object[dimsLength];
      for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
        Object dim = getDimValue(address, dimIndex);
        dims[dimIndex] = dim;
      }
      return new IncrementalIndexRow(timeStamp, dims, dimensionDescsList, rowIndex);
    }

    @Override
    public int calculateSize(IncrementalIndexRow incrementalIndexRow)
    {
      // The ByteBuffer will contain:
      // 1. long:        the timeStamp
      // 2. int:         dims.length
      // 3. int:         rowIndex (used for Plain mode only)
      // 4. multi-value: the serialization of each dim
      // 5. int array:   the array (for dims with capabilities of a String ValueType)
      int dimsLength = incrementalIndexRow.getDimsLength();
      int allocSize = Long.BYTES + (2 * Integer.BYTES) + (ALLOC_PER_DIM * dimsLength);

      // When the dimensionDesc's capabilities are of type ValueType.STRING,
      // the object in timeAndDims.dims is of type int[].
      // In this case, we need to know the array size before allocating the ByteBuffer.
      for (int i = 0; i < dimsLength; i++) {
        if (getDimValueType(i) != ValueType.STRING) {
          continue;
        }

        Object dim = incrementalIndexRow.getDim(i);
        if (dim != null) {
          allocSize += Integer.BYTES * ((int[]) dim).length;
        }
      }

      return allocSize;
    }
  }
}
