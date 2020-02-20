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

import com.oath.oak.UnsafeUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;

public final class OakKey
{
  static final Integer ALLOC_PER_DIM = 12;
  static final Integer NO_DIM = -1;
  static final Integer TIME_STAMP_INDEX = 0;
  static final Integer DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final Integer ROW_INDEX_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;
  static final Integer DIMS_INDEX = ROW_INDEX_INDEX + Integer.BYTES;
  // Serialization and deserialization offsets
  static final Integer VALUE_TYPE_OFFSET = 0;
  static final Integer DATA_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_INDEX_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_LENGTH_OFFSET = ARRAY_INDEX_OFFSET + Integer.BYTES;

  private OakKey()
  {
  }

  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(buff.position() + TIME_STAMP_INDEX);
  }

  static int getRowIndex(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + ROW_INDEX_INDEX);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + DIMS_LENGTH_INDEX);
  }

  static int getDimIndexInBuffer(int dimIndex)
  {
    return DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
  }

  static int getDimType(ByteBuffer buff, int dimIndex)
  {
    int dimIndexInBuffer = buff.position() + getDimIndexInBuffer(dimIndex);
    return buff.getInt(dimIndexInBuffer + OakKey.VALUE_TYPE_OFFSET);
  }

  static boolean isDimNull(ByteBuffer buff, int dimIndex)
  {
    int dimIndexInBuffer = buff.position() + getDimIndexInBuffer(dimIndex);
    return buff.getInt(dimIndexInBuffer) == OakKey.NO_DIM;
  }

  static final ValueType[] VALUE_ORDINAL_TYPES = ValueType.values();

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    int dimIndexInBuffer = buff.position() + getDimIndexInBuffer(dimIndex);
    int dimType = buff.getInt(dimIndexInBuffer);

    if (dimType == NO_DIM || dimType < 0 || dimType >= VALUE_ORDINAL_TYPES.length) {
      return null;
    }

    switch (VALUE_ORDINAL_TYPES[dimType]) {
      case DOUBLE:
        return buff.getDouble(dimIndexInBuffer + DATA_OFFSET);
      case FLOAT:
        return buff.getFloat(dimIndexInBuffer + DATA_OFFSET);
      case LONG:
        return buff.getLong(dimIndexInBuffer + DATA_OFFSET);
      case STRING:
        int arrayIndex = buff.position() + buff.getInt(dimIndexInBuffer + ARRAY_INDEX_OFFSET);
        int arraySize = buff.getInt(dimIndexInBuffer + ARRAY_LENGTH_OFFSET);
        int[] array = new int[arraySize];
        UnsafeUtils.unsafeCopyBufferToIntArray(buff, arrayIndex, array, arraySize);
        return array;
      default:
        return null;
    }
  }

  public static class StringDim implements IndexedInts {
    ByteBuffer dimensions;
    int dimIndex;
    int arraySize;
    int arrayIndex;

    public StringDim(final ByteBuffer dimensions) {
      this.dimensions = dimensions;
      dimIndex = -1;
    }

    public void reset(final ByteBuffer dimensions) {
      this.dimensions = dimensions;
      dimIndex = -1;
    }

    public void setDimIndex(final int dimIndex) {
      this.dimIndex = dimIndex;
      int dimIndexInBuffer = getDimIndexInBuffer(dimIndex);
      arrayIndex = dimensions.position() + dimensions.getInt(dimIndexInBuffer + ARRAY_INDEX_OFFSET);
      arraySize = dimensions.getInt(dimIndexInBuffer + ARRAY_LENGTH_OFFSET);
    }

    public int getDimIndex() {
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
      return UnsafeUtils.unsafeGetIntFromBuffer(dimensions, arrayIndex, index);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // nothing to inspect
    }
  }
}
