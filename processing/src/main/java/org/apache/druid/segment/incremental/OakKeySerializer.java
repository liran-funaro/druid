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
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import java.nio.ByteBuffer;
import java.util.List;


public class OakKeySerializer implements OakSerializer<IncrementalIndexRow>
{
  private final List<IncrementalIndex.DimensionDesc> dimensionDescsList;

  public OakKeySerializer(List<IncrementalIndex.DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  private ValueType getDimValueType(int dimIndex, List<IncrementalIndex.DimensionDesc> dimensionDescsList)
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
    long timestamp = incrementalIndexRow.getTimestamp();
    int dimsLength = incrementalIndexRow.getDimsLength();
    int rowIndex = incrementalIndexRow.getRowIndex();

    // calculating buffer indexes for writing the key data
    int buffIndex = byteBuffer.position();  // the first byte for writing the key
    int timeStampIndex = buffIndex + OakUtils.TIME_STAMP_INDEX;    // the timestamp index
    int dimsLengthIndex = buffIndex + OakUtils.DIMS_LENGTH_INDEX;  // the dims array length index
    int rowIndexIndex = buffIndex + OakUtils.ROW_INDEX_INDEX;      // the rowIndex index
    int dimsIndex = buffIndex + OakUtils.DIMS_INDEX;               // the dims array index
    int dimCapacity = OakUtils.ALLOC_PER_DIM;                      // the number of bytes required
    // a certain dim is null
    int dimsArraysIndex = dimsIndex + dimCapacity * dimsLength;    // the index for
    // writing the int arrays
    // of dims with a STRING type
    int dimsArrayOffset = dimsArraysIndex - buffIndex;             // for saving the array position
    // in the buffer
    int valueTypeOffset = OakUtils.VALUE_TYPE_OFFSET;              // offset from the dimIndex
    int dataOffset = OakUtils.DATA_OFFSET;                         // for non-STRING dims
    int arrayIndexOffset = OakUtils.ARRAY_INDEX_OFFSET;            // for STRING dims
    int arrayLengthOffset = OakUtils.ARRAY_LENGTH_OFFSET;          // for STRING dims

    byteBuffer.putLong(timeStampIndex, timestamp);
    byteBuffer.putInt(dimsLengthIndex, dimsLength);
    byteBuffer.putInt(rowIndexIndex, rowIndex);
    for (int i = 0; i < dimsLength; i++) {
      ValueType valueType = getDimValueType(i, dimensionDescsList);
      if (valueType == null || incrementalIndexRow.getDim(i) == null) {
        byteBuffer.putInt(dimsIndex, OakUtils.NO_DIM);
      } else {
        byteBuffer.putInt(dimsIndex + valueTypeOffset, valueType.ordinal());
        switch (valueType) {
          case FLOAT:
            byteBuffer.putFloat(dimsIndex + dataOffset, (Float) incrementalIndexRow.getDim(i));
            break;
          case DOUBLE:
            byteBuffer.putDouble(dimsIndex + dataOffset, (Double) incrementalIndexRow.getDim(i));
            break;
          case LONG:
            byteBuffer.putLong(dimsIndex + dataOffset, (Long) incrementalIndexRow.getDim(i));
            break;
          case STRING:
            int[] arr = (int[]) incrementalIndexRow.getDim(i);
            byteBuffer.putInt(dimsIndex + arrayIndexOffset, dimsArrayOffset);
            byteBuffer.putInt(dimsIndex + arrayLengthOffset, arr.length);
            UnsafeUtils.unsafeCopyIntArrayToBuffer(arr, byteBuffer, dimsArraysIndex, arr.length);
            dimsArraysIndex += (arr.length * Integer.BYTES);
            dimsArrayOffset += (arr.length * Integer.BYTES);
            break;
          default:
            byteBuffer.putInt(dimsIndex, OakUtils.NO_DIM);
        }
      }

      dimsIndex += dimCapacity;
    }
  }

  @Override
  public IncrementalIndexRow deserialize(ByteBuffer byteBuffer)
  {
    long timeStamp = OakUtils.getTimestamp(byteBuffer);
    int dimsLength = OakUtils.getDimsLength(byteBuffer);
    int rowIndex = OakUtils.getRowIndex(byteBuffer);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = OakUtils.getDimValue(byteBuffer, dimIndex);
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
    int allocSize = Long.BYTES + (2 * Integer.BYTES) + (OakUtils.ALLOC_PER_DIM * dimsLength);

    // When the dimensionDesc's capabilities are of type ValueType.STRING,
    // the object in timeAndDims.dims is of type int[].
    // In this case, we need to know the array size before allocating the ByteBuffer.
    for (int i = 0; i < dimsLength; i++) {
      if (getDimValueType(i, dimensionDescsList) != ValueType.STRING) {
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
