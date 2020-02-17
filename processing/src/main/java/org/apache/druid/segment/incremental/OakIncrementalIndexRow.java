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

import com.oath.oak.OakRBuffer;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex.DimensionDesc;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class OakIncrementalIndexRow extends IncrementalIndexRow
{
  private final OakRBuffer oakDimensions;
  private ByteBuffer dimensions;
  private final OakRBuffer oakAggregations;
  private ByteBuffer aggregations;
  private int dimsLength;
  @Nullable
  private IndexedInts stringDim;
  private int stringDimIndex;

  public OakIncrementalIndexRow(OakRBuffer dimentions,
                                List<DimensionDesc> dimensionDescsList,
                                OakRBuffer aggregations)
  {
    super(0, null, dimensionDescsList);
    this.oakDimensions = dimentions;
    this.oakAggregations = aggregations;
    this.dimensions = oakDimensions.getByteBuffer();
    this.aggregations = null;
    this.dimsLength = -1; // lazy initialization
    this.stringDim = null;
    this.stringDimIndex = -1;
  }

  public void reset()
  {
    this.dimsLength = -1;
    this.stringDim = null;
    this.stringDimIndex = -1;
    this.dimensions = oakDimensions.getByteBuffer();
    this.aggregations = null;
  }

  public ByteBuffer getAggregations()
  {
    if (aggregations == null) {
      aggregations = oakAggregations.getByteBuffer();
    }
    return aggregations;
  }

  @Override
  public long getTimestamp()
  {
    return OakUtils.getTimestamp(dimensions);
  }

  @Override
  public int getDimsLength()
  {
    // Read length only once
    if (dimsLength < 0) {
      dimsLength = OakUtils.getDimsLength(dimensions);
    }
    return dimsLength;
  }

  @Override
  public Object getDim(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return null;
    }
    return OakUtils.getDimValue(dimensions, dimIndex);
  }

  @Override
  public int getRowIndex()
  {
    return OakUtils.getRowIndex(dimensions);
  }

  @Override
  void setRowIndex(int rowIndex)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * bytesInMemory estimates the size of the serialized IncrementalIndexRow key.
   * Each serialized IncrementalRoeIndex contains:
   * 1. a timeStamp
   * 2. the dims array length
   * 3. the rowIndex
   * 4. the serialization of each dim
   * 5. the array (for dims with capabilities of a String ValueType)
   *
   * @return long estimated bytesInMemory
   */
  @Override
  public long estimateBytesInMemory()
  {

    long sizeInBytes = Long.BYTES + 2 * Integer.BYTES;
    for (int dimIndex = 0; dimIndex < getDimsLength(); dimIndex++) {
      sizeInBytes += OakUtils.ALLOC_PER_DIM;
      int dimType = getDimType(dimIndex);
      if (dimType == ValueType.STRING.ordinal()) {
        int dimIndexInBuffer = OakUtils.getDimIndexInBuffer(dimIndex);
        int arraySize = dimensions.getInt(dimIndexInBuffer + OakUtils.ARRAY_LENGTH_OFFSET);
        sizeInBytes += (arraySize * Integer.BYTES);
      }
    }
    return sizeInBytes;
  }

  public boolean isDimInBounds(int dimIndex)
  {
    return dimIndex >= 0 && dimIndex < getDimsLength();
  }

  //Faster to check this way if dim is null instead of deserializing
  @Override
  public boolean isDimNull(int dimIndex)
  {
    if (!isDimInBounds(dimIndex)) {
      return true;
    }
    return OakUtils.isDimNull(dimensions, dimIndex);
  }

  @Override
  public IndexedInts getStringDim(final int dimIndex)
  {
    if (stringDim != null && stringDimIndex == dimIndex) {
      return stringDim;
    }

    stringDim = OakUtils.getStringDim(dimensions, dimIndex);
    stringDimIndex = dimIndex;
    return stringDim;
  }

  /* ---------------- OakRBuffer utils -------------- */

  private int getDimType(int dimIndex)
  {
    if (!isDimInBounds(dimIndex)) {
      return OakUtils.NO_DIM;
    }
    return OakUtils.getDimType(dimensions, dimIndex);
  }
}
