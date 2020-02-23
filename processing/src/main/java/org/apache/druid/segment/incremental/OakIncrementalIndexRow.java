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
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex.DimensionDesc;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class OakIncrementalIndexRow extends IncrementalIndexRow
{
  private final OakRBuffer oakDimensions;
  private long dimensions;
  private final OakRBuffer oakAggregations;
  @Nullable
  private ByteBuffer aggregations;
  private int dimsLength;
  @Nullable
  private OakKey.StringDim stringDim;

  public OakIncrementalIndexRow(OakRBuffer dimensions,
                                List<DimensionDesc> dimensionDescsList,
                                OakRBuffer aggregations)
  {
    super(0, null, dimensionDescsList);
    this.oakDimensions = dimensions;
    this.oakAggregations = aggregations;
    this.dimensions = OakKey.getKeyAddress(oakDimensions.getByteBuffer());
    this.aggregations = null;
    this.dimsLength = -1; // lazy initialization
    this.stringDim = null;
  }

  public void reset()
  {
    dimsLength = -1;
    dimensions = OakKey.getKeyAddress(oakDimensions.getByteBuffer());
    aggregations = null;
    if (stringDim != null) {
      stringDim.reset(dimensions);
    }
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
    return OakKey.getTimestamp(dimensions);
  }

  @Override
  public int getDimsLength()
  {
    // Read length only once
    if (dimsLength < 0) {
      dimsLength = OakKey.getDimsLength(dimensions);
    }
    return dimsLength;
  }

  @Override
  public Object getDim(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return null;
    }
    return OakKey.getDimValue(dimensions, dimIndex);
  }

  @Override
  public int getRowIndex()
  {
    return OakKey.getRowIndex(dimensions);
  }

  @Override
  void setRowIndex(int rowIndex)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * @return long estimated bytesInMemory
   */
  @Override
  public long estimateBytesInMemory()
  {
    return OakKey.getTotalDimSize(dimensions);
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
    return OakKey.isDimNull(dimensions, dimIndex);
  }

  @Override
  public IndexedInts getStringDim(final int dimIndex)
  {
    if (stringDim == null) {
      stringDim = new OakKey.StringDim(dimensions);
    }

    if (stringDim.getDimIndex() != dimIndex) {
      stringDim.setDimIndex(dimIndex);
    }

    return stringDim;
  }
}
