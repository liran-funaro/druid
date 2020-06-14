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

import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.OakUnscopedBuffer;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.incremental.IncrementalIndex.DimensionDesc;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class OakIncrementalIndexRow extends IncrementalIndexRow
{
  public static final Object[] NO_DIMS = new Object[]{};

  private final OakUnsafeDirectBuffer oakDimensions;
  private ByteBuffer dimensionsBuffer;
  private int dimensionsOffset;
  private final OakUnsafeDirectBuffer oakAggregations;
  @Nullable
  private ByteBuffer aggregationsBuffer;
  private int aggregationsOffset;
  private int dimsLength;
  @Nullable
  private OakKey.StringDim stringDim;

  public OakIncrementalIndexRow(OakUnscopedBuffer dimensions,
                                List<DimensionDesc> dimensionDescsList,
                                OakUnscopedBuffer aggregations)
  {
    super(0, NO_DIMS, dimensionDescsList);
    this.oakDimensions = (OakUnsafeDirectBuffer) dimensions;
    this.oakAggregations = (OakUnsafeDirectBuffer) aggregations;
    this.dimensionsBuffer = oakDimensions.getByteBuffer();
    this.dimensionsOffset = oakDimensions.getOffset();
    this.aggregationsBuffer = null;
    this.aggregationsOffset = 0;
    this.dimsLength = -1; // lazy initialization
    this.stringDim = null;
  }

  /**
   * The key/value of the row is received as OakUnscopedBuffer.
   * When iterating through the index items, we use Oak's stream iterators.
   * In such iterators, the key/value OakUnscopedBuffer objects are reused for each next() call to avoid
   * redundant object instantiation.
   * So whenever we iterate through the index, we don't have to recreate the OakIncrementalIndexRow
   * object. We can just reset it.
   */
  public void reset()
  {
    dimsLength = -1;
    dimensionsBuffer = oakDimensions.getByteBuffer();
    dimensionsOffset = oakDimensions.getOffset();
    aggregationsBuffer = null;
    aggregationsOffset = 0;
    if (stringDim != null) {
      stringDim.reset(dimensionsBuffer, dimensionsOffset);
    }
  }

  private void updateAggregationsBuffer()
  {
    if (aggregationsBuffer == null) {
      aggregationsBuffer = oakAggregations.getByteBuffer();
      aggregationsOffset = oakAggregations.getOffset();
    }
  }

  public ByteBuffer getAggregationsBuffer()
  {
    updateAggregationsBuffer();
    return aggregationsBuffer;
  }

  public int getAggregationsOffset()
  {
    updateAggregationsBuffer();
    return aggregationsOffset;
  }

  @Override
  public long getTimestamp()
  {
    return OakKey.getTimestamp(dimensionsBuffer, dimensionsOffset);
  }

  @Override
  @Nullable
  public Object getDim(int dimIndex)
  {
    if (dimIndex >= getDimsLength()) {
      return null;
    }
    return OakKey.getDim(dimensionsBuffer, dimensionsOffset, dimIndex);
  }

  @Override
  public int getDimsLength()
  {
    // Read length only once
    if (dimsLength < 0) {
      dimsLength = OakKey.getDimsLength(dimensionsBuffer, dimensionsOffset);
    }
    return dimsLength;
  }

  /**
   * This allows faster null validation because it does not need to deserialize it first.
   */
  @Override
  public boolean isDimNull(int dimIndex)
  {
    if (!isDimInBounds(dimIndex)) {
      return true;
    }
    return OakKey.isDimNull(dimensionsBuffer, dimensionsOffset, dimIndex);
  }

  /**
   * This allows faster access to a string dimension because it uses lazy evaluation (no need for deserialization).
   */
  @Override
  public IndexedInts getStringDim(final int dimIndex)
  {
    if (stringDim == null) {
      stringDim = new OakKey.StringDim(dimensionsBuffer, dimensionsOffset);
    }

    if (stringDim.getDimIndex() != dimIndex) {
      stringDim.setDimIndex(dimIndex);
    }

    return stringDim;
  }

  @Override
  public int getRowIndex()
  {
    return OakKey.getRowIndex(dimensionsBuffer, dimensionsOffset);
  }

  @Override
  void setRowIndex(int rowIndex)
  {
    throw new UnsupportedOperationException();
  }

  public boolean isDimInBounds(int dimIndex)
  {
    return dimIndex >= 0 && dimIndex < getDimsLength();
  }

  @Override
  public long estimateBytesInMemory()
  {
    return OakKey.getTotalDimSize(dimensionsBuffer, dimensionsOffset);
  }
}
