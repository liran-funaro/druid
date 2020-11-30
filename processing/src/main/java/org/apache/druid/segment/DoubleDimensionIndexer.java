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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class DoubleDimensionIndexer implements DimensionIndexer<Double, Double, Double>
{
  public static final Comparator<Double> DOUBLE_COMPARATOR = Comparators.naturalNullsFirst();

  private volatile boolean hasNulls = false;

  @Nullable
  @Override
  public Double processRowValsToUnsortedEncodedKeyComponent(@Nullable Object dimValues, boolean reportParseExceptions)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }
    Double d = DimensionHandlerUtils.convertObjectToDouble(dimValues, reportParseExceptions);
    if (d == null) {
      hasNulls = NullHandling.sqlCompatible();
    }
    return d;
  }

  @Override
  public void setSparseIndexed()
  {
    hasNulls = NullHandling.sqlCompatible();
  }

  @Override
  public long estimateEncodedKeyComponentSize(Double key)
  {
    return Double.BYTES;
  }

  @Override
  public Double getUnsortedEncodedValueFromSorted(Double sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public CloseableIndexed<Double> getSortedIndexedValues()
  {
    throw new UnsupportedOperationException("Numeric columns do not support value dictionaries.");
  }

  @Override
  public Double getMinValue()
  {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public Double getMaxValue()
  {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public int getCardinality()
  {
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    ColumnCapabilitiesImpl builder = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.DOUBLE);
    if (hasNulls) {
      builder.setHasNulls(hasNulls);
    }
    return builder;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    return new DoubleWrappingDimensionSelector(makeColumnValueSelector(currEntry, desc), spec.getExtractionFn());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerDoubleColumnSelector implements DoubleColumnSelector
    {

      @Override
      public boolean isNull()
      {
        return hasNulls && currEntry.get().isDimNull(dimIndex);
      }

      @Override
      public double getDouble()
      {
        final Object dim = currEntry.get().getDim(dimIndex);

        if (dim == null) {
          assert NullHandling.replaceWithDefault();
          return 0.0;
        }
        return (Double) dim;
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Double getObject()
      {
        final Object dim = currEntry.get().getDim(dimIndex);

        if (dim == null) {
          return NullHandling.defaultDoubleValue();
        }
        return (Double) dim;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }

    return new IndexerDoubleColumnSelector();
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(@Nullable Double lhs, @Nullable Double rhs)
  {
    return DOUBLE_COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Double lhs, @Nullable Double rhs)
  {
    return Objects.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable Double key)
  {
    return DimensionHandlerUtils.nullToZero(key).hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(Double key)
  {
    return key;
  }

  @Override
  public ColumnValueSelector convertUnsortedValuesToSorted(ColumnValueSelector selectorWithUnsortedValues)
  {
    return selectorWithUnsortedValues;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Double key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
