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

package org.apache.druid.query.aggregation.datasketches.theta;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Union;
import org.apache.druid.query.aggregation.datasketches.GrowableMemoryAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

public class SketchBufferAggregator extends GrowableMemoryAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final int size;


  public SketchBufferAggregator(BaseObjectColumnValueSelector selector, int size, int maxIntermediateSize)
  {
    super(calcMinSize(size));
    this.selector = selector;
    this.size = size;
  }

  private static int calcMinSize(int size)
  {
    final int preambleLongs = Family.UNION.getMinPreLongs();

    SetOperationBuilder builder = SetOperation.builder().setNominalEntries(size);

    //Choose RF, minReqBytes, lgArrLongs.
    final int lgRF = builder.getResizeFactor().lg();
    final int lgArrLongs = (lgRF == 0) ? builder.getLgNominalEntries() + 1 : 7;
    return (8 << lgArrLongs) + (preambleLongs << 3);
  }

  private Union getUnion(WritableMemory mem)
  {
    return (Union) SetOperation.wrap(mem);
  }

  @Override
  protected void init(WritableMemory mem)
  {
    SetOperation.builder().setNominalEntries(size).build(Family.UNION, mem);
  }

  @Override
  protected void aggregate(WritableMemory mem)
  {
    Object update = selector.getObject();
    if (update == null) {
      return;
    }

    SketchAggregator.updateUnion(getUnion(mem), update);
  }

  @Override
  protected Object get(WritableMemory mem)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return SketchHolder.of(getUnion(mem).getResult(true, null));
  }

  @Override
  protected float getFloat(WritableMemory mem)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  protected long getLong(WritableMemory mem)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  protected double getDouble(WritableMemory mem)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
