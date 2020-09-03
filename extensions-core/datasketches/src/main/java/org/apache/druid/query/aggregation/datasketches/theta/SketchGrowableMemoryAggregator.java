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
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Union;
import org.apache.druid.query.aggregation.datasketches.EncapsulatedGrowableMemoryAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.lang.reflect.Field;

public class SketchGrowableMemoryAggregator extends EncapsulatedGrowableMemoryAggregator<Union>
{
  private final BaseObjectColumnValueSelector selector;
  private final int size;

  public SketchGrowableMemoryAggregator(BaseObjectColumnValueSelector selector, int size)
  {
    super(calcMinSize(size));
    this.selector = selector;
    this.size = size;
  }

  private static int calcMinSize(int size)
  {
    final int preambleLongs = Family.UNION.getMinPreLongs();

    final SetOperationBuilder builder = SetOperation.builder().setNominalEntries(size);
    final int lgNomLongs = builder.getLgNominalEntries();
    final ResizeFactor rf = builder.getResizeFactor();
    final int lgArrLongs = Util.startingSubMultiple(lgNomLongs + 1, rf, Util.MIN_LG_ARR_LONGS);
    return (8 << lgArrLongs) + (preambleLongs << 3);
  }

  @Override
  protected void init(WritableMemory mem)
  {
    SetOperation.builder().setMemoryRequestServer(null).setNominalEntries(size).build(Family.UNION, mem);
  }

  @Override
  protected Union wrap(WritableMemory mem)
  {
    return (Union) SetOperation.wrap(mem);
  }

  @Override
  protected Union wrap(WritableMemory mem, MemoryRequestServer memReqSvr)
  {
    Union obj = wrap(mem);

    try {
      Field gadgetField = obj.getClass().getDeclaredField("gadget_");
      gadgetField.setAccessible(true);
      Object gadget = gadgetField.get(obj);

      Field memReqSvrField = gadget.getClass().getDeclaredField("memReqSvr_");
      memReqSvrField.setAccessible(true);
      memReqSvrField.set(gadget, memReqSvr);
    }
    catch (NoSuchFieldException | IllegalAccessException e) {
      log.error(e, "Failed setting memory server");
    }

    return obj;
  }

  @Override
  protected void aggregate(Union obj)
  {
    Object update = selector.getObject();
    if (update == null) {
      return;
    }

    SketchAggregator.updateUnion(obj, update);
  }

  @Override
  protected Object get(Union obj)
  {
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return SketchHolder.of(obj.getResult(true, null));
  }

  @Override
  protected float getFloat(Union obj)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  protected long getLong(Union obj)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  protected double getDouble(Union obj)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
