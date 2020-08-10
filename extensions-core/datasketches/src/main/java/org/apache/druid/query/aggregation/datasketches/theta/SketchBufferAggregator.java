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
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SketchBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final int size;

  private final ConcurrentHashMap<Integer, Union> sketches = new ConcurrentHashMap<>();
  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  public SketchBufferAggregator(BaseObjectColumnValueSelector selector, int size, int ignored)
  {
    this.selector = selector;
    this.size = size;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    int index = indexIncrement.incrementAndGet();
    buf.putInt(position, index);
    Union union = (Union) SetOperation.builder().setNominalEntries(size).build(Family.UNION);
    sketches.put(index, union);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.getObject();
    if (update == null) {
      return;
    }

    Union union = getUnion(buf, position);
    SketchAggregator.updateUnion(union, update);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    Union union = getUnion(buf, position);
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return SketchHolder.of(union.getResult(true, null));
  }

  private Union getUnion(ByteBuffer buf, int position)
  {
    int index = buf.getInt(position);
    return sketches.get(index);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    int index = oldBuffer.getInt(oldPosition);
    newBuffer.putInt(newPosition, index);
    Union union = (Union) SetOperation.builder().setNominalEntries(size).build(Family.UNION);
    sketches.put(index, union);
  }
}
