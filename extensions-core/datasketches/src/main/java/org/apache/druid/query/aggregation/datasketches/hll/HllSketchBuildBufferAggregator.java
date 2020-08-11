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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.google.common.util.concurrent.Striped;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class HllSketchBuildBufferAggregator implements BufferAggregator
{

  /**
   * for locking per buffer position (power of 2 to make index computation faster)
   */
  private static final int NUM_STRIPES = 64;

  private final ColumnValueSelector<Object> selector;
  private final int lgK;
  private final TgtHllType tgtHllType;
  private final int size;
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(NUM_STRIPES);

  private final ConcurrentHashMap<Integer, HllSketch> sketches = new ConcurrentHashMap<>();
  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  public HllSketchBuildBufferAggregator(
      final ColumnValueSelector<Object> selector,
      final int lgK,
      final TgtHllType tgtHllType,
      final int size
  )
  {
    this.selector = selector;
    this.lgK = lgK;
    this.tgtHllType = tgtHllType;
    this.size = size;
  }

  @Override
  public int getOverheadPerEntryBytes()
  {
    return size;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    int index = indexIncrement.incrementAndGet();
    buf.putInt(position, index);
    HllSketch sketch = new HllSketch(lgK, tgtHllType);
    sketches.put(index, sketch);
  }

  private HllSketch getHll(ByteBuffer buf, int position)
  {
    int index = buf.getInt(position);
    return sketches.get(index);
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final Object value = selector.getObject();
    if (value == null) {
      return;
    }
    final Lock lock = stripedLock.getAt(lockIndex(position)).writeLock();
    lock.lock();
    try {
      final HllSketch sketch = getHll(buf, position);
      HllSketchBuildAggregator.updateSketch(sketch, value);
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final Lock lock = stripedLock.getAt(lockIndex(position)).readLock();
    lock.lock();
    try {
      return getHll(buf, position);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void close()
  {
  }

  @Override
  public float getFloat(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * In very rare cases sketches can exceed given memory, request on-heap memory and move there.
   * We need to identify such sketches and reuse the same objects as opposed to wrapping new memory regions.
   */
  @Override
  public void relocate(final int oldPosition, final int newPosition, final ByteBuffer oldBuf, final ByteBuffer newBuf)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * compute lock index to avoid boxing in Striped.get() call
   *
   * @param position
   *
   * @return index
   */
  static int lockIndex(final int position)
  {
    return smear(position) % NUM_STRIPES;
  }

  /**
   * see https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/Striped.java#L536-L548
   *
   * @param hashCode
   *
   * @return smeared hashCode
   */
  private static int smear(int hashCode)
  {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    // lgK should be inspected because different execution paths exist in HllSketch.update() that is called from
    // @CalledFromHotLoop-annotated aggregate() depending on the lgK.
    // See https://github.com/apache/druid/pull/6893#discussion_r250726028
    inspector.visit("lgK", lgK);
  }
}
