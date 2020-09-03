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

package org.apache.druid.query.aggregation.datasketches;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;

public abstract class EncapsulatedGrowableMemoryAggregator<T> extends GrowableMemoryAggregator
{
  public EncapsulatedGrowableMemoryAggregator(int minReqSize)
  {
    super(minReqSize);
  }

  /**
   * Wrap a memory and its memory request server to be encpsulated in a represnetative object.
   *
   * @param mem       memory to wrap
   * @param memReqSvr (optional) request server for larger memory size (might be null)
   * @return an encapsulated represnetative object
   */
  @CalledFromHotLoop
  protected abstract T wrap(WritableMemory mem, MemoryRequestServer memReqSvr);

  @CalledFromHotLoop
  protected abstract T wrap(WritableMemory mem);

  @CalledFromHotLoop
  protected abstract void aggregate(T obj);

  protected abstract Object get(T obj);

  protected abstract float getFloat(T obj);

  protected abstract long getLong(T obj);

  protected abstract double getDouble(T obj);

  @Override
  public void aggregate(WritableMemory mem, MemoryRequestServer memReqSvr)
  {
    aggregate(wrap(mem, memReqSvr));
  }

  @Override
  public Object get(WritableMemory mem)
  {
    return get(wrap(mem));
  }

  @Override
  public float getFloat(WritableMemory mem)
  {
    return getFloat(wrap(mem));
  }

  @Override
  public long getLong(WritableMemory mem)
  {
    return getLong(wrap(mem));
  }

  @Override
  public double getDouble(WritableMemory mem)
  {
    return getDouble(wrap(mem));
  }
}
