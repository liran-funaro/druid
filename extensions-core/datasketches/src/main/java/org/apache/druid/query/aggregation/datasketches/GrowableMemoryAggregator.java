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

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakMap;
import com.yahoo.oak.OakMapBuilder;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.incremental.OakIncrementalIndex;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class GrowableMemoryAggregator implements BufferAggregator
{
  private final AtomicInteger incrementalIndex = new AtomicInteger(0);
  private final OakMap<Integer, Integer> internalStorage;

  private static final Logger log = new Logger(GrowableMemoryAggregator.class);

  public GrowableMemoryAggregator(int minReqSize)
  {
    this.internalStorage = new OakMapBuilder<>(
      OakCommonBuildersFactory.DEFAULT_INT_COMPARATOR,
      OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER,
      new ValueSerializer(minReqSize),
      -1
  ).setMaxBlockSize(OakIncrementalIndex.OAK_MAX_BLOCK_SIZE)
      .setChunkMaxItems(OakIncrementalIndex.OAK_CUNK_MAX_ITEMS)
      .build();
  }

  protected abstract void init(WritableMemory mem);

  protected abstract void aggregate(WritableMemory mem);

  protected abstract Object get(WritableMemory mem);

  protected abstract float getFloat(WritableMemory mem);

  protected abstract long getLong(WritableMemory mem);

  protected abstract double getDouble(WritableMemory mem);

  @Override
  public long getOverheadBytes()
  {
    return internalStorage.memorySize();
  }

  @Override
  public void init(ByteBuffer indexBuf, int indexPos)
  {
    int index = insert(-1);
    indexBuf.putInt(indexPos, index);
  }

  @Override
  public void aggregate(ByteBuffer indexBuf, int indexPos)
  {
    aggregate(getMemory(indexBuf, indexPos));
  }

  @Override
  public Object get(ByteBuffer indexBuf, int indexPos)
  {
    return get(getMemory(indexBuf, indexPos));
  }

  @Override
  public float getFloat(ByteBuffer indexBuf, int indexPos)
  {
    return getFloat(getMemory(indexBuf, indexPos));
  }

  @Override
  public long getLong(ByteBuffer indexBuf, int indexPos)
  {
    return getLong(getMemory(indexBuf, indexPos));
  }

  @Override
  public double getDouble(ByteBuffer indexBuf, int indexPos)
  {
    return getDouble(getMemory(indexBuf, indexPos));
  }

  @Override
  public void close()
  {
    internalStorage.close();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    int index = oldBuffer.getInt(oldPosition);
    newBuffer.putInt(newPosition, index);
  }

  private static WritableMemory getMemory(OakBuffer oakBuf)
  {
    OakUnsafeDirectBuffer buf = (OakUnsafeDirectBuffer) oakBuf;
    return WritableMemory.wrap(buf.getByteBuffer(), ByteOrder.LITTLE_ENDIAN)
        .writableRegion(buf.getOffset(), buf.getLength());
  }

  private static void setMemoryRequestServer(WritableMemory mem, MemoryRequestServer memReqSvr)
  {
    try {
      Field memReqSvrField = mem.getClass().getDeclaredField("memReqSvr");
      memReqSvrField.setAccessible(true); // Force to access the field
      memReqSvrField.set(mem, memReqSvr);
    }
    catch (NoSuchFieldException | IllegalAccessException e) {
      log.error(e, "Failed setting memory server");
    }
  }

  private WritableMemory getMemory(final ByteBuffer indexBuf, final int indexPos)
  {
    return getMemory(indexBuf, indexPos, indexBuf.getInt(indexPos));
  }

  private WritableMemory getMemory(final ByteBuffer outterBuf, final int outterPos, final int index)
  {
    return getMemory(index, new AggregatorMemoryRequestServer(outterBuf, outterPos));
  }

  private WritableMemory getMemory(final int index, AggregatorMemoryRequestServer memReqSvr)
  {
    WritableMemory mem = getMemory(internalStorage.zc().get(index));
    setMemoryRequestServer(mem, memReqSvr);
    return mem;
  }

  private int insert(int capacityBytes)
  {
    final int newIndex = incrementalIndex.incrementAndGet();
    internalStorage.put(newIndex, capacityBytes);
    return newIndex;
  }

  private void replace(ByteBuffer indexBuf, int indexPos, int newIndex)
  {
    final int oldIndex = indexBuf.getInt(indexPos);
    if (oldIndex == newIndex) {
      return;
    }
    indexBuf.putInt(indexPos, newIndex);
    internalStorage.remove(oldIndex);
  }

  private class ValueSerializer implements OakSerializer<Integer>
  {
    final int minReqBytes;

    ValueSerializer(int minReqBytes)
    {
      this.minReqBytes = minReqBytes;
    }

    @Override
    public void serialize(Integer reqBytes, OakScopedWriteBuffer oakScopedWriteBuffer)
    {
      if (reqBytes < 0) {
        init(getMemory(oakScopedWriteBuffer));
      }
    }

    @Override
    public Integer deserialize(OakScopedReadBuffer oakScopedReadBuffer)
    {
      return 0;
    }

    @Override
    public int calculateSize(Integer reqBytes)
    {
      return Math.max(reqBytes, minReqBytes);
    }
  }

  private class AggregatorMemoryRequestServer implements MemoryRequestServer
  {
    final ByteBuffer indexBuf;
    final int indexPos;

    int newIndex;

    AggregatorMemoryRequestServer(ByteBuffer indexBuf, int indexPos)
    {
      this.indexBuf = indexBuf;
      this.indexPos = indexPos;
      this.newIndex = -1;
    }

    @Override
    public WritableMemory request(long capacityBytes)
    {
      newIndex = insert((int) capacityBytes);
      return getMemory(newIndex, this);
    }

    @Override
    public void requestClose(WritableMemory memToClose, WritableMemory newMemory)
    {
      replace(indexBuf, indexPos, newIndex);
    }
  }
}
