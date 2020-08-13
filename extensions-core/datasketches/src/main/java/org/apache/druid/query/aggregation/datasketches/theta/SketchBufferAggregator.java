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

import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakMap;
import com.yahoo.oak.OakMapBuilder;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.common.OakCommonBuildersFactory;
import org.apache.datasketches.Family;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Union;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.incremental.OakIncrementalIndex;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

public class SketchBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector selector;
  private final int size;
  private final int maxIntermediateSize;
  private final int minReqBytes;

  private final AtomicInteger incrementalIndex = new AtomicInteger(0);
  private final OakMap<Integer, Integer> sketches;

  private static final Logger log = new Logger(SketchBufferAggregator.class);

  public SketchBufferAggregator(BaseObjectColumnValueSelector selector, int size, int maxIntermediateSize)
  {
    this.selector = selector;
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;

    //Choose family, preambleLongs
    final int preambleLongs = Family.UNION.getMinPreLongs();

    SetOperationBuilder builder = SetOperation.builder().setNominalEntries(size);

    //Choose RF, minReqBytes, lgArrLongs.
    final int lgRF = builder.getResizeFactor().lg();
    final int lgArrLongs = (lgRF == 0) ? builder.getLgNominalEntries() + 1 : 7;
    this.minReqBytes = (8 << lgArrLongs) + (preambleLongs << 3);

    this.sketches = new OakMapBuilder<>(
      OakCommonBuildersFactory.DEFAULT_INT_COMPARATOR,
      OakCommonBuildersFactory.DEFAULT_INT_SERIALIZER,
      new OakSerializer<Integer>()
      {
        @Override
        public void serialize(Integer reqBytes, OakScopedWriteBuffer oakScopedWriteBuffer)
        {
          if (reqBytes < 0) {
            createNewUnion(oakScopedWriteBuffer);
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
          return reqBytes < minReqBytes ? minReqBytes : reqBytes;
        }
      },
      -1
  ).setMaxBlockSize(OakIncrementalIndex.OAK_MAX_BLOCK_SIZE)
      .setChunkMaxItems(OakIncrementalIndex.OAK_CUNK_MAX_ITEMS)
      .build();
  }

  @Override
  public long getOverheadBytes()
  {
    return sketches.memorySize();
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    int index = incrementalIndex.incrementAndGet();
    buf.putInt(position, index);
    sketches.put(index, -1);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.getObject();
    if (update == null) {
      return;
    }

    int index = buf.getInt(position);
    Union union = getUnion(buf, position, sketches.zc().get(index), index);
    SketchAggregator.updateUnion(union, update);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    int index = buf.getInt(position);
    Union union = getUnion(buf, position, sketches.zc().get(index), index);
    //in the code below, I am returning SetOp.getResult(true, null)
    //"true" returns an ordered sketch but slower to compute than unordered sketch.
    //however, advantage of ordered sketch is that they are faster to "union" later
    //given that results from the aggregator will be combined further, it is better
    //to return the ordered sketch here
    return SketchHolder.of(union.getResult(true, null));
  }

  private WritableMemory getMemory(OakBuffer oakBuf)
  {
    OakUnsafeDirectBuffer buf = (OakUnsafeDirectBuffer) oakBuf;
    return WritableMemory.wrap(buf.getByteBuffer(), ByteOrder.LITTLE_ENDIAN)
        .writableRegion(buf.getOffset(), buf.getLength());
  }

  private Union createNewUnion(OakBuffer oakBuf)
  {
    WritableMemory mem = getMemory(oakBuf);
    return (Union) SetOperation.builder().setNominalEntries(size).build(Family.UNION, mem);
  }

  private Union getUnion(ByteBuffer outterBuf, int outterPos, OakBuffer oakBuf, int index)
  {
    WritableMemory mem = getMemory(oakBuf);
    Union ret = (Union) SetOperation.wrap(mem);

    try {
      Field gadgetField = ret.getClass().getDeclaredField("gadget_");
      gadgetField.setAccessible(true); // Force to access the field
      Object gadget = gadgetField.get(ret);

      Field memReqSvrField = gadget.getClass().getDeclaredField("memReqSvr_");
      memReqSvrField.setAccessible(true); // Force to access the field
      memReqSvrField.set(gadget, new MemoryRequestServer()
      {
        WritableMemory curMem = mem;
        WritableMemory newMem = null;

        int curIndex = index;
        int newIndex = -1;

        @Override
        public WritableMemory request(long capacityBytes)
        {
          newIndex = incrementalIndex.incrementAndGet();
          sketches.put(newIndex, (int) capacityBytes);
          newMem = getMemory(sketches.zc().get(newIndex));
          return newMem;
        }

        @Override
        public void requestClose(WritableMemory memToClose, WritableMemory newMemory)
        {
          outterBuf.putInt(outterPos, newIndex);
          sketches.remove(curIndex);

          curMem = newMem;
          newMem = null;
          curIndex = newIndex;
          newIndex = -1;
        }
      });
    }
    catch (NoSuchFieldException | IllegalAccessException e) {
      log.error(e, "Failed setting memory server");
    }

    return ret;
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
    sketches.close();
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
  }

}
