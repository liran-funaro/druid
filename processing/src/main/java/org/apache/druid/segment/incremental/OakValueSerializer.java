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

import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;

import javax.annotation.Nullable;

/**
 * Responsible for the initialization of the aggregators of a new inserted row.
 */
public class OakValueSerializer implements OakSerializer<Row>
{
  private final OakIncrementalIndex.AggsManager aggsManager;

  @Nullable
  private ThreadLocal<InputRow> rowContainer;

  public OakValueSerializer(OakIncrementalIndex.AggsManager aggsManager)
  {
    this.aggsManager = aggsManager;
    this.rowContainer = null;
  }

  void setRowContainer(ThreadLocal<InputRow> rowContainer)
  {
    this.rowContainer = rowContainer;
  }

  @Override
  public void serialize(Row row, OakScopedWriteBuffer buffer)
  {
    OakUnsafeDirectBuffer unsafeBuffer = (OakUnsafeDirectBuffer) buffer;
    aggsManager.initValue(unsafeBuffer.getByteBuffer(), unsafeBuffer.getOffset(), (InputRow) row, rowContainer);
  }

  @Override
  public Row deserialize(OakScopedReadBuffer buffer)
  {
    // cannot deserialize without the IncrementalIndexRow
    throw new UnsupportedOperationException();
  }

  @Override
  public int calculateSize(Row row)
  {
    return aggsManager.aggsTotalSize();
  }
}
