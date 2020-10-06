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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "realtime", value = RealtimeTuningConfig.class)
})
public abstract class TuningConfig
{
  public static final boolean DEFAULT_LOG_PARSE_EXCEPTIONS = false;
  public static final int DEFAULT_MAX_PARSE_EXCEPTIONS = Integer.MAX_VALUE;
  public static final int DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS = 0;
  public static final int DEFAULT_MAX_ROWS_IN_MEMORY = 1_000_000;

  protected final int maxRowsInMemory;
  protected final long maxBytesInMemory;

  protected TuningConfig(
      @Nullable Integer maxRowsInMemory,
      @Nullable Long maxBytesInMemory
  )
  {
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    /** initializing this to 0, it will be lazily initialized to a value
     * @see #getMaxBytesInMemoryOrDefault() */
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
  }

  /**
   * Maximum number of rows in memory before persisting to local storage
   */
  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  /**
   * Maximum number of bytes (estimated) to store in memory before persisting to local storage
   */
  @JsonProperty
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  /**
   * Maximum number of bytes (estimated) to store in memory before persisting to local storage.
   * If getMaxBytesInMemory() returns 0, the appendable index default will be used.
   */
  public long getMaxBytesInMemoryOrDefault()
  {
    // In the main tuningConfig class constructor, we set the maxBytes to 0 if null to avoid setting
    // maxBytes to max jvm memory of the process that starts first. Instead we set the default based on
    // the actual task node's jvm memory.
    final long maxBytesInMemory = getMaxBytesInMemory();
    if (maxBytesInMemory > 0) {
      return maxBytesInMemory;
    } else if (maxBytesInMemory == 0) {
      // We initially estimated this to be 1/3(max jvm memory), but bytesCurrentlyInMemory only
      // tracks active index and not the index being flushed to disk, to account for that
      // we halved default to 1/6(max jvm memory)
      return JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 6;
    } else {
      return Long.MAX_VALUE;
    }
  }

  public abstract PartitionsSpec getPartitionsSpec();

  public abstract IndexSpec getIndexSpec();

  public abstract IndexSpec getIndexSpecForIntermediatePersists();
}
