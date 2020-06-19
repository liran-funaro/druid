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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.utils.JvmUtils;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "realtime", value = RealtimeTuningConfig.class)
})
public interface TuningConfig
{
  boolean DEFAULT_LOG_PARSE_EXCEPTIONS = false;
  int DEFAULT_MAX_PARSE_EXCEPTIONS = Integer.MAX_VALUE;
  int DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS = 0;
  String DEFAULT_INCREMENTAL_INDEX_TYPE = "onheap";
  int DEFAULT_MAX_ROWS_IN_MEMORY = 1_000_000;
  // We initially estimated this to be 1/3(max jvm memory), but bytesCurrentlyInMemory only
  // tracks active index and not the index being flushed to disk, to account for that
  // we halved default to 1/6(max jvm memory)
  long TOTAL_ONHEAP_BYTES = JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes();
  long DEFAULT_ONHEAP_MAX_BYTES_IN_MEMORY = TOTAL_ONHEAP_BYTES / 6;
  // In the realtime node, the entire JVM's direct memory is utilized for ingestion and persist operations.
  // But maxBytesInMemory only refers to the active index size and not to the index being flushed to disk and the
  // persist buffer.
  // To account for that, we set default to 1/2 of the max jvm's direct memory.
  long TOTAL_OFFHEAP_BYTES = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
  long DEFAULT_OFFHEAP_MAX_BYTES_IN_MEMORY = TOTAL_OFFHEAP_BYTES / 2;

  static long getMaxBytesInMemoryOrDefault(final long maxBytesInMemory)
  {
    return getMaxBytesInMemoryOrDefault(maxBytesInMemory, DEFAULT_INCREMENTAL_INDEX_TYPE);
  }

  static long getMaxBytesInMemoryOrDefault(final long maxBytesInMemory, final String incrementalIndexType)
  {
    // In the implementer classes, we set the maxBytes to 0 if null to avoid setting
    // maxBytes to max jvm memory of the process that starts first. Instead we set the default based on
    // the actual task node's jvm memory.
    if (maxBytesInMemory == 0) {
      switch (incrementalIndexType) {
        case "offheap":
        case "oak":
          return DEFAULT_OFFHEAP_MAX_BYTES_IN_MEMORY;
        case "onheap":
        default:
          return DEFAULT_ONHEAP_MAX_BYTES_IN_MEMORY;
      }
    } else if (maxBytesInMemory < 0) {
      switch (incrementalIndexType) {
        case "offheap":
        case "oak":
          return TOTAL_OFFHEAP_BYTES;
        case "onheap":
        default:
          return TOTAL_ONHEAP_BYTES;
      }
    }

    return maxBytesInMemory;
  }
}
