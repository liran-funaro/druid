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

package org.apache.druid.query.groupby.epinephelinae.vector;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.groupby.ResultRow;

/**
 * Column processor for groupBy dimensions.
 *
 * @see GroupByVectorColumnProcessorFactory
 */
public interface GroupByVectorColumnSelector
{
  /**
   * Get the size in bytes of the key parts generated by this column.
   */
  int getGroupingKeySize();

  /**
   * Write key parts for this column, from startRow (inclusive) to endRow (exclusive), into keySpace starting at
   * keyOffset.
   *
   * @param keySpace  key memory
   * @param keySize   size of the overall key (not just the part for this column)
   * @param keyOffset starting position for the first key part within keySpace
   * @param startRow  starting row (inclusive) within the current vector
   * @param endRow    ending row (exclusive) within the current vector
   */
  // False positive unused inspection warning for "keySize": https://youtrack.jetbrains.com/issue/IDEA-231034
  @SuppressWarnings("unused")
  void writeKeys(WritableMemory keySpace, int keySize, int keyOffset, int startRow, int endRow);

  /**
   * Write key parts for this column into a particular result row.
   *
   * @param keyMemory         key memory
   * @param keyOffset         starting position for this key part within keyMemory
   * @param resultRow         result row to receive key parts
   * @param resultRowPosition position within the result row for this key part
   */
  void writeKeyToResultRow(
      Memory keyMemory,
      int keyOffset,
      ResultRow resultRow,
      int resultRowPosition
  );
}
