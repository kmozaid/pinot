/**
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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


public class SegmentPartitionConfig extends BaseJsonConfig {
  public static final int INVALID_NUM_PARTITIONS = -1;

  private final Map<String, ColumnPartitionConfig> _columnPartitionMap;

  @JsonCreator
  public SegmentPartitionConfig(
      @JsonProperty(value = "columnPartitionMap", required = true) Map<String, ColumnPartitionConfig> columnPartitionMap) {
    Preconditions.checkArgument(columnPartitionMap != null, "'columnPartitionMap' must be configured");
    _columnPartitionMap = columnPartitionMap;
  }

  public static SegmentPartitionConfig fromJsonString(String jsonString)
      throws IOException {
    return JsonUtils.stringToObject(jsonString, SegmentPartitionConfig.class);
  }

  public Map<String, ColumnPartitionConfig> getColumnPartitionMap() {
    return _columnPartitionMap;
  }

  /**
   * Returns the partition function for the given column, null if there isn't one.
   *
   * @param column Column for which to return the partition function.
   * @return Partition function for the column.
   */
  @Nullable
  public String getFunctionName(String column) {
    ColumnPartitionConfig columnPartitionConfig = _columnPartitionMap.get(column);
    return (columnPartitionConfig != null) ? columnPartitionConfig.getFunctionName() : null;
  }

  /**
   * Set the number of partitions for all columns.
   *
   * @param numPartitions Number of partitions.
   */
  @JsonIgnore
  public void setNumPartitions(int numPartitions) {
    for (ColumnPartitionConfig columnPartitionConfig : _columnPartitionMap.values()) {
      columnPartitionConfig.setNumPartitions(numPartitions);
    }
  }

  /**
   * Returns the number of partitions for the specified column.
   * Returns {@link #INVALID_NUM_PARTITIONS} if it does not exist for the column.
   *
   * @param column Column for which to get number of partitions.
   * @return Number of partitions of the column.
   */
  public int getNumPartitions(String column) {
    ColumnPartitionConfig config = _columnPartitionMap.get(column);
    return (config != null) ? config.getNumPartitions() : INVALID_NUM_PARTITIONS;
  }
}
