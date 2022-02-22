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
package org.apache.pinot.segment.spi.partition;

import com.google.common.base.Preconditions;
import java.util.Map;


/**
 * Implementation of {@link PartitionFunction} which partitions based configured column values.
 *
 * "columnPartitionMap": {
 *   "subject": {
 *     "functionName": "BoundedColumnValue",
 *     "functionConfig": {
 *       "columnValues": "Maths|English|Chemistry"
 *     }
 *   }
 * }
 * With this partition config on column "subject", partitionId would be 1 for Maths, 2 for English and so on.
 * partitionId would be "0" for all other values which is not configured but may occur.
 */
public class BoundedColumnValuePartitionFunction implements PartitionFunction {
  private static final int DEFAULT_PARTITION_ID = 0;
  private static final String NAME = "BoundedColumnValue";
  private static final String COLUMN_VALUES = "columnValues";
  private static final String COLUMN_VALUES_DELIMITER = "columnValuesDelimiter";
  private final int _numPartitions;
  private final Map<String, String> _functionConfig;
  private final String[] _values;

  public BoundedColumnValuePartitionFunction(int numPartitions, Map<String, String> functionConfig) {
    Preconditions.checkArgument(functionConfig != null && functionConfig.size() > 0,
        "'functionConfig' should be present, specified", functionConfig);
    Preconditions.checkState(functionConfig.get(COLUMN_VALUES) != null, "columnValues must be configured");
    Preconditions.checkState(functionConfig.get(COLUMN_VALUES_DELIMITER) != null,
        "'columnValuesDelimiter' must be configured");
    _functionConfig = functionConfig;
    _values = functionConfig.get(COLUMN_VALUES).split(functionConfig.get(COLUMN_VALUES_DELIMITER));
    Preconditions.checkState(_values.length == numPartitions,
        "'numPartitions' must be equal to number of column values configured");
    _numPartitions = numPartitions;
  }

  @Override
  public int getPartition(Object value) {
    for (int i = 0; i < _numPartitions; i++) {
      if (_values[i].equalsIgnoreCase(value.toString())) {
        return i + 1;
      }
    }
    return DEFAULT_PARTITION_ID;
  }

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Returns number of column values configured plus one for default partitionId.
   * NOTE: This value is not used to generate partitionId.
   *
   * @return Total number of partitions for the function.
   */
  @Override
  public int getNumPartitions() {
    return _numPartitions + 1;
  }

  @Override
  public Map<String, String> getFunctionConfig() {
    return _functionConfig;
  }

  @Override
  public String toString() {
    return getName();
  }
}
