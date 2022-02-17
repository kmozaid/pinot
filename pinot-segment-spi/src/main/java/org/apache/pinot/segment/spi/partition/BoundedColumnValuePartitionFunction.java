package org.apache.pinot.segment.spi.partition;

import com.google.common.base.Preconditions;

import java.util.Map;

public class BoundedColumnValuePartitionFunction implements PartitionFunction {
    private static final int DEFAULT_PARTITION_ID = 0;
    private static final String NAME = "BoundedColumnValue";
    private static final String COLUMN_VALUES = "columnValues";
    private final Map<String, String> _functionConfig;
    private final String[] _values;
    private final int _noOfValues;

    public BoundedColumnValuePartitionFunction(Map<String, String> functionConfig) {
        Preconditions.checkArgument(functionConfig != null && functionConfig.size() > 0,
                "functionConfig should be present, specified", functionConfig);
        Preconditions.checkState(functionConfig.get(COLUMN_VALUES) != null,
                "columnValues must be configured");
        _functionConfig = functionConfig;
        _values = functionConfig.get(COLUMN_VALUES).split("\\|");
        _noOfValues = _values.length;
    }

    @Override
    public int getPartition(Object value) {
        String valueStr = String.valueOf(value);
        for (int i = 0; i < _noOfValues; i++) {
            if (_values[i].equals(valueStr)) {
                return i + 1;
            }
        }
        return DEFAULT_PARTITION_ID;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumPartitions() {
        // Number of different column values plus one for default partitionId.
        return _noOfValues + 1;
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
