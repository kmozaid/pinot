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
package org.apache.pinot.plugin.minion.tasks.purge;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.minion.SegmentPurger;

public class DefaultRecordPurgerFactory implements SegmentPurger.RecordPurgerFactory {

    private static final String _FACTORY_NAME = "default";

    private final String _tableName;
    private final Map<String, String> _purgeTaskConfig;

    public DefaultRecordPurgerFactory(String tableName, Map<String, String> purgeTaskConfig) {
        Preconditions.checkNotNull(tableName);
        _tableName = tableName;
        Preconditions.checkNotNull(purgeTaskConfig);
        _purgeTaskConfig = purgeTaskConfig;
    }

    public String getFactoryName() {
        return _FACTORY_NAME;
    }

    @Override
    public SegmentPurger.RecordPurger getRecordPurger(String rawTableName) {
        if (_tableName.equals(rawTableName)) {
            return row -> {
                Set<String> skipOrgs = Set.of(_purgeTaskConfig.get("skipOrgs").split(","));
                String orgId = String.valueOf(row.getValue(_purgeTaskConfig.get("orgIdColumnName")));
                if (skipOrgs.contains(orgId)) {
                    return false;
                }
                return true;
            };
        }
        return null;
    }

}
