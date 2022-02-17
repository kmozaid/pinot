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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.PurgeTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TaskGenerator
public class PurgeTaskGenerator implements PinotTaskGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(PurgeTaskGenerator.class);

    private ClusterInfoAccessor _clusterInfoAccessor;

    @Override
    public void init(ClusterInfoAccessor clusterInfoAccessor) {
        _clusterInfoAccessor = clusterInfoAccessor;
    }

    @Override
    public String getTaskType() {
        return PurgeTask.TASK_TYPE;
    }

    @Override
    public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
        String taskType = PurgeTask.TASK_TYPE;

        List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

        for (TableConfig tableConfig : tableConfigs) {
            String offlineTableName = tableConfig.getTableName();
            if (tableConfig.getTableType() != TableType.OFFLINE) {
                LOGGER.warn("Skip generating task: {} for non-OFFLINE table: {}", taskType, offlineTableName);
                continue;
            }
            TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
            Preconditions.checkState(tableTaskConfig != null);
            Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
            Preconditions.checkState(taskConfigs != null,
                    "Task config shouldn't be null for table: {}", offlineTableName);

            Duration segmentAge = Duration.parse(taskConfigs.get("segmentAge"));
            _clusterInfoAccessor.getSegmentsZKMetadata(offlineTableName).stream()
                    .filter(segmentMetadata -> {
                        Duration thisSegmentAge = Duration.between(
                                Instant.ofEpochMilli(segmentMetadata.getCreationTime()), Instant.now());
                        return segmentAge.minus(thisSegmentAge).isNegative();
                    }).map(segmentZKMetadata -> {
                        Map<String, String> configs = new HashMap<>();
                        configs.put(MinionConstants.TABLE_NAME_KEY, offlineTableName);
                        configs.put(MinionConstants.SEGMENT_NAME_KEY, segmentZKMetadata.getSegmentName());
                        configs.put(MinionConstants.DOWNLOAD_URL_KEY, segmentZKMetadata.getDownloadUrl());
                        configs.put(MinionConstants.UPLOAD_URL_KEY, _clusterInfoAccessor.getVipUrl() + "/segments");
                        configs.put(MinionConstants.ORIGINAL_SEGMENT_CRC_KEY,
                                String.valueOf(segmentZKMetadata.getCrc()));
                        return new PinotTaskConfig(MinionConstants.PurgeTask.TASK_TYPE, configs);
                    }).forEach(pinotTaskConfigs::add);
        }

        return pinotTaskConfigs;
    }

}
