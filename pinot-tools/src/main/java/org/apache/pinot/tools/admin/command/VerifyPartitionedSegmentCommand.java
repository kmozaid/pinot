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
package org.apache.pinot.tools.admin.command;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@CommandLine.Command(name = "VerifyPartitionedSegment")
public class VerifyPartitionedSegmentCommand extends AbstractBaseCommand implements Command {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyPartitionedSegmentCommand.class);
    private static final String TEMP_DIR_NAME = "temp/" + System.currentTimeMillis();

    @CommandLine.Option(names = {"-dataDir"}, required = true,
            description = "Path to data directory containing Pinot segments.")
    private String _dataDir;

    @CommandLine.Option(names = {"-partitionColumn"}, required = true,
            description = "column name on which segments were partitioned.")
    private String _partitionColumn;

    @CommandLine.Option(names = {"-columnValues"}, required = true, split = ",",
            description = "All possible values of partition column.")
    private Set<String> columnValues;

    @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
            description = "Print this message.")
    private boolean _help;

    @Override
    public boolean execute() throws Exception {
        try {
            Map<String, Map<String, String>> columnValueToSegmentPath = new HashMap<>();
            // Add all segments to the segment path map.
            Set<String> segmentPath = new HashSet<>();
            File dataDir = new File(_dataDir);
            File[] files = dataDir.listFiles();
            if (files == null || files.length == 0) {
                throw new RuntimeException("Data directory does not contain any files.");
            }
            for (File file : files) {
                String fileName = file.getName();
                if (file.isDirectory()) {
                    // Uncompressed segment.
                    getSegmentPaths(columnValueToSegmentPath, segmentPath, file, fileName);
                } else if (fileName.toLowerCase().endsWith(".tar.gz") || fileName.toLowerCase().endsWith(".tgz")) {
                    // Compressed segment.
                    File segment = TarGzCompressionUtils.untar(file, new File(TEMP_DIR_NAME, fileName)).get(0);
                    String segmentName = segment.getName();
                    getSegmentPaths(columnValueToSegmentPath, segmentPath, segment, segmentName);
                }
            }

            // Do the validation.
            for (Map.Entry<String, Map<String, String>> entry : columnValueToSegmentPath.entrySet()) {
                String columnValue = entry.getKey();
                for (Map.Entry<String, String> segment : entry.getValue().entrySet()) {
                    String segmentDir = segment.getValue();
                    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader(new File(segmentDir))) {
                        GenericRow row = new GenericRow();
                        while (recordReader.hasNext()) {
                            row = recordReader.next(row);
                        }
                        String value = (String) row.getValue(_partitionColumn);
                        if (!value.equalsIgnoreCase(columnValue)) {
                            throw new RuntimeException("Segment " + segmentDir + " contains row for "
                                    + value + ". It should have contained " + columnValue);
                        }
                    }
                    LOGGER.info("Verified segment {} for column value {}", segment.getKey(), columnValue);
                }
            }
            LOGGER.info("Finish validating segment.");
        } finally {
            FileUtils.deleteQuietly(new File(TEMP_DIR_NAME));
        }
        return true;
    }

    private void getSegmentPaths(Map<String, Map<String, String>> columnValueToSegmentPath,
                                 Set<String> segmentPath,
                                 File file,
                                 String fileName) {
        if (segmentPath.contains(fileName)) {
            throw new RuntimeException("Multiple segments with the same segment name: " + fileName);
        }
        for (String columnValue : columnValues) {
            if (fileName.contains(columnValue)) {
                columnValueToSegmentPath.computeIfAbsent(columnValue, key -> new HashMap<>())
                        .put(fileName, file.getAbsolutePath());
                segmentPath.add(file.getAbsolutePath());
                break;
            }
        }
    }

    @Override
    public String description() {
        return "Verifies column value based partitioned segments";
    }

    @Override
    public boolean getHelp() {
        return _help;
    }

}
