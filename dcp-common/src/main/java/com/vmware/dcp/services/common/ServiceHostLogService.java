/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.vmware.dcp.services.common;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatelessService;
import com.vmware.dcp.common.UriUtils;

/**
 * JVM logs for the service host.
 */
public class ServiceHostLogService extends StatelessService {
    public static final String DEFAULT_LOG_FILE_PATH = "/tmp";

    public static final String DEFAULT_SYSTEM_LOG_NAME =
            System.getProperty("os.name").equals("Mac OS X") ?
                    "/var/log/system.log" : "/var/log/syslog";

    private static String DEFAULT_PROCESS_LOG_NAME =
            LogManager.getLogManager().getProperty("java.util.logging.FileHandler.pattern");

    private String logFileName;

    public static String getDefaultProcessLogName() {
        return DEFAULT_PROCESS_LOG_NAME;
    }

    public static void setDefaultProcessLogName(String name) {
        DEFAULT_PROCESS_LOG_NAME = name;
    }

    public static String getDefaultGoDcpProcessLogName() {
        return getDefaultLogFilePath("go-dcp.log");
    }

    public static String getDefaultLogFilePath(String fileName) {
        String base = DEFAULT_LOG_FILE_PATH;

        // Use same directory as Java process log if available
        if (DEFAULT_PROCESS_LOG_NAME != null) {
            File logDir = new File(DEFAULT_PROCESS_LOG_NAME).getParentFile();
            if (logDir != null && logDir.exists()) {
                base = logDir.getPath();
            }
        }

        return base + "/" + fileName;
    }

    public static class LogServiceState extends ServiceDocument {
        /** Log file lines **/
        public List<String> items;
    }

    public ServiceHostLogService(String logFileName) {
        super(LogServiceState.class);
        this.logFileName = logFileName;
    }

    @Override
    public void handleGet(Operation get) {
        Map<String, String> params = UriUtils.parseUriQueryParams(get.getUri());

        String generation = params.get("logFileNumber");
        if (generation == null) {
            generation = "0";
        }
        String file = this.logFileName.replaceFirst("%g", generation);

        int max = Integer.MAX_VALUE;
        String lineCount = params.get("lineCount");
        if (lineCount != null) {
            max = Integer.parseInt(lineCount);
        }

        AsyncLogFileReader reader = AsyncLogFileReader.create(getSelfLink());

        try {
            reader.start(get, file, max);
        } catch (IOException e) {
            get.fail(e);
        }
    }

    public static void setProcessLogFile(String path) {
        ServiceHostLogService.DEFAULT_PROCESS_LOG_NAME = path;
    }
}
