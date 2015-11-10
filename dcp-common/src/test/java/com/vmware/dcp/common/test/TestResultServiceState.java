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

package com.vmware.dcp.common.test;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;

import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.SystemHostInfo;
import com.vmware.dcp.common.test.monitoring.JVMStatsService.JVMStats;

/**
 * Basic statistics being gathered -- more to be added.
 */
public class TestResultServiceState extends ServiceDocument {

    /*
     * Artifact-testcase information.
     */
    public String gitCommit;

    /**
     * user running the test
     */
    public String userName;

    /**
     * The following combination: `test name` + `.` +  `test method name`.
     */
    public String testName;

    /**
     * in the 255.255.255.255 form -- can help debuging by co-location/subnet
     */
    public String testSiteAddress;

    /**
     * Filter perf-monitoring by architecture/operating system/runtime.
     */
    public String arch;
    public String os;
    public String osVersion;
    public String runtimeVersion;
    public double loadAverage;
    public int processorCount;
    public SystemHostInfo systemInfo;
    public JVMStats jvmStats;

    /**
     * Micros since UNIX epoch
     */
    public long startTimeMicros;
    public long endTimeMicros;

    /**
     * Environmental Variables
     */
    public Map<String, String> environment;

    /**
     * Capture whether test failed or succeeded; if failed, log info in reasonFailed
     */
    public boolean isSuccess;
    public String failureReason;

    /**
     * Auto-fill most of the properties -- except host-related (e.g., commit, address) and time-related (e.g., start/end
     * time).
     */
    public static TestResultServiceState populate() {
        TestResultServiceState state = new TestResultServiceState();
        state.userName = System.getProperty("user.name");
        state.arch = System.getProperty("os.arch");
        state.os = System.getProperty("os.name");
        state.osVersion = System.getProperty("os.version");
        state.runtimeVersion = System.getProperty("java.version");
        state.environment = new HashMap<>(System.getenv());
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        state.processorCount = os.getAvailableProcessors();
        state.loadAverage = os.getSystemLoadAverage();
        return state;
    }
}
