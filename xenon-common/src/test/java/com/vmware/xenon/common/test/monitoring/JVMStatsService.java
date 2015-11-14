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

package com.vmware.xenon.common.test.monitoring;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.Utils;

/**
 * Service to periodically gather statistics (e.g., memory, object reclamation) of the current JVM instance.
 */
public class JVMStatsService extends StatefulService {
    public static class JVMStatsFactoryService extends FactoryService {
        public static final String SELF_LINK = "/monitoring/jvmstats";

        public JVMStatsFactoryService() {
            super(JVMStats.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new JVMStatsService();
        }
    }

    /**
     * A timeseries of statistics, gathered periodically through the StatsService.
     */
    public static class JVMStats extends ServiceDocument {

        /**
         * Information gathered per tick of maintenance interval
         */
        public static class JVMStatsSample {
            /**
             * Memory Statistics
             */
            long heapAllocated;
            long heapUsed;
            long heapMax;

            long nonHeapAllocated;
            long nonHeapUsed;
            long nonHeapMax;

            long pendingFinalization;

            /**
             * Class-loading Statistics
             */
            long currentlyLoadedClasses;
            // since the JVM started execution
            long totalLoadedClasses;
            long unloadedClasses;

            /**
             * Runtime Statistics
             */
            long jvmUptimeMillis;

            /**
             * Multi-threading Statistics
             */
            ThreadInfo[] threadInfos;

            /**
             * GC Statistics
             */
            List<Long> collectionCount;
            List<Long> collectionTimeMillis;

            /**
             * Other Info about stats
             */
            long currentSampleMicros;
        }

        /**
         * A time-series of ticks
         */
        List<JVMStatsSample> timeSeries;

    }

    /**
     * Periodic stats service makes use of periodic maintenance -- no need to hold
     * state persistently or have replicas around.
     */
    public JVMStatsService() {
        super(JVMStats.class);
        toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
    }

    /**
     * Periodically collect stats
     *
     * @param op the incoming operation
     */
    @Override
    public void handleMaintenance(Operation op) {
        collectStats(op);
        op.complete();
    }

    /**
     * Get a request to collect stats _now_, bypassing the pre-scheduled interval
     *
     * @param op the incoming operation
     */
    @Override
    public void handlePatch(Operation op) {
        collectStats(op);
        op.complete();
    }

    /**
     * Initiate runtime statistics collection.
     *
     * @param op the incoming operation
     */
    private void collectStats(Operation op) {
        JVMStats.JVMStatsSample statsSample = gatherStats();
        JVMStats stats = getState(op);
        stats.timeSeries.add(statsSample);
    }

    /**
     * Creates a new page (cell) of information gathered during this tic.
     *
     * @throws Throwable
     */
    private static JVMStats.JVMStatsSample gatherStats() {
        JVMStats.JVMStatsSample sample = new JVMStats.JVMStatsSample();

        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        sample.heapAllocated = memory.getHeapMemoryUsage().getCommitted();
        sample.heapUsed = memory.getHeapMemoryUsage().getUsed();
        sample.heapMax = memory.getHeapMemoryUsage().getMax();
        sample.nonHeapAllocated = memory.getNonHeapMemoryUsage().getCommitted();
        sample.nonHeapUsed = memory.getNonHeapMemoryUsage().getUsed();
        sample.nonHeapMax = memory.getNonHeapMemoryUsage().getMax();
        sample.pendingFinalization = memory.getObjectPendingFinalizationCount();

        ClassLoadingMXBean classloading = ManagementFactory.getClassLoadingMXBean();
        sample.currentlyLoadedClasses = classloading.getLoadedClassCount();
        sample.totalLoadedClasses = classloading.getTotalLoadedClassCount();
        sample.unloadedClasses = classloading.getUnloadedClassCount();

        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        sample.jvmUptimeMillis = runtime.getUptime();

        ThreadMXBean thread = ManagementFactory.getThreadMXBean();
        sample.threadInfos = thread.dumpAllThreads(false, false);

        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            sample.collectionCount.add(gcBean.getCollectionCount());
            sample.collectionTimeMillis.add(gcBean.getCollectionTime());
        }

        sample.currentSampleMicros = Utils.getNowMicrosUtc();

        return sample;
    }

}
