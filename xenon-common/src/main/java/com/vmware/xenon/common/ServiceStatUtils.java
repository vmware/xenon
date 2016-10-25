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

package com.vmware.xenon.common;

import java.util.EnumSet;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;

public class ServiceStatUtils {

    public static ServiceStat getHistogramStat(Service service, String name) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat stat = service.getStat(name);
        synchronized (stat) {
            if (stat.logHistogram == null) {
                stat.logHistogram = new ServiceStatLogHistogram();
            }
        }
        return stat;
    }

    public static ServiceStat getTimeSeriesStat(Service service, String name, int numBins,
            long binDurationMillis, EnumSet<AggregationType> aggregationType) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat stat = service.getStat(name);
        synchronized (stat) {
            if (stat.timeSeriesStats == null) {
                stat.timeSeriesStats = new TimeSeriesStats(numBins, binDurationMillis,
                        aggregationType);
            } else if (!stat.timeSeriesStats.aggregationType.equals(aggregationType)) {
                throw new IllegalStateException("Mismatched aggregation type "
                        + stat.timeSeriesStats.aggregationType);
            }
        }
        return stat;
    }

    public static ServiceStat getTimeSeriesHistogramStat(Service service, String name, int numBins,
            long binDurationMillis, EnumSet<AggregationType> aggregationType) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat stat = service.getStat(name);
        synchronized (stat) {
            if (stat.logHistogram == null) {
                stat.logHistogram = new ServiceStatLogHistogram();
            }
            if (stat.timeSeriesStats == null) {
                stat.timeSeriesStats = new TimeSeriesStats(numBins, binDurationMillis,
                        aggregationType);
            } else if (!stat.timeSeriesStats.aggregationType.equals(aggregationType)) {
                throw new IllegalStateException("Mismatched aggregation type "
                        + stat.timeSeriesStats.aggregationType);
            }
        }
        return stat;
    }
}
