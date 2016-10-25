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

    /**
     * Creates a count stat structure for tracking event occurrences. This structure tracks the
     * total number of occurrences of an event, as well as the number of occurrences in a given
     * period using a time series record.
     */
    public static ServiceStat getCountStat(Service service, String name, int numBins,
            long binDurationMillis) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.timeSeriesStats == null) {
                serviceStat.timeSeriesStats = new TimeSeriesStats(numBins, binDurationMillis,
                        EnumSet.of(AggregationType.SUM));
            } else if (!serviceStat.timeSeriesStats.aggregationType.contains(AggregationType.SUM)) {
                throw new IllegalStateException("Unexpected aggregation type for count stat: "
                        + serviceStat.timeSeriesStats.aggregationType);
            }
        }
        return serviceStat;
    }

    /**
     * Creates a duration stat for tracking event durations. This structure tracks the distribution
     * of event durations in a log histogram structure and the average duration in a given period
     * using a time series record.
     */
    public static ServiceStat getDurationStat(Service service, String name, int numBins,
            long binDurationMillis) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.logHistogram == null) {
                serviceStat.logHistogram = new ServiceStatLogHistogram();
            }
            if (serviceStat.timeSeriesStats == null) {
                serviceStat.timeSeriesStats = new TimeSeriesStats(numBins, binDurationMillis,
                        EnumSet.of(AggregationType.AVG));
            } else if (!serviceStat.timeSeriesStats.aggregationType.contains(AggregationType.AVG)) {
                throw new IllegalStateException("Unexpected aggregation type for duration stat: "
                        + serviceStat.timeSeriesStats.aggregationType);
            }
        }
        return serviceStat;
    }

    /**
     * Creates a service stat with an associated log-10 distribution histogram.
     */
    public static ServiceStat getHistogramStat(Service service, String name) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.logHistogram == null) {
                serviceStat.logHistogram = new ServiceStatLogHistogram();
            }
        }
        return serviceStat;
    }

    /**
     * Creates a service stat with an associated time series record.
     */
    public static ServiceStat getTimeSeriesStat(Service service, String name, int numBins,
            long binDurationMillis) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.timeSeriesStats == null) {
                serviceStat.timeSeriesStats = new TimeSeriesStats(numBins, binDurationMillis,
                        EnumSet.of(AggregationType.AVG));
            }
        }
        return serviceStat;
    }

    /**
     * N.B. This routine writes the delta to the time series record -- unlike adjustStat below,
     * which writes the updated value to the time series record.
     */
    public static void incrementCountStat(ServiceStat serviceStat, double delta) {
        if (serviceStat == null) {
            return;
        }
        synchronized (serviceStat) {
            serviceStat.version++;
            serviceStat.latestValue += delta;
            serviceStat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (serviceStat.logHistogram != null) {
                throw new IllegalStateException("Log histogram is not supported for count stats");
            }
            if (serviceStat.timeSeriesStats != null) {
                writeTimeSeriesValue(serviceStat, delta);
            }
        }
    }

    public static void setStat(ServiceStat serviceStat, double value) {
        if (serviceStat == null) {
            return;
        }
        synchronized (serviceStat) {
            serviceStat.version++;
            serviceStat.accumulatedValue += value;
            serviceStat.latestValue = value;
            serviceStat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (serviceStat.logHistogram != null) {
                writeHistogramValue(serviceStat, value);
            }
            if (serviceStat.timeSeriesStats != null) {
                writeTimeSeriesValue(serviceStat, value);
            }
        }
    }

    public static void adjustStat(ServiceStat serviceStat, double delta) {
        if (serviceStat == null) {
            return;
        }
        synchronized (serviceStat) {
            serviceStat.version++;
            serviceStat.latestValue += delta;
            serviceStat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (serviceStat.logHistogram != null) {
                writeHistogramValue(serviceStat, delta);
            }
            if (serviceStat.timeSeriesStats != null) {
                writeTimeSeriesValue(serviceStat, serviceStat.latestValue);
            }
        }
    }

    private static void writeHistogramValue(ServiceStat serviceStat, double value) {
        int binIndex = 0;
        if (value > 0.0) {
            binIndex = (int) Math.log10(value);
        }
        if (binIndex >= 0 && binIndex < serviceStat.logHistogram.bins.length) {
            serviceStat.logHistogram.bins[binIndex]++;
        }
    }

    private static void writeTimeSeriesValue(ServiceStat serviceStat, double value) {
        if (serviceStat.sourceTimeMicrosUtc != null) {
            serviceStat.timeSeriesStats.add(serviceStat.sourceTimeMicrosUtc, value);
        } else {
            serviceStat.timeSeriesStats.add(serviceStat.lastUpdateMicrosUtc, value);
        }
    }
}
