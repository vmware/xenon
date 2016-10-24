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
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;

public class ServiceStatUtils {

    public static ServiceStat getCountStat(Service service, String name) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.timeSeriesStats == null) {
                serviceStat.timeSeriesStats = createTimeSeriesStats(AggregationType.SUM);
            }
        }
        return serviceStat;
    }

    public static void incrementCountStat(Service service, String name, double increment) {
        ServiceStat serviceStat = getCountStat(service, name);
        if (serviceStat != null) {
            incrementCountStat(serviceStat, increment);
        }
    }

    public static ServiceStat getDurationStat(Service service, String name) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.logHistogram == null) {
                serviceStat.logHistogram = new ServiceStatLogHistogram();
            }
            if (serviceStat.timeSeriesStats == null) {
                serviceStat.timeSeriesStats = createTimeSeriesStats(AggregationType.AVG);
            }
        }
        return serviceStat;
    }

    public static void setDurationStat(Service service, String name, double value) {
        ServiceStat serviceStat = getDurationStat(service, name);
        if (serviceStat != null) {
            setStat(serviceStat, value);
        }
    }

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

    public static void setHistogramStat(Service service, String name, double value) {
        ServiceStat serviceStat = getHistogramStat(service, name);
        if (serviceStat != null) {
            setStat(serviceStat, value);
        }
    }

    public static ServiceStat getTimeSeriesStat(Service service, String name) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat serviceStat = service.getStat(name);
        synchronized (serviceStat) {
            if (serviceStat.timeSeriesStats == null) {
                serviceStat.timeSeriesStats = createTimeSeriesStats(AggregationType.AVG);
            }
        }
        return serviceStat;
    }

    public static void setTimeSeriesStat(Service service, String name, double value) {
        ServiceStat serviceStat = getTimeSeriesStat(service, name);
        if (serviceStat != null) {
            setStat(serviceStat, value);
        }
    }

    public static void setStat(ServiceStat serviceStat, double value) {
        synchronized (serviceStat) {
            serviceStat.version++;
            serviceStat.accumulatedValue += value;
            serviceStat.latestValue = value;
            serviceStat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (serviceStat.logHistogram != null) {
                writeLogHistogramValue(serviceStat, value);
            }
            if (serviceStat.timeSeriesStats != null) {
                writeTimeSeriesValue(serviceStat, value);
            }
        }
    }

    public static void adjustStat(ServiceStat serviceStat, double delta) {
        synchronized (serviceStat) {
            serviceStat.version++;
            serviceStat.latestValue += delta;
            serviceStat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (serviceStat.logHistogram != null) {
                writeLogHistogramValue(serviceStat, delta);
            }
            if (serviceStat.timeSeriesStats != null) {
                writeTimeSeriesValue(serviceStat, serviceStat.latestValue);
            }
        }
    }

    public static void incrementCountStat(ServiceStat serviceStat, double increment) {
        synchronized (serviceStat) {
            serviceStat.version++;
            serviceStat.latestValue += increment;
            serviceStat.lastUpdateMicrosUtc = Utils.getNowMicrosUtc();
            if (serviceStat.logHistogram != null) {
                throw new IllegalStateException("Log histogram is not supported for count stats");
            }
            if (serviceStat.timeSeriesStats != null) {
                writeTimeSeriesValue(serviceStat, increment);
            }
        }
    }

    private static TimeSeriesStats[] createTimeSeriesStats(AggregationType aggregationType) {
        TimeSeriesStats[] timeSeriesStats = new TimeSeriesStats[2];
        timeSeriesStats[0] = new TimeSeriesStats(
                ServiceStats.STAT_NAME_SUFFIX_PER_DAY,
                (int) TimeUnit.DAYS.toHours(1),
                TimeUnit.HOURS.toMillis(1),
                EnumSet.of(aggregationType));
        timeSeriesStats[1] = new TimeSeriesStats(
                ServiceStats.STAT_NAME_SUFFIX_PER_HOUR,
                (int) TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MINUTES.toMillis(1),
                EnumSet.of(aggregationType));
        return timeSeriesStats;
    }

    private static void writeLogHistogramValue(ServiceStat serviceStat, double value) {
        int binIndex = 0;
        if (value > 0.0) {
            binIndex = (int) Math.log10(value);
        }
        if (binIndex >= 0 && binIndex < serviceStat.logHistogram.bins.length) {
            serviceStat.logHistogram.bins[binIndex]++;
        }
    }

    private static void writeTimeSeriesValue(ServiceStat seriesStat, double value) {
        for (int i = 0; i < seriesStat.timeSeriesStats.length; i++) {
            if (seriesStat.sourceTimeMicrosUtc != null) {
                seriesStat.timeSeriesStats[i].add(seriesStat.sourceTimeMicrosUtc, value);
            } else {
                seriesStat.timeSeriesStats[i].add(seriesStat.lastUpdateMicrosUtc, value);
            }
        }
    }
}
