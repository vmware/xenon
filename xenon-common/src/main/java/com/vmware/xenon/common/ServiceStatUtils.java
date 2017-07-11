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
import java.util.function.Supplier;

import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;

public final class ServiceStatUtils {

    private ServiceStatUtils() {

    }

    public static ServiceStat getOrCreateHistogramStat(Service service, String name) {
        return getOrCreateStat(service, name, true, null);
    }

    public static TimeSeriesStats createHourlyTimeSeriesStat(EnumSet<AggregationType> aggregationTypes) {
        return new TimeSeriesStats((int) TimeUnit.HOURS.toMinutes(1), TimeUnit.MINUTES.toMillis(1), aggregationTypes);
    }

    public static TimeSeriesStats createDailyTimeSeriesStat(EnumSet<AggregationType> aggregationTypes) {
        return new TimeSeriesStats((int) TimeUnit.DAYS.toHours(1), TimeUnit.HOURS.toMillis(1), aggregationTypes);
    }

    public static ServiceStat getOrCreateHourlyTimeSeriesStat(Service service, String prefix, EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        return getOrCreateStat(service, statName, false, () -> createHourlyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateDailyTimeSeriesStat(Service service, String prefix, EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        return getOrCreateStat(service, statName, false, () -> createDailyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateHourlyTimeSeriesHistogramStat(Service service, String prefix, EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        return getOrCreateStat(service, statName, true, () -> createHourlyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateDailyTimeSeriesHistogramStat(Service service, String prefix, EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        return getOrCreateStat(service, statName, true, () -> createDailyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateTimeSeriesStat(Service service, String name, Supplier<TimeSeriesStats> timeSeriesStatsSupplier) {
        return getOrCreateStat(service, name, false, timeSeriesStatsSupplier);
    }


    private static ServiceStat getOrCreateStat(Service service, String name, boolean createHistogram, Supplier<TimeSeriesStats> timeSeriesStatsSupplier) {
        if (!service.hasOption(ServiceOption.INSTRUMENTATION)) {
            return null;
        }
        ServiceStat stat = service.getStat(name);
        synchronized (stat) {
            if (stat.logHistogram == null && createHistogram) {
                stat.logHistogram = new ServiceStatLogHistogram();
            }
            if (stat.timeSeriesStats == null && timeSeriesStatsSupplier != null) {
                stat.timeSeriesStats = timeSeriesStatsSupplier.get();
            }
        }
        return stat;
    }
}
