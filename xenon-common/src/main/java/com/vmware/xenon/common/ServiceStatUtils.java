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

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.Service.ServiceOption;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.ServiceStatLogHistogram;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;

public final class ServiceStatUtils {

    static final String GET_DURATION = Action.GET + Service.STAT_NAME_OPERATION_DURATION;
    static final String POST_DURATION = Action.POST + Service.STAT_NAME_OPERATION_DURATION;
    static final String PATCH_DURATION = Action.PATCH + Service.STAT_NAME_OPERATION_DURATION;
    static final String PUT_DURATION = Action.PUT + Service.STAT_NAME_OPERATION_DURATION;
    static final String DELETE_DURATION = Action.DELETE + Service.STAT_NAME_OPERATION_DURATION;
    static final String OPTIONS_DURATION = Action.OPTIONS + Service.STAT_NAME_OPERATION_DURATION;

    static final String GET_REQUEST_COUNT = Action.GET + Service.STAT_NAME_REQUEST_COUNT;
    static final String POST_REQUEST_COUNT = Action.POST + Service.STAT_NAME_REQUEST_COUNT;
    static final String PATCH_REQUEST_COUNT = Action.PATCH + Service.STAT_NAME_REQUEST_COUNT;
    static final String PUT_REQUEST_COUNT = Action.PUT + Service.STAT_NAME_REQUEST_COUNT;
    static final String DELETE_REQUEST_COUNT = Action.DELETE + Service.STAT_NAME_REQUEST_COUNT;
    static final String OPTIONS_REQUEST_COUNT = Action.OPTIONS + Service.STAT_NAME_REQUEST_COUNT;

    static final String GET_QLATENCY = Action.GET + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY;
    static final String POST_QLATENCY = Action.POST + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY;
    static final String PATCH_QLATENCY = Action.PATCH + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY;
    static final String PUT_QLATENCY = Action.PUT + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY;
    static final String DELETE_QLATENCY = Action.DELETE + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY;
    static final String OPTIONS_QLATENCY = Action.OPTIONS + Service.STAT_NAME_OPERATION_QUEUEING_LATENCY;

    static final String GET_HANDLER_LATENCY = Action.GET + Service.STAT_NAME_SERVICE_HANDLER_LATENCY;
    static final String POST_HANDLER_LATENCY = Action.POST + Service.STAT_NAME_SERVICE_HANDLER_LATENCY;
    static final String PATCH_HANDLER_LATENCY = Action.PATCH + Service.STAT_NAME_SERVICE_HANDLER_LATENCY;
    static final String PUT_HANDLER_LATENCY = Action.PUT + Service.STAT_NAME_SERVICE_HANDLER_LATENCY;
    static final String DELETE_HANDLER_LATENCY = Action.DELETE + Service.STAT_NAME_SERVICE_HANDLER_LATENCY;
    static final String OPTIONS_HANDLER_LATENCY = Action.OPTIONS + Service.STAT_NAME_SERVICE_HANDLER_LATENCY;

    static final EnumSet<AggregationType> AGGREGATION_TYPE_AVG_MAX =
            EnumSet.of(AggregationType.AVG, AggregationType.MAX);

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

    public static ServiceStat getOrCreateHourlyTimeSeriesStat(Service service, String prefix,
            EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        return getOrCreateStat(service, statName, false, () -> createHourlyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateDailyTimeSeriesStat(Service service, String prefix,
            EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        return getOrCreateStat(service, statName, false, () -> createDailyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateHourlyTimeSeriesHistogramStat(Service service, String prefix,
            EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_HOUR;
        return getOrCreateStat(service, statName, true, () -> createHourlyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateDailyTimeSeriesHistogramStat(Service service, String prefix,
            EnumSet<AggregationType> aggregationTypes) {
        String statName = prefix + ServiceStats.STAT_NAME_SUFFIX_PER_DAY;
        return getOrCreateStat(service, statName, true, () -> createDailyTimeSeriesStat(aggregationTypes));
    }

    public static ServiceStat getOrCreateTimeSeriesStat(Service service, String name,
            Supplier<TimeSeriesStats> timeSeriesStatsSupplier) {
        return getOrCreateStat(service, name, false, timeSeriesStatsSupplier);
    }

    static String getPerActionDurationName(Action a) {
        switch (a) {
        case GET:
            return GET_DURATION;
        case POST:
            return POST_DURATION;
        case PATCH:
            return PATCH_DURATION;
        case PUT:
            return PUT_DURATION;
        case DELETE:
            return DELETE_DURATION;
        case OPTIONS:
            return OPTIONS_DURATION;
        default:
            throw new IllegalArgumentException("Unknown action " + a);
        }
    }

    static String getPerActionRequestCountName(Action a) {
        switch (a) {
        case GET:
            return GET_REQUEST_COUNT;
        case POST:
            return POST_REQUEST_COUNT;
        case PATCH:
            return PATCH_REQUEST_COUNT;
        case PUT:
            return PUT_REQUEST_COUNT;
        case DELETE:
            return DELETE_REQUEST_COUNT;
        case OPTIONS:
            return OPTIONS_REQUEST_COUNT;
        default:
            throw new IllegalArgumentException("Unknown action " + a);
        }
    }

    static String getPerActionQueueLatencyName(Action a) {
        switch (a) {
        case GET:
            return GET_QLATENCY;
        case POST:
            return POST_QLATENCY;
        case PATCH:
            return PATCH_QLATENCY;
        case PUT:
            return PUT_QLATENCY;
        case DELETE:
            return DELETE_QLATENCY;
        case OPTIONS:
            return OPTIONS_QLATENCY;
        default:
            throw new IllegalArgumentException("Unknown action " + a);
        }
    }

    static String getPerActionServiceHandlerLatencyName(Action a) {
        switch (a) {
        case GET:
            return GET_HANDLER_LATENCY;
        case POST:
            return POST_HANDLER_LATENCY;
        case PATCH:
            return PATCH_HANDLER_LATENCY;
        case PUT:
            return PUT_HANDLER_LATENCY;
        case DELETE:
            return DELETE_HANDLER_LATENCY;
        case OPTIONS:
            return OPTIONS_HANDLER_LATENCY;
        default:
            throw new IllegalArgumentException("Unknown action " + a);
        }
    }

    private static ServiceStat getOrCreateStat(Service service, String name, boolean createHistogram,
            Supplier<TimeSeriesStats> timeSeriesStatsSupplier) {
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
