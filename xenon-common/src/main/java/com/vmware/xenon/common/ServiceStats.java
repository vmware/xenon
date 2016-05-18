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

import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Document describing the <service>/stats REST API
 */
public class ServiceStats extends ServiceDocument {
    public static final String KIND = Utils.buildKind(ServiceStats.class);

    public static class ServiceStatLogHistogram {
        /**
         * Each bin tracks a power of 10. Bin[0] tracks all values between 0 and 9, Bin[1] tracks
         * values between 10 and 99, Bin[2] tracks values between 100 and 999, and so forth
         */
        public long[] bins = new long[15];
    }

    public static enum AggregationType {
        AVG, MIN, MAX
    }

    public static class TimeSeriesDataPoint {
        public Double avg;
        public Double min;
        public Double max;
        public Double count;
    }

    public static class TimeSeriesStats {
        public SortedMap<Long, TimeSeriesDataPoint> dataPoints;
        int numBuckets;
        long bucketDurationMillis;
        EnumSet<AggregationType> aggregationType;

        public TimeSeriesStats(int numBuckets, long bucketDurationMillis,
                EnumSet<AggregationType> aggregationType) {
            this.numBuckets = numBuckets;
            this.bucketDurationMillis = bucketDurationMillis;
            this.dataPoints = new TreeMap<Long, TimeSeriesDataPoint>();
            this.aggregationType = aggregationType;
        }

        public synchronized void setDataPoint(long timestampMicros, double value) {
            long bucketId = floorData(timestampMicros, this.bucketDurationMillis);
            TimeSeriesDataPoint dataBucket = null;
            if (this.dataPoints.containsKey(bucketId)) {
                dataBucket = this.dataPoints.get(bucketId);
            } else {
                if (this.dataPoints.size() == this.numBuckets) {
                    if (this.dataPoints.firstKey() > timestampMicros) {
                        // incoming data is too old; ignore
                        return;
                    }
                    // boot out the oldest entry
                    this.dataPoints.remove(this.dataPoints.firstKey());
                }
                dataBucket = new TimeSeriesDataPoint();
                this.dataPoints.put(bucketId, dataBucket);
            }
            if (this.aggregationType.contains(AggregationType.AVG)) {
                if (dataBucket.avg == null) {
                    dataBucket.avg = new Double(value);
                    dataBucket.count = new Double(1);
                } else {
                    double newAvg = ((dataBucket.avg * dataBucket.count)  + value) / (dataBucket.count + 1);
                    dataBucket.avg = newAvg;
                    dataBucket.count++;
                }
            }
            if (this.aggregationType.contains(AggregationType.MAX)) {
                if (dataBucket.max == null) {
                    dataBucket.max = new Double(value);
                } else if (dataBucket.max < value) {
                    dataBucket.max = value;
                }
            }
            if (this.aggregationType.contains(AggregationType.MIN)) {
                if (dataBucket.min == null) {
                    dataBucket.min = new Double(value);
                } else if (dataBucket.min > value) {
                    dataBucket.min = value;
                }
            }
        }

        private long floorData(long timestampMicros, long bucketDurationMillis) {
            long timeMillis = TimeUnit.MICROSECONDS.toMillis(timestampMicros);
            timeMillis -= (timeMillis % bucketDurationMillis);
            return timeMillis;
        }

    }

    public static class ServiceStat {
        public static final String KIND = Utils.buildKind(ServiceStat.class);
        public String name;
        public double latestValue;
        public double accumulatedValue;
        public long version;
        public long lastUpdateMicrosUtc;
        public String kind = KIND;

        /**
         * Source (provider) for this stat
         */
        public URI serviceReference;

        public ServiceStatLogHistogram logHistogram;

        public TimeSeriesStats timeSeriesStats;
    }

    public String kind = KIND;

    public Map<String, ServiceStat> entries = new HashMap<>();

}