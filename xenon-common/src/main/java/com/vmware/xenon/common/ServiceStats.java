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

    /**
     * Data structure representing time series data
     * Users can specify the number of datapoints to be stored and the granularity of the datapoints
     * Any data to be stored will be normalized to an UTC boundary based on the the data granularity
     * If a bucket already exists for that timestamp, the existing value is updated based on the
     * specified AggregationType
     * If the number of datapoints equals the number of buckets, the oldest datapoint will be dropped
     * on any further insertion
     */
    public static class TimeSeriesStats {

        public static class DataPoint {
            public Double avg;
            public Double min;
            public Double max;
            public double count;
        }

        public static enum AggregationType {
            AVG, MIN, MAX
        }

        public SortedMap<Long, DataPoint> dataPoints;
        public int numBuckets;
        public long bucketDurationMillis;
        public EnumSet<AggregationType> aggregationType;

        public TimeSeriesStats(int numBuckets, long bucketDurationMillis,
                EnumSet<AggregationType> aggregationType) {
            this.numBuckets = numBuckets;
            this.bucketDurationMillis = bucketDurationMillis;
            this.dataPoints = new TreeMap<Long, DataPoint>();
            this.aggregationType = aggregationType;
        }

        public void add(long timestampMicros, double value) {
            synchronized (this.dataPoints) {
                long bucketId = normalizeTimestamp(timestampMicros, this.bucketDurationMillis);
                DataPoint dataBucket = null;
                if (this.dataPoints.containsKey(bucketId)) {
                    dataBucket = this.dataPoints.get(bucketId);
                } else {
                    if (this.dataPoints.size() == this.numBuckets) {
                        if (this.dataPoints.firstKey() > timestampMicros) {
                            // incoming data is too old; ignore
                            return;
                        }
                        // remove the oldest entry
                        this.dataPoints.remove(this.dataPoints.firstKey());
                    }
                    dataBucket = new DataPoint();
                    this.dataPoints.put(bucketId, dataBucket);
                }
                if (this.aggregationType.contains(AggregationType.AVG)) {
                    if (dataBucket.avg == null) {
                        dataBucket.avg = new Double(value);
                        dataBucket.count = 1;
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
        }

        /**
         * This method normalizes the input timestamp at UTC time boundaries
         *  based on the bucket size, effectively creating time series that are comparable to each other
         */
        private long normalizeTimestamp(long timestampMicros, long bucketDurationMillis) {
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
        public String unit;

        /**
         * Source (provider) for this stat
         */
        public URI serviceReference;

        /**
         *  histogram of logarithmic time scale
         */
        public ServiceStatLogHistogram logHistogram;

        /**
         * time series data. If set,  the stat value is
         * maintained over time. Users can choose the number
         * of samples to maintain and the time window into
         * which each datapoint falls. If more than one entry
         * exists, aggregates (average, minimum, maximum; based
         * on user choice) are maintained
         */
        public TimeSeriesStats timeSeriesStats;
    }

    public String kind = KIND;

    public Map<String, ServiceStat> entries = new HashMap<>();

}