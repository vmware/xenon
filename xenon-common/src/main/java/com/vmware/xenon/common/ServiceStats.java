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
import java.util.HashMap;
import java.util.Map;

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
    }

    public String kind = KIND;

    public Map<String, ServiceStat> entries = new HashMap<>();

}