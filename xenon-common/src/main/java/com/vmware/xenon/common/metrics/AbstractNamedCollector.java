/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

public abstract class AbstractNamedCollector extends Collector {
    public static final String SYSTEM_THREADPOOL = "threadpool";
    public static final String NS = "xn";

    protected final String name;

    protected AbstractNamedCollector(String name) {
        this.name = name;
    }

    protected List<String> labels(String... values) {
        return Arrays.asList(values);
    }

    protected List<String> labels(String s) {
        return Collections.singletonList(s);
    }

    protected GaugeMetricFamily gauge(String metricName, String help, double value) {
        return new GaugeMetricFamily(metricName, help, value);
    }

    protected String name(String subsystem, String part) {
        return "xn_" + subsystem + "_" + Collector.sanitizeMetricName(this.name) + "_" + Collector
                .sanitizeMetricName(part);
    }
}
