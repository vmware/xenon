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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.Describable;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

import com.vmware.xenon.common.Service.Action;

/**
 * Collects http request stats for all services on a host.
 */
public class HostHttpCollector extends Collector implements Describable {

    private final Summary responseTimeMillis;

    private final Counter requestCount;

    private final Summary requestSize;

    private final Summary responseSize;

    public HostHttpCollector() {
        this.responseTimeMillis = Summary.build()
                .namespace("xn")
                .subsystem("http")
                .name("response_time_millis")
                .labelNames("method", "service", "status")
                .help("Request completed time in milliseconds")
                .create();

        this.requestCount = Counter.build()
                .namespace("xn")
                .subsystem("http")
                .name("requests_total")
                .labelNames("method", "service", "status")
                .help("Total http requests")
                .create();

        this.requestSize = Summary.build()
                .namespace("xn")
                .subsystem("http")
                .name("requests_size_bytes")
                .labelNames("service")
                .help("Request size in bytes.")
                .create();

        this.responseSize = Summary.build()
                .namespace("xn")
                .subsystem("http")
                .name("response_size_bytes")
                .labelNames("service")
                .help("Response size in bytes.")
                .create();
    }

    Summary.Child responseTimeMillis(Action action, String selfLink, int statusCode) {
        return this.responseTimeMillis.labels(action.toString(), selfLink, Integer.toString(statusCode));
    }

    Summary.Child responseSize(String selfLink) {
        return this.responseSize.labels(selfLink);
    }

    Summary.Child requestSize(String selfLink) {
        return this.requestSize.labels(selfLink);
    }

    public Counter.Child requestCount(Action action, String selfLink, int statusCode) {
        return this.requestCount.labels(action.toString(), selfLink, Integer.toString(statusCode));
    }

    public ServiceHttpCollector createCollectorForService(String selfLink) {
        return new ServiceHttpCollector(this, selfLink);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> res = new ArrayList<>();
        res.addAll(this.responseTimeMillis.collect());
        res.addAll(this.requestCount.collect());
        return res;
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.emptyList();
    }
}
