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

import io.prometheus.client.Summary;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.metrics.PerActionAndStatusCodeLookup.Entry;

public class ServiceHttpCollector {
    private final String selfLink;
    private final Summary.Child requestSize;
    private final Summary.Child responseSize;
    private final HostHttpCollector hostHttpCollector;
    private final PerActionAndStatusCodeLookup lookup;

    public ServiceHttpCollector(HostHttpCollector hostHttpCollector, String selfLink) {
        this.hostHttpCollector = hostHttpCollector;
        this.requestSize = hostHttpCollector.requestSize(selfLink);
        this.responseSize = hostHttpCollector.responseSize(selfLink);
        this.selfLink = selfLink;
        this.lookup = new PerActionAndStatusCodeLookup(selfLink, hostHttpCollector);
    }

    public void observeRequestDuration(Action action, int statusCode, long millis) {
        Entry entry = this.lookup.get(action, statusCode);
        if (entry != null) {
            entry.counter.inc();
            entry.responseTimeMillis.observe(millis);
        } else {
            this.hostHttpCollector.requestCount(action, this.selfLink, statusCode).inc();
            this.hostHttpCollector.responseTimeMillis(action, this.selfLink, statusCode).observe(millis);
        }
    }

    public void observeRequestSize(long bytes) {
        this.requestSize.observe(bytes);
    }

    public void observeResponseSize(long bytes) {
        this.responseSize.observe(bytes);
    }
}
