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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import io.prometheus.client.CollectorRegistry;
import org.junit.Test;

import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.metrics.PerActionAndStatusCodeLookup.Entry;

public class PerActionAndStatusCodeLookupTest {
    @Test
    public void hash() throws Exception {
        CollectorRegistry registry = new CollectorRegistry();
        HostHttpCollector collector = new HostHttpCollector();
        collector.register(registry);

        String selfLink = "/self/link";
        PerActionAndStatusCodeLookup ph = new PerActionAndStatusCodeLookup(selfLink, collector);
        for (Action action : PerActionAndStatusCodeLookup.COMMON_ACTIONS) {
            for (int statusCode : PerActionAndStatusCodeLookup.COMMON_STATUS_CODES) {
                Entry entry = ph.get(action, statusCode);
                assertNotNull(entry);
                assertNotNull(entry.counter);
                assertNotNull(entry.responseTimeMillis);
                assertSame(entry.counter, collector.requestCount(action, selfLink, statusCode));
                assertSame(entry.responseTimeMillis, collector.responseTimeMillis(action, selfLink, statusCode));
            }
        }
    }
}