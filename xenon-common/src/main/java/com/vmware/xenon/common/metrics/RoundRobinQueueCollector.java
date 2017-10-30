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
import java.util.List;
import java.util.Map.Entry;

import com.vmware.xenon.common.RoundRobinOperationQueue;

public class RoundRobinQueueCollector extends AbstractNamedCollector {
    public static final String SYSTEM_OPQUEUE = "opqueue";
    private final RoundRobinOperationQueue queue;

    public RoundRobinQueueCollector(RoundRobinOperationQueue queue, String name) {
        super(name);
        this.queue = queue;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        int deepestQueue = 0;
        int totalItems = 0;
        for (Entry<String, Integer> entry : this.queue.sizesByKey().entrySet()) {
            deepestQueue = Math.max(deepestQueue, entry.getValue());
            totalItems += entry.getValue();
        }

        return Arrays.asList(
                gauge(name(SYSTEM_OPQUEUE, "depth_total"),
                        "Aggregated queue depth", totalItems),
                gauge(name(SYSTEM_OPQUEUE, "depth_max"),
                        "The deepest queue for some user", deepestQueue)
        );
    }
}
