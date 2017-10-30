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
import java.util.concurrent.ForkJoinPool;

public final class ForkJoinPoolCollector extends AbstractNamedCollector {

    private final ForkJoinPool pool;

    public ForkJoinPoolCollector(ForkJoinPool pool, String name) {
        super(name);
        this.pool = pool;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return Arrays.asList(
                gauge(name(SYSTEM_THREADPOOL, "pending_tasks"),
                        "Pending tasks count", this.pool.getQueuedTaskCount()),
                gauge(name(SYSTEM_THREADPOOL, "active_threads"),
                        "Thread count", this.pool.getActiveThreadCount()),
                gauge(name(SYSTEM_THREADPOOL, "thread_count"),
                        "Thread count", this.pool.getRunningThreadCount())
        );
    }
}
