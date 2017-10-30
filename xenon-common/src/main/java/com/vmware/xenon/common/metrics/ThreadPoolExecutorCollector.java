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
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadPoolExecutorCollector extends AbstractNamedCollector {
    private final ThreadPoolExecutor pool;

    public ThreadPoolExecutorCollector(Executor pool, String name) {
        super(name);
        if (pool instanceof ThreadPoolExecutor) {
            this.pool = (ThreadPoolExecutor) pool;
        } else {
            this.pool = null;
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        if (this.pool == null) {
            return Collections.emptyList();
        }

        return Arrays.asList(
                gauge(name(SYSTEM_THREADPOOL, "pending_tasks"),
                        "Pending tasks count", this.pool.getQueue().size()),
                gauge(name(SYSTEM_THREADPOOL, "active_threads"),
                        "Thread count", this.pool.getActiveCount()),
                gauge(name(SYSTEM_THREADPOOL, "thread_count"),
                        "Thread count", this.pool.getPoolSize())
        );
    }
}
