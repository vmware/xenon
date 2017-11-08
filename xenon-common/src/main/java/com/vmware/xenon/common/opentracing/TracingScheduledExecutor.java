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

package com.vmware.xenon.common.opentracing;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.opentracing.Tracer;

/**
 * A decorator for executors that adds OpenTracing support to instrument time spent in queue.
 */
public class TracingScheduledExecutor extends TracingExecutor implements ScheduledExecutorService {

    private final ScheduledExecutorService scheduledExecutor;

    private TracingScheduledExecutor(ScheduledExecutorService executor, Tracer tracer) {
        super(executor, tracer);
        this.scheduledExecutor = executor;
    }

    /**
     * Construct a tracing ScheduledExecutorService. If tracing is not enabled then the original
     * executor is returned regardless of the tracer object.
     *
     * @param executor
     * @param tracer
     * @return
     */
    public static ScheduledExecutorService create(ScheduledExecutorService executor, Tracer tracer) {
        if (!TracerFactory.factory.enabled()) {
            return executor;
        } else {
            return new TracingScheduledExecutor(executor, tracer);
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return this.scheduledExecutor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return this.scheduledExecutor.schedule(callable, delay, unit);

    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return this.scheduledExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return this.scheduledExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
}
