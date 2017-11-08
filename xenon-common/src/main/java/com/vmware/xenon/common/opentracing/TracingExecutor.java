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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.opentracing.ActiveSpan;
import io.opentracing.ActiveSpan.Continuation;
import io.opentracing.Span;
import io.opentracing.Tracer;

/**
 * A decorator for executors that adds OpenTracing support to instrument time spent in queue.
 */
public class TracingExecutor implements ExecutorService {

    private final ExecutorService executor;
    protected final Tracer tracer;

    protected TracingExecutor(ExecutorService executor, Tracer tracer) {
        this.executor = executor;
        this.tracer = tracer;
    }

    /**
     * Construct a tracing ExecutorService. If tracing is not enabled then the original
     * executor is returned regardless of the tracer object.
     *
     * @param executor
     * @param tracer
     * @return
     */
    public static ExecutorService create(ExecutorService executor, Tracer tracer) {
        if (!TracerFactory.factory.enabled()) {
            return executor;
        } else {
            return new TracingExecutor(executor, tracer);
        }
    }

    @Override
    public void shutdown() {
        this.executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return this.executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return this.executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return this.executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return this.executor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return this.executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return this.executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return this.executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return this.executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return this.executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return this.executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    @SuppressWarnings("try")
    public void execute(Runnable command) {
        if (this.tracer.activeSpan() != null) {
            Span queueSpan = this.tracer.buildSpan("Queue").startManual();
            Continuation cont = this.tracer.activeSpan().capture();
            try {
                this.executor.execute(() -> {
                    queueSpan.finish();
                    try (ActiveSpan parentSpan = cont.activate()) {
                        command.run();
                    }
                });
            } catch (RejectedExecutionException e) {
                Map<String, Object> map = new HashMap<>();
                map.put("error.kind", "exception");
                map.put("error.object", e);
                map.put("message", "could not submit to executor queue");
                queueSpan.log(map);
                queueSpan.finish();
                cont.activate().close();
                throw e;
            }
        } else {
            this.executor.execute(command);
        }
    }
}
