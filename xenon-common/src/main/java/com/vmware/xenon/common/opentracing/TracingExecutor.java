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

import java.util.ArrayDeque;
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

    private final ArrayDeque<Thunk> free;
    private final ExecutorService executor;
    protected final Tracer tracer;

    private class Thunk implements Runnable {
        Runnable command;
        Span queueSpan;
        Continuation cont;

        void setContents(Runnable command, Span queueSpan, Continuation cont) {
            this.command = command;
            this.queueSpan = queueSpan;
            this.cont = cont;
        }

        void clearContents() {
            this.command = null;
            this.queueSpan = null;
            this.cont = null;
        }

        @Override
        @SuppressWarnings("try")
        public void run() {
            Runnable command = this.command;
            Span queueSpan = this.queueSpan;
            Continuation cont = this.cont;
            clearContents();
            TracingExecutor.this.push(this);
            queueSpan.finish();
            try (ActiveSpan parentSpan = cont.activate()) {
                command.run();
            }
        }
    }

    protected TracingExecutor(ExecutorService executor, Tracer tracer) {
        this.executor = executor;
        this.tracer = tracer;
        this.free = new ArrayDeque<Thunk>();
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

    /**
     * Put thunk into the freelist for reuse. Always clear the thunk first to avoid keeping references
     * arbitrarily long. (Not done within the function to keep lock granularity fine.
     */
    private synchronized void push(Thunk thunk) {
        this.free.push(thunk);
    }

    /**
     * Get a thunk for passing to the underlying executor. At most the executors max queue length + max concurrent
     * running threads + 1 thunks will be created: one for every item in the queue, one for every thunk that has just
     * started being executed, and one created for submission to the executor-but-fails-on-oversize.
     *
     * @return @{link Thunk}
     */
    synchronized Thunk getThunk() {
        Thunk result = this.free.poll();
        if (result == null) {
            result = new Thunk();
        }
        return result;
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
    public void execute(Runnable command) {
        if (this.tracer.activeSpan() != null) {
            Span queueSpan = this.tracer.buildSpan("Queue").startManual();
            Continuation cont = this.tracer.activeSpan().capture();
            Thunk thunk = getThunk();
            thunk.setContents(command, queueSpan, cont);
            try {
                this.executor.execute(thunk);
            } catch (RejectedExecutionException e) {
                thunk.clearContents();
                this.push(thunk);
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
