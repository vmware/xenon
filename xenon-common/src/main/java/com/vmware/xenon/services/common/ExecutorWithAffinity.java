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

package com.vmware.xenon.services.common;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.NamedThreadFactory;

public class ExecutorWithAffinity {
    private final ThreadLocal<Integer> workerId;

    private final ExecutorService[] workers;

    private final AtomicInteger counter = new AtomicInteger();
    private boolean suspeded;

    /**
     * Create a fixed number of pools for the given total and per-pool threads. This constructor
     * will create exactly <code>totalThreadCount</code> threads, possibly giving the last pool a few
     * threads less.
     * If <code>threadsPerPool == totalThreadCount</code> the behaves as if there was a single pool
     * thus negating the purpose of this class. Such a configuration is still allowed for testing
     * purposes.
     *
     * @param threadsPerPool the size of the pool, coerced to 1 if the value is negative or zero
     * @param totalThreadCount total number of threads across all pools
     */
    public ExecutorWithAffinity(int threadsPerPool, int totalThreadCount) {
        this.workerId = ThreadLocal.withInitial(() -> -1);
        threadsPerPool = Math.max(1, threadsPerPool);
        int poolCount = totalThreadCount / threadsPerPool;
        int lastPoolSize;
        if (poolCount == 0) {
            lastPoolSize = totalThreadCount;
        } else {
            lastPoolSize = totalThreadCount - (poolCount - 1) * threadsPerPool;
        }

        this.workers = new ExecutorService[poolCount];
        for (int i = 0; i < poolCount - 1; i++) {
            ThreadFactory threadFactory = newThreadFactory(i);
            ExecutorService executor = Executors.newFixedThreadPool(threadsPerPool, threadFactory);
            this.workers[i] = executor;
        }

        ThreadFactory threadFactory = newThreadFactory(poolCount - 1);
        this.workers[poolCount - 1] = Executors.newFixedThreadPool(lastPoolSize, threadFactory);
    }

    protected ThreadFactory newThreadFactory(int poolId) {
        return new NamedThreadFactory("AffinityPool-" + poolId);
    }

    /**
     * Picks a random pool and submits the command there.
     *
     * @param command
     */
    public void submitToRandomPool(Runnable command) {
        if (this.suspeded) {
            return;
        }

        int poolId = nextValidPoolId();

        this.workers[poolId].execute(() -> {
            this.workerId.set(poolId);
            command.run();
        });
    }

    public int nextValidPoolId() {
        return this.counter.getAndIncrement() % this.workers.length;
    }

    /**
     * Submits a task to the given pool.
     *
     *  @param poolId
     * @param task
     * @param <T>
     * @return
     */
    public <T> Future<T> resubmit(int poolId, Callable<T> task) {
        return this.workers[poolId].submit(task);
    }

    /**
     * Gets the poolId of the current thread or -1 is called from a thread
     * not managed by this executor.
     *
     * @return
     */
    public int getCurrentPoolId() {
        return this.workerId.get();
    }

    /**
     * @see ExecutorService#shutdown()
     */
    public void shutdown() {
        Arrays.stream(this.workers).forEach(ExecutorService::shutdown);
    }

    /**
     * @see ExecutorService#awaitTermination(long, TimeUnit)
     * @param time
     * @param unit
     */
    public void awaitTermination(int time, TimeUnit unit) {
        Arrays.stream(this.workers).forEach(executor -> {
            try {
                executor.awaitTermination(time, unit);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Visible for testing only.
     * @param suspeded
     */
    void setSuspended(boolean suspeded) {
        this.suspeded = suspeded;
    }
}
