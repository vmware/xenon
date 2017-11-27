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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorWithAffinity {
    private final ThreadLocal<Integer> poolId;

    private final ThreadPoolExecutor[] executorPool;

    // ThreadPoolExecutor is a singleton despite the name
    private final Random rand = ThreadLocalRandom.current();

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
        this.poolId = ThreadLocal.withInitial(() -> -1);
        threadsPerPool = Math.max(1, threadsPerPool);
        int poolCount = totalThreadCount / threadsPerPool;

        // if the totalThreadCount is not divisible by threadsPerPool
        // then spread the remaining threads as evenly as possible
        int remainingThreads = totalThreadCount - poolCount * threadsPerPool;
        int bonusPerPool = Math.max(1, remainingThreads / poolCount);

        this.executorPool = new ThreadPoolExecutor[poolCount];
        for (int i = 0; i < poolCount; i++) {
            ThreadFactory threadFactory = newThreadFactory(i);
            int nThreads = threadsPerPool;
            if (remainingThreads > 0) {
                nThreads += bonusPerPool;
                remainingThreads -= bonusPerPool;
                if (i == poolCount - 1 && remainingThreads > 0) {
                    // start all remaining threads, if any, in the last pool
                    nThreads += remainingThreads;
                }
            }

            this.executorPool[i] = newExecutor(nThreads, threadFactory);
        }
    }

    protected ThreadPoolExecutor newExecutor(int threadsPerPool, ThreadFactory threadFactory) {
        return (ThreadPoolExecutor) Executors.newFixedThreadPool(threadsPerPool, threadFactory);
    }

    protected ThreadFactory newThreadFactory(int poolId) {
        return (r) -> new Thread(r, "AffinityPool-" + poolId);
    }

    /**
     * Picks a random pool and submits the command there.
     *
     * @param command
     */
    public void submitToRandomPool(Runnable command) {
        int poolId = this.rand.nextInt(this.executorPool.length);

        this.executorPool[poolId].execute(() -> {
            this.poolId.set(poolId);
            command.run();
        });
    }

    public int selectIdlePool() {
        // use power of two choices to select indexA less loaded pool
        // https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
        int indexA = this.rand.nextInt(this.executorPool.length);
        int indexB = this.rand.nextInt(this.executorPool.length);

        int sizeA = this.executorPool[indexA].getQueue().size();
        int sizeB = this.executorPool[indexB].getQueue().size();

        // return the pool with shorter queue
        if (sizeB < sizeA) {
            return indexB;
        } else if (sizeA == sizeB) {
            // equal size, pick random
            return this.rand.nextBoolean() ? indexA : indexB;
        }

        return indexA;
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
        return this.executorPool[poolId].submit(task);
    }

    /**
     * Gets the poolId of the current thread or -1 is called from a thread
     * not managed by this executor.
     *
     * @return
     */
    public int getCurrentPoolId() {
        return this.poolId.get();
    }

    int getPoolCount() {
        return this.executorPool.length;
    }

    /**
     * @see ExecutorService#shutdown()
     */
    public void shutdown() {
        Arrays.stream(this.executorPool).forEach(ExecutorService::shutdown);
    }

    public void awaitTermination(int timeoutSeconds, TimeUnit unit) throws InterruptedException {
        for (ThreadPoolExecutor threadPoolExecutor : this.executorPool) {
            threadPoolExecutor.awaitTermination(timeoutSeconds, unit);
        }
    }

    /**
     * Visible for testing
     *
     * @param poolId
     * @return
     */
    ThreadPoolExecutor getPool(int poolId) {
        return this.executorPool[poolId];
    }
}
