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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ThreadPoolExecutor;

import org.junit.After;
import org.junit.Test;

public class ExecutorWithAffinityTest {

    private ExecutorWithAffinity executor;

    @After
    public void cleanup() {
        this.executor.shutdown();
    }

    @Test
    public void testSpreadExact() {
        this.executor = new ExecutorWithAffinity(2, 20);
        assertEquals(10, this.executor.getPoolCount());
        assertEquals(20, sumThreads(this.executor));
    }

    @Test
    public void testSpread1PerPool() {
        this.executor = new ExecutorWithAffinity(1, 17);
        assertEquals(17, this.executor.getPoolCount());
        assertEquals(17, sumThreads(this.executor));
    }

    @Test
    public void testSpreadSinglePool() {
        this.executor = new ExecutorWithAffinity(19, 19);
        assertEquals(1, this.executor.getPoolCount());
        assertEquals(19, sumThreads(this.executor));
    }

    @Test
    public void testSpreadWithRemainingThreads() {
        this.executor = new ExecutorWithAffinity(3, 32);
        assertEquals(10, this.executor.getPoolCount());
        assertEquals(32, sumThreads(this.executor));
    }

    @Test
    public void testSpreadLastPoolMustBeLarger() {
        this.executor = new ExecutorWithAffinity(8, 23);
        // 2 pools * 8 threads account for 16, the remaining 7 a split 3:4 between the two final pools
        assertEquals(2, this.executor.getPoolCount());
        assertEquals(23, sumThreads(this.executor));
    }

    private int sumThreads(ExecutorWithAffinity executor) {
        int res = 0;
        for (int i = 0; i < executor.getPoolCount(); i++) {
            res += ((ThreadPoolExecutor) executor.getPool(i)).getMaximumPoolSize();
        }

        return res;
    }
}