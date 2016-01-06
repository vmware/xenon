/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.serialization;

import static org.junit.Assert.assertNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;

public class TestJsonMapper {
    private static final int NUM_THREADS = 2;

    // An error may happen when two threads try to serialize a recursive
    // type for the very first time concurrently. Type caching logic in GSON
    // doesn't deal well with recursive types being generated concurrently.
    // Also see: https://github.com/google/gson/issues/764
    @Test
    public void testConcurrency() throws InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        Query q = Builder.create()
                .addFieldClause(
                        "kind",
                        "foo")
                .addFieldClause(
                        "name",
                        "jane")
                .build();

        for (int i = 0; i < 1000; i++) {
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch finish = new CountDownLatch(NUM_THREADS);
            final JsonMapper mapper = new JsonMapper();

            for (int j = 0; j < NUM_THREADS; j++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        mapper.toJson(q);
                    } catch (Throwable t) {
                        failure.set(t);
                    } finally {
                        finish.countDown();
                    }
                });
            }

            start.countDown();
            finish.await();
            assertNull(failure.get());
        }
    }
}
