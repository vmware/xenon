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

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.junit.Test;

import com.vmware.xenon.common.ServiceHost.Arguments;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
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
            final TestContext finish = new TestContext(NUM_THREADS, Duration.ofSeconds(30));
            final JsonMapper mapper = new JsonMapper();

            for (int j = 0; j < NUM_THREADS; j++) {
                executor.execute(() -> {
                    try {
                        start.await();
                        mapper.toJson(q);
                        finish.complete();
                    } catch (Throwable t) {
                        finish.fail(t);
                    }
                });
            }

            start.countDown();
            finish.await();
        }
    }

    @Test
    public void testPathJsonSerialization() {
        Path p = Paths.get("test");

        String jsonRepr = Utils.toJson(p);
        assertEquals("\"" + p.toAbsolutePath().toAbsolutePath() + "\"", jsonRepr);

        Arguments arguments = new Arguments();
        Logger.getAnonymousLogger().info(Utils.toJsonHtml(arguments));
    }
}
