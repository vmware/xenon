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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

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
    public void testObjectValue() {

        Map<String, Object> srcMap = new HashMap<>();

        srcMap.put("str", "string");
        //srcMap.put("int", 3);   // cannot map back to int, all ints mapped to long
        srcMap.put("long", Long.MAX_VALUE);
        srcMap.put("double", new Double(3.14));
        srcMap.put("boolean", Boolean.TRUE);

        // fields with null values are ignored by default in Gson but this can be overridden
        // see https://sites.google.com/site/gson/gson-user-guide#TOC-Null-Object-Support
        // srcMap.put("null", null);

        String json = Utils.toJson(srcMap);

        @SuppressWarnings("unchecked")
        Map<String, Object> dstMap = (Map<String, Object>) Utils.fromJson(json, ObjectMapTypeConverter.TYPE);

        for (Map.Entry<String, Object> entry : srcMap.entrySet()) {
            String key = entry.getKey();
            Object srcValue = entry.getValue();
            Assert.assertTrue("Missing key: " + key, dstMap.containsKey(key));
            Object dstValue = dstMap.get(key);
            if (srcValue != null) {
                Assert.assertEquals(srcValue, dstValue);
            } else {
                Assert.assertNull(dstMap.get(key));
            }
        }

    }
}
