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

package com.vmware.dcp.common;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Common thread for all JS ops. Since Rhino relies on context bound to a thread and in browser JavaScript always
 * work in a single thread - all test JS operations should be executed using this single thread executor.
 */
public class JsExecutor {
    public static final String JS_THREAD_NAME = "JS";

    private JsExecutor() {
    }

    public static final ExecutorService executor = Executors.newSingleThreadExecutor(
            r -> {
                Thread thread = new Thread(r, JS_THREAD_NAME);
                thread.setDaemon(true);
                return thread;
            }
            );

    /**
     * Executes a callable synchronously.
     *
     * @param c   Callable to execute.
     * @param <V> Callable return type.
     * @return Value returned by callable.
     */
    public static <V> V executeSynchronously(Callable<V> c) {
        try {
            final AtomicReference<V> result = new AtomicReference<>();
            final AtomicReference<Exception> err = new AtomicReference<>();
            final CountDownLatch doneLatch = new CountDownLatch(1);
            executor.execute(() -> {
                try {
                    result.set(c.call());
                } catch (Exception e) {
                    err.set(e);
                } finally {
                    doneLatch.countDown();
                }
            });
            doneLatch.await();
            Exception ex = err.get();
            if (ex != null) {
                throw ex;
            } else {
                return result.get();
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Executes runnable synchronously.
     *
     * @param r Runnable to execute.
     */
    public static void executeSynchronously(Runnable r) {
        executeSynchronously(() -> {
            r.run();
            return null;
        });
    }
}
