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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentracing.ActiveSpan;
import io.opentracing.Tracer;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import org.junit.Rule;
import org.junit.Test;

public class TracingExecutorTest {

    @Rule
    public InjectMockTracer injectMockTracer = new InjectMockTracer();

    private TracingExecutor newExecutor(Tracer tracer) {
        return new TracingExecutor(Executors.newSingleThreadExecutor(), tracer);
    }

    @Test
    @SuppressWarnings("try")
    public void testQueueTimeMeasured() throws Throwable {
        /*
         * Make an executor
         * Make a span
         * Naively submit to it
         * That should wrap the activescope around the thing it calls.
         */
        MockTracer tracer = this.injectMockTracer.tracer;
        TracingExecutor executor = newExecutor(tracer);
        Logger log = Logger.getAnonymousLogger();
        executor.execute(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) { }
        });
        try (ActiveSpan parent = tracer.buildSpan("parent").startActive()) {
            executor.execute(() -> {
                try (ActiveSpan span = tracer.buildSpan("work").startActive()) {
                    span.setTag("work", "done");
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        List<MockSpan> finishedSpans = tracer.finishedSpans();
        /* We expect 3 spans:
         *  [-----parent-----]
         *  [ queued ][ work ]
         */
        assertEquals(3, finishedSpans.toArray().length);
        MockSpan parentSpan = null;
        MockSpan queuedSpan = null;
        MockSpan workSpan = null;
        long traceId = finishedSpans.get(0).context().traceId();
        for (MockSpan span : finishedSpans) {
            log.log(Level.INFO, String.format("span %s", span.toString()));
            assertEquals(String.format("broken trace span %s", span.toString()), traceId, span.context().traceId());
            if (span.operationName().equals("parent")) {
                parentSpan = span;
            } else if (span.operationName().equals("work")) {
                workSpan = span;
            } else if (span.operationName().equals("Queue")) {
                queuedSpan = span;
            }
        }
        /* The parent of queued should be parent */
        assertEquals(queuedSpan.parentId(), parentSpan.context().spanId());
        /* The parent of work should be parent */
        assertEquals(workSpan.parentId(), parentSpan.context().spanId());
        /* The queued span should finish before the work span starts, not after */
        assertTrue(workSpan.startMicros() >= queuedSpan.finishMicros());
    }

    @Test
    @SuppressWarnings("try")
    public void testEnqueueFailureDoesNotLeak() throws Throwable {
        /*
         * Make an executor with a very short queue
         * submit slow work to the expected depth
         * Make a span
         * Submit it
         * It should throw an error, and when we close the span
         * we should see the queued span with error details.
         */
        MockTracer tracer = this.injectMockTracer.tracer;
        TracingExecutor executor = new TracingExecutor(
                new ThreadPoolExecutor(1, 1, 500, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<Runnable>(1)), tracer);
        Logger log = Logger.getAnonymousLogger();
        executor.execute(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) { }
        });
        executor.execute(() -> { });
        Boolean rejected = false;
        try (ActiveSpan parent = tracer.buildSpan("parent").startActive()) {
            try {
                executor.execute(() -> {
                    try (ActiveSpan span = tracer.buildSpan("work").startActive()) {
                        span.setTag("work", "done");
                    }
                });
            } catch (RejectedExecutionException e) {
                rejected = true;
            }
        }
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        List<MockSpan> finishedSpans = tracer.finishedSpans();
        /* We expect 2 spans:
         *  [-----parent-----]
         *  []<- queued;
         */
        assertEquals(true, rejected);
        assertEquals(2, finishedSpans.toArray().length);
        MockSpan parentSpan = null;
        MockSpan queuedSpan = null;
        long traceId = finishedSpans.get(0).context().traceId();
        for (MockSpan span : finishedSpans) {
            log.log(Level.INFO, String.format("span %s", span.toString()));
            assertEquals(String.format("broken trace span %s", span.toString()), traceId, span.context().traceId());
            if (span.operationName().equals("parent")) {
                parentSpan = span;
            } else if (span.operationName().equals("Queue")) {
                queuedSpan = span;
            }
        }
        /* The parent of queued should be parent */
        assertEquals(queuedSpan.parentId(), parentSpan.context().spanId());
        /* The queued span should have metadata on the failure. The exception in particular */
        assertEquals(1, queuedSpan.logEntries().size());
    }

    @Test
    public void testNoSpanNoErrorAndNoSpans() throws Throwable {
        /*
         * Make an executor
         * submit work to it with no span activated
         * no error, and no spans generated
         */
        MockTracer tracer = this.injectMockTracer.tracer;
        TracingExecutor executor = newExecutor(tracer);
        executor.execute(() -> { });
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        List<MockSpan> finishedSpans = tracer.finishedSpans();
        assertEquals(0, finishedSpans.toArray().length);
    }
}
