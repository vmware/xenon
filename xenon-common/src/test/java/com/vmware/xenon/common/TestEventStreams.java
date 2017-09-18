/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common;

import java.net.ProtocolException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.services.common.EventStreamService;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestEventStreams extends BasicReusableHostTestCase {
    private static final List<ServerSentEvent> EVENTS = Arrays.asList(
            new ServerSentEvent().setData("test1\ntest2"),
            new ServerSentEvent().setEvent("test type").setData("some: data"),
            new ServerSentEvent().setId("id-1").setEvent("test type").setData("data1\ndata2\n"),
            new ServerSentEvent().setEvent("DONE")
    );

    private static final long EVENT_EMIT_PERIOD_MS = 500;
    private static final long INITIAL_DELAY_MS = 100;
    private static final int PARALLELISM = 10;

    private EventStreamService service;

    @Before
    public void setup() throws Throwable {
        this.service = new EventStreamService(EVENTS, INITIAL_DELAY_MS, EVENT_EMIT_PERIOD_MS, TimeUnit.MILLISECONDS, PARALLELISM);
        this.host.startService(this.service);
        this.host.waitForServiceAvailable(EventStreamService.SELF_LINK);
    }

    @After
    public void tearDown() throws Throwable {
        this.host.stopService(this.service);
    }

    @Test
    public void testSimpleLocal() throws Throwable {
        doSimpleTest(false);
    }

    @Test
    public void testSimpleRemote() throws Throwable {
        doSimpleTest(true);
    }

    private void doSimpleTest(boolean isRemote) {
        TestContext ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        List<ServerSentEvent> events = new ArrayList<>();
        List<Long> timesReceived = new ArrayList<>();
        Operation get = Operation.createGet(this.host, EventStreamService.SELF_LINK)
                .setHeadersReceivedHandler(op -> {
                    Assert.assertEquals(Operation.MEDIA_TYPE_TEXT_EVENT_STREAM,
                            op.getContentType());
                    Assert.assertEquals(0, events.size());
                })
                .setServerSentEventHandler(event -> {
                    timesReceived.add(System.currentTimeMillis());
                    events.add(event);
                })
                .setCompletion(ctx.getCompletion());
        if (isRemote) {
            get.forceRemote();
        }
        this.host.send(get);
        ctx.await();
        Assert.assertEquals(EVENTS, events);
        double averageDelay = IntStream.range(1, timesReceived.size())
                .mapToLong(i -> timesReceived.get(i) - timesReceived.get(i - 1))
                .average().getAsDouble();
        Assert.assertTrue(averageDelay >= EVENT_EMIT_PERIOD_MS / 2.0);
    }

    @Test
    public void testMaxParallelismLocal() {
        long idealDuration = INITIAL_DELAY_MS + (EVENTS.size() - 1) * EVENT_EMIT_PERIOD_MS;
        long startTime = System.currentTimeMillis();
        doParallelTest(PARALLELISM, false);
        long actualDuration = System.currentTimeMillis() - startTime;
        Assert.assertTrue(actualDuration >= idealDuration);
        Assert.assertTrue(actualDuration < idealDuration * 2);
    }

    @Test
    public void testMaxParallelismRemote() {
        long idealDuration = INITIAL_DELAY_MS + (EVENTS.size() - 1) * EVENT_EMIT_PERIOD_MS;
        long startTime = System.currentTimeMillis();
        doParallelTest(PARALLELISM, true);
        long actualDuration = System.currentTimeMillis() - startTime;
        Assert.assertTrue(actualDuration >= idealDuration);
        Assert.assertTrue(actualDuration < idealDuration * 2);
    }

    private void doParallelTest(int parallelism, boolean isRemote) {
        TestContext ctx = TestContext.create(parallelism, TimeUnit.MINUTES.toMicros(1));
        List<ServerSentEvent> events = new ArrayList<>();
        for (int i = 0; i < parallelism; ++i) {
            Operation get = Operation.createGet(this.host, EventStreamService.SELF_LINK)
                    .setServerSentEventHandler(event -> {
                        synchronized (events) {
                            events.add(event);
                        }
                    })
                    .setCompletion(ctx.getCompletion());
            if (isRemote) {
                get.forceRemote();
            }
            this.host.send(get);
        }
        ctx.await();
        Assert.assertEquals(EVENTS.size() * parallelism, events.size());
    }

    @Test
    public void testWithLoad() throws Throwable {
        Level level = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        try {
            doTestWithLoad();
        } finally {
            ResourceLeakDetector.setLevel(level);
        }
    }

    private void doTestWithLoad() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            this.doParallelTest(PARALLELISM, true);
        });
        future.whenComplete(ctx.getCompletionDeferred());
        List<DeferredResult<String>> deferredResults = new ArrayList<>();
        while (!future.isDone() && deferredResults.size() < 500) {
            ExampleServiceState state = new ExampleServiceState();
            state.name = "test";
            Operation postOp = Operation.createPost(this.host, ExampleService.FACTORY_LINK)
                    .setBody(state).forceRemote();
            deferredResults.add(this.host.sendWithDeferredResult(postOp, ExampleServiceState.class)
                    .thenApply(s -> s.documentSelfLink));
            Thread.sleep(3); // Slowdown
            if (Math.random() < 1 / 100.0) {
                System.gc();
            }
        }
        this.host.log("Requests sent: %d", deferredResults.size());
        ctx.await();
        ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        DeferredResult.allOf(deferredResults)
                .whenComplete(ctx.getCompletionDeferred());
        ctx.await();
        System.gc();
    }

    @Test
    public void testWithExceptionLocal() throws Throwable {
        String message = "Test failure";
        this.service.setFailException(new RuntimeException(message));
        try {
            this.doSimpleTest(false);
            Assert.fail("Expected to fail");
        } catch (RuntimeException e) {
            Assert.assertEquals(message, e.getMessage());
        } finally {
            this.service.setFailException(null);
        }
    }

    @Test
    public void testWithExceptionRemote() throws Throwable {
        String message = "Test failure";
        this.service.setFailException(new RuntimeException(message));
        try {
            List<ServerSentEvent> events = new ArrayList<>();
            Operation get = Operation.createGet(this.host, EventStreamService.SELF_LINK)
                    .setHeadersReceivedHandler(op -> {
                        Assert.assertEquals(Operation.MEDIA_TYPE_TEXT_EVENT_STREAM,
                                op.getContentType());
                    })
                    .setServerSentEventHandler(events::add);
            get.forceRemote();
            FailureResponse response = this.host.getTestRequestSender().sendAndWaitFailure(get);
            Assert.assertEquals(EVENTS, events);
            Throwable e = response.failure;
            Assert.assertNotNull(e);
            Assert.assertEquals(ProtocolException.class, e.getClass());
            Assert.assertTrue(e.getMessage().contains(message));
        } finally {
            this.service.setFailException(null);
        }
    }
}
