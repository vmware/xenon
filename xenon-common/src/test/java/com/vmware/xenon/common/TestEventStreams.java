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

import static org.junit.Assert.assertEquals;

import java.net.ProtocolException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
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

    public static class SessionClientService extends StatelessService {
        public static final String SELF_LINK = "session-client";
        public static final String STAT_NAME_EVENT_COUNT = "sessionClient.eventCount";
        public static final String STAT_NAME_IS_CONNECTED = "sessionClient.isConnected";
        private URI serverUri;

        public SessionClientService(URI serverUri) {
            this.serverUri = serverUri;
            toggleOption(ServiceOption.INSTRUMENTATION, true);
        }

        @Override
        public void handleStart(Operation start) {
            Operation.createPost(UriUtils.buildUri(this.serverUri, EventStreamService.SELF_LINK))
                    .setBody(new ServiceDocument())
                    .setHeadersReceivedHandler(i -> {
                        setStat(STAT_NAME_IS_CONNECTED, 1.0);
                        start.complete();
                    })
                    .setServerSentEventHandler(i -> adjustStat(STAT_NAME_EVENT_COUNT, 1.0))
                    .setCompletion((o, e) -> setStat(STAT_NAME_IS_CONNECTED, 0.0))
                    .sendWith(this);
        }
    }

    private EventStreamService service;

    public int connectionCount = 2;

    public int eventsPerConnection = 100;

    private List<VerificationHost> hostsToCleanup = new ArrayList<>();

    @Rule
    public TestResults testResults = new TestResults();
    private EventStreamService torrent;

    @Before
    public void setup() throws Throwable {
        this.service = new EventStreamService(EVENTS, INITIAL_DELAY_MS, EVENT_EMIT_PERIOD_MS, TimeUnit.MILLISECONDS,
                PARALLELISM, 1);
        this.host.startService(this.service);

        this.torrent = new EventStreamService(EVENTS, 0, 0, TimeUnit.MILLISECONDS, PARALLELISM,
                (this.eventsPerConnection / EVENTS.size()) + 1);
        this.host.startService(Operation.createPost(UriUtils.buildUri(this.host, "/torrent")), this.torrent);

        this.host.waitForServiceAvailable("/torrent", EventStreamService.SELF_LINK);
    }

    @After
    public void tearDown() throws Throwable {
        this.host.stopService(this.service);
        this.host.stopService(this.torrent);
        this.hostsToCleanup.forEach(VerificationHost::tearDown);
        this.hostsToCleanup.clear();
    }

    @Test
    public void testSimpleLocal() throws Throwable {
        doSimpleTest(false);
    }

    @Test
    public void testSimpleRemote() throws Throwable {
        doSimpleTest(true);
    }

    @Test
    public void testThroughput() throws Throwable {
        TestContext ctx = TestContext.create(this.connectionCount, TimeUnit.MINUTES.toMicros(1));

        AtomicInteger receivedCount = new AtomicInteger();
        long start = System.nanoTime();
        for (int i = 0; i < this.connectionCount; ++i) {
            Operation get = Operation.createGet(this.host, "/torrent")
                    .setServerSentEventHandler(event -> {
                        int n = receivedCount.incrementAndGet();
                        if (n % 500 == 0) {
                            this.host.log("Received event %d", n);
                        }
                    })
                    .setCompletion(ctx.getCompletion());
            get.forceRemote();
            this.host.send(get);
        }
        ctx.await();

        long end = System.nanoTime();

        int n = receivedCount.get();
        double durationSeconds = (end - start) / 1_000_000_000.0;
        double thput = n / durationSeconds;

        this.testResults.getReport().lastValue(TestResults.KEY_THROUGHPUT, thput);

        this.testResults.getReport().lastValue("events", n);
        this.testResults.getReport().lastValue("connections", this.connectionCount);
        this.testResults.getReport().lastValue("events/conn", this.eventsPerConnection);
        this.testResults.getReport().lastValue("parallelism", PARALLELISM);

        this.host.log("throughput (events/s): %f", thput);
        this.host.log("events: %s", n);
        this.host.log("connections: %s", this.connectionCount);
        this.host.log("events/conn: %s", this.eventsPerConnection);
        this.host.log("parallelism: %s", PARALLELISM);
    }

    private void doSimpleTest(boolean isRemote) {
        TestContext ctx = TestContext.create(1, TimeUnit.MINUTES.toMicros(1));
        List<ServerSentEvent> events = new ArrayList<>();
        List<Long> timesReceived = new ArrayList<>();
        Operation get = Operation.createGet(this.host, EventStreamService.SELF_LINK)
                .setHeadersReceivedHandler(op -> {
                    assertEquals(Operation.MEDIA_TYPE_TEXT_EVENT_STREAM,
                            op.getContentType());
                    assertEquals(0, events.size());
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
        assertEquals(EVENTS, events);
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
        assertEquals(EVENTS.size() * parallelism, events.size());
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
            assertEquals(message, e.getMessage());
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
                        assertEquals(Operation.MEDIA_TYPE_TEXT_EVENT_STREAM,
                                op.getContentType());
                    })
                    .setServerSentEventHandler(events::add);
            get.forceRemote();
            FailureResponse response = this.host.getTestRequestSender().sendAndWaitFailure(get);
            assertEquals(EVENTS, events);
            Throwable e = response.failure;
            Assert.assertNotNull(e);
            assertEquals(ProtocolException.class, e.getClass());
            Assert.assertTrue(e.getMessage().contains(message));
        } finally {
            this.service.setFailException(null);
        }
    }

    @Test
    public void testClientDisconnects() throws Throwable {
        VerificationHost clientHost = VerificationHost.create(0);
        clientHost.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        clientHost.start();
        this.hostsToCleanup.add(clientHost);

        clientHost.startServiceAndWait(new SessionClientService(this.host.getUri()),
                SessionClientService.SELF_LINK, null);

        Map<String, ServiceStats.ServiceStat> stats;
        URI clientUri = UriUtils.buildUri(clientHost, SessionClientService.SELF_LINK);

        // Verify that client was able to connect to the service
        stats = this.host.getServiceStats(clientUri);
        assertEquals(1.0, stats.get(SessionClientService.STAT_NAME_IS_CONNECTED).latestValue, 0);

        // Send some events to the client
        final int eventCount = 10;
        List<Operation> patchOps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Operation patch = Operation.createPatch(this.host, EventStreamService.SELF_LINK)
                    .setBody("{ \"key\": \"value\" }");
            patchOps.add(patch);
        }
        this.sender.sendAndWait(patchOps);

        // Verify that the client received the expected number of events
        this.host.waitFor("Client did not receive the expected number of events", () -> {
            Map<String, ServiceStats.ServiceStat> svcStats = this.host.getServiceStats(clientUri);
            ServiceStats.ServiceStat countStat = svcStats.get(SessionClientService.STAT_NAME_EVENT_COUNT);
            return countStat != null && countStat.latestValue == eventCount;
        });

        // Stop the client to simulate a disconnect
        clientHost.tearDown();
        this.hostsToCleanup.clear();

        // Let's try to send another event to the client.
        // This time the server should fail because the client is disconnected
        Operation patch = Operation.createPatch(this.host, EventStreamService.SELF_LINK)
                .setBody("{ \"key\": \"value\" }");
        this.sender.sendAndWaitFailure(patch);
    }
}
