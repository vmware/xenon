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

package com.vmware.xenon.common.http.netty;

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceClient.ConnectionPoolMetrics;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.TestResults;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.MinimalTestService;

/**
 * Tests for our Netty-based HTTP/2 implementation
 */
public class NettyHttp2Test {

    private VerificationHost host;

    // Large operation body size used in basicHttp test.
    // Body size is marked larger than the MAX_FRAME_SIZE to
    // verify that the channel can handle frame aggregation correctly.
    public int largeBodySize = 100000;

    // Number of GETs done in basicHttp2()
    public int requestCount = 10;

    // Number of service instances to target
    public int serviceCount = 32;

    // number of iterations per test (that supports multiple iterations)
    public int iterationCount = 1;

    @Rule
    public TestResults testResults = new TestResults();

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }

    private void setUpHost(boolean forceHttps) throws Throwable {
        this.host = VerificationHost.create(0);
        this.host.setRequestPayloadSizeLimit(1024 * 512);
        this.host.setResponsePayloadSizeLimit(1024 * 512);
        CommandLineArgumentParser.parseFromProperties(this.host);
        this.host.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        this.host.setStressTest(this.host.isStressTest);
        this.host.setOperationTimeOutMicros(
                TimeUnit.SECONDS.toMicros(this.host.getTimeoutSeconds()));

        if (forceHttps) {
            this.host.setPort(ServiceHost.PORT_VALUE_LISTENER_DISABLED);
            this.host.setSecurePort(0);
        }

        VerificationHost.createAndAttachSSLClient(this.host);

        try {
            this.host.start();
            if (this.host.isStressTest()) {
                this.host.getClient().setPendingRequestQueueLimit(1000000);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Throwable {
        NettyChannelContext.setMaxStreamId(Integer.MAX_VALUE / 2);
        if (this.host == null) {
            return;
        }
        this.host.tearDown();
        this.host = null;
    }

    @Test
    public void basicHttp2() throws Throwable {
        setUpHost(false);
        doBasicHttp2();
    }

    @Test
    public void basicHttp2OverSsl() throws Throwable {
        setUpHost(true);
        doBasicHttp2();
    }

    /**
     * A very basic verification that HTTP/2 appears to work: Do a PUT of a large body and then
     * GET it and validate it's correct.
     *
     * Note that this test fails with Netty 5.0alpha2 due to a bug when the HTTP/2 window
     * gets full:
     * https://github.com/netty/netty/commit/44ee2cac433a6f8640d01a70e8b90b70852aeeae
     *
     * The bug is triggered by the fact that we do enough GETs on the large body that we'll
     * fill up the window and one of the responses is broken into two frames, but (in alpha 2)
     * the second frame is never sent.
     *
     * It works with Netty 4.1.
     */
    private void doBasicHttp2() throws Throwable {
        this.host.log("Starting test: basicHttp2");
        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);

        MinimalTestServiceState largeState = new MinimalTestServiceState();
        final String largeBody = createLargeBody(this.largeBodySize);
        largeState.id = "";
        largeState.stringValue = largeBody;

        URI u = service.getUri();

        // Part 1: Verify we can PUT a large body
        this.host.testStart(1);
        Operation put = Operation.createPut(u)
                .forceRemote()
                .setConnectionSharing(true)
                .setBody(largeState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    MinimalTestServiceState st = o.getBody(MinimalTestServiceState.class);
                    try {
                        assertTrue(st.id != null);
                        assertTrue(st.documentSelfLink != null);
                        assertTrue(st.documentUpdateTimeMicros > 0);
                        assertTrue(st.stringValue.equals(largeBody));
                    } catch (Throwable ex) {
                        this.host.failIteration(ex);
                    }
                    this.host.completeIteration();
                });

        this.host.send(put);
        this.host.testWait();

        // Part 2: GET the large state and ensure it is correct.
        Operation get = Operation.createGet(u)
                .forceRemote()
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    MinimalTestServiceState st = o.getBody(MinimalTestServiceState.class);
                    try {
                        assertTrue(st.id != null);
                        assertTrue(st.documentSelfLink != null);
                        assertTrue(st.documentUpdateTimeMicros > 0);
                        assertTrue(st.stringValue.equals(largeBody));
                    } catch (Throwable ex) {
                        this.host.failIteration(ex);
                    }
                    this.host.completeIteration();
                });

        this.host.testStart(this.requestCount);
        for (int i = 0; i < this.requestCount; i++) {
            this.host.send(get);
        }
        this.host.testWait();
        this.host.log("Test passed: basicHttp2");
    }

    @Test
    public void validateHttp2Multiplexing() throws Throwable {
        setUpHost(false);
        doValidateHttp2Multiplexing();
    }

    @Test
    public void validateHttp2MultiplexingOverSsl() throws Throwable {
        setUpHost(true);
        doValidateHttp2Multiplexing();
    }

    /**
     * When using HTTP/2, we only use a single connection. A naive implementation would see
     * requests completed in the same order that they were sent. HTTP/2 supports multiplexing,
     * so in general we may get responses in a different order. We verify this works by sending
     * a series of requests and forcing the first one to have a significant delay: we should
     * receive the response after the others.
     */
    private void doValidateHttp2Multiplexing() throws Throwable {
        this.host.log("Starting test: validateHttp2Multiplexing");
        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);

        // we must set connection limit to 1, to ensure a single http2 connection
        this.host.connectionTag = ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT;
        this.host.getClient().setConnectionLimitPerTag(this.host.connectionTag, 1);

        // We do an initial GET, which opens the connection. We don't get multiplexing
        // until after the connection has been opened.
        URI serviceUri = service.getUri();
        this.host.getServiceState(
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2),
                MinimalTestServiceState.class, serviceUri);

        // Now we do parallel GETs and ensure that the first GET doesn't complete first.
        int count = 10;
        final long[] completionTimes = new long[count];
        this.host.testStart(count);
        for (int i = 0; i < count; i++) {
            final int operationId = i;
            URI u;
            if (i == 0) {
                // 1 means "1 second delay"
                u = UriUtils.extendUriWithQuery(serviceUri,
                        MinimalTestService.QUERY_DELAY_COMPLETION, "1");
            } else {
                u = serviceUri;
            }
            Operation getRequest = Operation
                    .createGet(u)
                    .forceRemote()
                    .setConnectionSharing(true)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        completionTimes[operationId] = Utils.getNowMicrosUtc();
                        this.host.completeIteration();
                    });
            this.host.send(getRequest);
        }
        this.host.testWait();
        assertTrue(completionTimes[0] > completionTimes[1]);

        String tag = ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT;
        ConnectionPoolMetrics tagInfo = this.host.getClient().getConnectionPoolMetrics(tag);
        assertTrue(tagInfo != null);
        assertTrue(tagInfo.inUseConnectionCount > 0);
        assertTrue(tagInfo.pendingRequestCount == 0);
    }

    @Test
    public void validateHttp2Timeouts() throws Throwable {
        setUpHost(false);
        doValidateHttp2Timeouts(false);
    }

    @Test
    public void validateHttp2TimeoutsOverSsl() throws Throwable {
        setUpHost(true);
        doValidateHttp2Timeouts(true);
    }

    /**
     * Validate that when we have a request that times out, everything proceeds as expected.
     *
     * Note that this test throws a lot of log spew because operations that are timed out are
     * logged.
     */
    private void doValidateHttp2Timeouts(boolean useHttps) throws Throwable {
        this.host.log("Starting test: validateHttp2Timeouts");
        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);

        // force single connection
        this.host.connectionTag = ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT;
        this.host.getClient().setConnectionLimitPerTag(this.host.connectionTag, 1);

        // do an initial GET to force the establishment of the HTTP/2 connection, so that the
        // subsequent requests are not cancelled by the host before the connection is set up.
        URI serviceUri = service.getUri();
        this.host.getServiceState(
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2),
                MinimalTestServiceState.class, serviceUri);

        // now toggle negative test mode and set the timeout value to something low.
        this.host.toggleNegativeTestMode(true);
        this.host.setOperationTimeOutMicros(TimeUnit.MILLISECONDS.toMicros(250));

        // send a request to the MinimalTestService, with a body that makes it NOT complete it
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_TIMEOUT_REQUEST;

        for (int i = 0; i < this.iterationCount; i++) {
            TestContext ctx = this.host.testCreate(this.requestCount);
            for (int j = 0; j < this.requestCount; j++) {
                Operation request = Operation.createPatch(serviceUri)
                        .forceRemote()
                        .setConnectionSharing(true)
                        .setBody(body)
                        .setCompletion((o, e) -> {
                            if (e != null && e instanceof TimeoutException) {
                                ctx.complete();
                                return;
                            }
                            if (e != null) {
                                ctx.fail(e);
                                return;
                            }
                            ctx.fail(new IllegalStateException("Request should have timed out"));
                        });
                this.host.send(request);
            }
            this.host.testWait(ctx);
        }

        this.host.toggleNegativeTestMode(false);

        // Validate that we used a single connection for this. We do this indirectly:
        // We know that each request will create a new stream. Also client-initiated streams
        // are only odd-numbered (1, 3, ...). So if we have 2*count streams, we re-used a single
        // connection for the entire test.
        //
        // Note that the operations can be cancelled by the HTTP client (and thus for the
        // completion handlers to be invoked) before the stream is ready and the HTTP/2 settings
        // have been negotiated, so it's necessary to wait here for the stream to be ready.
        NettyHttpServiceClient client = (NettyHttpServiceClient) this.host.getClient();
        NettyChannelContext context;
        if (useHttps) {
            context = client.getInUseHttp2SslContext(this.host.connectionTag,
                    ServiceHost.LOCAL_HOST, this.host.getSecurePort());
        } else {
            context = client.getInUseHttp2Context(this.host.connectionTag, ServiceHost.LOCAL_HOST,
                    this.host.getPort());
        }

        assertTrue(context != null);
        this.host.log("Largest stream ID: %d", context.getLargestStreamId());
        assertTrue(context.getLargestStreamId() >
                2 * (1 + this.iterationCount * this.requestCount));
        assertTrue(!context.hasActiveStreams());
    }

    /**
     * HTTP/2 has a limited number of streams that we can use per connection. When we've used
     * them all, a new connection has to be reopened. This tests that we do that correctly.
     * @throws Throwable
     */
    @Ignore("https://www.pivotaltracker.com/story/show/120392043")
    @Test
    public void validateStreamExhaustion() throws Throwable {
        this.host.log("Starting test: validateStreamExhaustion");
        int maxStreamId = 5;
        // Allow one request to be sent per connection by artificially limiting the
        // maximum stream id to 5. Why 5? Clients use only odd-numbered streams and
        // stream 1 is for negotiating settings. Therefore streams 3 and 5 are our
        // first two requests.
        NettyChannelContext.setMaxStreamId(5);

        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);

        // While it's sufficient to test with a much smaller number (this used to be 9)
        // this helps us verify that we're not hitting an old Netty bug (found in Netty 4.1b8)
        // in which we'd sometimes fail to open a connection. Netty would incorrectly claim
        // that we had sent data before the SETTINGS frame, which was not true. This tests runs
        // in about a third of a second on a Macbook Pro, so it's not too intense for daily tests
        //
        // NOTE: Increasing this number (e.g. 9999) fails test with
        //  "java.net.SocketException: Too many open files in system" or similar.
        //  It is expected behavior with following reason: closing connection is not keeping up
        //  with the pace of opening new connections.
        //
        //  Problem Details:
        //  "channel.close()" is called in "NettyChannelPool.closeHtp2Context()" which is called
        //  from "client.handleMaintenance()". However, performing closing channel is delayed by
        //  two reasons.
        //    1) "NettyChannelPool.closeHtp2Context()" has threashold time. Thereby calling
        //        "channel.close()" only happens when it exceeds the threashold time.
        //    2) In netty implementation, actual logic closing channel inside of "channel.close()"
        //       is done asynchronously. Therefore, even though "channel.close()" is called, it
        //       may not perform immediately.
        //
        //  Due to the aforementioned reasons, opening connections(new open file descriptor)
        //  surpass the number of closing connections, thus from outside, it looks like open file
        //  descriptors are kept increasing, and it reaches the max number of open files in
        //  operating system.
        //  Therefore, test case with "setMaxStreamId=5" and "counter=9999" fails. By setting
        //  "setMaxStreamId" to larger number(e.g.: 51), it slows down the new number of
        //  connections and test will pass.
        //
        //  for more detail: https://www.pivotaltracker.com/story/show/110535602
        int count = 99;
        this.host.connectionTag = ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT;
        this.host.getClient().setConnectionLimitPerTag(this.host.connectionTag, 1);
        URI serviceUri = service.getUri();
        for (int i = 0; i < count; i++) {
            MinimalTestServiceState getResult = this.host.getServiceState(
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2),
                    MinimalTestServiceState.class, serviceUri);
            assertTrue(getResult.stringValue.equals(initialState.stringValue));
        }

        NettyHttpServiceClient client = (NettyHttpServiceClient) this.host.getClient();
        NettyChannelContext context = client.getInUseHttp2Context(
                this.host.connectionTag, ServiceHost.LOCAL_HOST, this.host.getPort());
        assertTrue(context.getLargestStreamId() <= maxStreamId);
        this.host.log("HTTP/2 connections correctly reopen when streams are exhausted");

        this.host.waitFor("exhausted http2 channels not closed", () -> {
            // We run the maintenance, then ensure we have one connection open.
            client.handleMaintenance(Operation.createPost(null));
            int c = client.getInUseContextCount(
                    this.host.connectionTag, ServiceHost.LOCAL_HOST, this.host.getPort());
            this.host.log("Active http2 streams: %d, expected 1", c);
            return c == 1;
        });

        NettyChannelContext.setMaxStreamId(NettyChannelContext.DEFAULT_MAX_STREAM_ID);
        this.host.log("Test passed: validateStreamExhaustion");

        // This test is apparently hard on Netty 4, and sometimes it causes other tests to timeout.
        // We stop and start the client to ensure we get a new connection for the other tests.
        client.stop();
        client.start();
    }

    @Test
    public void pendingRequestLimit() throws Throwable {
        for (int i = 0; i < this.iterationCount; i++) {
            this.tearDown();
            setUpHost(false);

            List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                    MinimalTestService.class,
                    this.host.buildMinimalTestState(),
                    null, null);

            // we need a fresh connection pool, with no connections already created
            // HTTP 1.1 test (connectionSharing == false)
            String tag = ServiceClient.CONNECTION_TAG_DEFAULT;
            int http11Count = Math.max(services.size() * this.requestCount,
                    services.size() * this.host.getClient().getConnectionLimitPerTag(tag) * 4);
            verifyPerHostPendingRequestLimit(this.host, services,
                    tag,
                    http11Count / services.size(),
                    false);

            // HTTP/2 test (connectionSharing == true)
            // Limit the per-tag connection limit to 1 to reduce the likelihood that a connection
            // will be fully established before subsequent operations are submitted.
            tag = ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT;
            this.host.getClient().setConnectionLimitPerTag(tag, 1);
            verifyPerHostPendingRequestLimit(this.host, services,
                    tag,
                    this.requestCount,
                    true);
            this.host.getClient().stop();
        }
    }

    @Test
    public void throughputPutRemote() throws Throwable {
        setUpHost(false);
        doThroughputPutRemote();
    }

    @Test
    public void throughputPutRemoteOverSsl() throws Throwable {
        setUpHost(true);
        doThroughputPutRemote();
    }

    private void doThroughputPutRemote() throws Throwable {

        List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        // use global limit, which applies by default to all tags
        int limit = this.host.getClient()
                .getConnectionLimitPerTag(ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT);

        this.host.connectionTag = null;
        this.host.log("Using default http2 connection limit %d", limit);

        for (int i = 0; i < 5; i++) {
            double throughput = this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2),
                    services);
            this.testResults.getReport().all("HTTP/2 JSON PUTS/sec", throughput);
            this.host.waitForGC();
            throughput = this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2,
                            TestProperty.BINARY_SERIALIZATION),
                    services);
            this.testResults.getReport().all("HTTP/2 binary PUTS/sec", throughput);
            this.host.waitForGC();
        }

        // do some in process runs, do verify perf is not degraded
        for (int i = 0; i < 5; i++) {
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.noneOf(TestProperty.class),
                    services);
            this.host.waitForGC();
        }

        String tag = ServiceClient.CONNECTION_TAG_HTTP2_DEFAULT;
        ConnectionPoolMetrics tagInfo = this.host.getClient().getConnectionPoolMetrics(tag);
        assertTrue(tagInfo != null);
        assertTrue(tagInfo.inUseConnectionCount > 0);
        assertTrue(tagInfo.pendingRequestCount == 0);

        // set a specific tag limit
        limit = 16;
        this.host.connectionTag = "http2test";
        this.host.log("Using tag specific connection limit %d", limit);
        this.host.getClient().setConnectionLimitPerTag(this.host.connectionTag, limit);
        this.host.doPutPerService(
                this.requestCount,
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2),
                services);

        tag = this.host.connectionTag;
        tagInfo = this.host.getClient().getConnectionPoolMetrics(tag);
        assertTrue(tagInfo != null);
        assertTrue(tagInfo.inUseConnectionCount > 0);
        assertTrue(tagInfo.pendingRequestCount == 0);
    }

    /**
     * Create a large string
     */
    String createLargeBody(int minimumSize) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() <= minimumSize) {
            sb.append(UUID.randomUUID().toString());
        }
        return sb.toString();
    }

    private static void verifyPerHostPendingRequestLimit(
            VerificationHost host,
            List<Service> services,
            String tag,
            long requestCount,
            boolean connectionSharing) {
        int pendingLimit = host.getClient().getPendingRequestQueueLimit();
        try {
            host.getClient().setPendingRequestQueueLimit(1);
            // verify pending request limit enforcement
            TestContext ctx = host.testCreate(services.size() * requestCount);
            ctx.setTestName("Request limit validation").logBefore();
            AtomicInteger limitFailures = new AtomicInteger();
            for (Service s : services) {
                for (int i = 0; i < requestCount; i++) {
                    Operation put = Operation.createPut(s.getUri())
                            .forceRemote()
                            .setBodyNoCloning(host.buildMinimalTestState())
                            .setConnectionSharing(connectionSharing)
                            .setCompletion((o, e) -> {
                                if (e == null) {
                                    ctx.complete();
                                    return;
                                }
                                ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                                if (ServiceErrorResponse.ERROR_CODE_CLIENT_QUEUE_LIMIT_EXCEEDED == rsp
                                        .getErrorCode()) {
                                    limitFailures.incrementAndGet();
                                }
                                ctx.complete();
                            });
                    host.send(put);
                }
            }
            ctx.await();
            ctx.logAfter();
            host.log("Queue limit failures: %d", limitFailures.get());
            assertTrue(limitFailures.get() > 0);
            if (!connectionSharing) {
                NettyHttpServiceClientTest.validateTagInfo(host, tag);
            }
        } finally {
            host.getClient().setPendingRequestQueueLimit(pendingLimit);
        }
    }
}
