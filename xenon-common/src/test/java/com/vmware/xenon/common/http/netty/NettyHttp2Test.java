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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.MinimalTestService;

/**
 * Tests for our Netty-based HTTP/2 implementation
 */
public class NettyHttp2Test {

    private static VerificationHost HOST;

    private VerificationHost host;

    // Operation timeout is in seconds
    public int operationTimeout = 5;

    @BeforeClass
    public static void setUpOnce() throws Exception {

        NettyChannelContext.setMaxRequestSize(1024 * 512);
        HOST = VerificationHost.create(0);
        CommandLineArgumentParser.parseFromProperties(HOST);
        HOST.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));

        ServiceClient client = NettyHttpServiceClient.create(
                NettyHttpServiceClientTest.class.getCanonicalName(),
                Executors.newFixedThreadPool(4),
                Executors.newScheduledThreadPool(1), HOST);

        HOST.setClient(client);

        try {
            HOST.start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = HOST;
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(this.operationTimeout));
    }

    @AfterClass
    public static void tearDown() {
        HOST.tearDown();
    }

    @After
    public void cleanUp() {
    }

    /**
     * A very basic verification that HTTP/2 appears to work: Just do 10 GETs and check the response
     * @throws Throwable
     */
    @Test
    public void basicHttp2() throws Throwable {
        int numGets = 10;

        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();
        // produce a JSON PODO that serialized is about 2048 bytes
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 53; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        body.stringValue = sb.toString();

        List<Service> services = this.host.doThroughputServiceStart(
                1,
                MinimalTestService.class,
                body,
                null, null);
        Service service = services.get(0);
        URI u = service.getUri();

        this.host.testStart(numGets);

        Operation get = Operation.createGet(u)
                .forceRemote()
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_USE_HTTP2)
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
                        assertTrue(st.stringValue != null);
                    } catch (Throwable ex) {
                        this.host.failIteration(ex);
                    }
                    this.host.completeIteration();
                });

        for (int i = 0; i < numGets; i++) {
            this.host.send(get);
        }
        this.host.testWait();
        this.host.log("Basic HTTP/2 validation passed");
    }

    /**
     * When using HTTP/2, we only use a single connection. A naive implementation would see
     * requests completed in the same order that they were sent. HTTP/2 supports multiplexing,
     * so in general we may get responses in a different order. We verify this works by sending
     * a series of requests and forcing the first one to have a significant delay: we should
     * receive the response after the others.
     * @param useCallback
     * @throws Throwable
     */
    @Test
    public void validateHttp2Multiplexing() throws Throwable {
        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);

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
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_USE_HTTP2)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.failIteration(e);
                            return;
                        }
                        completionTimes[operationId] = Utils.getNowMicrosUtc();
                        this.host.completeIteration();
                        return;
                    });
            this.host.send(getRequest);
        }
        this.host.testWait();
        assertTrue(completionTimes[0] > completionTimes[1]);
        this.host.log("HTTP/2 connections are being multiplexed.");
    }


    /**
     * Validate that when we have a request that times out, everything proceeds as expected.
     *
     * Note that this test throws a lot of log spew because operations that are timed out are
     * logged.
     *
     * @throws Throwable
     */
    @Test
    public void validateHttp2Timeouts() throws Throwable {
        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);
        this.host.toggleNegativeTestMode(true);
        this.host.setOperationTimeOutMicros(TimeUnit.MILLISECONDS.toMicros(250));

        // send a request to the MinimalTestService, with a body that makes it NOT complete it
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_TIMEOUT_REQUEST;

        int count = 10;
        this.host.testStart(count);
        for (int i = 0; i < count; i++) {
            Operation request = Operation
                    .createPatch(service.getUri())
                    .forceRemote()
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_USE_HTTP2)
                    .setBody(body)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            // timeout occurred, good
                            this.host.completeIteration();
                            return;
                        }
                        this.host.failIteration(new IllegalStateException(
                                "Request should have timed out"));
                    });
            this.host.send(request);
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);

        // Validate that we used a single connection for this. We do this indirectly:
        // We know that each request will create a new stream. Also client-initiated streams
        // are only odd-numbered (1, 3, ...). So if we have 2*count streams, we re-used a single
        // connection for the entire test.
        NettyHttpServiceClient client = (NettyHttpServiceClient) this.host.getClient();
        NettyChannelContext context = client.getCurrentHttp2Context(
                ServiceHost.LOCAL_HOST, this.host.getPort());
        assertTrue(context.getLargestStreamId() > count * 2);
        this.host.log("HTTP/2 operations are correctly timed-out.");
    }

    /**
     * HTTP/2 has a limited number of streams that we can use per connection. When we've used
     * them all, a new connection has to be reopened. This tests that we do that correctly.
     * @throws Throwable
     */
    @Test
    public void validateStreamExhaustion() throws Throwable {
        // Allow two requests to be sent per connection by artificially limiting the
        // maximum stream id to 5. Why 5? Clients use only odd-numbered streams and
        // stream 1 is for negotiating settings. Therefore streams 3 and 5 are our
        // first two requests.
        NettyChannelContext.setMaxStreamId(5);

        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = UUID.randomUUID().toString();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);

        int count = 9;
        URI serviceUri = service.getUri();
        for (int i = 0; i < count; i++) {
            this.host.getServiceState(
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.HTTP2),
                    MinimalTestServiceState.class, serviceUri);
        }

        NettyHttpServiceClient client = (NettyHttpServiceClient) this.host.getClient();
        NettyChannelContext context = client.getCurrentHttp2Context(
                ServiceHost.LOCAL_HOST, this.host.getPort());
        // We sent an odd number of requests, so we should see that exactly one request was sent
        // on the most recent connection.
        assertTrue(context.getLargestStreamId() == 3);
        this.host.log("HTTP/2 connections correctly reopen when streams are exhausted");

        // We run the maintenance, then ensure we have one connection open.
        client.handleMaintenance(Operation.createPost(null)
                .setCompletion((o, e) -> {
                }));
        assertTrue(client.countHttp2Contexts(ServiceHost.LOCAL_HOST, this.host.getPort()) == 1);
        NettyChannelContext.setMaxStreamId(NettyChannelContext.DEFAULT_MAX_STREAM_ID);
    }

}
