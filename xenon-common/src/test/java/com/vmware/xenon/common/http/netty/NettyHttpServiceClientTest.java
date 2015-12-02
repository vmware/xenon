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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleFactoryService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalTestService;

public class NettyHttpServiceClientTest {

    private static VerificationHost HOST;

    private VerificationHost host;

    public String testURI;

    public int requestCount = 16;

    public int serviceCount = 16;

    public int connectionCount = 32;

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

        SSLContext clientContext = SSLContext.getInstance(ServiceClient.TLS_PROTOCOL_NAME);
        clientContext.init(null, InsecureTrustManagerFactory.INSTANCE.getTrustManagers(), null);
        client.setSSLContext(clientContext);
        HOST.setClient(client);

        SelfSignedCertificate ssc = new SelfSignedCertificate();
        HOST.setCertificateFileReference(ssc.certificate().toURI());
        HOST.setPrivateKeyFileReference(ssc.privateKey().toURI());

        try {
            HOST.start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDown() {
        HOST.tearDown();
    }

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = HOST;
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(this.operationTimeout));
    }

    @After
    public void cleanUp() {
        ((NettyHttpServiceClient) this.host.getClient()).setConnectionLimitPerHost(
                NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST);
    }

    @Test
    public void throughputGetRemote() throws Throwable {
        if (this.testURI == null) {
            return;
        }
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(120));
        this.host.setTimeoutSeconds(120);
        this.host
                .log(
                        "Starting HTTP GET stress test against %s, request count: %d, connection limit: %d",
                        this.testURI, this.requestCount, this.connectionCount);
        this.host.getClient().setConnectionLimitPerHost(this.connectionCount);
        for (int i = 0; i < 3; i++) {
            long start = Utils.getNowMicrosUtc();
            getThirdPartyServerResponse(this.testURI, this.requestCount);
            long end = Utils.getNowMicrosUtc();
            double thpt = this.requestCount / ((end - start) / 1000000.0);
            this.host.log("Connection limit: %d, Request count: %d, Requests per second:%f",
                    this.connectionCount, this.requestCount, thpt);
            System.gc();
        }
    }

    @Test
    public void throughputPostRemote() throws Throwable {
        if (this.testURI == null) {
            return;
        }
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(120));
        this.host.setTimeoutSeconds(120);
        this.host
                .log(
                        "Starting HTTP POST stress test against %s, request count: %d, connection limit: %d",
                        this.testURI, this.requestCount, this.connectionCount);
        this.host.getClient().setConnectionLimitPerHost(this.connectionCount);
        long start = Utils.getNowMicrosUtc();
        ExampleServiceState body = new ExampleServiceState();
        body.name = UUID.randomUUID().toString();
        this.host.sendHttpRequest(this.host.getClient(), this.testURI, Utils.toJson(body),
                this.requestCount);
        long end = Utils.getNowMicrosUtc();
        double thpt = this.requestCount / ((end - start) / 1000000.0);
        this.host.log("Connection limit: %d, Request count: %d, Requests per second:%f",
                this.connectionCount, this.requestCount, thpt);
    }

    @Test
    public void httpsGetAndPut() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(10,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        List<URI> uris = new ArrayList<>();
        for (Service s : services) {
            URI u = UriUtils.extendUri(this.host.getSecureUri(), s.getSelfLink());
            uris.add(u);
        }

        this.host.getServiceState(null, MinimalTestServiceState.class, uris);

        this.host.testStart(uris.size());
        for (URI u : uris) {
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            Operation put = Operation.createPut(u).setBody(body)
                    .setCompletion(this.host.getCompletion());
            this.host.send(put);
        }
        this.host.testWait();
    }

    @Test
    public void httpsFailure() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(10,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        List<URI> uris = new ArrayList<>();
        for (Service s : services) {
            URI u = UriUtils.extendUri(this.host.getSecureUri(), s.getSelfLink());
            uris.add(u);
        }

        this.host.getServiceState(null, MinimalTestServiceState.class, uris);

        this.host.testStart(uris.size());
        for (URI u : uris) {
            MinimalTestServiceState body = (MinimalTestServiceState) this.host
                    .buildMinimalTestState();
            // simulate exception to reproduce https connection pool blocking on failure
            body.id = MinimalTestService.STRING_MARKER_HAS_CONTEXT_ID;
            Operation put = Operation.createPatch(u).setBody(body)
                    .setCompletion((o, e) -> {
                        this.host.completeIteration();
                    });
            this.host.send(put);
        }
        this.host.testWait();
    }

    @Test
    public void getSingleNoQueueingNotFound() throws Throwable {
        this.host.testStart(1);
        Operation get = Operation
                .createGet(UriUtils.buildUri(this.host, UUID.randomUUID().toString()))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_QUEUING)
                .setCompletion(
                        (op, ex) -> {
                            if (op.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_NOT_FOUND"));
                        });

        this.host.send(get);
        this.host.testWait();
    }

    @Test
    public void getQueueServiceAvailability() throws Throwable {
        String targetPath = UUID.randomUUID().toString();
        Operation startOp = Operation.createPost(UriUtils.buildUri(this.host, targetPath))
                .setCompletion(this.host.getCompletion());
        StatelessService testStatelessService = new StatelessService() {
            @Override
            public void handleRequest(Operation update) {
                update.complete();
            }
        };
        this.host.testStart(2);
        Operation get = Operation
                .createGet(UriUtils.buildUri(this.host, targetPath))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_QUEUE_FOR_SERVICE_AVAILABILITY)
                .setCompletion(
                        (op, ex) -> {
                            if (op.getStatusCode() == Operation.STATUS_CODE_OK) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_OK"));
                        });

        this.host.send(get);
        this.host.startService(startOp, testStatelessService);
        this.host.testWait();
    }

    @Test
    public void remotePatchTimeout() throws Throwable {
        doRemotePatchWithTimeout(false);
    }

    @Test
    public void remotePatchWithCallbackTimeout() throws Throwable {
        doRemotePatchWithTimeout(true);
    }

    private void doRemotePatchWithTimeout(boolean useCallback) throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);
        this.host.toggleNegativeTestMode(true);
        this.host.setOperationTimeOutMicros(TimeUnit.MILLISECONDS.toMicros(250));

        // send a request to the MinimalTestService, with a body that makes it NOT complete it
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_TIMEOUT_REQUEST;

        int count = NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST * 2;
        // timeout tracking currently works only for remote requests ...
        this.host.testStart(count);
        for (int i = 0; i < count; i++) {
            Operation request = Operation
                    .createPatch(services.get(0).getUri())
                    .forceRemote()
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
            if (useCallback) {
                this.host.sendRequestWithCallback(request.setReferer(this.host.getReferer()));
            } else {
                this.host.send(request);
            }
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(10));
    }

    @Test
    public void putSingle() throws Throwable {
        long serviceCount = 1;
        List<Service> services = this.host.doThroughputServiceStart(serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);
        this.host.doPutPerService(
                EnumSet.of(TestProperty.SINGLE_ITERATION),
                services);
        this.host.doPutPerService(
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.SINGLE_ITERATION),
                services);
        this.host.doPutPerService(
                EnumSet.of(TestProperty.CALLBACK_SEND, TestProperty.SINGLE_ITERATION),
                services);
        this.host.doPutPerService(
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.CALLBACK_SEND,
                        TestProperty.SINGLE_ITERATION),
                services);

        // check that content type is set and preserved
        URI u = services.get(0).getUri();
        this.host.testStart(1);
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_USE_DIFFERENT_CONTENT_TYPE;
        Operation patch = Operation
                .createPatch(u)
                .setBody(body)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                this.host.failIteration(e);
                                return;
                            }

                            if (!Operation.MEDIA_TYPE_APPLICATION_X_WWW_FORM_ENCODED.equals(o
                                    .getContentType())) {
                                this.host.failIteration(new IllegalArgumentException(
                                        "unexpected content type: " + o.getContentType()));
                                return;
                            }

                            this.host.completeIteration();
                        });
        this.host.send(patch);
        this.host.testWait();

        // verify content de-serializes with slightly different content type
        String contentType = "application/json; charset=UTF-8";
        this.host.testStart(1);
        MinimalTestServiceState body1 = new MinimalTestServiceState();
        body1.id = UUID.randomUUID().toString();
        body1.stringValue = UUID.randomUUID().toString();
        patch = Operation
                .createPatch(u)
                .setBody(body1)
                .setContentType(contentType)
                .forceRemote();

        Operation p = patch;
        p.setCompletion((o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }
            try {
                MinimalTestServiceState rsp = o.getBody(MinimalTestServiceState.class);
                assertEquals(body1.stringValue, rsp.stringValue);
                assertEquals(o.getContentType(), p.getContentType());
                this.host.completeIteration();
            } catch (Throwable ex) {
                this.host.failIteration(ex);
            }
        });
        this.host.send(p);
        this.host.testWait();
    }

    @Test
    public void putSingleNoQueueing() throws Throwable {
        long s = Utils.getNowMicrosUtc();
        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);

        URI uriToMissingService = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK
                + "/"
                + UUID.randomUUID().toString());

        this.host.testStart(1);
        // Use a URI that belongs to a replicated factory, like Examples, which would normally
        // cause the this.host to queue the request until the child became available
        Operation put = Operation.createPut(uriToMissingService)
                .setBody(this.host.buildMinimalTestState())
                .addRequestHeader(Operation.PRAGMA_HEADER, Operation.PRAGMA_DIRECTIVE_NO_QUEUING)
                .setCompletion(this.host.getExpectedFailureCompletion());

        this.host.send(put);
        this.host.testWait();

        uriToMissingService = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK + "/"
                + UUID.randomUUID().toString());

        this.host.testStart(1);
        put = Operation
                .createPut(
                        uriToMissingService)
                .setBody(this.host.buildMinimalTestState())
                .forceRemote()
                .addRequestHeader(Operation.PRAGMA_HEADER, Operation.PRAGMA_DIRECTIVE_NO_QUEUING)
                .setCompletion(this.host.getExpectedFailureCompletion());

        this.host.send(put);
        this.host.testWait();
        long e = Utils.getNowMicrosUtc();

        if (e - s > this.host.getOperationTimeoutMicros() / 2) {
            throw new TimeoutException("Request got queued, it should have bypassed queuing");
        }

        uriToMissingService = UriUtils.buildUri(this.host, ExampleFactoryService.SELF_LINK + "/"
                + UUID.randomUUID().toString());

        ServiceClient nonXenonLookingClient = null;
        try {
            nonXenonLookingClient = NettyHttpServiceClient.create(UUID.randomUUID().toString(),
                    Executors.newFixedThreadPool(1), Executors.newScheduledThreadPool(1));
            nonXenonLookingClient.start();
            s = Utils.getNowMicrosUtc();
            // try a JAVA HTTP client and verify we do not queue.
            this.host.sendWithJavaClient(uriToMissingService,
                    Operation.MEDIA_TYPE_APPLICATION_JSON,
                    Utils.toJson(new ExampleServiceState()));

            // try a Xenon client but with user agent saying its NOT Xenon. Notice that there is no
            // pragma directive so unless the service this.host detects the user agent, it will try
            // to queue and wait for service
            this.host.testStart(1);
            put = Operation
                    .createPut(
                            uriToMissingService)
                    .setBody(this.host.buildMinimalTestState())
                    .setExpiration(Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(1000))
                    .setReferer(this.host.getReferer())
                    .forceRemote()
                    .setCompletion(this.host.getExpectedFailureCompletion());
            nonXenonLookingClient.send(put);
            this.host.testWait();

            e = Utils.getNowMicrosUtc();
            if (e - s > this.host.getOperationTimeoutMicros() / 2) {
                throw new TimeoutException("Request got queued, it should have bypassed queuing");
            }
        } finally {
            if (nonXenonLookingClient != null) {
                nonXenonLookingClient.stop();
            }
        }
    }

    @Test
    public void putRemoteLargeAndBinaryBody() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(
                1, MinimalTestService.class, this.host.buildMinimalTestState(), null,
                null);

        // large, binary body
        this.host.doPutPerService(EnumSet.of(TestProperty.FORCE_REMOTE,
                TestProperty.SINGLE_ITERATION, TestProperty.LARGE_PAYLOAD,
                TestProperty.BINARY_PAYLOAD), services);

        // try local (do not force remote)
        this.host.doPutPerService(
                EnumSet.of(TestProperty.SINGLE_ITERATION, TestProperty.LARGE_PAYLOAD,
                        TestProperty.BINARY_PAYLOAD),
                services);

        // large, string (JSON) body
        this.host.doPutPerService(EnumSet.of(TestProperty.FORCE_REMOTE,
                TestProperty.SINGLE_ITERATION, TestProperty.LARGE_PAYLOAD),
                services);
    }

    @Test
    public void putOverMaxRequestLimit() throws Throwable {
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(1));
        List<Service> services = this.host.doThroughputServiceStart(
                8, MinimalTestService.class, this.host.buildMinimalTestState(), null,
                null);
        // force failure by using a payload higher than max size
        this.host.doPutPerService(1,
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.LARGE_PAYLOAD,
                        TestProperty.BINARY_PAYLOAD, TestProperty.FORCE_FAILURE),
                services);
    }

    @Test
    public void putSingleWithFailure() throws Throwable {
        long serviceCount = 1;
        List<Service> services = this.host.doThroughputServiceStart(serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);
        this.host.doPutPerService(
                EnumSet.of(TestProperty.FORCE_FAILURE,
                        TestProperty.SINGLE_ITERATION),
                services);

        this.host.doPutPerService(
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.FORCE_FAILURE,
                        TestProperty.SINGLE_ITERATION),
                services);

        // send some garbage that the service will not even be able to parse
        this.host.testStart(1);
        this.host.send(Operation.createPut(services.get(0).getUri())
                .setBody("this is not JSON")
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();

        // create an operation with no body and verify completion gets called with
        // failure
        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();
    }

    @Test
    public void throughputPutRemote() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        if (!this.host.isStressTest()) {
            this.host.log("Single connection runs");
            ((NettyHttpServiceClient) this.host.getClient()).setConnectionLimitPerHost(1);
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
        }

        for (int i = 0; i < 5; i++) {
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
            for (int k = 0; k < 5; k++) {
                Runtime.getRuntime().gc();
                Runtime.getRuntime().runFinalization();
            }
        }

        if (!this.host.isStressTest()) {
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.CALLBACK_SEND),
                    services);
        }
    }

    @Test
    public void throughputNonPersistedServiceGetSingleConnection() throws Throwable {
        long serviceCount = 256;
        ((NettyHttpServiceClient) this.host.getClient()).setConnectionLimitPerHost(1);
        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();

        EnumSet<TestProperty> props = EnumSet.of(TestProperty.FORCE_REMOTE);
        long c = this.host.computeIterationsFromMemory(props, (int) serviceCount);
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                body,
                EnumSet.noneOf(Service.ServiceOption.class), null);

        doGetThroughputTest(props, body, c, services);
    }

    @Test
    public void throughputNonPersistedServiceGet() throws Throwable {
        int serviceCount = 1;
        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();
        // produce a JSON PODO that serialized is about 2048 bytes
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 53; i++) {
            sb.append(UUID.randomUUID().toString());
        }
        body.stringValue = sb.toString();

        long c = this.requestCount;
        List<Service> services = this.host.doThroughputServiceStart(
                serviceCount, MinimalTestService.class,
                body,
                EnumSet.noneOf(Service.ServiceOption.class), null);

        // in memory test, just cloning and serialization, avoid sockets
        for (int i = 0; i < 3; i++) {
            doGetThroughputTest(EnumSet.noneOf(TestProperty.class), body, c, services);
        }

        // using loop back, sockets
        for (int i = 0; i < 3; i++) {
            doGetThroughputTest(EnumSet.of(TestProperty.FORCE_REMOTE), body, c, services);
        }

        // again but skip serialization, ask service to return string for response
        for (int i = 0; i < 3; i++) {
            doGetThroughputTest(EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.TEXT_RESPONSE),
                    body, c, services);
        }
    }

    public void doGetThroughputTest(EnumSet<TestProperty> props, MinimalTestServiceState body,
            long c,
            List<Service> services) throws Throwable {

        long concurrencyFactor = c / 10;
        this.host.log("Properties: %s, count: %d, bytes per rsp: %d", props, c,
                Utils.toJson(body).getBytes().length);
        URI u = services.get(0).getUri();

        AtomicInteger inFlight = new AtomicInteger();
        this.host.testStart(c);

        Operation get = Operation.createGet(u)
                .setCompletion((o, e) -> {
                    inFlight.decrementAndGet();
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    MinimalTestServiceState st = o.getBody(MinimalTestServiceState.class);
                    try {
                        assertTrue(st.id != null);
                        assertTrue(st.documentSelfLink != null);
                        assertTrue(st.documentUpdateTimeMicros > 0);
                    } catch (Throwable ex) {
                        this.host.failIteration(ex);
                    }
                    this.host.completeIteration();
                });
        if (props.contains(TestProperty.FORCE_REMOTE)) {
            get.forceRemote();
        }

        if (props.contains(TestProperty.TEXT_RESPONSE)) {
            get.addRequestHeader("Accept", Operation.MEDIA_TYPE_TEXT_PLAIN);
        }

        for (int i = 0; i < c; i++) {
            inFlight.incrementAndGet();
            this.host.send(get);
            if (inFlight.get() < concurrencyFactor) {
                continue;
            }
            while (inFlight.get() > concurrencyFactor) {
                Thread.sleep(10);
            }
        }
        this.host.testWait();
        this.host.logThroughput();
    }

    private String getThirdPartyServerResponse(String uri, int count) throws Throwable {
        return this.host.sendHttpRequest(this.host.getClient(), uri, null, count);
    }

    public void singleCookieTest(boolean forceRemote) throws Throwable {
        String link = UUID.randomUUID().toString();
        this.host.startServiceAndWait(CookieService.class, link);

        // Ask cookie service to set a cookie
        CookieServiceState setState = new CookieServiceState();
        setState.action = CookieAction.SET;
        setState.cookies = new HashMap<>();
        setState.cookies.put("key", "value");

        Operation setOp = Operation
                .createPatch(UriUtils.buildUri(this.host, link))
                .setCompletion(this.host.getCompletion())
                .setBody(setState);
        if (forceRemote) {
            setOp.forceRemote();
        }
        this.host.testStart(1);
        this.host.send(setOp);
        this.host.testWait();

        // Retrieve set cookies
        List<Map<String, String>> actualCookies = new ArrayList<>();
        Operation getOp = Operation
                .createGet(UriUtils.buildUri(this.host, link))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    CookieServiceState getState = o.getBody(CookieServiceState.class);
                    actualCookies.add(getState.cookies);
                    this.host.completeIteration();
                });
        if (forceRemote) {
            getOp.forceRemote();
        }
        this.host.testStart(1);
        this.host.send(getOp);
        this.host.testWait();

        assertNotNull("expect cookies to be set", actualCookies.get(0));
        assertEquals(1, actualCookies.get(0).size());
        assertEquals("value", actualCookies.get(0).get("key"));
    }

    @Test
    public void singleCookieLocal() throws Throwable {
        singleCookieTest(false);
    }

    @Test
    public void singleCookieRemote() throws Throwable {
        singleCookieTest(true);
    }

    public enum CookieAction {
        SET, DELETE,
    }

    public static class CookieServiceState extends ServiceDocument {
        public CookieAction action;
        public Map<String, String> cookies;
    }

    public static class CookieService extends StatefulService {
        public CookieService() {
            super(CookieServiceState.class);
        }

        @Override
        public void handleGet(Operation op) {
            CookieServiceState state = new CookieServiceState();
            state.cookies = op.getCookies();
            op.setBody(state).complete();
        }

        @Override
        public void handlePatch(Operation op) {
            CookieServiceState state = op.getBody(CookieServiceState.class);
            if (state == null) {
                op.fail(new IllegalArgumentException("body required"));
                return;
            }

            switch (state.action) {
            case SET:
                for (Entry<String, String> e : state.cookies.entrySet()) {
                    op.addResponseCookie(e.getKey(), e.getValue());
                }
                break;
            case DELETE:
                break;
            default:
                op.fail(new IllegalArgumentException("invalid action"));
                return;
            }

            op.complete();
        }
    }

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
                EnumSet.noneOf(Service.ServiceOption.class), null);
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
        this.host.logThroughput();
    }

}
