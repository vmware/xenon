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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.AuthorizationHelper;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestProperty;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.ReplicationFactoryTestService;
import com.vmware.xenon.services.common.ReplicationTestService;
import com.vmware.xenon.services.common.ReplicationTestService.ReplicationTestServiceState;

public class NettyHttpServiceClientTest {

    private static VerificationHost HOST;

    private static final boolean ENABLE_AUTH = false;

    private static final String SAMPLE_EMAIL = "sample@vmware.com";
    private static final String SAMPLE_PASSWORD = "password";

    private VerificationHost host;

    public String testURI;

    public int requestCount = 16;

    public int serviceCount = 32;

    public int connectionCount = 32;

    // Operation timeout is in seconds
    public int operationTimeout = 5;

    @BeforeClass
    public static void setUpOnce() throws Throwable {
        HOST = VerificationHost.create(0);
        HOST.setAuthorizationEnabled(ENABLE_AUTH);
        HOST.setRequestPayloadSizeLimit(1024 * 512);
        HOST.setResponsePayloadSizeLimit(1024 * 512);

        CommandLineArgumentParser.parseFromProperties(HOST);
        HOST.setMaintenanceIntervalMicros(
                TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));

        ServiceClient client = NettyHttpServiceClient.create(
                NettyHttpServiceClientTest.class.getSimpleName(),
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
            CommandLineArgumentParser.parseFromProperties(HOST);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        if (ENABLE_AUTH) {
            // Create example user auth related objects
            AuthorizationSetupHelper.AuthSetupCompletion authCompletion = (ex) -> {
                if (ex == null) {
                    HOST.completeIteration();
                } else {
                    HOST.failIteration(ex);
                }
            };

            HOST.setSystemAuthorizationContext();
            HOST.testStart(1);
            AuthorizationSetupHelper.create()
                    .setHost(HOST)
                    .setUserEmail(SAMPLE_EMAIL)
                    .setUserPassword(SAMPLE_PASSWORD)
                    .setUserSelfLink(SAMPLE_EMAIL)
                    .setIsAdmin(true)
                    .setCompletion(authCompletion)
                    .start();
            HOST.testWait();
            HOST.resetAuthorizationContext();
        }

    }

    @AfterClass
    public static void tearDown() {
        HOST.log("final teardown");
        HOST.tearDown();
    }

    @Before
    public void setUp() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        this.host = HOST;
        this.host.log("restoring operation timeout");
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(this.operationTimeout));

        if (ENABLE_AUTH) {
            // find user with system auth context, then assumeIdentity will reset with found user
            HOST.setSystemAuthorizationContext();
            String userServicePath = new AuthorizationHelper(this.host)
                    .findUserServiceLink(SAMPLE_EMAIL);
            this.host.assumeIdentity(userServicePath);
        }
    }

    @After
    public void cleanUp() {
        this.host.log("cleanup");

        if (ENABLE_AUTH) {
            this.host.resetAuthorizationContext();
        }
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
                            int statusCode = op.getStatusCode();
                            if (statusCode == Operation.STATUS_CODE_OK) {
                                this.host.completeIteration();
                                return;
                            }

                            this.host.failIteration(new Throwable(
                                    "Expected Operation.STATUS_CODE_OK but was " + statusCode));
                        });

        this.host.send(get);
        this.host.startService(startOp, testStatelessService);
        this.host.testWait();
    }

    @Test
    public void sendRequestWithTimeout() throws Throwable {
        doRemotePatchWithTimeout(false);
    }

    @Test
    public void sendRequestWithCallbackWithTimeout() throws Throwable {
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

        this.host.getClient()
                .setConnectionLimitPerHost(NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST);
        int count = NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST;

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

            request.toggleOption(OperationOption.SEND_WITH_CALLBACK, useCallback);
            this.host.send(request);

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
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);

        URI uriToMissingService = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK
                + "/"
                + UUID.randomUUID().toString());

        this.host.testStart(1);
        // Use a URI that belongs to a replicated factory, like Examples, which would normally
        // cause the this.host to queue the request until the child became available
        Operation put = Operation.createPut(uriToMissingService)
                .setBody(this.host.buildMinimalTestState())
                .setCompletion(this.host.getExpectedFailureCompletion());

        this.host.send(put);
        this.host.testWait();

        uriToMissingService = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK + "/"
                + UUID.randomUUID().toString());

        this.host.testStart(1);
        put = Operation
                .createPut(
                        uriToMissingService)
                .setBody(this.host.buildMinimalTestState())
                .forceRemote()
                .setCompletion(this.host.getExpectedFailureCompletion());

        this.host.send(put);
        this.host.testWait();
        long e = Utils.getNowMicrosUtc();

        if (e - s > this.host.getOperationTimeoutMicros() / 2) {
            throw new TimeoutException("Request got queued, it should have bypassed queuing");
        }

        uriToMissingService = UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK + "/"
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
                2, MinimalTestService.class, this.host.buildMinimalTestState(), null,
                null);
        // force failure by using a payload higher than max size
        this.host.doPutPerService(1,
                EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.LARGE_PAYLOAD,
                        TestProperty.BINARY_PAYLOAD, TestProperty.FORCE_FAILURE),
                services);

        // create a PUT request larger than the allowed size of a request and verify that it fails.
        ServiceDocument largeState = this.host.buildMinimalTestState(
                this.host.getClient().getRequestPayloadSizeLimit() + 100);
        this.host.testStart(1);
        Operation put = Operation.createPut(services.get(0).getUri())
                .forceRemote()
                .setBody(largeState)
                .setCompletion((o, e) -> {
                    if (e != null && e instanceof IllegalArgumentException &&
                            e.getMessage().contains("is greater than max size allowed")) {
                        this.host.completeIteration();
                        return;
                    }
                    this.host.failIteration(
                            new IllegalStateException("Operation was expected to fail because " +
                            "op.getContentLength() is more than allowed"));
                });
        this.host.send(put);
        this.host.testWait();
    }

    @Test
    public void putSingleWithFailure() throws Throwable {
        long serviceCount = 1;
        List<Service> services = this.host.doThroughputServiceStart(serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        verifyErrorResponseBodyHandling(services);

        // induce a failure that does warrant a retry. Verify we do get proper retries
        verifyRequestRetryPolicy(services);

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

        this.host.testStart(1);
        this.host.send(Operation.createPut(services.get(0).getUri())
                .setBody("this is not JSON")
                .forceRemote()
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();

        // create an operation with no body and verify completion gets called with
        // failure
        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();

        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .forceRemote()
                .setCompletion(this.host.getExpectedFailureCompletion()));
        this.host.testWait();
    }

    private void verifyErrorResponseBodyHandling(List<Service> services) throws Throwable {
        // send a body with instructions to the test service to fail the
        // request, but set the error body as plain text. This verifies the runtime
        // preserves the plain text error response
        CompletionHandler ch = (o, e) -> {
            if (e == null) {
                this.host.failIteration(new IllegalStateException("expected failure"));
                return;
            }

            Object rsp = o.getBodyRaw();
            if (!o.getContentType().equals(Operation.MEDIA_TYPE_TEXT_PLAIN)
                    || !(rsp instanceof String)) {
                this.host.failIteration(new IllegalStateException(
                        "expected text plain content type and response"));
                return;
            }
            this.host.completeIteration();
        };

        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_FAIL_WITH_PLAIN_TEXT_RESPONSE;
        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .setBody(body)
                .setCompletion(ch));
        this.host.testWait();

        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .setBody(body)
                .forceRemote()
                .setCompletion(ch));
        this.host.testWait();

        // now verify we leave binary or custom content type error responses alone
        // in process response will stay as string
        ch = (o, e) -> {
            if (e == null) {
                this.host.failIteration(new IllegalStateException("expected failure"));
                return;
            }

            Object rsp = o.getBodyRaw();
            if (!o.getContentType().equals(MinimalTestService.CUSTOM_CONTENT_TYPE)
                    || !(rsp instanceof String)) {
                this.host.failIteration(new IllegalStateException(
                        "expected custom content type and binary response"));
                return;
            }
            this.host.completeIteration();
        };

        body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_FAIL_WITH_CUSTOM_CONTENT_TYPE_RESPONSE;
        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .setBody(body)
                .setCompletion(ch));
        this.host.testWait();

        // cross node response will stay as binary
        ch = (o, e) -> {
            if (e == null) {
                this.host.failIteration(new IllegalStateException("expected failure"));
                return;
            }

            Object rsp = o.getBodyRaw();
            if (!o.getContentType().equals(MinimalTestService.CUSTOM_CONTENT_TYPE)
                    || !(rsp instanceof byte[])) {
                this.host.failIteration(new IllegalStateException(
                        "expected custom content type and binary response"));
                return;
            }
            this.host.completeIteration();
        };

        this.host.testStart(1);
        this.host.send(Operation.createPatch(services.get(0).getUri())
                .setBody(body)
                .forceRemote()
                .setCompletion(ch));
        this.host.testWait();
    }

    private void verifyRequestRetryPolicy(List<Service> services) throws Throwable {
        MinimalTestService targetService = (MinimalTestService) services.get(0);
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_RETRY_REQUEST;
        body.stringValue = MinimalTestService.STRING_MARKER_RETRY_REQUEST;
        Operation patchWithRetry = Operation.createPatch(targetService.getUri())
                .setCompletion(this.host.getCompletion())
                .setBody(body)
                .forceRemote()
                .setRetryCount(1)
                .setContextId(UUID.randomUUID().toString());
        // the service should fail the request, the client should then retry, the service will then
        // succeed it. We use the context id to track and correlate the retried requests
        this.host.sendAndWait(patchWithRetry);

        // create a replicated, owner selected Service, since its the replication code that
        // does implicit retries, and we want to verify it does not do them unless its a replication
        // conflict

        ReplicationFactoryTestService replFactory = new ReplicationFactoryTestService();
        this.host.startServiceAndWait(replFactory,
                ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK, null);

        // create a child service
        ReplicationTestServiceState initState = new ReplicationTestServiceState();
        initState.documentSelfLink = UUID.randomUUID().toString();
        Operation post = Operation.createPost(replFactory.getUri())
                .setBody(initState)
                .setCompletion(this.host.getCompletion());
        this.host.sendAndWait(post);
        URI childURI = UriUtils.buildUri(this.host.getUri(),
                ReplicationFactoryTestService.OWNER_SELECTION_SELF_LINK,
                initState.documentSelfLink);

        // verify that we do NOT retry, unless the service error response has SHOULD_RETRY
        // enabled
        initState.stringField = ReplicationTestService.STRING_MARKER_FAIL_WITH_CONFLICT_CODE;
        Operation put = Operation.createPut(childURI)
                .setCompletion(
                        this.host.getExpectedFailureCompletion(Operation.STATUS_CODE_CONFLICT))
                .setBody(initState)
                .forceRemote()
                .setRetryCount(1)
                .setContextId(UUID.randomUUID().toString());
        // if the replication code retries, the request will timeout, since it retries until expiration. We check
        // for CONFLICT code explicitly so the test will fail if fail due to timeout
        this.host.sendAndWait(put);
    }

    @Test
    public void throughputPutRemote() throws Throwable {
        List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        if (!this.host.isStressTest()) {
            this.host.log("Single connection runs");
            this.host.getClient().setConnectionLimitPerHost(1);
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
            this.host.getClient()
                    .setConnectionLimitPerHost(NettyHttpServiceClient.DEFAULT_CONNECTIONS_PER_HOST);
        } else {
            this.host.setOperationTimeOutMicros(
                    TimeUnit.SECONDS.toMicros(this.host.getTimeoutSeconds()));
        }

        // use global limit, which applies by default to all tags
        int limit = this.host.getClient().getConnectionLimitPerHost();
        this.host.connectionTag = null;
        this.host.log("Using client global connection limit %d", limit);

        for (int i = 0; i < 5; i++) {
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE),
                    services);
            this.host.waitForGC();
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.BINARY_SERIALIZATION),
                    services);
            this.host.waitForGC();
        }

        limit = 8;
        this.host.connectionTag = "http1.1test";
        this.host.log("Using tag specific connection limit %d", limit);
        this.host.getClient().setConnectionLimitPerTag(this.host.connectionTag, limit);
        this.host.doPutPerService(
                this.requestCount,
                EnumSet.of(TestProperty.FORCE_REMOTE),
                services);
    }

    @Test
    public void throughputPutRemoteWithCallback() throws Throwable {
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(120));
        List<Service> services = this.host.doThroughputServiceStart(this.serviceCount,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                null, null);

        for (int i = 0; i < 5; i++) {
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.CALLBACK_SEND),
                    services);
            this.host.doPutPerService(
                    this.requestCount,
                    EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.BINARY_SERIALIZATION,
                            TestProperty.CALLBACK_SEND),
                    services);
        }
    }

    @Test
    public void throughputNonPersistedServiceGetSingleConnection() throws Throwable {
        long serviceCount = 256;
        this.host.getClient().setConnectionLimitPerHost(1);
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

        // using loop back, sockets, and callback pattern
        for (int i = 0; i < 3; i++) {
            doGetThroughputTest(EnumSet.of(TestProperty.FORCE_REMOTE, TestProperty.CALLBACK_SEND),
                    body, c, services);
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

                    if (!props.contains(TestProperty.TEXT_RESPONSE)) {
                        if (!o.hasBody()) {
                            this.host.failIteration(new IllegalStateException("no body"));
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
                    }
                    this.host.completeIteration();
                });
        if (props.contains(TestProperty.FORCE_REMOTE)) {
            get.forceRemote();
        }

        if (props.contains(TestProperty.TEXT_RESPONSE)) {
            get.addRequestHeader("Accept", Operation.MEDIA_TYPE_TEXT_PLAIN);
        }

        if (props.contains(TestProperty.CALLBACK_SEND)) {
            get.toggleOption(OperationOption.SEND_WITH_CALLBACK, true);
        }

        for (int i = 0; i < c; i++) {
            inFlight.incrementAndGet();
            this.host.send(get.setExpiration(this.host.getOperationTimeoutMicros()
                    + Utils.getNowMicrosUtc()));
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

    /**
     * Here we can test that headers sent by the client are sent correctly. The MinimalTestService
     * can return the headers it receives. Currently we are only testing the Accept header.
     */
    @Test
    public void validateHeaders() throws Throwable {
        MinimalTestService service = new MinimalTestService();
        MinimalTestServiceState initialState = new MinimalTestServiceState();
        initialState.id = "";
        initialState.stringValue = "";

        this.host.setSystemAuthorizationContext();
        this.host.startServiceAndWait(service, UUID.randomUUID().toString(), initialState);
        this.host.resetAuthorizationContext();

        Map<String, String> headers;

        headers = getHeaders(service.getUri(), false);
        assertTrue(headers != null);
        assertTrue(headers.containsKey(HttpHeaderNames.ACCEPT.toString()));
        assertTrue(headers.get(HttpHeaderNames.ACCEPT.toString()).equals("*/*"));

        headers = getHeaders(service.getUri(), true);
        assertTrue(headers != null);
        assertTrue(headers.containsKey(HttpHeaderNames.ACCEPT.toString()));
        assertTrue(headers.get(HttpHeaderNames.ACCEPT.toString())
                .equals(Operation.MEDIA_TYPE_APPLICATION_JSON));

        this.host.log("Headers validated");
    }

    /**
     * GET the headers the client sent by querying the MinimalTestService
     */
    Map<String, String> getHeaders(URI serviceUri, boolean setAccept) throws Throwable {
        final String[] headersRaw = new String[1];
        URI queryUri = UriUtils.extendUriWithQuery(
                serviceUri, MinimalTestService.QUERY_HEADERS, "true");
        Operation get = Operation.createGet(queryUri)
                .forceRemote()
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        this.host.failIteration(ex);
                        return;
                    }
                    MinimalTestServiceState s = op.getBody(MinimalTestServiceState.class);
                    headersRaw[0] = s.stringValue;
                    this.host.completeIteration();
                });

        if (setAccept) {
            get.addRequestHeader(HttpHeaderNames.ACCEPT.toString(),
                    Operation.MEDIA_TYPE_APPLICATION_JSON);
        }

        this.host.testStart(1);
        this.host.send(get);
        this.host.testWait();

        if (headersRaw[0] == null) {
            return null;
        }
        String[] headerLines = headersRaw[0].split("\\n");
        Map<String, String> headers = new HashMap<>();
        for (String headerLine : headerLines) {
            String[] splitHeader = headerLine.split(":", 2);
            if (splitHeader.length == 2) {
                headers.put(splitHeader[0], splitHeader[1]);
            }
        }
        return headers;
    }

    /**
     * Validate that we throw reasonable exceptions when the URI is null, or the URI's host is null.
     */
    @Test
    public void validateOperationChecks() throws Throwable {
        URI noUri = null;
        URI noHostUri = new URI("/foo/bar/baz");

        Operation noUriOp = Operation.createGet(noUri).setReferer(noHostUri);
        Operation noHostOp = Operation.createGet(noHostUri).setReferer(noHostUri);

        this.host.testStart(2);
        noUriOp.setCompletion((op, ex) -> {
            if (ex == null) {
                this.host.failIteration(ex);
                return;
            }
            if (!ex.getMessage().contains("Uri is required")) {
                this.host.failIteration(new IllegalStateException("Unexpected exception"));
                return;
            }
            this.host.completeIteration();
        });

        noHostOp.setCompletion((op, ex) -> {
            if (ex == null) {
                this.host.failIteration(ex);
                return;
            }
            if (!ex.getMessage().contains("host")) {
                this.host.failIteration(new IllegalStateException("Unexpected exception"));
                return;
            }
            this.host.completeIteration();
        });

        this.host.toggleNegativeTestMode(true);
        ServiceClient cl = this.host.getClient();
        cl.send(noUriOp);
        cl.send(noHostOp);
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
    }

}
