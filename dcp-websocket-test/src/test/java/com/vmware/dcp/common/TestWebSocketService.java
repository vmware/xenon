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

import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;

import com.vmware.dcp.services.common.ExampleFactoryService;
import com.vmware.dcp.services.common.ExampleService;
import com.vmware.dcp.services.common.FileContentService;
import com.vmware.dcp.services.common.ServiceUriPaths;

/**
 * JavaScript-based WebSocket-connected DCP service test
 * <p/>
 * Based on Mozilla Rhino JavaScript engine.
 * <p/>
 * ToDo: migrate to PhantomJS whenever 2.* is available in Maven Central for all platforms
 */
public class TestWebSocketService extends BasicTestCase {
    public static final long LONG_TIMEOUT_MILLIS = 2000;
    public static final int WAIT_TICK = 10;
    public static final String WS_TEST_JS_PATH = "/ws-test/ws-test.js";
    public static final String HOST = "host";
    public static final String LOCATION = "location";
    public static final String DOCUMENT = "document";
    public static final String WS_TEST_JS = "ws-test.js";
    public static final String OBJECTS_CREATED = "objectsCreated";
    public static final String EXAMPLES_SUBSCRIPTIONS =
            ExampleFactoryService.SELF_LINK + ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS;

    private String echoServiceUri;
    private String observerServiceUri;
    private Context context;
    private Scriptable scope;

    private static class EchoServiceResponse {
        public String method;
        public EchoServiceRequest requestBody;
    }

    private static class EchoServiceRequest {
        String someValue;
    }

    @Before
    public void setUp() throws Throwable {
        // Bootstrap auxiliary test services
        List<Operation> ops = new LinkedList<>();
        ops.add(Operation.createPost(UriUtils.buildUri(host, WS_TEST_JS_PATH)));
        List<Service> svcs = new LinkedList<>();
        svcs.add(new FileContentService(new File(getClass().getResource(WS_TEST_JS_PATH)
                .toURI())));
        host.startCoreServicesSynchronously(ops, svcs);

        // Prepare JavaScript context with WebSocket API emulation
        JsExecutor
                .executeSynchronously(() -> {
                    this.context = Context.enter();
                    this.scope = this.context.initStandardObjects();
                    ScriptableObject.defineClass(this.scope, JsWebSocket.class);
                    ScriptableObject.defineClass(this.scope, JsDocument.class);
                    ScriptableObject.defineClass(this.scope, JsA.class);
                    NativeObject location = new NativeObject();
                    location.defineProperty(HOST, host.getPublicUri().getHost() + ":"
                            + host.getPublicUri().getPort(),
                            NativeObject.READONLY);
                    ScriptableObject.putProperty(this.scope, LOCATION, location);
                    ScriptableObject.putProperty(this.scope, DOCUMENT,
                            this.context.newObject(this.scope, JsDocument.CLASS_NAME));
                    this.context.evaluateReader(
                            this.scope,
                            new InputStreamReader(
                                    UriUtils.buildUri(host, ServiceUriPaths.WS_SERVICE_LIB_JS_PATH)
                                            .toURL().openStream()),
                            ServiceUriPaths.WS_SERVICE_LIB_JS, 1, null);
                    this.context.evaluateReader(this.scope, new InputStreamReader(UriUtils
                            .buildUri(host,
                                    WS_TEST_JS_PATH).toURL().openStream()), WS_TEST_JS, 1, null);
                    return null;
                });
        this.echoServiceUri = waitAndGetValue("echoServiceUri");
        this.observerServiceUri = waitAndGetValue("observerServiceUri");

    }

    /**
     * Tests that GET method is correctly forwarded to JS and response is correctly forwarded back
     */
    @Test
    public void testGet() throws Exception {
        Operation op = Operation.createGet(URI.create(this.echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that POST method is correctly forwarded to JS and response is correctly forwarded back
     */
    @Test
    public void testPost() throws Exception {
        Operation op = Operation.createPost(URI.create(this.echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that PUT method is correctly forwarded to JS and response is correctly forwarded back
     */
    @Test
    public void testPut() throws Exception {
        Operation op = Operation.createPut(URI.create(this.echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that PATCH method is correctly forwarded to JS and response is correctly forwarded back
     */
    @Test
    public void testPatch() throws Exception {
        Operation op = Operation.createPatch(URI.create(this.echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that DELETE method is correctly forwarded to JS and response is correctly forwarded back
     */
    @Test
    public void testDelete() throws Exception {
        Operation op = Operation.createDelete(URI.create(this.echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that JS service can subscribe and receive notifications and then that it can gracefully unsubscribe
     */
    @Test
    public void testSubscribeUnsubscribe() throws Exception {
        // Validate that observer service is subscribed to example factory
        String someValue = UUID.randomUUID().toString();
        URI observerUri = URI.create(this.observerServiceUri);
        waitForSubscriptionToAppear(observerUri, EXAMPLES_SUBSCRIPTIONS);

        // Validate that observer receives notifications
        Operation postExample = Operation.createPost(UriUtils.buildUri(host,
                ExampleFactoryService.SELF_LINK));
        ExampleService.ExampleServiceState body = new ExampleService.ExampleServiceState();
        body.name = someValue;
        postExample.setBody(body);
        postExample.setReferer(observerUri);
        Operation postRes = completeOperationSynchronously(postExample);
        String created = waitAndGetArrayAsText(OBJECTS_CREATED);
        Assert.assertEquals("Document self link",
                postRes.getBody(ExampleService.ExampleServiceState.class).documentSelfLink, created);

        JsExecutor.executeSynchronously(() -> {
            this.context.evaluateString(
                    this.scope, "observerService.unsubscribe('/core/examples/subscriptions')",
                    "<cmd>", 1, null);
        });
        // Invoke unsubscribe() method and verify that subscription is unregistered
        waitForSubscriptionToDisappear(observerUri, EXAMPLES_SUBSCRIPTIONS);
    }

    /**
     * Tests that JS service can subscribe and receive notifications and that subscription is removed when service is
     * stopped
     */
    @Test
    public void testSubscribeStop() throws Exception {
        // Validate that observer service is subscribed to example factory
        String someValue = UUID.randomUUID().toString();
        URI observerUri = URI.create(this.observerServiceUri);
        waitForSubscriptionToAppear(observerUri, EXAMPLES_SUBSCRIPTIONS);

        // Validate that observer receives notifications
        Operation postExample = Operation.createPost(UriUtils.buildUri(host,
                ExampleFactoryService.SELF_LINK));
        ExampleService.ExampleServiceState body = new ExampleService.ExampleServiceState();
        body.name = someValue;
        postExample.setBody(body);
        postExample.setReferer(observerUri);
        Operation postRes = completeOperationSynchronously(postExample);
        String created = waitAndGetArrayAsText(OBJECTS_CREATED);
        Assert.assertEquals("Document self link",
                postRes.getBody(ExampleService.ExampleServiceState.class).documentSelfLink, created);

        // Invoke stop() method and verify that subscription is unregistered
        JsExecutor.executeSynchronously(() -> this.context.evaluateString(this.scope,
                "observerService.stop()", "<cmd>", 1, null));
        waitForSubscriptionToDisappear(observerUri, EXAMPLES_SUBSCRIPTIONS);
    }

    /**
     * Tests that JS service can subscribe and receive notifications and that subscription is removed when connection
     * is closed ungracefully (i.e. browser tab is closed).
     */
    @Test
    public void testSubscribeClose() throws Exception {
        // Validate that observer service is subscribed to example factory
        String someValue = UUID.randomUUID().toString();
        URI observerUri = URI.create(this.observerServiceUri);
        waitForSubscriptionToAppear(observerUri, EXAMPLES_SUBSCRIPTIONS);

        // Validate that observer receives notifications
        Operation postExample = Operation.createPost(UriUtils.buildUri(host,
                ExampleFactoryService.SELF_LINK));
        ExampleService.ExampleServiceState body = new ExampleService.ExampleServiceState();
        body.name = someValue;
        postExample.setBody(body);
        postExample.setReferer(observerUri);
        Operation postRes = completeOperationSynchronously(postExample);
        String created = waitAndGetArrayAsText(OBJECTS_CREATED);
        Assert.assertEquals("Document self link",
                postRes.getBody(ExampleService.ExampleServiceState.class).documentSelfLink, created);
        ((JsWebSocket) JsExecutor.executeSynchronously(() -> this.context.evaluateString(
                this.scope,
                "connection.webSocket",
                "<cmd>", 1,
                null))).close();

        waitForSubscriptionToDisappear(observerUri, EXAMPLES_SUBSCRIPTIONS);
    }

    private String waitAndGetValue(String varName) throws Exception {
        long timeoutMillis = LONG_TIMEOUT_MILLIS;
        while (true) {
            Object v = JsExecutor.executeSynchronously(() -> ScriptableObject.getProperty(
                    this.scope,
                    varName));
            String value = v == null ? null : v.toString();
            if (value != null && !value.isEmpty() && !(v instanceof Undefined)) {
                return value;
            }
            timeoutMillis -= WAIT_TICK;
            if (timeoutMillis < 0) {
                Assert.fail("Failed to get echo service URI");
            }
            Thread.sleep(WAIT_TICK);
        }
    }

    private Operation completeOperationSynchronously(Operation op) throws InterruptedException,
            java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        CompletableFuture<Operation> future = new CompletableFuture<>();
        op.setCompletion((completedOp, failure) -> future.complete(completedOp));
        host.send(op);
        return future.get(1, TimeUnit.SECONDS);
    }

    private void testEchoOperation(Operation op) throws InterruptedException,
            java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        String someValue = UUID.randomUUID().toString();
        EchoServiceRequest echoServiceRequest = new EchoServiceRequest();
        echoServiceRequest.someValue = someValue;
        op.setBody(echoServiceRequest);
        Operation resp = completeOperationSynchronously(op);
        Assert.assertEquals("statusCode", Operation.STATUS_CODE_OK, resp.getStatusCode());
        EchoServiceResponse body = resp.getBody(EchoServiceResponse.class);
        Assert.assertEquals("method", op.getAction().name(), body.method);
        Assert.assertEquals("body", someValue, body.requestBody.someValue);
    }

    private void waitForSubscriptionToAppear(URI observerUri, String subscriptionPath)
            throws Exception {
        Operation getSubscriptions = Operation.createGet(UriUtils.buildUri(host, subscriptionPath));
        getSubscriptions.setReferer(observerUri);
        long timeout = LONG_TIMEOUT_MILLIS;
        for (;;) {
            Operation res = completeOperationSynchronously(getSubscriptions);
            ServiceSubscriptionState state = res.getBody(ServiceSubscriptionState.class);
            if (state.subscribers.containsKey(observerUri)) {
                break;
            }

            timeout -= WAIT_TICK;
            if (timeout < 0) {
                Assert.fail("Subscription is not set up");
            }
            Thread.sleep(WAIT_TICK);
        }
    }

    private void waitForSubscriptionToDisappear(URI observerUri, String subscriptionPath)
            throws Exception {
        Operation getSubscriptions = Operation.createGet(UriUtils.buildUri(host, subscriptionPath));
        getSubscriptions.setReferer(observerUri);
        long timeout = LONG_TIMEOUT_MILLIS;
        for (;;) {
            Operation res = completeOperationSynchronously(getSubscriptions);
            ServiceSubscriptionState state = res.getBody(ServiceSubscriptionState.class);
            if (!state.subscribers.containsKey(observerUri)) {
                break;
            }

            timeout -= WAIT_TICK;
            if (timeout < 0) {
                Assert.fail("Subscription is not unregistered");
            }
            Thread.sleep(WAIT_TICK);
        }
    }

    private String waitAndGetArrayAsText(String name) throws InterruptedException {
        long timeoutMillis = LONG_TIMEOUT_MILLIS;
        while (true) {
            Scriptable o = (Scriptable) JsExecutor.executeSynchronously(() -> ScriptableObject
                    .getProperty(this.scope, name));
            if (o.getIds().length > 0) {
                List<String> values = new LinkedList<>();
                for (Object id : o.getIds()) {
                    Object v = o.get((Integer) id, null);
                    values.add(v == null ? null : v.toString());
                }
                return String.join(Operation.CR_LF, values);
            }
            timeoutMillis -= WAIT_TICK;
            if (timeoutMillis < 0) {
                Assert.fail("Failed to get echo service URI");
            }
            Thread.sleep(WAIT_TICK);
        }
    }

}
