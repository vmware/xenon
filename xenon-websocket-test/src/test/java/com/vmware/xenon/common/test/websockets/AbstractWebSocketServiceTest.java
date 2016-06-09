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

package com.vmware.xenon.common.test.websockets;

import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.FileContentService;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * JavaScript-based WebSocket-connected service test
 * <p/>
 * Based on Mozilla Rhino JavaScript engine.
 * <p/>
 * ToDo: migrate to PhantomJS whenever 2.* is available in Maven Central for all platforms
 */
public abstract class AbstractWebSocketServiceTest extends BasicTestCase {
    public static final String WS_TEST_JS_PATH = "/ws-test/ws-test.js";
    public static final String HOST = "host";
    public static final String PROTOCOL = "protocol";
    public static final String LOCATION = "location";
    public static final String DOCUMENT = "document";
    public static final String WS_TEST_JS = "ws-test.js";
    public static final String OBJECTS_CREATED = "objectsCreated";
    public static final String EXAMPLES_SUBSCRIPTIONS = ExampleService.FACTORY_LINK
            + ServiceHost.SERVICE_URI_SUFFIX_SUBSCRIPTIONS;
    public static final String ERROR_VARIABLE = "errorOccurred";

    private static String echoServiceUri;
    private static String observerServiceUriForStop;
    private static String observerServiceUriForClose;
    private static String observerServiceUriForUnsubscribe;
    private static Context context;
    private static Scriptable scope;

    private static class EchoServiceResponse {
        public String method;
        public EchoServiceRequest requestBody;
    }

    private static class EchoServiceRequest {
        String someValue;
    }

    @Before
    public void setUp() throws Throwable {
        JsExecutor.host = this.host;

        if (this.host.getServiceStage(WS_TEST_JS_PATH) != null) {
            // already configured on reusable host
            return;
        }

        // Bootstrap auxiliary test services
        Service fs = new FileContentService(new File(getClass().getResource(WS_TEST_JS_PATH)
                .toURI()));
        this.host.startServiceAndWait(fs, WS_TEST_JS_PATH, null);

        // Prepare JavaScript context with WebSocket API emulation
        JsExecutor
                .executeSynchronously(() -> {
                    context = Context.enter();
                    scope = context.initStandardObjects();
                    ScriptableObject.defineClass(scope, JsWebSocket.class);
                    ScriptableObject.defineClass(scope, JsDocument.class);
                    ScriptableObject.defineClass(scope, JsA.class);
                    NativeObject location = new NativeObject();
                    location.defineProperty(HOST, this.host.getPublicUri().getHost() + ":"
                            + this.host.getPublicUri().getPort(),
                            NativeObject.READONLY);
                    location.defineProperty(PROTOCOL, this.host.getPublicUri().getScheme() + ":", NativeObject.READONLY);
                    ScriptableObject.putProperty(scope, LOCATION, location);
                    ScriptableObject.putProperty(scope, DOCUMENT,
                            context.newObject(scope, JsDocument.CLASS_NAME));
                    context.evaluateReader(
                            scope,
                            new InputStreamReader(
                                    UriUtils.buildUri(this.host, ServiceUriPaths.WS_SERVICE_LIB_JS_PATH)
                                            .toURL().openStream()),
                            ServiceUriPaths.WS_SERVICE_LIB_JS, 1, null);
                    context.evaluateReader(scope, new InputStreamReader(UriUtils
                            .buildUri(this.host,
                                    WS_TEST_JS_PATH)
                            .toURL().openStream()), WS_TEST_JS, 1, null);
                    return null;
                });
        echoServiceUri = waitAndGetValue("echoServiceUri");

        observerServiceUriForStop = waitAndGetValue("observerServiceUriForStop");
        observerServiceUriForClose = waitAndGetValue("observerServiceUriForClose");
        observerServiceUriForUnsubscribe = waitAndGetValue("observerServiceUriForUnsubscribe");
        // verify that we got notifications for the three observers created above
        waitAndGetArrayCount(OBJECTS_CREATED, 3);
    }

    /**
     * Tests that GET method is correctly forwarded to JS and response is correctly forwarded back
     * @throws Throwable
     */
    protected void testGet() throws Throwable {
        Operation op = Operation.createGet(URI.create(echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that POST method is correctly forwarded to JS and response is correctly forwarded back
     * @throws Throwable
     */
    protected void testPost() throws Throwable {
        Operation op = Operation.createPost(URI.create(echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that PUT method is correctly forwarded to JS and response is correctly forwarded back
     * @throws Throwable
     */
    protected void testPut() throws Throwable {
        Operation op = Operation.createPut(URI.create(echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that PATCH method is correctly forwarded to JS and response is correctly forwarded back
     * @throws Throwable
     */
    protected void testPatch() throws Throwable {
        Operation op = Operation.createPatch(URI.create(echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that DELETE method is correctly forwarded to JS and response is correctly forwarded back
     * @throws Throwable
     */
    protected void testDelete() throws Throwable {
        Operation op = Operation.createDelete(URI.create(echoServiceUri));
        testEchoOperation(op);
    }

    /**
     * Tests that JS service can subscribe and receive notifications and then that it can gracefully unsubscribe
     *
     * @param nameValue Value for example service name.
     * @throws Throwable
     */
    protected void subscribeUnsubscribe(String nameValue) throws Throwable {
        // Validate that observer service is subscribed to example factory
        URI observerUri = URI.create(observerServiceUriForUnsubscribe);
        waitForSubscriptionToAppear(observerUri, EXAMPLES_SUBSCRIPTIONS);

        // Validate that observer receives notifications
        verifyNotification(nameValue, observerUri);

        JsExecutor.executeSynchronously(() -> {
            context.evaluateString(
                    scope,
                    "observerServiceForUnsubscribe.unsubscribe('/core/examples/subscriptions')",
                    "<cmd>", 1, null);
        });
        // Invoke unsubscribe() method and verify that subscription is unregistered
        waitForSubscriptionToDisappear(observerUri, EXAMPLES_SUBSCRIPTIONS);
    }

    /**
     * Tests that JS service can subscribe and receive notifications and that subscription is removed when service is
     * stopped
     *
     * @param nameValue Value for example service name.
     * @throws Throwable
     */
    protected void subscribeStop(String nameValue) throws Throwable {
        URI observerUri = URI.create(observerServiceUriForStop);
        waitForSubscriptionToAppear(observerUri, EXAMPLES_SUBSCRIPTIONS);
        verifyNotification(nameValue, observerUri);

        // Invoke stop() method and verify that subscription is unregistered
        JsExecutor.executeSynchronously(() -> context.evaluateString(scope,
                "observerServiceForStop.stop()", "<cmd>", 1, null));
        waitForSubscriptionToDisappear(observerUri, EXAMPLES_SUBSCRIPTIONS);
    }

    /**
     * Tests that JS service can subscribe and receive notifications and that subscription is removed when connection is
     * closed ungracefully (i.e. browser tab is closed).
     *
     * @param nameValue Value for example service name.
     * @throws Throwable
     */
    protected void subscribeClose(String nameValue) throws Throwable {
        URI observerUri = URI.create(observerServiceUriForClose);
        waitForSubscriptionToAppear(observerUri, EXAMPLES_SUBSCRIPTIONS);
        verifyNotification(nameValue, observerUri);
        ((JsWebSocket) JsExecutor.executeSynchronously(() -> context.evaluateString(
                scope,
                "connection.webSocket",
                "<cmd>", 1,
                null))).close();

        waitForSubscriptionToDisappear(observerUri, EXAMPLES_SUBSCRIPTIONS);
    }

    private void verifyNotification(String someValue, URI observerUri) throws Throwable {
        Operation postExample = Operation.createPost(UriUtils.buildFactoryUri(this.host,
                ExampleService.class));
        ExampleService.ExampleServiceState body = new ExampleService.ExampleServiceState();
        body.name = someValue;
        postExample.setBody(body);
        postExample.setReferer(observerUri);
        Operation postRes = completeOperationSynchronously(postExample);
        String link = postRes.getBody(ExampleService.ExampleServiceState.class).documentSelfLink;
        waitAndGetArrayAsText(OBJECTS_CREATED, link);
    }

    private String waitAndGetValue(String varName) throws Exception {
        Date exp = this.host.getTestExpiration();
        while (new Date().before(exp)) {
            Object v = JsExecutor.executeSynchronously(() -> ScriptableObject.getProperty(
                    scope,
                    varName));
            String value = v == null ? null : v.toString();
            if (value != null && !value.isEmpty() && !(v instanceof Undefined)) {
                return value;
            }
            checkError();
            Thread.sleep(100);
        }

        throw new TimeoutException();
    }

    private void checkError() {
        Object e = JsExecutor.executeSynchronously(() -> ScriptableObject.getProperty(
                scope, ERROR_VARIABLE));
        String ev = e == null ? null : e.toString();
        if (ev != null) {
            Assert.fail("JavaScript error: " + ev);
        }
    }

    private Operation completeOperationSynchronously(Operation op) throws Throwable {
        Operation[] res = new Operation[1];
        this.host.testStart(1);
        this.host.send(op.setCompletion((o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
            } else {
                res[0] = o;
                this.host.completeIteration();
            }
        }));
        this.host.testWait();
        return res[0];
    }

    private void testEchoOperation(Operation op) throws Throwable {
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
            throws Throwable {
        Operation getSubscriptions = Operation.createGet(UriUtils.buildUri(this.host, subscriptionPath));
        getSubscriptions.setReferer(observerUri);
        this.host.waitFor("Timeout waiting for subscriptions to appear", () -> {
            Operation res = completeOperationSynchronously(getSubscriptions);
            ServiceSubscriptionState state = res.getBody(ServiceSubscriptionState.class);
            if (state.subscribers.containsKey(observerUri)) {
                return true;
            }
            return false;
        });
    }

    private void waitForSubscriptionToDisappear(URI observerUri, String subscriptionPath)
            throws Throwable {
        Operation getSubscriptions = Operation.createGet(UriUtils.buildUri(this.host, subscriptionPath));
        getSubscriptions.setReferer(observerUri);
        this.host.waitFor("Timeout waiting for subscriptions to disappear", () -> {
            Operation res = completeOperationSynchronously(getSubscriptions);
            ServiceSubscriptionState state = res.getBody(ServiceSubscriptionState.class);
            if (!state.subscribers.containsKey(observerUri)) {
                return true;
            }
            checkError();
            return false;
        });
    }

    private void waitAndGetArrayAsText(String name, String expected) throws Throwable {
        this.host.waitFor("Timeout getting object array", () -> {
            Scriptable o = (Scriptable) JsExecutor.executeSynchronously(() -> ScriptableObject
                    .getProperty(scope, name));
            if (o.getIds().length > 0) {
                List<String> values = new LinkedList<>();
                for (Object id : o.getIds()) {
                    Object v = o.get((Integer) id, null);
                    values.add(v == null ? null : v.toString());
                }
                String s = String.join(Operation.CR_LF, values);
                if (s.contains(expected)) {
                    return true;
                }
            }
            checkError();
            return false;
        });
    }

    private void waitAndGetArrayCount(String name, int expected) throws Throwable {
        this.host.waitFor("Timeout getting object array count", () -> {
            Scriptable o = (Scriptable) JsExecutor.executeSynchronously(() -> ScriptableObject
                    .getProperty(scope, name));
            if (o.getIds().length == expected) {
                return true;
            }
            checkError();
            return false;
        });
    }

}
