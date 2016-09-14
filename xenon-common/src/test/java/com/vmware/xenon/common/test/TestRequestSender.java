/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.test;

import static java.util.stream.Collectors.toList;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceRequestSender;

/**
 * Provides synchronous/asynchronous send operations for test.
 */
public class TestRequestSender implements ServiceRequestSender {

    public static class FailureResponse {
        public Operation op;
        public Throwable failure;
    }

    private static ThreadLocal<String> authenticationToken = new ThreadLocal<>();

    private ServiceRequestSender sender;
    private Duration timeout = Duration.ofSeconds(30);
    private URI referer;


    /**
     * Set auth token to current thread. The token is used when this class sends a request.
     */
    public static void setAuthToken(String authToken) {
        authenticationToken.set(authToken);
    }

    public static void clearAuthToken() {
        authenticationToken.remove();
    }


    public TestRequestSender(ServiceRequestSender sender) {
        this.sender = sender;

        if (this.sender instanceof VerificationHost) {
            this.timeout = Duration.ofSeconds(((VerificationHost) this.sender).getTimeoutSeconds());
        }
    }

    /**
     * Asynchronously perform operation
     */
    @Override
    public void sendRequest(Operation op) {
        URI referer = this.referer;
        if (referer == null) {
            referer = getDefaultReferer();
        }
        op.setReferer(referer);

        String authToken = authenticationToken.get();
        if (authToken != null) {
            op.addRequestHeader(Operation.REQUEST_AUTH_TOKEN_HEADER, authToken);
        }

        this.sender.sendRequest(op);
    }

    private URI getDefaultReferer() {
        // populate referer from current stack information
        Optional<StackTraceElement> stackElement = Arrays.stream(Thread.currentThread().getStackTrace())
                .filter(elem -> {
                    // filter out Thread and this class
                    String className = elem.getClassName();
                    return !(Thread.class.getName().equals(className) || getClass().getName().equals(className));
                }).findFirst();

        String refererString;
        if (stackElement.isPresent()) {
            StackTraceElement elem = stackElement.get();
            refererString = String.format("http://localhost/%s?line=%s&class=%s&method=%s",
                    getClass().getSimpleName(), elem.getLineNumber(), elem.getClassName(), elem.getMethodName());
        } else {
            refererString = String.format("http://localhost/%s@%s", getClass().getSimpleName(), this.hashCode());
        }
        return URI.create(refererString);
    }

    /**
     * Synchronously perform operation.
     *
     * Expecting provided {@link Operation} to be successful.
     * {@link Operation.CompletionHandler} does NOT need explicitly pass/use of {@link TestContext}.
     *
     * @param op       operation to perform
     * @param bodyType returned body type
     * @param <T>      ServiceDocument
     * @return body document
     */
    public <T extends ServiceDocument> T sendAndWait(Operation op, Class<T> bodyType) {
        Operation response = sendAndWait(op);
        return response.getBody(bodyType);
    }

    /**
     * Perform given operations in parallel, then wait all ops to finish with success.
     *
     * Expecting all {@link Operation} to be successful.
     * The order of result corresponds to the input order.
     *
     * @param ops      operations to perform
     * @param bodyType returned body type
     * @param <T>      ServiceDocument
     * @return body documents
     */
    public <T extends ServiceDocument> List<T> sendAndWait(List<Operation> ops, Class<T> bodyType) {
        return sendAndWait(ops).stream().map(op -> op.getBody(bodyType)).collect(toList());
    }

    /**
     * Synchronously perform operation.
     *
     * Expecting provided operation to be successful.
     * {@link Operation.CompletionHandler} does NOT need explicitly pass/use of {@link TestContext}.
     *
     * @param op operation to perform
     * @return callback operation
     */
    public Operation sendAndWait(Operation op) {
        List<Operation> ops = new ArrayList<>();
        ops.add(op);
        return sendAndWait(ops).get(0);
    }

    /**
     * Perform given operations in parallel, then wait all ops to finish with success.
     * The order of result corresponds to the input order.
     *
     * @param ops operations to perform
     * @return callback operations
     */
    public List<Operation> sendAndWait(List<Operation> ops) {
        return sendAndWait(ops, true);
    }

    /**
     * Perform given operations in parallel, then wait all ops to finish with success.
     * The order of result corresponds to the input order.
     *
     * @param ops operations to perform
     * @return callback operations
     */
    public List<Operation> sendAndWait(List<Operation> ops, boolean checkResponse) {
        Operation[] response = new Operation[ops.size()];

        // Keep caller stack information for operation failure.
        // Actual operation failure will be added to this exception as suppressed exception.
        // This way, it'll display the original caller location in stacktrace
        String callerStackMessage = "Received Failure response. (See suppressed exception for detail)";
        Exception callerStack = new RuntimeException(callerStackMessage);

        TestContext waitContext = new TestContext(ops.size(), this.timeout);
        for (int i = 0; i < ops.size(); i++) {
            int index = i;
            Operation op = ops.get(i);
            op.appendCompletion((o, e) -> {
                if (e != null && checkResponse) {
                    callerStack.addSuppressed(e);
                    waitContext.fail(callerStack);
                    return;
                }
                response[index] = o;
                waitContext.complete();
            });
            sendRequest(op);
        }
        waitContext.await();

        return Arrays.asList(response);
    }

    /**
     * Synchronously perform GET to given url.
     *
     * Expecting GET to be successful.
     *
     * @param url      URL
     * @param bodyType returned body type
     * @param <T>      ServiceDocument
     * @return body document
     */
    public <T extends ServiceDocument> T sendGetAndWait(String url, Class<T> bodyType) {
        return sendAndWait(Operation.createGet(URI.create(url)), bodyType);
    }

    /**
     * Synchronously perform POST to given url.
     *
     * Expecting POST to be successful.
     *
     * @param url      URL
     * @param bodyType returned body type
     * @param <T>      ServiceDocument
     * @return body document
     */
    public <T extends ServiceDocument> T sendPostAndWait(String url, Class<T> bodyType) {
        return sendAndWait(Operation.createPost(URI.create(url)), bodyType);
    }


    /**
     * Synchronously perform operation.
     *
     * Expecting provided operation to be failed.
     *
     * @param op operation to perform
     * @return callback operation and failure
     */
    public FailureResponse sendAndWaitFailure(Operation op) {
        FailureResponse response = new FailureResponse();

        // Prepare caller stack information for operation success.
        String msg = String.format("Expected operation failure but was successful. uri=%s ", op.getUri());
        Exception callerStack = new RuntimeException(msg);

        TestContext waitContext = new TestContext(1, this.timeout);
        op.appendCompletion((o, e) -> {
            if (e != null) {
                response.op = o;
                response.failure = e;
                waitContext.complete();
                return;
            }
            waitContext.fail(callerStack);
        });
        sendRequest(op);
        waitContext.await();

        return response;
    }

    public Duration getTimeout() {
        return this.timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public URI getReferer() {
        return this.referer;
    }

    public void setReferer(URI referer) {
        this.referer = referer;
    }
}
