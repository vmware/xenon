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

import java.time.Duration;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRequestSender;

/**
 * Provides synchronous/asynchronous send operations for test.
 */
public class TestRequestSender {

    public static class FailureResponse {
        public Operation op;
        public Throwable failure;
    }

    private ServiceRequestSender sender;
    private Duration timeout = Duration.ofSeconds(30);

    public TestRequestSender(ServiceRequestSender sender) {
        this.sender = sender;

        // set timeout from VerificationHost
        if (this.sender instanceof VerificationHost) {
            this.timeout = Duration.ofSeconds(((VerificationHost) this.sender).getTimeoutSeconds());
        }
    }

    /**
     * Asynchronously perform operation
     */
    public void sendRequest(Operation op) {
        // TODO: populate some stack information, so that it can know where this operation has been called
        // TODO: set referrer
        op.setReferer("/");
        this.sender.sendRequest(op);
    }

    /**
     * Synchronously perform operation.
     *
     * Expecting provided {@link Operation} to be successful.
     * {@link Operation.CompletionHandler} does NOT need explicitly pass/use of {@link WaitContext}.
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
     * Synchronously perform operation.
     *
     * Expecting provided operation to be successful.
     * {@link Operation.CompletionHandler} does NOT need explicitly pass/use of {@link WaitContext}.
     *
     * @param op operation to perform
     * @return callback operation
     */
    public Operation sendAndWait(Operation op) {
        Operation[] response = new Operation[1];

        WaitContext waitContext = new WaitContext(1, this.timeout);
        op.appendCompletion((o, e) -> {
            if (e != null) {
                waitContext.fail(e);
                return;
            }
            response[0] = o;
            waitContext.complete();
        });
        sendRequest(op);
        waitContext.await();

        return response[0];
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

        WaitContext waitContext = new WaitContext(1, this.timeout);
        op.appendCompletion((o, e) -> {
            if (e != null) {
                response.op = o;
                response.failure = e;
                waitContext.complete();
                return;
            }
            String msg = String.format("Expected operation failure but was successful. uri=%s", o.getUri());
            waitContext.fail(new RuntimeException(msg));
        });
        sendRequest(op);
        waitContext.await();

        return response;
    }

    private ServiceHost getSender() {

        if (this.sender instanceof ServiceHost) {
            return (ServiceHost) this.sender;
        } else if (this.sender instanceof Service) {
            return ((Service) this.sender).getHost();
        }

        throw new UnsupportedOperationException("Not supported to " + this.sender);
    }

    public Duration getTimeout() {
        return this.timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }
}
