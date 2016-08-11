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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRequestSender;

/**
 * Provides synchronous send operations for test.
 */
public class TestRequestSender {

    @FunctionalInterface
    interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);
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
    public void sendAsync(Operation op) {
        // TODO: populate some stack information, so that it can know where this operation has been called
        // TODO: set referrer
        op.setReferer("/");
        this.sender.sendRequest(op);
    }

    /**
     * Synchronously perform operation.
     * {@link CompletionHandler} does NOT need explicitly pass/use of {@link WaitContext}.
     */
    public void send(Operation op) {
        sendThen(op, op.getCompletion());
    }

    @SuppressWarnings("unchecked")
    public <T extends ServiceDocument> T sendThenGetBody(Operation op, Class<T> bodyType) {
        Object[] body = new Object[1];
        sendExpectSuccess(op, (o) -> {
            CompletionHandler handler = op.getCompletion();
            if (handler != null) {
                handler.handle(o, null);
            }
            body[0] = o.getBody(bodyType);
        });
        return (T) body[0];
    }

    /**
     * Perform given {@link Operation} and return {@link OperationResponse}.
     */
    public OperationResponse sendThenGetResponse(Operation op) {
        OperationResponse response = new OperationResponse();
        sendThen(op, (o, e) -> {
            response.operation = o;
            response.failure = e;
        });
        return response;
    }

    public String sendThenGetSelfLink(Operation post) {
        // TODO: impl
        return null;
    }

    public void sendExpectSuccess(Operation op) {
        sendExpectSuccess(op, (o) -> {
        });
    }

    public void sendExpectSuccess(Operation op, Consumer<Operation> successCallback) {
        sendThen(op, (waitContext, o, e) -> {
            if (e != null) {
                waitContext.fail(e);
                return;
            }
            successCallback.accept(o);
            waitContext.complete();
        });
    }

    public void sendExpectFailure(Operation op) {
        sendExpectFailure(op, (o, e) -> {
        });
    }

    public void sendExpectFailure(Operation op, BiConsumer<Operation, Throwable> failureCallback) {
        sendThen(op, (waitContext, o, e) -> {
            if (e != null) {
                failureCallback.accept(o, e);
                waitContext.complete();
                return;
            }
            String msg = String.format("Expected operation failure but was successful. uri=%s", o.getUri());
            waitContext.fail(new RuntimeException(msg));
        });
    }

    public void sendThen(Operation op, CompletionHandler handler) {
        sendThen(op, (waitContext, o, e) -> {
            handler.handle(o, e);
            waitContext.complete();
        });
    }


    public void sendThen(Operation op, TriConsumer<WaitContext, Operation, Throwable> consumer) {
        WaitContext waitContext = new WaitContext(1, this.timeout);

        op.appendCompletion((o, e) -> {
            consumer.accept(waitContext, o, e);
        });
        sendAsync(op);

        waitContext.await();
    }


    public void get(String servicePath, Consumer<Operation> buildOperation) {
        get(servicePath, buildOperation, op -> {
        });
    }

    public void get(String servicePath, Consumer<Operation> buildOperation, Consumer<Operation> successCallback) {
        Operation op = Operation.createGet(getSender(), servicePath);
        buildOperation.accept(op);
        sendExpectSuccess(op, successCallback);
    }


    public <T extends ServiceDocument> T getThenGetBody(String servicePath, Class<T> bodyType, Consumer<Operation> buildOperation) {
        return performOperationThenGetBody(this::get, servicePath, bodyType, buildOperation);
    }

    public void post(String servicePath, Consumer<Operation> buildOperation) {
        post(servicePath, buildOperation, op -> {
        });
    }

    public void post(String servicePath, Consumer<Operation> buildOperation, Consumer<Operation> successCallback) {
        Operation op = Operation.createPost(getSender(), servicePath);
        buildOperation.accept(op);
        sendExpectSuccess(op, successCallback);
    }

    public <T extends ServiceDocument> T postThenGetBody(String servicePath, Class<T> bodyType, Consumer<Operation> buildOperation) {
        return performOperationThenGetBody(this::post, servicePath, bodyType, buildOperation);
    }

    public void put(String servicePath, Consumer<Operation> buildOperation) {
        put(servicePath, buildOperation, op -> {
        });
    }

    public void put(String servicePath, Consumer<Operation> buildOperation, Consumer<Operation> successCallback) {
        Operation op = Operation.createPut(getSender(), servicePath);
        buildOperation.accept(op);
        sendExpectSuccess(op, successCallback);
    }

    public <T extends ServiceDocument> T putThenGetBody(String servicePath, Class<T> bodyType, Consumer<Operation> buildOperation) {
        return performOperationThenGetBody(this::put, servicePath, bodyType, buildOperation);
    }

    public void patch(String servicePath, Consumer<Operation> buildOperation) {
        patch(servicePath, buildOperation, op -> {
        });
    }

    public void patch(String servicePath, Consumer<Operation> buildOperation, Consumer<Operation> successCallback) {
        Operation op = Operation.createPatch(getSender(), servicePath);
        buildOperation.accept(op);
        sendExpectSuccess(op, successCallback);
    }

    public <T extends ServiceDocument> T patchThenGetBody(String servicePath, Class<T> bodyType, Consumer<Operation> buildOperation) {
        return performOperationThenGetBody(this::patch, servicePath, bodyType, buildOperation);
    }

    public void delete(String servicePath) {
        delete(servicePath, (op) -> {
        });
    }

    public void delete(String servicePath, Consumer<Operation> buildOperation) {
        delete(servicePath, buildOperation, op -> {
        });
    }

    public void delete(String servicePath, Consumer<Operation> buildOperation, Consumer<Operation> successCallback) {
        Operation op = Operation.createDelete(getSender(), servicePath);
        buildOperation.accept(op);
        sendExpectSuccess(op, successCallback);
    }

    public <T extends ServiceDocument> T deleteThenGetBody(String servicePath, Class<T> bodybodyType, Consumer<Operation> buildOperation) {
        return performOperationThenGetBody(this::delete, servicePath, bodybodyType, buildOperation);
    }


    @SuppressWarnings("unchecked")
    private <T extends ServiceDocument> T performOperationThenGetBody(
            TriConsumer<String, Consumer<Operation>, Consumer<Operation>> operation,
            String servicePath, Class<T> bodyType, Consumer<Operation> buildOperation) {
        ServiceDocument[] body = new ServiceDocument[1];
        operation.accept(servicePath, buildOperation, op -> {
            body[0] = op.getBody(bodyType);
        });
        return (T) body[0];
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
