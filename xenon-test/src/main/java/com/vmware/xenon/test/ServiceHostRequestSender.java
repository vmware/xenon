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

package com.vmware.xenon.test;

import java.util.function.Consumer;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;

/**
 * RequestSender using ServiceHost.
 */
public class ServiceHostRequestSender<H extends ServiceHost> implements TargetedRequestSender {

    private H serviceHost;

    public ServiceHostRequestSender(H serviceHost) {
        this.serviceHost = serviceHost;
    }

    public H getHost() {
        return this.serviceHost;
    }


    /**
     * Asynchronously send {@link Operation} via host.
     */
    @Override
    public void sendAsync(Operation op) {
        // TODO: set referrer
        op.setReferer("/");
        this.serviceHost.sendRequest(op);
    }

    /**
     * Synchronously send {@link Operation} via host.
     */
    @Override
    public void send(Operation op) {
        sendThen(op, op.getCompletion());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ServiceDocument> T sendThenGetBody(Operation op, Class<T> type) {
        Object[] body = new Object[1];
        sendExpectSuccess(op, (o) -> {
            CompletionHandler handler = op.getCompletion();
            if (handler != null) {
                handler.handle(o, null);
            }
            body[0] = o.getBody(type);
        });
        return (T) body[0];
    }

    @Override
    public String sendThenGetSelfLink(Operation post) {
        // TODO: impl
        return null;
    }

    @Override
    public void sendExpectSuccess(Operation op) {
        sendExpectSuccess(op, (o) -> {
        });
    }

    @Override
    public void sendExpectSuccess(Operation op, Consumer<Operation> successCallback) {
        sendThen(op, (testContext, o, e) -> {
            if (e != null) {
                testContext.failIteration(e);
                return;
            }
            successCallback.accept(o);
            testContext.completeIteration();
        });
    }

    @Override
    public void sendThen(Operation op, CompletionHandler handler) {
        sendThen(op, (testContext, o, e) -> {
            handler.handle(o, e);
            testContext.completeIteration();
        });
    }

    @Override
    public void sendThen(Operation op, TriConsumer<TestContext, Operation, Throwable> consumer) {
        TestContext testContext = new TestContext(1);

        Operation clone = op.clone();
        clone.setCompletion((o, e) -> {
            consumer.accept(testContext, o, e);
        });
        sendAsync(clone);

        testContext.await();
    }


    @Override
    public void sendGet(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createGet(this.serviceHost, servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }

    @Override
    public void sendPost(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createPost(this.serviceHost, servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }
}
