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
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceRequestSender;

public class TestClient {

    @FunctionalInterface
    interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }

    private ServiceRequestSender sender;

    public TestClient(ServiceRequestSender sender) {
        this.sender = sender;
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
     * {@link CompletionHandler} does NOT need explicitly pass/use {@link TestContext}.
     */
    public void send(Operation op) {
        sendThen(op, op.getCompletion());
    }

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

    public String sendThenGetSelfLink(Operation post) {
        // TODO: impl
        return null;
    }

    public void sendExpectSuccess(Operation op) {
        sendExpectSuccess(op, (o) -> {
        });
    }

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

    public void sendExpectFailure(Operation op) {
        sendThen(op, (testContext, o, e) -> {
            if (e != null) {
                testContext.completeIteration();
                return;
            }
            String msg = String.format("Expected operation failure but was successful. uri=%s", o.getUri());
            testContext.failIteration(new XenonTestException(msg));
        });
    }

    public void sendThen(Operation op, CompletionHandler handler) {
        sendThen(op, (testContext, o, e) -> {
            handler.handle(o, e);
            testContext.completeIteration();
        });
    }


    public void sendThen(Operation op, TriConsumer<TestContext, Operation, Throwable> consumer) {
        TestContext testContext = new TestContext(1);

        Operation clone = op.clone();
        clone.setCompletion((o, e) -> {
            consumer.accept(testContext, o, e);
        });
        sendAsync(clone);

        testContext.await();
    }


    public void sendGet(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createGet(getSender(), servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }

    public void sendPost(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createPost(getSender(), servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }

    public void sendPut(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createPut(getSender(), servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }

    public void sendPatch(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createPatch(getSender(), servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }

    public void sendDelete(String servicePath) {
        sendDelete(servicePath, (op) -> {
        });
    }

    public void sendDelete(String servicePath, Consumer<Operation> consumer) {
        Operation op = Operation.createDelete(getSender(), servicePath);
        consumer.accept(op);
        sendExpectSuccess(op);
    }


    private ServiceHost getSender() {

        if (this.sender instanceof ServiceHost) {
            return (ServiceHost) this.sender;
        } else if (this.sender instanceof Service) {
            return ((Service) this.sender).getHost();
        } else if (this.sender instanceof InProcessNodeGroup) {
            return ((InProcessNodeGroup) this.sender).getHost();
        }

        throw new UnsupportedOperationException("Not supported to " + this.sender);
    }
}
