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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceHost;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A service client providing synchronous operations.
 */
public class NodeClient<H extends ServiceHost> {

    private int timeoutSeconds = 30;
    private H serviceHost;

    public NodeClient(H serviceHost) {
        this.serviceHost = serviceHost;
    }

    public H getServiceHost() {
        return serviceHost;
    }

    public TestContext testCreate(int c) {
        return TestContext.create(c, TimeUnit.SECONDS.toMicros(this.timeoutSeconds));
    }

    /**
     * Asynchronously perform operation
     */
    public void sendAsync(Operation op) {
        // TODO: set referrer
        op.setReferer("/");
        this.serviceHost.sendRequest(op);
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

    public void sendThen(Operation op, CompletionHandler handler) {
        sendThen(op, (testContext, o, e) -> {
            handler.handle(o, e);
            testContext.completeIteration();
        });
    }

    @FunctionalInterface
    interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }

    public void sendThen(Operation op, TriConsumer<TestContext, Operation, Throwable> consumer) {
        TestContext testContext = testCreate(1);

        Operation clone = op.clone();
        clone.setCompletion((o, e) -> {
            consumer.accept(testContext, o, e);
        });
        sendAsync(clone);

        testContext.await();
    }


/*
    public static void main(String[] args) throws Throwable {
        ExampleServiceHost host = new ExampleServiceHost();
        host.initialize(new ExampleHostArguments());
        host.setPort(8001);
        host.start();
        NodeClient client = new NodeClient(host);

        Operation get = Operation.createGet(UriUtils.buildUri("http://localhost:8000/"))
                .setCompletion((op, e) -> {
                    System.out.println("2 OPID=" + op.getId());

                    if (e != null) {
                        System.out.println("FAIL");
                        return;
                    }
                    System.out.println(
                            "3 PASS " + op.getBody(ServiceDocument.class).documentOwner);
                });

        System.out.println("1 GET OPID=" + get.getId());
        //        client.send(get);
        ServiceDocument d = client.sendThenGetBody(get, ServiceDocument.class);
        System.out.println("4 DONE " + d.documentOwner);
    }
*/

}
