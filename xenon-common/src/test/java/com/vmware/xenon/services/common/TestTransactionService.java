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

package com.vmware.xenon.services.common;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.UriUtils;

public class TestTransactionService extends BasicReusableHostTestCase {

    @Before
    public void prepare() throws Throwable {
        this.host.waitForServiceAvailable(ExampleFactoryService.SELF_LINK);
        this.host.waitForServiceAvailable(TransactionFactoryService.SELF_LINK);
        this.host.setOperationTimeOutMicros(TimeUnit.SECONDS.toMicros(1000));
    }

    /**
     * Test only the stateless asynchronous transaction resolution service
     *
     * @throws Throwable
     */
    @Test
    public void transactionResolution() throws Throwable {
        ExampleService.ExampleServiceState verifyState;
        List<URI> exampleURIs = new ArrayList<>();
        // create example service documents across all nodes
        this.host.createExampleServices(this.host, 1, exampleURIs, null);

        TransactionService.TransactionServiceState tx1 = new TransactionService.TransactionServiceState();
        String txId = UUID.randomUUID().toString();
        tx1.documentSelfLink = txId;
        Operation operation = Operation
                .createPost(UriUtils.buildUri(this.host, TransactionFactoryService.class))
                .setBody(tx1)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException("Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();

        ExampleService.ExampleServiceState initialState = new ExampleService.ExampleServiceState();
        initialState.name = "zero";
        initialState.counter = 0L;
        operation = Operation
                .createPut(exampleURIs.get(0))
                .setTransactionId(txId)
                .setBody(initialState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException(
                                    "Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();

        TransactionService.ResolutionRequest commmit = new TransactionService.ResolutionRequest();
        commmit.kind = TransactionService.ResolutionKind.COMMIT;
        commmit.pendingOperations = 1;
        operation = Operation
                .createPatch(UriUtils.buildTransactionResolutionUri(this.host, txId))
                .setBody(commmit)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException(
                                    "Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();
        // This should be equal to the newest state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, initialState.name);
        assertEquals(verifyState.documentTransactionId, "");

    }
    /**
     * Test a number of scenarios in the happy, single-instance transactions. Testing a single transactions allows
     * us to invoke the coordinator interface directly, without going through "resolution" interface -- eventually
     * though, even single tests should go through this interface, since the current setup causes races.
     * @throws Throwable
     */
    @Test
    public void singleUpdate() throws Throwable {
        // used to verify current state
        ExampleService.ExampleServiceState verifyState;
        List<URI> exampleURIs = new ArrayList<>();
        // create example service documents across all nodes
        this.host.createExampleServices(this.host, 1, exampleURIs, null);

        // 0 -- no transaction
        ExampleService.ExampleServiceState initialState = new ExampleService.ExampleServiceState();
        initialState.name = "zero";
        initialState.counter = 0L;
        Operation operation = Operation
                .createPut(exampleURIs.get(0))
                .setBody(initialState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException(
                                    "Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();
        // This should be equal to the current state -- since we did not use transactions
        verifyState = this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, initialState.name);

        // 1 -- tx1

        TransactionService.TransactionServiceState tx1 = new TransactionService.TransactionServiceState();
        tx1.documentSelfLink = UUID.randomUUID().toString();
        operation = Operation
                .createPost(UriUtils.buildUri(this.host, TransactionFactoryService.class))
                .setBody(tx1)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException("Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });

        host.testStart(1);
        this.host.send(operation);
        host.testWait();


        ExampleService.ExampleServiceState newState = new ExampleService.ExampleServiceState();
        newState.name = "one";
        newState.counter = 1L;
        operation = Operation
                .createPut(exampleURIs.get(0))
                .setBody(newState)
                .setTransactionId(tx1.documentSelfLink)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException("Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });

        host.testStart(1);
        this.host.send(operation);
        host.testWait();

        // get outside a transaction -- ideally should get old version -- for now, it should fail
        host.toggleNegativeTestMode(true);
        this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleURIs.get(0));
        host.toggleNegativeTestMode(false);

        // get within a transaction -- the callback should bring latest
        operation = Operation
                .createGet(exampleURIs.get(0))
                .setBody(initialState)
                .setTransactionId(tx1.documentSelfLink)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException("Missing error response"));
                            return;
                        }
                    } else {
                        ExampleService.ExampleServiceState rsp = o.getBody(ExampleService.ExampleServiceState.class);
                        assertEquals(rsp.name, newState.name);
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();

        // now commit
        TransactionService.ResolutionRequest commit = new TransactionService.ResolutionRequest();
        commit.kind = TransactionService.ResolutionKind.COMMIT;
        commit.pendingOperations = 1;
        operation = Operation
                .createPatch(
                        UriUtils.buildTransactionResolutionUri(this.host, tx1.documentSelfLink))
                .setBody(commit)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException(
                                    "Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();
        // This should be equal to the newest state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // 2 -- tx2

        TransactionService.TransactionServiceState tx2 = new TransactionService.TransactionServiceState();
        tx2.documentSelfLink = UUID.randomUUID().toString();
        operation = Operation
                .createPost(UriUtils.buildUri(this.host, TransactionFactoryService.class))
                .setBody(tx2)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException("Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });

        host.testStart(1);
        this.host.send(operation);
        host.testWait();

        ExampleService.ExampleServiceState abortState = new ExampleService.ExampleServiceState();
        abortState.name = "two";
        abortState.counter = 2L;
        operation = Operation
                .createPut(exampleURIs.get(0))
                .setBody(abortState)
                .setTransactionId(tx2.documentSelfLink)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException("Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();
        // This should be equal to the newest state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // now abort
        TransactionService.ResolutionRequest abort = new TransactionService.ResolutionRequest();
        abort.kind = TransactionService.ResolutionKind.ABORT;
        abort.pendingOperations = 1;
        operation = Operation
                .createPatch(UriUtils.buildTransactionUri(this.host, tx2.documentSelfLink))
                .setBody(abort)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
                        if (rsp.message == null || rsp.message.isEmpty()) {
                            this.host.failIteration(new IllegalStateException(
                                    "Missing error response"));
                            return;
                        }
                    }
                    this.host.completeIteration();
                });
        host.testStart(1);
        this.host.send(operation);
        host.testWait();
        // This should be equal to the previous state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleService.ExampleServiceState.class, exampleURIs.get(0));
        // TODO re-enable when abort logic is debugged
        //assertEquals(verifyState.name, newState.name);
    }
}
