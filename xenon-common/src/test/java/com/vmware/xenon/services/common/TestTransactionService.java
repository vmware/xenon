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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;
import com.vmware.xenon.services.common.TransactionService.TransactionServiceState;

public class TestTransactionService extends BasicReusableHostTestCase {

    @Before
    public void prepare() throws Throwable {
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
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
        this.host.createExampleServices(this.host, 1, exampleURIs, null);

        String txid = newTransaction();

        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = "zero";
        initialState.counter = 0L;
        updateExampleService(txid, exampleURIs.get(0), initialState);

        boolean committed = commit(txid, 1);
        assertTrue(committed);

        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(initialState.name, verifyState.name);
        assertEquals(null, verifyState.documentTransactionId);

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
        ExampleServiceState verifyState;
        List<URI> exampleURIs = new ArrayList<>();
        // create example service documents across all nodes
        this.host.createExampleServices(this.host, 1, exampleURIs, null);

        // 0 -- no transaction
        ExampleServiceState initialState = new ExampleServiceState();
        initialState.name = "zero";
        initialState.counter = 0L;
        updateExampleService(null, exampleURIs.get(0), initialState);
        // This should be equal to the current state -- since we did not use transactions
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, initialState.name);

        // 1 -- tx1
        String txid1 = newTransaction();
        ExampleServiceState newState = new ExampleServiceState();
        newState.name = "one";
        newState.counter = 1L;
        updateExampleService(txid1, exampleURIs.get(0), newState);

        // get outside a transaction -- ideally should get old version -- for now, it should fail
        host.toggleNegativeTestMode(true);
        this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        host.toggleNegativeTestMode(false);

        // get within a transaction -- the callback should bring latest
        verifyExampleServiceState(txid1, exampleURIs.get(0), newState);

        // now commit
        boolean committed = commit(txid1, 1);
        assertTrue(committed);
        // This should be equal to the newest state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // 2 -- tx2
        String txid2 = newTransaction();
        ExampleServiceState abortState = new ExampleServiceState();
        abortState.name = "two";
        abortState.counter = 2L;
        updateExampleService(txid2, exampleURIs.get(0), abortState);
        // This should be equal to the latest committed state -- since the txid2 is still in-progress
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        assertEquals(verifyState.name, newState.name);

        // now abort
        boolean aborted = abort(txid2, 1);
        assertTrue(aborted);
        // This should be equal to the previous state -- since the transaction committed
        verifyState = this.host.getServiceState(null, ExampleServiceState.class, exampleURIs.get(0));
        // TODO re-enable when abort logic is debugged
        //assertEquals(verifyState.name, newState.name);
    }

    private String newTransaction() throws Throwable {
        String txid = UUID.randomUUID().toString();

        this.host.testStart(1);
        TransactionServiceState initialState = new TransactionServiceState();
        initialState.documentSelfLink = txid;
        Operation post = Operation
                .createPost(getTransactionFactoryUri())
                .setBody(initialState).setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(post);
        this.host.testWait();

        return txid;
    }

    private boolean commit(String txid, int pendingOperations) throws Throwable {
        this.host.testStart(1);
        ResolutionRequest body = new ResolutionRequest();
        body.kind = TransactionService.ResolutionKind.COMMIT;
        body.pendingOperations = pendingOperations;
        boolean[] succeeded = new boolean[1];
        Operation commit = Operation
                .createPatch(UriUtils.buildTransactionResolutionUri(this.host, txid))
                .setBody(body)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    this.host.completeIteration();
                });
        this.host.send(commit);
        this.host.testWait();

        return succeeded[0];
    }

    private boolean abort(String txid, int pendingOperations) throws Throwable {
        this.host.testStart(1);
        ResolutionRequest body = new ResolutionRequest();
        body.kind = TransactionService.ResolutionKind.ABORT;
        body.pendingOperations = pendingOperations;
        boolean[] succeeded = new boolean[1];
        Operation abort = Operation
                .createPatch(UriUtils.buildTransactionUri(this.host, txid))
                .setBody(body)
                .setCompletion((o, e) -> {
                    succeeded[0] = e == null;
                    this.host.completeIteration();
                });
        this.host.send(abort);
        this.host.testWait();

        return succeeded[0];
    }

    private URI getTransactionFactoryUri() {
        return UriUtils.buildUri(this.host, TransactionFactoryService.class);
    }

    private void updateExampleService(String txid, URI exampleServiceUri, ExampleServiceState exampleServiceState) throws Throwable {
        this.host.testStart(1);
        Operation put = Operation
                .createPut(exampleServiceUri)
                .setTransactionId(txid)
                .setBody(exampleServiceState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                });
        this.host.send(put);
        this.host.testWait();
    }

    private void verifyExampleServiceState(String txid, URI exampleServiceUri, ExampleServiceState exampleServiceState) throws Throwable {
        Operation operation = Operation
                .createGet(exampleServiceUri)
                .setTransactionId(txid)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }

                    ExampleServiceState rsp = o.getBody(ExampleServiceState.class);
                    assertEquals(exampleServiceState.name, rsp.name);
                    this.host.completeIteration();
                });
        this.host.testStart(1);
        this.host.send(operation);
        this.host.testWait();
    }

}
