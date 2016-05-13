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

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import org.junit.Test;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceHost.ServiceNotFoundException;
import com.vmware.xenon.common.test.OperationFutures;
import com.vmware.xenon.services.common.AuthCredentialsService;
import com.vmware.xenon.services.common.AuthCredentialsService.AuthCredentialsServiceState;

/**
 */
public class TestFutures extends BasicReusableHostTestCase {
    @Test
    public void asBiFunction() {
        Operation op = Operation.createGet(URI.create("/hello"));
        Throwable th = new Exception();

        CompletionHandler h = (o, e) -> {
            assertSame(op, o);
            assertSame(th, e);
        };

        BiFunction<Operation, Throwable, Void> func = OperationFutures.asFunc(h);

        func.apply(op, th);
    }

    @Test
    public void completeExce() throws InterruptedException {
        CompletableFuture<Operation> f = this.host.sendWithFuture(Operation
                .createGet(UriUtils.buildUri(this.host, "/hello"))
                .setReferer(this.host.getReferer()));

        // blocking here is OK as it is JUnit's thread that's blocked
        try {
            f.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ServiceNotFoundException);
        }
    }

    @Test
    public void completeOk() throws InterruptedException, ExecutionException {
        CompletableFuture<Operation> f = this.host.sendWithFuture(Operation
                .createGet(UriUtils.buildUri(this.host, "/")));

        // callback with only a success branch
        ServiceDocumentQueryResult body = f.get().getBody(ServiceDocumentQueryResult.class);
        assertTrue(body.documentLinks.size() > 0);
    }

    @Test
    public void completeOkWithCompletionHandler() throws Throwable {
        CompletableFuture<Operation> f = this.host.sendWithFuture(Operation
                .createGet(UriUtils.buildUri(this.host, "/")));

        host.testStart(1);

        f.handle((o, e) -> {
            if (e != null) {
                host.failIteration(e);
            } else {
                host.completeIteration();
            }

            return null;
        });

        host.testWait();
    }

    @Test
    public void seq() throws InterruptedException, ExecutionException {
        ArrayList<CompletableFuture<Operation>> futs = new ArrayList<CompletableFuture<Operation>>();

        // create 10 services in parallel
        for (int i = 0; i < 10; i++) {
            AuthCredentialsServiceState state = new AuthCredentialsServiceState();
            state.documentSelfLink = "" + i;
            state.privateKey = "" + i;

            CompletableFuture<Operation> f = this.host.sendWithFuture(Operation
                    .createPost(this.host, AuthCredentialsService.FACTORY_LINK)
                    .setBody(state)
                    .setReferer(this.host.getReferer()));
            futs.add(f);
        }

        // wait for the 10 requests to complete
        CompletableFuture<List<Operation>> join = OperationFutures.join(futs);
        List<Operation> allServices = join.get();

        for (Operation service : allServices) {
            this.host.log(service.getBody(AuthCredentialsServiceState.class).documentSelfLink);
        }

        // retrieve all created service in sequence
        Operation getFirst = Operation.createGet(this.host,
                UriUtils.buildUriPath(AuthCredentialsService.FACTORY_LINK, "0"));

        // chain 10
        CompletableFuture<Operation> f = this.host.sendWithFuture(getFirst);
        for (int i = 1; i < 10; i++) {
            f = f.thenComposeAsync(this::getNextService);
        }

        assertEquals(f.get().getBody(AuthCredentialsServiceState.class).privateKey, "9");
    }

    /**
     * Decides what operation to do based on the previous result. Here it just goes and retrieves
     * the object whose selfLink is +1 greater then own.
     *
     * @param o
     * @return
     */
    private CompletableFuture<Operation> getNextService(Operation o) {
        AuthCredentialsServiceState s = o.getBody(AuthCredentialsServiceState.class);
        int index = Integer.parseInt(s.privateKey);

        // "complex" operation to decide what to request next
        index++;

        return this.host.sendWithFuture(Operation.createGet(this.host,
                UriUtils.buildUriPath(AuthCredentialsService.FACTORY_LINK, "" + index)));

    }
}
