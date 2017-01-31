/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

/**
 *
 */
public class TestDebugging extends BasicReusableHostTestCase {

    @Before
    public void before() {
        this.host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
    }


    private AtomicInteger counter = new AtomicInteger();

    @Test
    public void test() throws Throwable {

        int nodeCount = 3;
        this.host.setPeerSynchronizationEnabled(true);
        this.host.setUpPeerHosts(nodeCount);
        this.host.joinNodesAndVerifyConvergence(nodeCount, true);
        this.host.setNodeGroupQuorum(nodeCount);

        for (VerificationHost host : this.host.getInProcessHostMap().values()) {
            host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        }


        TestRequestSender sender = this.host.getTestRequestSender();

        int iteration = 100;
        int numOfData = 100;

        for (int i = 0; i < iteration; i++) {
            this.host.log("======================== Iteration=" + i);

            VerificationHost targetHost = this.host.getPeerHost();
            URI factoryLink = UriUtils.buildUri(targetHost, ExampleService.FACTORY_LINK);

            List<Operation> initilOps = new ArrayList<>();
            for (int j = 0; j < numOfData; j++) {
                ExampleServiceState body = new ExampleService.ExampleServiceState();
                body.name = "doc-" + String.format("%06d", i) + String.format("-%04d", j);
                body.documentSelfLink = body.name;
                body.counter = (long) i;
                Operation initial = Operation.createPost(factoryLink).setBody(body);
                initilOps.add(initial);
            }
            List<ExampleServiceState> initialDataList = sender.sendAndWait(initilOps, ExampleServiceState.class);

            List<DeferredResult<Operation>> deferredResults = new ArrayList<>();
            for (ExampleServiceState initData : initialDataList) {
                initData.name += "-updated";

                String selfLink = initData.documentSelfLink;
                URI targetUri = UriUtils.buildUri(targetHost, selfLink);
                List<Operation> ops = createOps(factoryLink, targetUri, initData);
                DeferredResult<Operation> def = createDeferredResult(ops, selfLink);

                deferredResults.add(def);
            }


            TestContext ctx = this.host.testCreate(1);
            DeferredResult.allOf(deferredResults)
                    .whenComplete((retryOps, retryEx) -> {
                        int size = deferredResults.size();
                        int count = this.counter.get();
                        this.host.log("DEBUG all done in whenComplete. size=%s, count=%s", size, count);
                        ctx.complete();
                    });
            ctx.await();

            int size = deferredResults.size();
            // reset counter
            int count = this.counter.getAndSet(0);
            this.host.log("DEBUG all done before assert. size=%s, count=%s", size, count);
            assertEquals("iteration=%s" + i, size, count);
        }
    }

    private List<Operation> createOps(URI factoryLink, URI selfLink, ExampleServiceState repostBody) {
        List<Operation> ops = new ArrayList<>();
        Operation delete = Operation.createDelete(selfLink)
                .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);

        Operation post = Operation.createPost(factoryLink)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .setBodyNoCloning(repostBody);

        ops.add(delete);
        ops.add(post);

        return ops;
    }

    private DeferredResult<Operation> createDeferredResult(List<Operation> ops, String selfLink) {
        DeferredResult<Operation> deferredResult = DeferredResult.completed(new Operation());
        deferredResult = deferredResult.thenApply(ignore -> {
            int count = this.counter.incrementAndGet();
            this.host.log("DEBUG: deferredResult. count=%s, link=%s", count, selfLink);
            return ignore;
        });
        deferredResult = deferredResult.exceptionally(throwable -> {
            this.host.log("DEBUG: exception. link=%s, ex=%s", selfLink, throwable);
            throw new CompletionException(throwable);
        });
        for (Operation op : ops) {
            deferredResult = deferredResult.thenCompose(o -> {
                this.host.log("DEBUG: calling uri=%s, action=%s", op.getUri(), op.getAction());
                return this.host.sendWithDeferredResult(op);
            });
        }

        return deferredResult;
    }
}
