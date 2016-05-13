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

package com.vmware.xenon.services.common;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestDocumentRecreation extends BasicReusableHostTestCase {

    private URI exampleFactoryUri;

    private int nodeCount = 3;

    @Before
    public void setUp() throws Throwable {
        if (this.host.getInProcessHostMap().isEmpty()) {
            this.host.setStressTest(this.host.isStressTest);
            this.host.setPeerSynchronizationEnabled(true);
            this.host.setUpPeerHosts(this.nodeCount);
            getHost().setNodeGroupQuorum(this.nodeCount);
            this.host.joinNodesAndVerifyConvergence(this.nodeCount, true);
            this.host.setNodeGroupQuorum(this.nodeCount);;

            for (VerificationHost host : this.host.getInProcessHostMap().values()) {
                host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
            }

            this.exampleFactoryUri = UriUtils.buildUri(getHost(), ExampleService.FACTORY_LINK);
        }
    }

    @Test
    public void postDeletePost() throws Throwable {
        // creating one document
        String link = createExampleDocuments(this.exampleFactoryUri, getHost(), 1).iterator().next();
        ExampleServiceState exampleServiceState = getHost().getServiceState(null, ExampleServiceState.class, UriUtils.buildUri(getHost(), link));

        // deleting the document
        TestContext deleteCtx = testCreate(1);
        Operation opDelete = Operation.createDelete(UriUtils.buildUri(getHost(), link))
                .setCompletion((o, e) -> {
                    deleteCtx.completeIteration();
                });
        getHost().send(opDelete);
        testWait(deleteCtx);

        exampleServiceState.documentSelfLink =  exampleServiceState.documentSelfLink.replace(ExampleService.FACTORY_LINK + "/", "");

        // re-creating same document using PRAGMA
        TestContext postCtx = testCreate(1);
        Operation opPost = Operation.createPost(UriUtils.buildUri(getHost(), ExampleService.FACTORY_LINK))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .setBody(exampleServiceState)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        postCtx.failIteration(e);
                    } else {
                        postCtx.completeIteration();
                    }
                });
        getHost().send(opPost);
        testWait(postCtx);
    }

    private Collection<String> createExampleDocuments(URI exampleSourceFactory,
            VerificationHost host, long documentNumber) throws Throwable {
        Collection<String> links = new ArrayList<>();
        Collection<Operation> ops = new ArrayList<>();
        TestContext ctx = testCreate((int) documentNumber);
        for (; documentNumber > 0; documentNumber--) {
            ExampleServiceState exampleServiceState = new ExampleService.ExampleServiceState();
            exampleServiceState.name = UUID.randomUUID().toString();
            exampleServiceState.documentSelfLink = exampleServiceState.name;
            exampleServiceState.counter = Long.valueOf(documentNumber);
            ops.add(Operation.createPost(exampleSourceFactory)
                    .setBody(exampleServiceState)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            this.host.log("Post service error: %s", Utils.toString(e));
                            ctx.failIteration(e);
                            return;
                        }
                        synchronized (ops) {
                            links.add(o.getBody(
                                    ExampleService.ExampleServiceState.class).documentSelfLink);
                        }
                        ctx.completeIteration();
                    }));
        }
        ops.stream().forEach(op -> host.send(op));
        testWait(ctx);
        return links;
    }

    private VerificationHost getHost() {
        return this.host.getInProcessHostMap().values().iterator().next();
    }
}
