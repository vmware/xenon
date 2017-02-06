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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestPostAfterPatchAndDelete extends BasicReusableHostTestCase {

    private int nodeCount = 3;

    @Test
    public void patchDeleteThenPost() throws Throwable {

        this.host.setPeerSynchronizationEnabled(true);
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount, true);
        this.host.setNodeGroupQuorum(this.nodeCount);

        for (VerificationHost host : this.host.getInProcessHostMap().values()) {
            host.waitForServiceAvailable(ExampleService.FACTORY_LINK);
        }

        TestRequestSender sender = this.host.getTestRequestSender();

        VerificationHost targetHost = this.host.getPeerHost();

        List<Operation> posts = new ArrayList<>();
        List<Operation> patches = new ArrayList<>();
        List<Operation> deletes = new ArrayList<>();
        List<Operation> newPosts = new ArrayList<>();
        for (int i = 0; i < this.serviceCount; i++) {
            String name = "doc-" + i;
            String selfLink = UriUtils.buildUriPath(ExampleService.FACTORY_LINK, name);

            // POST
            ExampleServiceState doc = new ExampleServiceState();
            doc.name = name;
            doc.documentSelfLink = selfLink;
            posts.add(Operation.createPost(targetHost, ExampleService.FACTORY_LINK).setBody(doc));

            // PATCH
            doc.name += "-patch";
            patches.add(Operation.createPatch(targetHost, selfLink)
                    .setBody(doc));

            // DELETE
            deletes.add(Operation.createDelete(targetHost, selfLink)
                    .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL)
            );

            // POST
            doc = new ExampleServiceState();
            doc.name = name + "-new-post";
            doc.documentSelfLink = selfLink;
            newPosts.add(Operation.createPost(targetHost, ExampleService.FACTORY_LINK)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                    .setBody(doc)
            );
        }
        sender.sendAndWait(posts);
        sender.sendAndWait(patches);
        sender.sendAndWait(deletes);
        sender.sendAndWait(newPosts);
    }
}
