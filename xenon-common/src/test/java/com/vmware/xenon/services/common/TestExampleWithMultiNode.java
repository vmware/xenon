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

import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

/**
 * This class is to demonstrate how to use {@link TestNodeGroupManager}.
 */
public class TestExampleWithMultiNode {

    @Test
    public void basicMultiNode() throws Throwable {
        // scenario:
        //   create 2 nodes and join to default group
        //   post & get example service

        // prepare multiple nodes (for simplicity, using VerificationHost)
        VerificationHost host1 = VerificationHost.create(0);
        VerificationHost host2 = VerificationHost.create(0);
        host1.start();
        host2.start();

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHost(host1);
        nodeGroup.addHost(host2);

        // make node group join the "default" node group, then wait cluster to be stabilized
        nodeGroup.joinNodeGroupAndWaitForConvergence();

        // wait the service to be available in cluster
        nodeGroup.waitForFactoryServiceAvailable("/core/examples");

        // prepare operation sender(client)
        ServiceHost peer = nodeGroup.getHost();
        TestRequestSender sender = new TestRequestSender(peer);

        // POST request
        ExampleServiceState body = new ExampleServiceState();
        body.documentSelfLink = "/foo";
        body.name = "foo";
        Operation post = Operation.createPost(peer, "/core/examples").setBody(body);

        // verify post response
        ExampleServiceState result = sender.sendAndWait(post, ExampleServiceState.class);

        // validate post result...

        // make get and validate result
        Operation get = Operation.createGet(peer, "/core/examples/foo");
        ExampleServiceState getResult = sender.sendAndWait(get, ExampleServiceState.class);

        // validate get result...
    }

    @Test
    public void multipleNodeGroups() throws Throwable {
        // scenario:
        //   create 3 nodes and create custom node groups

        // prepare multiple nodes (for simplicity, using VerificationHost
        VerificationHost host1 = VerificationHost.create(0);  // will join groupA & groupB
        VerificationHost host2 = VerificationHost.create(0);  // will join groupA & groupB
        VerificationHost host3 = VerificationHost.create(0);  // will join groupA only
        host1.start();
        host2.start();
        host3.start();

        TestNodeGroupManager groupA = new TestNodeGroupManager("groupA");
        groupA.addHost(host1);
        groupA.addHost(host2);
        groupA.addHost(host3);
        groupA.createNodeGroup();  // create groupA
        groupA.joinNodeGroupAndWaitForConvergence();  // make them join groupA

        TestNodeGroupManager groupB = new TestNodeGroupManager("groupB");
        groupB.addHost(host1);
        groupB.addHost(host2);
        groupB.createNodeGroup();  // create groupB
        groupB.joinNodeGroupAndWaitForConvergence();  // make them join groupB

        // wait the service to be available in cluster
        groupA.waitForFactoryServiceAvailable("/core/examples");

        // perform operation and verify...
    }
}
