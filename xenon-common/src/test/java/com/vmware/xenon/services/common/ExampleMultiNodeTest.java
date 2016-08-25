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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.ServiceHostTestUtils;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class ExampleMultiNodeTest {

    //    @Test
    public void basicMultiNode() {
        // scenario:
        //   create 2 nodes and join to default group
        //   post & get example service

        // prepare multiple nodes
        MyServiceHost host1 = new MyServiceHost();
        MyServiceHost host2 = new MyServiceHost();

        TestNodeGroupManager nodeGroup = new TestNodeGroupManager();
        nodeGroup.addHost(host1);
        nodeGroup.addHost(host2);

        // make node group join the "default" node group, then wait cluster to be stabilized
        nodeGroup.joinNodeGroupAndWaitForConvergence("default");

        // wait the service to be available in cluster
        nodeGroup.waitForServiceAvailable("/examples");

        // prepare operation sender(client)
        ServiceHost peer = nodeGroup.getHost();
        TestRequestSender sender = new TestRequestSender(peer);

        // POST request
        ExampleServiceState body = new ExampleServiceState();
        body.documentSelfLink = "/foo";
        body.name = "foo";
        Operation post = Operation.createPost(peer, "/examples").setBody(body);

        // verify post response
        ExampleServiceState result = sender.sendAndWait(post, ExampleServiceState.class);

        // validate post result...

        // make get and validate result
        Operation get = Operation.createGet(peer, "/foo");
        ExampleServiceState getResult = sender.sendAndWait(get, ExampleServiceState.class);

        // validate get result...
    }

    //    @Test
    public void multipleNodeGroups() {
        // scenario:
        //   create 3 nodes and create custom node groups

        // prepare multiple nodes
        MyServiceHost host1 = new MyServiceHost();  // will join groupA & groupB
        MyServiceHost host2 = new MyServiceHost();  // will join groupA & groupB
        MyServiceHost host3 = new MyServiceHost();  // will join groupA only

        // create custom nodegroup on each node
        ServiceHostTestUtils.createNodeGroup(host1, "groupA");
        ServiceHostTestUtils.createNodeGroup(host2, "groupA");
        ServiceHostTestUtils.createNodeGroup(host3, "groupA");
        ServiceHostTestUtils.createNodeGroup(host1, "groupB");
        ServiceHostTestUtils.createNodeGroup(host2, "groupB");

        TestNodeGroupManager groupA = new TestNodeGroupManager();
        groupA.addHost(host1);
        groupA.addHost(host2);
        groupA.addHost(host3);
        groupA.joinNodeGroupAndWaitForConvergence("groupA");  // make them join groupA

        TestNodeGroupManager groupB = new TestNodeGroupManager();
        groupB.addHost(host1);
        groupB.addHost(host2);
        groupB.joinNodeGroupAndWaitForConvergence("groupB");  // make them join groupB

        // wait the service to be available in cluster
        groupA.waitForServiceAvailable("/examples");

        // perform operation and verify...
    }

    // below are just for compilation, they will be implemented in somewhere. please ignore for now.

    private static class MyServiceHost extends ServiceHost {
    }

}
