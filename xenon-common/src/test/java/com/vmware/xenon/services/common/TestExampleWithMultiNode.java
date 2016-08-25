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

import static java.util.stream.Collectors.toSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

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

        // make node group join to the "default" node group, then wait cluster to be stabilized
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
        assertEquals("foo", result.name);

        // make get and validate result
        Operation get = Operation.createGet(peer, "/core/examples/foo");
        ExampleServiceState getResult = sender.sendAndWait(get, ExampleServiceState.class);

        // validate get result...
        assertEquals("foo", getResult.name);
    }

    @Test
    public void multipleNodeGroups() throws Throwable {
        // scenario:
        //   create custom node groups - groupA and groupB
        //   post to groupA
        //   change quorum in groupA

        // prepare multiple nodes (for simplicity, using VerificationHost
        VerificationHost host1 = VerificationHost.create(0);  // will join groupA & groupB
        VerificationHost host2 = VerificationHost.create(0);  // will join groupA only
        VerificationHost host3 = VerificationHost.create(0);  // will join groupA only
        VerificationHost host4 = VerificationHost.create(0);  // will join groupB only
        host1.start();
        host2.start();
        host3.start();
        host4.start();

        // create a request sender which can be any host in this scenario
        TestRequestSender sender = new TestRequestSender(host1);

        TestNodeGroupManager groupA = new TestNodeGroupManager("groupA")
                .addHost(host1)
                .addHost(host2)
                .addHost(host3)
                .createNodeGroup() // create groupA
                .joinNodeGroupAndWaitForConvergence();  // make them join groupA

        TestNodeGroupManager groupB = new TestNodeGroupManager("groupB")
                .addHost(host1)
                .addHost(host4)
                .createNodeGroup()  // create groupB
                .joinNodeGroupAndWaitForConvergence();  // make them join groupB

        // wait the service to be available in cluster
        groupA.waitForFactoryServiceAvailable("/core/examples");
        groupB.waitForFactoryServiceAvailable("/core/examples");

        // perform operation and verify...

        // POST request to host2 which is in groupA
        ExampleServiceState body = new ExampleServiceState();
        body.documentSelfLink = "/foo";
        body.name = "foo";
        Operation post = Operation.createPost(host2, "/core/examples").setBody(body);

        ExampleServiceState postResult = sender.sendAndWait(post, ExampleServiceState.class);

        // verify owner is one of the host in groupA
        Set<String> expectedOwnerIds = groupA.getAllHosts().stream()
                .map(ServiceHost::getId)
                .collect(toSet());
        String msg = String.format("DocumentOwner %s is not in %s", postResult.documentOwner, expectedOwnerIds);
        assertTrue(msg, expectedOwnerIds.contains(postResult.documentOwner));


        // send a GET to host4 which is in groupB. It should fail.
        Operation get = Operation.createGet(host4, "/core/examples/foo");
        FailureResponse failure = sender.sendAndWaitFailure(get);
        assertEquals(failure.op.getStatusCode(), 404);


        // another use case. relaxing quorum

        // check current quorum is 3
        verifyCurrentQuorum(groupA, 3);

        // relax quorum to 2
        groupA.updateQuorum(2);

        // internally updateQuorum() checks the new quorum value, so this check is not required but
        // just for demo purpose.
        verifyCurrentQuorum(groupA, 2);
    }

    private void verifyCurrentQuorum(TestNodeGroupManager manager, int expectedQuorum) {
        ServiceHost node = manager.getHost();
        TestRequestSender sender = new TestRequestSender(node);

        String nodeGroupPath = "/core/node-groups/" + manager.getNodeGroupName();
        Operation op = Operation.createGet(node, nodeGroupPath);
        NodeGroupState nodeGroupState = sender.sendAndWait(op, NodeGroupState.class);

        nodeGroupState.nodes.values().forEach(
                nodeState -> assertEquals("quorum on host=" + nodeState.id, expectedQuorum, nodeState.membershipQuorum));
    }
}
