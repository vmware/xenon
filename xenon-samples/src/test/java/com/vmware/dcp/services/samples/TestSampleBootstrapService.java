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

package com.vmware.dcp.services.samples;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.samples.SampleBootstrapService;

public class TestSampleBootstrapService {

    private static VerificationHost HOST;
    private static VerificationHost PEER;

    @BeforeClass
    public static void setUp() throws Throwable {
        HOST = VerificationHost.create(0);
        HOST.start();

        // setup PEER
        HOST.setUpPeerHosts(3);
        PEER = HOST.getInProcessHostMap().values().stream().findFirst().get();

        // create a nodegroup with peers. HOST will NOT join the node group.
        HOST.joinNodesAndVerifyConvergence(3);
        HOST.waitForNodeGroupConvergence(3);

        List<VerificationHost> peers = new ArrayList<>(HOST.getInProcessHostMap().values());

        for (VerificationHost h : peers) {
            h.startServiceAndWait(SampleBootstrapService.createFactory(),
                    SampleBootstrapService.FACTORY_LINK, null);
        }


        PEER.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(PEER, SampleBootstrapService.FACTORY_LINK));

        // triggering(creating) the service to all nodes.
        // only one will be created on owner node, and rest will be ignored after converted to PUT
        for (VerificationHost h : peers) {
            SampleBootstrapService.startTask(h, ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                    SampleBootstrapService.FACTORY_LINK);
        }
    }

    @AfterClass
    public static void tearDown() {
        HOST.tearDownInProcessPeers();
        HOST.tearDown();
    }

    @Test
    public void taskCreation() throws Throwable {
        PEER.sendAndWaitExpectSuccess(
                Operation.createGet(PEER, "/core/bootstrap/preparation-task"));

        // admin@vmware.com must be created
        PEER.sendAndWaitExpectSuccess(
                Operation.createGet(PEER, "/core/authz/users/admin@vmware.com"));
    }

    @Test
    public void put() throws Throwable {
        // direct put should fail
        Operation op = Operation.createPut(PEER, "/core/bootstrap/preparation-task")
                .setBody(new ServiceDocument());
        PEER.sendAndWaitExpectFailure(op);
    }

}
