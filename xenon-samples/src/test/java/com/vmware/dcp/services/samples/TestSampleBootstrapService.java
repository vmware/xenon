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

import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.samples.SampleBootstrapService;

public class TestSampleBootstrapService extends BasicTestCase {

    public int nodeCount = 3;

    @Before
    public void setUp() throws Throwable {

        // setup peer
        this.host.setUpPeerHosts(this.nodeCount);
        VerificationHost peer = this.host.getPeerHost();

        // create a nodegroup with peers. HOST will NOT join the node group.
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.waitForNodeGroupConvergence(this.nodeCount);

        this.host.getPeerHost();

        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startServiceAndWait(SampleBootstrapService.createFactory(),
                    SampleBootstrapService.FACTORY_LINK, null);
        }

        peer.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(peer, SampleBootstrapService.FACTORY_LINK));

        // triggering(creating) the service to all nodes.
        // only one will be created on owner node, and rest will be ignored after converted to PUT
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            SampleBootstrapService.startTask(h, ServiceUriPaths.DEFAULT_NODE_SELECTOR,
                    SampleBootstrapService.FACTORY_LINK);
        }

        host.waitFor("Failed to verify completion of bootstrap/preparation-task", () -> {
            boolean[] isReady = new boolean[1];

            Operation get = Operation.createGet(peer, "/core/bootstrap/preparation-task")
                    .setCompletion((o, e) -> {
                        if (o.getStatusCode() == Operation.STATUS_CODE_NOT_FOUND) {
                            // when it gets 404, wait for service to be ready
                            host.completeIteration();
                            return;
                        }

                        if (e != null) {
                            host.failIteration(e);
                            return;
                        }

                        isReady[0] = true;
                        host.completeIteration();
                    });
            host.sendAndWait(get);
            return isReady[0];
        });

    }

    @Override
    public void beforeHostTearDown(VerificationHost host) {
        this.host.tearDownInProcessPeers();
    }

    @Test
    public void taskCreation() throws Throwable {
        VerificationHost peer = this.host.getPeerHost();

        peer.sendAndWaitExpectSuccess(
                Operation.createGet(peer, "/core/bootstrap/preparation-task"));

        // admin@vmware.com must be created
        peer.sendAndWaitExpectSuccess(
                Operation.createGet(peer, "/core/authz/users/admin@vmware.com"));
    }

    @Test
    public void put() throws Throwable {
        VerificationHost peer = this.host.getPeerHost();

        // direct put should fail
        Operation op = Operation.createPut(peer, "/core/bootstrap/preparation-task")
                .setBody(new ServiceDocument());
        peer.sendAndWaitExpectFailure(op, Operation.STATUS_CODE_BAD_METHOD);
    }

}
