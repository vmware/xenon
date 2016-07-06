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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;

public class TestServiceHostWithMultipleNodes extends BasicTestCase {

    public int nodeCount = 3;

    @Override
    public void beforeHostTearDown(VerificationHost host) {
        this.host.tearDownInProcessPeers();
    }

    @Test
    public void registerForServiceAvailabilityAfterLocalServiceCreation() throws Throwable {

        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.waitForNodeGroupConvergence(this.nodeCount);

        TestContext testContext = testCreate(1);
        CompletionHandler ch = (op, ex) -> {
            if (ex != null) {
                String msg = "serviceAvailability callback received exception";
                testContext.failIteration(new RuntimeException(msg, ex));
                return;
            }
            testContext.completeIteration();
        };

        VerificationHost peer = this.host.getPeerHost();
        peer.registerForServiceAvailability(ch, true, ExampleService.FACTORY_LINK);

        testContext.await();
    }

    @Test
    public void registerForServiceAvailabilityBeforeLocalServiceCreation() throws Throwable {

        // create peer hosts but do not start yet
        List<VerificationHost> peers = new ArrayList<>();
        for (int i = 0; i < this.nodeCount; i++) {
            peers.add(VerificationHost.create(0));
        }

        // register callback before starting peers
        TestContext testContext = testCreate(1);
        boolean[] isCalled = new boolean[1];
        CompletionHandler ch = (op, ex) -> {
            isCalled[0] = true;
            if (ex != null) {
                String msg = "serviceAvailability callback received exception";
                testContext.failIteration(new RuntimeException(msg, ex));
                return;
            }
            testContext.completeIteration();
        };

        VerificationHost peer = peers.get(0);
        peer.registerForServiceAvailability(ch, true, ExampleService.FACTORY_LINK);

        assertFalse("callback should not be called yet", isCalled[0]);

        for (VerificationHost peerHost : peers) {
            peerHost.start();
            this.host.addPeerNode(peerHost);
        }
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.waitForNodeGroupConvergence(this.nodeCount);

        testContext.await();
        assertTrue("callback should be called", isCalled[0]);

    }

}
