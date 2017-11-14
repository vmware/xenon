/*
 * Copyright (c) 2014-2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.test;

import java.net.URI;
import java.util.function.BiPredicate;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SynchronizationManagementService;

public class TestContainerVerificationHost {

    ContainerVerificationHost host;
    // set long time out for container environment
    private static final int DEFAULT_TEST_TIMEOUT_IN_SECOND = 600;
    public String factoryLink = ExampleService.FACTORY_LINK;
    public int nodeCount = 3;
    public int serviceCount = 10;

    private BiPredicate<ExampleService.ExampleServiceState, ExampleService.ExampleServiceState> exampleStateConvergenceChecker = (
            initial, current) -> {
        if (current.name == null) {
            return false;
        }

        return current.name.equals(initial.name);
    };

    @Before
    public void setUp() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        // create verification host which create xenon node in docker container
        this.host = ContainerVerificationHost.create(0);
        this.host.setTimeoutSeconds(DEFAULT_TEST_TIMEOUT_IN_SECOND);
        this.host.start();
    }

    @Ignore
    @Test
    public void testNodeLeftThenJoinSyncInContainer() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        // create more example services
        this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.factoryLink);
        ContainerVerificationHost h0 = this.host.getPeerHost();
        // stop one node with index removed
        this.host.stopHost(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence(this.host.getPeerCount());
        waitForFactoryAvailable();
        // create more examples
        this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.factoryLink);
        this.host.setUpPeerHosts(1);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        waitForFactoryAvailable();
    }

    @Ignore
    @Test
    public void testNodeRestartSyncInContainer() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        //create services
        this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.factoryLink);
        ContainerVerificationHost h0 = this.host.getPeerHost();
        // stop service preserve index
        this.host.stopHostAndPreserveState(h0);
        this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
        this.host.waitForNodeGroupConvergence(this.host.getPeerCount());
        waitForFactoryAvailable();
        // create more services
        this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.factoryLink);
        // restart node with preserved index
        this.host.resumeHostInContainer(h0);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        waitForFactoryAvailable();
    }

    public void waitForFactoryAvailable() {
        this.host.waitFor("Factory availability timeout", () -> {
            URI uri = UriUtils.buildUri(this.host.getPeerHost(), SynchronizationManagementService.class);
            ServiceDocumentQueryResult result = null;
            try {
                result = this.host.getTestRequestSender().sendAndWait(Operation.createGet(uri), ServiceDocumentQueryResult.class);
            } catch (RuntimeException e) {
                // receive failed response
                return false;
            }
            SynchronizationManagementService.SynchronizationManagementState state =
                    Utils.fromJson(result.documents.get(this.factoryLink), SynchronizationManagementService.SynchronizationManagementState.class);
            if (state.owner == null) {
                return false;
            }
            if (!this.host.containPeerId(state.owner)) {
                return false;
            }
            if (state.status != SynchronizationManagementService.SynchronizationManagementState.Status.AVAILABLE) {
                return false;
            }
            return true;
        });
    }

    @After
    public void cleanUp() {
        this.host.tearDown();
        this.host.tearDownInProcessPeers();
    }

}
