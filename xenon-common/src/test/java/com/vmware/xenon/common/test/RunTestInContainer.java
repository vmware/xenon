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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.vmware.xenon.common.CommandLineArgumentParser;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.SynchronizationManagementService;

public class RunTestInContainer {

    ContainerVerificationHost host;
    // set long time out for container environment
    private static final int MAX_SLEEP_IN_MINUTE = 1;
    public String factoryLink = ExampleService.FACTORY_LINK;
    public int nodeCount = 3;
    public int serviceCount = 10;
    // sum of operation timeout and test duration
    public long expireDuration = TimeUnit.HOURS.toSeconds(10);
    public int iteration = 1;

    @Before
    public void setUp() throws Throwable {
        CommandLineArgumentParser.parseFromProperties(this);
        // create verification host which create xenon node in docker container
        this.host = ContainerVerificationHost.create(0);
        int timeout = this.host.getTimeoutSeconds();
        this.host.testDurationSeconds = this.expireDuration - timeout;

        this.host.start();
    }

    @Test
    public void longRunSync() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
        this.host.setNodeGroupQuorum(this.nodeCount - 1);
        //create services
        this.host.createExampleServices(this.host.getPeerHost(), this.serviceCount, null, this.factoryLink);
        for (int i = 0; i < this.iteration; i ++) {
            long start = Utils.getNowMicrosUtc();
            ContainerVerificationHost h0 = this.host.getPeerHost();
            // delete node
            this.host.stopHost(h0);
            this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
            this.host.waitForNodeGroupConvergence(this.nodeCount - 1);
            Random rand = new Random();
            int r = rand.nextInt(MAX_SLEEP_IN_MINUTE) + 1;
            Thread.sleep(TimeUnit.MINUTES.toMillis(r));
            // add node
            this.host.setUpPeerHosts(1);
            this.host.joinNodesAndVerifyConvergence(this.nodeCount);
            r = rand.nextInt(MAX_SLEEP_IN_MINUTE) + 1;
            Thread.sleep(TimeUnit.MINUTES.toMillis(r));
            // restart node
            h0 = this.host.getPeerHost();
            this.host.stopHostAndPreserveState(h0);
            this.host.waitForNodeUnavailable(ServiceUriPaths.DEFAULT_NODE_GROUP, this.host.getInProcessHostMap().values(), h0);
            this.host.resumeHostInContainer(h0);
            this.host.joinNodesAndVerifyConvergence(this.nodeCount);
            r = rand.nextInt(MAX_SLEEP_IN_MINUTE) + 1;
            Thread.sleep(TimeUnit.MINUTES.toMillis(r));
            waitForFactoryAvailable();
            long end = Utils.getNowMicrosUtc();
            this.host.log(Level.INFO, "iteration %d time cost %d millis\n", i, (end - start) / 1000);
        }
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
