/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.time.Duration;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestNewOwnerServesLatestStateOnSecondOwnerStop {

    private VerificationHost peer;
    private TestRequestSender sender;
    private TestNodeGroupManager nodeGroup;
    private VerificationHost owner;
    private String serviceLink;

    private static final String INITIAL_NAME_VALUE = "initial-name";
    private static final String FIRST_PATCH_NAME_VALUE = "first-name";
    private static final String SECOND_PATCH_NAME_VALUE = "second-name";
    private static final String THIRD_PATCH_NAME_VALUE = "third-name";

    @Test(timeout = 1000000)
    public void testNewOwnerServesLatestState() throws Throwable {

        setup();

        // OK: post to factory with initial state and verify response
        this.serviceLink = postDocumentAndVerifyAndGetLink();

        // OK: get the service link and validate
        getAndVerifyNameValue(INITIAL_NAME_VALUE);

        // OK: patch a state field and verify response
        patchDocument(FIRST_PATCH_NAME_VALUE);

        // OK: get the state and verify patched field
        String ownerId = getAndVerifyNameValue(FIRST_PATCH_NAME_VALUE).documentOwner;

        // OK: find out which is the the owner node and stop it; wait for
        // convergence
        stopOwnerNode(ownerId);
        pickAvailableNode();
        Thread.sleep(3000);

        // OK: get after node stop and verify state and owner change
        ExampleServiceState getAfterNodeStop = getAndVerifyNameValue(FIRST_PATCH_NAME_VALUE);
        assertFalse(getAfterNodeStop.documentOwner.contentEquals(this.owner.getId()));

        // OK: patch document on new owner
        patchDocument(SECOND_PATCH_NAME_VALUE);
        //OK: get state after second patch
        getAndVerifyNameValue(SECOND_PATCH_NAME_VALUE);

        //start the owner
        startOwner();

        Thread.sleep(3000);

        // OK: get after owner restart, verify patched state and that document is returned to owner
        ExampleServiceState getAfterOwnerRestart = getAndVerifyNameValue(SECOND_PATCH_NAME_VALUE);
        assertEquals(ownerId, getAfterOwnerRestart.documentOwner);

        //patch document after owner restarted
        patchDocument(THIRD_PATCH_NAME_VALUE);
        // OK: verify patched state
        getAndVerifyNameValue(THIRD_PATCH_NAME_VALUE);

        //stop owner for the second time
        stopOwnerNode(ownerId);

        Thread.sleep(3000);

        // FAILS: verify state after second owner stop
        getAndVerifyNameValue(THIRD_PATCH_NAME_VALUE);

        tearDown();
    }

    private void setup() throws Throwable {
        this.nodeGroup = new TestNodeGroupManager();

        // create and start 3 verification hosts
        for (int i = 0; i < 3; i++) {
            VerificationHost host = createAndStartHost();
            this.nodeGroup.addHost(host);
        }
        // and join nodes
        this.nodeGroup.joinNodeGroupAndWaitForConvergence();
        this.nodeGroup.setTimeout(Duration.ofSeconds(30));
        this.nodeGroup.updateQuorum(2);

        // wait for factory availability
        this.nodeGroup.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);

        // choose random host as sender
        this.peer = (VerificationHost) this.nodeGroup.getHost();
        this.sender = new TestRequestSender(this.peer);
    }

    private void tearDown() {
        if (this.nodeGroup != null) {
            for (ServiceHost host : this.nodeGroup.getAllHosts()) {
                ((VerificationHost) host).tearDown();
            }
            this.nodeGroup = null;
        }
    }

    private void startOwner() throws Throwable {
        VerificationHost.restartStatefulHost(this.owner);
        this.nodeGroup.addHost(this.owner);
        this.nodeGroup.waitForConvergence();
    }

    private VerificationHost createAndStartHost() throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        // host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        host.start();
        return host;
    }

    private String postDocumentAndVerifyAndGetLink() {
        ExampleServiceState body = new ExampleServiceState();
        body.name = INITIAL_NAME_VALUE;
        Operation post = Operation.createPost(this.peer, ExampleService.FACTORY_LINK).setBody(body);
        ExampleServiceState postResult = this.sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals(INITIAL_NAME_VALUE, postResult.name);
        assertEquals(0, postResult.documentVersion);

        // cache the service link
        return postResult.documentSelfLink;
    }

    private void patchDocument(String patchValue) {
        ExampleServiceState patchBody = new ExampleServiceState();
        patchBody.name = patchValue;
        Operation patch = Operation.createPatch(this.peer, this.serviceLink).setBody(patchBody);
        ExampleServiceState patchResult = this.sender.sendAndWait(patch, ExampleServiceState.class);
        assertEquals(patchValue, patchResult.name);
    }

    private ExampleServiceState getAndVerifyNameValue(String expectedNameValue) {
        Operation get = Operation.createGet(this.peer, this.serviceLink);
        ExampleServiceState getResult = this.sender.sendAndWait(get, ExampleServiceState.class);
        assertEquals(expectedNameValue, getResult.name);
        return getResult;
    }

    private void stopOwnerNode(String hostId) {
        this.owner = (VerificationHost) this.nodeGroup.getAllHosts().stream()
                .filter(host -> host.getId().contentEquals(hostId)).findFirst()
                .orElseThrow(() -> new RuntimeException("couldn't find owner node"));
        this.owner.log(Level.INFO, "Stopping owner node...");
        this.owner.stopHostAndPreserveState(this.owner);
        this.nodeGroup.removeHost(this.owner);
    }

    private void pickAvailableNode() {
        // get remaining peers
        List<VerificationHost> availablePeers = this.nodeGroup.getAllHosts().stream()
                .filter(host -> !host.getId().contentEquals(this.owner.getId())).map(host -> (VerificationHost) host)
                .collect(Collectors.toList());

        // use one for sender
        this.peer = availablePeers.get(0);
        this.sender = new TestRequestSender(this.peer);

        this.nodeGroup.waitForConvergence();

        // nodeGroup.waitForFactoryServiceAvailable(ExampleService.FACTORY_LINK);
        availablePeers.forEach(p -> p.waitForServiceAvailable(this.serviceLink));
        availablePeers.forEach(p -> p.waitForServiceAvailable(ExampleService.FACTORY_LINK));
    }

}