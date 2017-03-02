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

import java.time.Duration;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.test.TestNodeGroupManager;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;

public class TestNotPersistedServiceIsAvailableAfterOwnerRestart {

    private static final String INITIAL_NAME_FIELD_VALUE = "initial-name";

    private VerificationHost peer;
    private TestRequestSender sender;
    private TestNodeGroupManager nodeGroup;

    // reusing ExampleService, just turn off PERSISTENCE option (and CONCURRENT_GET_HANDLING
    // which is implicitly turned on byt the PERSISTENCE)
    public static class ExampleNotPersistedService extends ExampleService {

        public static final String FACTORY_LINK = "/core/not_persisted";

        public ExampleNotPersistedService() {
            super();
            toggleOption(ServiceOption.PERSISTENCE, false);
            toggleOption(ServiceOption.CONCURRENT_GET_HANDLING, false);
        }
    }

    @Test
    public void testNotPersistedServiceIsAvailableAfterOwnerRestart() throws Throwable {

        setup();

        // post to factory with initial state and verify response
        ExampleServiceState initialResult = postDocumentAndVerifyAndGetResult();

        // we do a get to ensure service is available
        getServiceLinkAndVerify(initialResult.documentSelfLink);

        // find out which is the the owner node and restart it
        restartOwnerNode(initialResult);

        // we wait for a while because if we do a get immediately we always
        // receive error response
        // whether it's a persisted service or not
        Thread.sleep(3000);

        // ------------------------------------------
        // Receives service not found failure response.
        Operation op = Operation.createGet(this.peer, initialResult.documentSelfLink);
        ExampleServiceState state = this.sender.sendAndWait(op, ExampleServiceState.class);
        Assert.assertNotNull(state);

        // NOTE:
        // querying the factory of the restarted node returns empty result
        // if we query the factories of the not-restarted peers we get the document link
        // but if we expand the query, no document is expanded and log states:
        //   failure expanding /core/not_persisted/c5682f1f34ec673d549ac62b3daf0:
        //   Service http://127.0.0.1:41095/core/not_persisted/c5682f1f34ec673d549ac62b3daf0
        //   returned error 404 for GET.
        // changing the ServiceOption.PERSISTENCE to true passes the test

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
        this.nodeGroup.waitForFactoryServiceAvailable(ExampleNotPersistedService.FACTORY_LINK);

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

    private void getServiceLinkAndVerify(String documentSelfLink) {
        ExampleServiceState getResult = this.sender.sendAndWait(Operation.createGet(this.peer, documentSelfLink),
                ExampleServiceState.class);
        assertEquals(INITIAL_NAME_FIELD_VALUE, getResult.name);
    }

    private VerificationHost createAndStartHost() throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        host.start();
        host.startFactory(ExampleNotPersistedService.class,
                () -> FactoryService.create(ExampleNotPersistedService.class));
        return host;
    }

    private ExampleServiceState postDocumentAndVerifyAndGetResult() {
        ExampleServiceState body = new ExampleServiceState();
        body.name = INITIAL_NAME_FIELD_VALUE;
        Operation post = Operation.createPost(this.peer, ExampleNotPersistedService.FACTORY_LINK).setBody(body);
        ExampleServiceState postResult = this.sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals(INITIAL_NAME_FIELD_VALUE, postResult.name);
        assertEquals(0, postResult.documentVersion);
        return postResult;
    }

    private void restartOwnerNode(ExampleServiceState state) throws Throwable {
        VerificationHost owner = (VerificationHost) this.nodeGroup.getAllHosts().stream()
                .filter(host -> host.getId().contentEquals(state.documentOwner)).findFirst()
                .orElseThrow(() -> new RuntimeException("couldn't find owner node"));
        owner.log(Level.INFO, "Restarting owner node...");
        owner.stopHostAndPreserveState(owner);
        VerificationHost.restartStatefulHost(owner);
        owner.startFactory(ExampleNotPersistedService.class,
                () -> FactoryService.create(ExampleNotPersistedService.class));
        this.nodeGroup.waitForConvergence();
        this.nodeGroup.waitForFactoryServiceAvailable(ExampleNotPersistedService.FACTORY_LINK);
    }

}
