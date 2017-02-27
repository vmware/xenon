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
 * specific language governing permissions and limitations under the License``.
 */

package com.vmware.xenon.common;

import static org.junit.Assert.assertEquals;

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
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestNewOwnerServesLatestState {

    private static final String INITIAL_NAME_FIELD_VALUE = "initial-name";
    private static final String PATCHED_NAME_FIELD_VALUE = "patched-name";

    private ServiceHost peer;
    private TestRequestSender sender;
    private TestNodeGroupManager nodeGroup;

    @Test(timeout = 1000000)
    public void testNewOwnerServesLatestState() throws Throwable {

        setup();

        // OK: post to factory with initial state and verify response
        String serviceLink = postDocumentAndVerifyAndGetLink();

        // OK: get the service link and validate
        getAndVerifyAfterPost(serviceLink);

        // OK: patch a state field and verify response
        patchDocument(serviceLink);

        // OK: get the state and verify patched field
        ExampleServiceState getAfterPatchResult = getAndVerifyAfterPatch(serviceLink);

        // OK: find out which is the the owner node and stop it; wait for convergence
        stopOwnerNode(serviceLink, getAfterPatchResult);

        // OK: query the document index and verify latest state
        queryDocumentIndexAndVerify(serviceLink);

        // ------------------------------------------
        // get the state from service link and verify
        Operation op = Operation.createGet(this.peer, serviceLink);
        ExampleServiceState state = this.sender.sendAndWait(op, ExampleServiceState.class);

        // FAIL: assert state has the patched field
        assertEquals(PATCHED_NAME_FIELD_VALUE, state.name);
        // FAIL: assert document version
        assertEquals(1, state.documentVersion);
        //assertEquals(1, stateFromQuery.documentEpoch.intValue());

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
        this.peer = this.nodeGroup.getHost();
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

    private VerificationHost createAndStartHost() throws Throwable {
        VerificationHost host = VerificationHost.create(0);
        // host.setMaintenanceIntervalMicros(TimeUnit.MILLISECONDS.toMicros(VerificationHost.FAST_MAINT_INTERVAL_MILLIS));
        host.start();

        return host;
    }

    private String postDocumentAndVerifyAndGetLink() {
        ExampleServiceState body = new ExampleServiceState();
        body.name = INITIAL_NAME_FIELD_VALUE;
        Operation post = Operation.createPost(this.peer, ExampleService.FACTORY_LINK).setBody(body);
        ExampleServiceState postResult = this.sender.sendAndWait(post, ExampleServiceState.class);
        assertEquals(INITIAL_NAME_FIELD_VALUE, postResult.name);
        assertEquals(0, postResult.documentVersion);

        // cache the service link
        return postResult.documentSelfLink;
    }

    private void patchDocument(String serviceLink) {
        ExampleServiceState patchBody = new ExampleServiceState();
        patchBody.name = PATCHED_NAME_FIELD_VALUE;
        Operation patch = Operation.createPatch(this.peer, serviceLink).setBody(patchBody);
        ExampleServiceState patchResult = this.sender.sendAndWait(patch, ExampleServiceState.class);
        assertEquals(PATCHED_NAME_FIELD_VALUE, patchResult.name);
        assertEquals(1, patchResult.documentVersion);
    }

    private void getAndVerifyAfterPost(String serviceLink) {
        Operation get = Operation.createGet(this.peer, serviceLink);
        ExampleServiceState getResult = this.sender.sendAndWait(get, ExampleServiceState.class);
        assertEquals(INITIAL_NAME_FIELD_VALUE, getResult.name);
        assertEquals(0, getResult.documentVersion);
    }

    private ExampleServiceState getAndVerifyAfterPatch(String serviceLink) {
        Operation getAfterPatch = Operation.createGet(this.peer, serviceLink);
        ExampleServiceState getAfterPatchResult = this.sender
                .sendAndWait(getAfterPatch, ExampleServiceState.class);
        assertEquals(PATCHED_NAME_FIELD_VALUE, getAfterPatchResult.name);
        assertEquals(1, getAfterPatchResult.documentVersion);
        return getAfterPatchResult;
    }

    private void stopOwnerNode(String serviceLink,
            ExampleServiceState getAfterPatchResult) {
        VerificationHost owner = (VerificationHost) this.nodeGroup.getAllHosts().stream()
                .filter(host -> host.getId().contentEquals(getAfterPatchResult.documentOwner))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("couldn't find owner node"));
        owner.log(Level.INFO, "Stopping owner node...");
        owner.tearDown();
        this.nodeGroup.removeHost(owner);

        // get remaining peers
        List<VerificationHost> availablePeers = this.nodeGroup.getAllHosts().stream()
                .filter(host -> !host.getId().contentEquals(owner.getId()))
                .map(host -> (VerificationHost) host)
                .collect(Collectors.toList());

        // use one for sender
        this.peer = availablePeers.get(0);
        this.sender = new TestRequestSender(this.peer);

        this.nodeGroup.waitForConvergence();
        availablePeers.forEach(p -> p.waitForServiceAvailable(serviceLink));
    }

    private void queryDocumentIndexAndVerify(String serviceLink) {
        Query query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, serviceLink)
                .build();
        QueryTask queryTask = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.EXPAND_CONTENT)
                .setQuery(query).build();
        Operation queryOp = Operation
                .createPost(UriUtils.buildUri(this.peer, ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(queryTask);
        QueryTask queryResponse = this.sender.sendAndWait(queryOp, QueryTask.class);
        assertEquals(1, queryResponse.results.documents.size());
        ExampleServiceState stateFromQuery = Utils
                .fromJson(queryResponse.results.documents.get(serviceLink),
                        ExampleServiceState.class);
        assertEquals(PATCHED_NAME_FIELD_VALUE, stateFromQuery.name);
        assertEquals(1, stateFromQuery.documentVersion);
        //assertEquals(1, stateFromQuery.documentEpoch.intValue());
    }

}