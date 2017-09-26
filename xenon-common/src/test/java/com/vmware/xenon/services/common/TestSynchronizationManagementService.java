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

package com.vmware.xenon.services.common;


import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.URI;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import com.vmware.xenon.common.BasicReusableHostTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.VerificationHost;
import com.vmware.xenon.services.common.SynchronizationManagementService.SynchronizationManagementState;


public class TestSynchronizationManagementService extends BasicReusableHostTestCase {
    public int nodeCount = 3;

    public static class BadExampleFactoryService extends FactoryService {
        public static final String FACTORY_LINK = "/test/examples-bad";

        public BadExampleFactoryService() {
            super(ExampleService.ExampleServiceState.class);
        }

        @Override
        public Service createServiceInstance() throws Throwable {
            return new ExampleService();
        }

        @Override
        public void handleConfigurationRequest(Operation request) {
            request.fail(new Throwable());
        }
    }

    private void setUpMultiNode() throws Throwable {
        this.host.setUpPeerHosts(this.nodeCount);
        this.host.joinNodesAndVerifyConvergence(this.nodeCount);
    }

    @Test
    public void availableFactories() throws Throwable {
        setUpMultiNode();
        this.host.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(this.host.getPeerHost(), ExampleService.FACTORY_LINK));

        URI uri = UriUtils.buildUri(this.host.getPeerHost(), SynchronizationManagementService.class);

        Operation op = this.sender.sendAndWait(Operation.createGet(uri));
        ServiceDocumentQueryResult result = op.getBody(ServiceDocumentQueryResult.class);

        // Verify that all factories have some host
        result.documents.forEach((factory, state) -> {
            SynchronizationManagementState s =
                    Utils.fromJson(state, SynchronizationManagementState.class);
            assertNotEquals(null, s.owner);
        });

        // Verify that Example service is AVAILABLE
        SynchronizationManagementState state =
                Utils.fromJson(result.documents.get(ExampleService.FACTORY_LINK), SynchronizationManagementState.class);
        assertNotEquals(null, state.owner);
        assertEquals(SynchronizationManagementState.Status.AVAILABLE, state.status);

        // Verify GET with query works
        URI serviceUri = UriUtils.extendUriWithQuery(uri, ServiceDocument.FIELD_NAME_SELF_LINK,
                ExampleService.FACTORY_LINK);
        op = this.sender.sendAndWait(Operation.createGet(serviceUri));
        result = op.getBody(ServiceDocumentQueryResult.class);
        assertEquals(result.documents.size(), 1);

        // Verify GET with query works if factory is not found
        serviceUri = UriUtils.extendUriWithQuery(uri, ServiceDocument.FIELD_NAME_SELF_LINK, "fake");
        this.sender.sendAndWaitFailure(Operation.createGet(serviceUri));

        // Verify GET with query works with wrong property
        serviceUri = UriUtils.extendUriWithQuery(uri, "fake", "fake");
        op = this.sender.sendAndWait(Operation.createGet(serviceUri));
        result = op.getBody(ServiceDocumentQueryResult.class);
        state = Utils.fromJson(result.documents.get(ExampleService.FACTORY_LINK), SynchronizationManagementState.class);
        assertNotEquals(null, state.owner);
        assertEquals(SynchronizationManagementState.Status.AVAILABLE, state.status);
    }

    @Test
    public void unavailableNodeSelector() throws Throwable {
        this.host.tearDownInProcessPeers();
        setUpMultiNode();
        this.host.setNodeGroupQuorum(this.nodeCount);
        this.host.waitForNodeGroupConvergence();

        this.host.stopHost(this.host.getPeerHost());
        this.host.waitForNodeGroupConvergence();

        VerificationHost peer = this.host.getPeerHost();
        this.host.waitFor("Node selector did not become unavailable!",
                () -> {
                    Operation op = this.sender.sendAndWait(Operation.createGet(peer, ServiceUriPaths.DEFAULT_NODE_SELECTOR));
                    NodeSelectorState ns = op.getBody(NodeSelectorState.class);
                    return ns.status == NodeSelectorState.Status.UNAVAILABLE;
                });

        URI serviceUri = UriUtils.buildUri(peer, SynchronizationManagementService.class);
        Operation op = this.sender.sendAndWait(Operation.createGet(serviceUri));
        ServiceDocumentQueryResult result = op.getBody(ServiceDocumentQueryResult.class);

        // Verify that factory with UNAVAILABLE node selector is also UNAVAILABLE
        SynchronizationManagementState s =
                Utils.fromJson(result.documents.get(ExampleService.FACTORY_LINK), SynchronizationManagementState.class);
        assertEquals(null, s.owner);
        assertEquals(SynchronizationManagementState.Status.UNAVAILABLE, s.status);
    }

    @Test
    public void badFactoryIsUnavailable() throws Throwable {
        setUpMultiNode();
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startServiceAndWait(BadExampleFactoryService.class,
                    BadExampleFactoryService.FACTORY_LINK);
        }

        URI serviceUri = UriUtils.buildUri(this.host.getPeerHost(), SynchronizationManagementService.class);
        Operation op = this.sender.sendAndWait(Operation.createGet(serviceUri));
        ServiceDocumentQueryResult result = op.getBody(ServiceDocumentQueryResult.class);

        // Verify that bad factory is UNAVAILABLE
        SynchronizationManagementState s =
                Utils.fromJson(result.documents.get(BadExampleFactoryService.FACTORY_LINK), SynchronizationManagementState.class);
        assertEquals(null, s.owner);
        assertEquals(SynchronizationManagementState.Status.UNAVAILABLE, s.status);
    }

    @Test
    public void synchronizationRequest() throws Throwable {
        setUpMultiNode();
        VerificationHost peer = this.host.getPeerHost();
        peer.waitForReplicatedFactoryServiceAvailable(UriUtils.buildUri(peer, ExampleService.FACTORY_LINK));
        List<URI> exampleServices = peer.createExampleServices(peer, this.serviceCount, null);
        URI serviceUri = UriUtils.buildUri(peer, SynchronizationManagementService.class);
        String owner = waitForStatus(peer, status -> status.equals(SynchronizationManagementState.Status.AVAILABLE));
        peer = this.host.getInProcessHostMap().values().stream().filter(h -> h.getId().equals(owner)).collect(toList()).get(0);

        // Set factory unavailable
        setFactoryAvailability(peer, 0.0);
        waitForStatus(peer,  status -> !status.equals(SynchronizationManagementState.Status.AVAILABLE));

        // Call the synchronization API and verify that factory is available afterwards.
        SynchronizationRequest request = SynchronizationRequest.create();
        request.documentSelfLink = ExampleService.FACTORY_LINK;
        this.sender.sendAndWait(Operation.createPatch(serviceUri).setBody(request));
        waitForStatus(peer, status -> status.equals(SynchronizationManagementState.Status.AVAILABLE));

        // Verify synchronization on a single child service passes.
        request.documentSelfLink = exampleServices.get(0).getPath();
        this.sender.sendAndWait(Operation.createPatch(serviceUri).setBody(request));

        // Verify calling with fake factory fails.
        request.documentSelfLink = "/core/fake-factory";
        this.sender.sendAndWaitFailure(Operation.createPatch(serviceUri).setBody(request));

        // Verify calling with fake kind fails.
        request.kind = "fake";
        request.documentSelfLink = ExampleService.FACTORY_LINK;
        this.sender.sendAndWaitFailure(Operation.createPatch(serviceUri).setBody(request));
    }

    private String waitForStatus(VerificationHost peer,
                                 Function<SynchronizationManagementState.Status, Boolean> f) {
        SynchronizationManagementState[] state = new SynchronizationManagementState[1];
        URI serviceUri = UriUtils.buildUri(peer, SynchronizationManagementService.class);
        peer.waitFor("Wait for the required status failed", () -> {
            Operation op = this.sender.sendAndWait(Operation.createGet(serviceUri));
            ServiceDocumentQueryResult result = op.getBody(ServiceDocumentQueryResult.class);
            state[0] = Utils.fromJson(result.documents.get(ExampleService.FACTORY_LINK), SynchronizationManagementState.class);
            return f.apply(state[0].status);
        });
        return state[0].owner;
    }

    private void setFactoryAvailability(VerificationHost peer, double isAvailable) {
        ServiceStats.ServiceStat body = new ServiceStats.ServiceStat();
        body.name = Service.STAT_NAME_AVAILABLE;
        body.latestValue = isAvailable;
        Operation put = Operation.createPut(
                UriUtils.buildAvailableUri(peer, ExampleService.FACTORY_LINK))
                .setBody(body);
        this.sender.sendAndWait(put);
    }
}
