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

import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import com.vmware.xenon.common.BasicTestCase;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.test.TestRequestSender;
import com.vmware.xenon.common.test.VerificationHost;

public class TestNodeGroupDocumentOwnerChange extends BasicTestCase {

    private static final ConcurrentMap<String, Set<String>> ownerChangeEventsByHostId = new ConcurrentHashMap<>();
    private static final Set<String> docIdsWithDocumentOwner = Collections.synchronizedSet(new HashSet<>());
    private static final Set<String> docIdsWithoutDocumentOwner = Collections.synchronizedSet(new HashSet<>());

    public static class NodeGroupChangeCheckService extends StatefulService {
        public static final String FACTORY_LINK = "/owner-checks";

        public NodeGroupChangeCheckService() {
            super(NodeGroupChangeCheckState.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
            toggleOption(ServiceOption.REPLICATION, true);
            toggleOption(ServiceOption.OWNER_SELECTION, true);
        }

        @Override
        public void handleNodeGroupMaintenance(Operation post) {
            ServiceMaintenanceRequest request = post.getBody(ServiceMaintenanceRequest.class);
            if (request.configUpdate != null
                    && request.configUpdate.addOptions != null
                    && request.configUpdate.addOptions.contains(ServiceOption.DOCUMENT_OWNER)) {

                String hostId = this.getHost().getId();
                ownerChangeEventsByHostId.compute(hostId, (key, selfLinks) -> {
                    if (selfLinks == null) {
                        selfLinks = new HashSet<>();
                    }
                    selfLinks.add(this.getSelfLink());
                    return selfLinks;
                });
            }
            super.handleNodeGroupMaintenance(post);
        }

        @Override
        public void handleGet(Operation get) {
            // when parameter is present, add selfLink to static variable for verification
            String query = get.getUri().getQuery();
            if (query != null && query.contains("CHECK_OWNER")) {
                if (getOptions().contains(ServiceOption.DOCUMENT_OWNER)) {
                    docIdsWithDocumentOwner.add(getSelfLink());
                } else {
                    docIdsWithoutDocumentOwner.add(getSelfLink());
                }
            }
            super.handleGet(get);
        }
    }

    public static class NodeGroupChangeCheckState extends ServiceDocument {
        public String name;
    }

    @Override
    public void beforeHostTearDown(VerificationHost host) {
        this.host.tearDownInProcessPeers();
    }

    @Test
    public void documentOwnerChange() throws Throwable {

        // shutdown a node and verify newly elected owner services receive document-owner-change-event.
        // Also, checks all service instances have DOCUMENT_OWNER flag toggled.

        int nodeCount = 3;
        int quorumCount = 2;
        int numOfDocs = 1000;

        // setup peers
        this.host.setUpPeerHosts(nodeCount);
        VerificationHost peer = this.host.getPeerHost();

        // create a nodegroup with peers. HOST will NOT join the node group.
        this.host.joinNodesAndVerifyConvergence(nodeCount);
        this.host.setNodeGroupQuorum(quorumCount);

        // start service
        for (VerificationHost h : this.host.getInProcessHostMap().values()) {
            h.startServiceAndWait(FactoryService.create(NodeGroupChangeCheckService.class),
                    NodeGroupChangeCheckService.FACTORY_LINK, null);
        }

        peer.waitForReplicatedFactoryServiceAvailable(
                UriUtils.buildUri(peer, NodeGroupChangeCheckService.FACTORY_LINK));


        TestRequestSender sender = new TestRequestSender(this.host);

        // find factory owner node for the service
        VerificationHost factoryOwner = findFactoryOwner();

        // create documents
        List<Operation> posts = new ArrayList<>();
        for (int i = 0; i < numOfDocs; i++) {
            NodeGroupChangeCheckState body = new NodeGroupChangeCheckState();
            body.name = "foo-" + i;
            body.documentSelfLink = String.valueOf(i);
            Operation post = Operation.createPost(peer, NodeGroupChangeCheckService.FACTORY_LINK).setBody(body);
            posts.add(post);
        }
        List<NodeGroupChangeCheckState> postResults = sender.sendAndWait(posts, NodeGroupChangeCheckState.class);

        // pick non-factory-owner node to shutdown
        VerificationHost nodeToShutdown = this.host.getInProcessHostMap().values().stream()
                .filter(node -> node != factoryOwner)
                .findFirst()
                .get();


        List<NodeGroupChangeCheckState> docsOnShutdownNode = postResults.stream()
                .filter(doc -> doc.documentOwner.equals(nodeToShutdown.getId()))
                .collect(toList());


        // clear event count before shutting down node
        ownerChangeEventsByHostId.clear();

        // shutdown docOwner node
        this.host.stopHost(nodeToShutdown);

        this.host.waitForNodeGroupConvergence(nodeCount - 1);

        VerificationHost newPeer = this.host.getPeerHost();

        // issue GET to the docs which were on shutdown node
        List<Operation> getOps = docsOnShutdownNode.stream()
                .map(doc -> doc.documentSelfLink)
                .map(selfLink -> Operation.createGet(newPeer, selfLink))
                .collect(toList());
        List<NodeGroupChangeCheckState> getResults = sender.sendAndWait(getOps, NodeGroupChangeCheckState.class);

        // for these docs, new doc owner is NOT on factory owner node
        List<NodeGroupChangeCheckState> docsOnNonFactoryNode = getResults.stream()
                .filter(doc -> !(doc.documentOwner.equals(factoryOwner.getId())))
                .collect(toList());


        // Verify DOCUMENT_OWNER option was enabled on service instances

        // check routed service instance has DOCUMENT_OWNER toggled
        List<Operation> getToCheckOwnerDocument = new ArrayList<>();
        for (int i = 0; i < numOfDocs; i++) {
            String selfLinkPath = NodeGroupChangeCheckService.FACTORY_LINK + "/" + i + "?CHECK_OWNER";
            Operation get = Operation.createGet(newPeer, selfLinkPath);
            getToCheckOwnerDocument.add(get);
        }
        sender.sendAndWait(getToCheckOwnerDocument);

        // All service instances should have DOCUMENT_OWNER toggled
        if (!docIdsWithoutDocumentOwner.isEmpty()) {
            String msg = String.format(
                    "Expected all service instances have DOCUMENT_OWNER, but %d services don't have",
                    docIdsWithoutDocumentOwner.size());
            fail(msg);
        }
        String message = "All service instance should have DOCUMENT_OWNER enabled";
        assertEquals(message, numOfDocs, docIdsWithDocumentOwner.size());


        // Verify handleNodeGroupMaintenance with DOCUMENT_OWNER have triggered on all docs
        // previously on the shutdown node


        // wait until all handleNodeGroupMaintenance events to occur since it is asynchronous
        String msg = String.format(
                "Expected all docs on shutdown node should trigger events that adds DOCUMENT_OWNER. expected=%d",
                docsOnNonFactoryNode.size());
        this.host.waitFor(msg, () -> {
            Set<String> allDocIds = new HashSet<>();
            synchronized (ownerChangeEventsByHostId) {
                ownerChangeEventsByHostId.values().forEach(allDocIds::addAll);
            }
            long totalNumOfOwnerChangeEvents = allDocIds.size();
            return docsOnNonFactoryNode.size() == totalNumOfOwnerChangeEvents;
        });
    }

    private VerificationHost findFactoryOwner() {

        TestRequestSender sender = new TestRequestSender(this.host.getPeerHost());

        List<Operation> ops = new ArrayList<>();
        for (VerificationHost node : this.host.getInProcessHostMap().values()) {
            ops.add(Operation.createGet(UriUtils.buildStatsUri(node, NodeGroupChangeCheckService.FACTORY_LINK)));
        }

        List<ServiceStats> response = sender.sendAndWait(ops, ServiceStats.class);
        String factoryOwnerId = response.stream()
                .filter(stats -> {
                    ServiceStat stat = stats.entries.get(Service.STAT_NAME_AVAILABLE);
                    return stat != null && stat.latestValue == Service.STAT_VALUE_TRUE;
                })
                .findFirst()
                .map(stat -> stat.documentOwner)
                .orElseThrow(() -> new RuntimeException("Cannot find factory owner"));

        return this.host.getInProcessHostMap().values().stream()
                .filter(node -> node.getId().equals(factoryOwnerId))
                .findFirst()
                .get();
    }
}
