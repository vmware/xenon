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

package com.vmware.xenon.common.test;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static com.vmware.xenon.common.test.TestContext.waitFor;
import static com.vmware.xenon.common.test.VerificationHost.FAST_MAINT_INTERVAL_MILLIS;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;

import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceStats;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.test.TestContext.WaitConfig;
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupService.UpdateQuorumRequest;
import com.vmware.xenon.services.common.NodeGroupUtils;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * This class represents a node group and provides node group related methods for test.
 *
 * This class does NOT keep any state of the node group; in other words, all provided methods will
 * interact with one/some/all of nodes in order to perform the operation.
 *
 * It is caller's responsibility that added hosts meet the condition that aligns with logical
 * concept of node group. For example, if you add a node which is NOT part of the node group
 * represented by this class, and calling methods from this class may cause unexpected result.
 *
 * Authentication is not yet supported.
 */
public class TestNodeGroupManager {

    private Set<ServiceHost> hosts = new HashSet<>();

    private Duration timeout = TestContext.DEFAULT_WAIT_DURATION;

    private String nodeGroupName = ServiceUriPaths.DEFAULT_NODE_GROUP_NAME;

    public TestNodeGroupManager() {
    }

    public TestNodeGroupManager(String nodeGroupName) {
        this.nodeGroupName = nodeGroupName;
    }

    public TestNodeGroupManager addHost(ServiceHost serviceHost) {
        this.hosts.add(serviceHost);
        return this;
    }

    public TestNodeGroupManager addHosts(Collection<ServiceHost> serviceHosts) {
        this.hosts.addAll(serviceHosts);
        return this;
    }

    public Set<ServiceHost> getAllHosts() {
        return Collections.unmodifiableSet(this.hosts);
    }

    public boolean removeHost(String hostId) {
        return this.hosts.removeIf(host -> host.getId().equals(hostId));
    }

    public boolean removeHost(ServiceHost host) {
        return this.hosts.remove(host);
    }

    /**
     * Return randomly chosen {@link ServiceHost}.
     */
    public ServiceHost getHost() {
        if (this.hosts.isEmpty()) {
            throw new RuntimeException("hosts are empty");
        }
        return this.hosts.stream().findAny().get();
    }

    public Optional<ServiceHost> getHost(String hostId) {
        return this.hosts.stream().filter(h -> h.getId().equals(hostId)).findAny();
    }

    /**
     * Send {@link Operation} using randomly chosen {@link ServiceHost}
     */
    public void sendRequest(Operation op) {
        getHost().sendRequest(op);
    }

    private TestRequestSender getTestRequestSender() {
        ServiceHost peer = getHost();
        return new TestRequestSender(peer);
    }

    /**
     * Create a node group service instance on each of the attached hosts
     */
    public TestNodeGroupManager createNodeGroup() {
        List<Operation> ops = this.hosts.stream()
                .map(host -> UriUtils.buildUri(host.getUri(), ServiceUriPaths.NODE_GROUP_FACTORY))
                .map(uri -> {
                    NodeGroupState body = new NodeGroupState();
                    body.documentSelfLink = this.nodeGroupName;
                    return Operation.createPost(uri).setBodyNoCloning(body);
                })
                .collect(toList());

        // choose one peer and send all request in parallel then wait.
        getTestRequestSender().sendAndWait(ops);

        return this;
    }

    /**
     * Make all hosts join the node group and wait for convergence
     */
    public TestNodeGroupManager joinNodeGroupAndWaitForConvergence() {
        long startTime = Utils.getNowMicrosUtc();

        // set quorum
        int quorum = this.hosts.size();
        updateQuorum(quorum);

        // join node group
        String nodeGroupPath = getNodeGroupPath();

        // pick one node. rest of them join the nodegroup through this node
        ServiceHost peer = getHost();

        List<Operation> ops = this.hosts.stream()
                .filter(host -> !Objects.equals(host, peer))
                .map(host -> {
                    URI peerNodeGroup = UriUtils.buildUri(peer, nodeGroupPath);
                    URI newNodeGroup = UriUtils.buildUri(host, nodeGroupPath);
                    host.log(Level.INFO, "Joining %s through %s", newNodeGroup, peerNodeGroup);

                    JoinPeerRequest body = JoinPeerRequest.create(peerNodeGroup, quorum);
                    return Operation.createPost(newNodeGroup).setBodyNoCloning(body);
                })
                .collect(toList());

        TestRequestSender sender = new TestRequestSender(peer);
        sender.sendAndWait(ops);

        // wait convergence

        // check membershipUpdateTimeMicros has updated
        waitFor(this.timeout, () -> {
            List<NodeGroupState> nodeGroupStates = getNodeGroupStates();
            return nodeGroupStates.stream()
                    .map(state -> state.membershipUpdateTimeMicros)
                    .allMatch(updateTime -> startTime < updateTime);
        }, "membershipUpdateTimeMicros has not updated.");

        getHost().log(Level.INFO, "membershipUpdateTimeMicros has updated");

        waitForConvergence();

        return this;
    }

    /**
     * Issue a quorum update request to all nodes
     * Method will wait until all requests return successful responses
     */
    public TestNodeGroupManager updateQuorum(int quorum) {

        TestRequestSender sender = getTestRequestSender();

        List<Operation> ops = this.hosts.stream()
                .map(host -> {
                    String nodeGroupPath = getNodeGroupPath();
                    UpdateQuorumRequest body = UpdateQuorumRequest.create(false);
                    body.setMembershipQuorum(quorum);
                    return Operation.createPatch(host, nodeGroupPath).setBodyNoCloning(body);
                })
                .collect(toList());
        sender.sendAndWait(ops);

        // verify all stats now have new quorum set
        waitFor(this.timeout, () -> {
            List<NodeGroupState> nodeGroupStates = getNodeGroupStates();

            Set<Integer> quorums = nodeGroupStates.stream()
                    .flatMap(state -> state.nodes.values().stream())
                    .map(nodeState -> nodeState.membershipQuorum)
                    .distinct()
                    .collect(toSet());

            return quorums.size() == 1 && quorums.contains(quorum);
        }, () -> "Failed to set quorum to = " + quorum);

        return this;
    }

    private List<NodeGroupState> getNodeGroupStates() {
        TestRequestSender sender = getTestRequestSender();
        String nodeGroupPath = getNodeGroupPath();

        List<Operation> nodeGroupOps = this.hosts.stream()
                .map(host -> UriUtils.buildUri(host.getUri(), nodeGroupPath))
                .map(Operation::createGet)
                .collect(toList());
        return sender.sendAndWait(nodeGroupOps, NodeGroupState.class);
    }

    /**
     * wait until cluster is ready
     *
     * logic:
     * 1) NodeGroupUtils.checkForConvergence
     * 2) Once 1) is true, use NodeGroupUtils.isNodeGroupAvailable()
     *
     * Due to the implementation of {@link NodeGroupUtils#isNodeGroupAvailable}, quorum needs to
     * be set to less than the available node counts.
     *
     * @see NodeGroupUtils#checkConvergenceFromAnyHost(ServiceHost, NodeGroupState, Operation)
     * @see NodeGroupUtils#isNodeGroupAvailable(ServiceHost, NodeGroupState)
     */
    public TestNodeGroupManager waitForConvergence() {

        ServiceHost peer = getHost();

        Duration checkTimeout = Duration.ofMillis(FAST_MAINT_INTERVAL_MILLIS * 2);
        Duration checkInterval = Duration.ofMillis(FAST_MAINT_INTERVAL_MILLIS * 2);

        WaitConfig waitConfig = new WaitConfig().setDuration(this.timeout).setInterval(checkInterval);

        // Step 1: NodeGroupUtils.checkConvergenceFromAnyHost
        waitFor(waitConfig, () -> {

            List<NodeGroupState> nodeGroupStates = getNodeGroupStates();

            for (NodeGroupState nodeGroupState : nodeGroupStates) {
                TestContext testContext = new TestContext(1, checkTimeout);
                testContext.setCheckInterval(checkInterval);

                // placeholder operation
                Operation parentOp = Operation.createGet(peer, "/")
                        .setReferer(peer.getUri())
                        .setCompletion(testContext.getCompletion());

                try {
                    NodeGroupUtils.checkConvergenceFromAnyHost(peer, nodeGroupState, parentOp);
                    testContext.await();
                } catch (Exception e) {
                    return false;
                }
            }

            return true;
        }, () -> "Step 1: NodeGroupUtils.checkConvergenceFromAnyHost failed");


        // Step 2: NodeGroupUtils.isNodeGroupAvailable
        waitFor(waitConfig, () -> {

            for (ServiceHost host : this.hosts) {

                TestRequestSender sender = getTestRequestSender();
                String nodeGroupPath = getNodeGroupPath();
                Operation nodeGroupStateGetOp = Operation.createGet(host, nodeGroupPath);
                NodeGroupState nodeGroupState = sender.sendAndWait(nodeGroupStateGetOp, NodeGroupState.class);

                boolean isAvailable = NodeGroupUtils.isNodeGroupAvailable(host, nodeGroupState);
                if (!isAvailable) {
                    return false;
                }

            }

            return true;
        }, () -> "Step 2: NodeGroupUtils.isNodeGroupAvailable check failed");

        return this;
    }

    private boolean isServiceStatAvailable(ServiceStats stats) {
        ServiceStat stat = stats.entries.get(Service.STAT_NAME_AVAILABLE);
        return stat != null && stat.latestValue == Service.STAT_VALUE_TRUE;
    }

    /**
     * wait until replicated, owner selected factory service is ready
     *
     * @see com.vmware.xenon.services.common.NodeGroupUtils#checkServiceAvailability
     */
    public TestNodeGroupManager waitForFactoryServiceAvailable(String factoryServicePath) {

        ServiceHost peer = getHost();

        SelectAndForwardRequest body = new SelectAndForwardRequest();
        body.key = factoryServicePath;

        waitFor(this.timeout, () -> {

            // find factory owner node
            String nodeSelector = ServiceUriPaths.DEFAULT_NODE_SELECTOR;
            Operation selectPost = Operation.createPost(peer, nodeSelector).setBody(body);

            Operation selectResponse = getTestRequestSender().sendAndWait(selectPost);
            SelectOwnerResponse selectOwnerResponse = selectResponse
                    .getBody(SelectOwnerResponse.class);
            URI ownerNodeGroupReference = selectOwnerResponse.ownerNodeGroupReference;

            // retrieves host id
            String ownerId = selectOwnerResponse.selectedNodes.stream()
                    .filter(node -> ownerNodeGroupReference.equals(node.groupReference))
                    .map(node -> node.id)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("couldn't find owner node id"));

            ServiceHost ownerHost = getHost(ownerId)
                    .orElseThrow(() -> new RuntimeException("couldn't find owner node"));

            // check factory service stats on factory owner node
            URI factoryOwnerServiceStatsUri = UriUtils.buildStatsUri(ownerHost, factoryServicePath);
            Operation statsOp = Operation.createGet(factoryOwnerServiceStatsUri);

            ServiceStats stats = getTestRequestSender().sendAndWait(statsOp, ServiceStats.class);
            return isServiceStatAvailable(stats);

        }, () -> String.format("%s factory service didn't become available.", factoryServicePath));

        return this;
    }

    private String getNodeGroupPath() {
        return ServiceUriPaths.NODE_GROUP_FACTORY + "/" + this.nodeGroupName;
    }

    public Duration getTimeout() {
        return this.timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public String getNodeGroupName() {
        return this.nodeGroupName;
    }

    public void setNodeGroupName(String nodeGroupName) {
        this.nodeGroupName = nodeGroupName;
    }
}
