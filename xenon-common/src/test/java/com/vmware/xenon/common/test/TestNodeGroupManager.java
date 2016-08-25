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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import static com.vmware.xenon.common.test.TestContext.waitFor;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import com.vmware.xenon.services.common.NodeGroupService.JoinPeerRequest;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupService.UpdateQuorumRequest;
import com.vmware.xenon.services.common.NodeState.NodeStatus;
import com.vmware.xenon.services.common.ServiceUriPaths;

/**
 * Provides node group related methods for test.
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

    public void addHost(ServiceHost serviceHost) {
        this.hosts.add(serviceHost);
    }

    public Set<ServiceHost> getAllHosts() {
        return Collections.unmodifiableSet(this.hosts);
    }

    public boolean removeHost(String hostId) {
        Optional<ServiceHost> host = getHost(hostId);
        if (host.isPresent()) {
            return this.hosts.remove(host.get());
        }
        return false;
    }

    public Map<String, ServiceHost> getHostMap() {
        return getAllHosts().stream().collect(toMap(ServiceHost::getId, identity()));
    }

    /**
     * Return randomly chosen {@link ServiceHost}.
     */
    public ServiceHost getHost() {
        if (this.hosts.isEmpty()) {
            // TODO: throw exception
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
     * Create a node group to all the hosts
     */
    public void createNodeGroup() {
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
    }

    /**
     * Make all hosts join the node group and wait for convergence
     */
    public void joinNodeGroupAndWaitForConvergence() {
        long startTime = Utils.getNowMicrosUtc();

        // set quorum
        int quorum = this.hosts.size();
        setQuorum(quorum);

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
            List<Operation> nodeGroupOps = getNodeGroupOps(nodeGroupPath);
            List<NodeGroupState> responses = sender.sendAndWait(nodeGroupOps, NodeGroupState.class);
            return responses.stream()
                    .map(state -> state.membershipUpdateTimeMicros)
                    .allMatch(updateTime -> startTime < updateTime);
        }, "membershipUpdateTimeMicros has not updated.");

        getHost().log(Level.INFO, "membershipUpdateTimeMicros has updated");

        waitForConvergence();
    }

    /**
     * Set quorum to all nodes.
     * Method will wait until all requests return successful responses
     */
    public void setQuorum(int quorum) {

        String nodeGroupPath = getNodeGroupPath();
        TestRequestSender sender = getTestRequestSender();

        List<Operation> ops = this.hosts.stream()
                .map(host -> UriUtils.buildUri(host.getUri(), nodeGroupPath))
                .map(uri -> {
                    UpdateQuorumRequest body = UpdateQuorumRequest.create(false);
                    body.setMembershipQuorum(quorum);
                    return Operation.createPatch(uri).setBodyNoCloning(body);
                })
                .collect(toList());
        sender.sendAndWait(ops);

        // verify all stats now have new quorum set
        waitFor(this.timeout, () -> {
            List<Operation> nodeGroupOps = getNodeGroupOps(nodeGroupPath);
            List<NodeGroupState> responses = sender.sendAndWait(nodeGroupOps, NodeGroupState.class);
            Set<Integer> quorums = responses.stream()
                    .flatMap(state -> state.nodes.values().stream())
                    .map(nodeState -> nodeState.membershipQuorum)
                    .distinct()
                    .collect(toSet());

            return quorums.size() == 1 && quorums.contains(quorum);
        }, () -> "Failed to set quorum to = " + quorum);

    }

    private List<Operation> getNodeGroupOps(String nodeGroupPath) {
        return this.hosts.stream()
                .map(host -> UriUtils.buildUri(host.getUri(), nodeGroupPath))
                .map(Operation::createGet)
                .collect(toList());
    }

    /**
     * wait until cluster is ready
     *
     * "/core/node-group/{group}"
     *   -  check "membershipUpdateTimeMicros" in all nodes are same
     *   -  check number of healthy nodes(NodeStatus==AVAILABLE)
     *
     * "/core/node-group/{group}/stats"
     *   - check "isAvailable"
     *
     * @see VerificationHost#waitForNodeGroupConvergence
     * @see VerificationHost#waitForNodeGroupIsAvailableConvergence
     */
    public void waitForConvergence() {

        TestRequestSender sender = getTestRequestSender();
        String nodeGroupPath = getNodeGroupPath();

        // check membershipUpdateTimeMicros & status of each node for the node-group
        waitFor(this.timeout, () -> {
            List<Operation> nodeGroupOps = getNodeGroupOps(nodeGroupPath);
            List<NodeGroupState> responses = sender.sendAndWait(nodeGroupOps, NodeGroupState.class);
            long updateTimeCount = responses.stream()
                    .map(state -> state.membershipUpdateTimeMicros)
                    .distinct()
                    .count();

            if (updateTimeCount != 1) {
                getHost().log(Level.INFO, "membershipUpdateTimeMicros has not converged yet");
                return false;
            }

            List<NodeStatus> nodeStats = responses.stream()
                    .flatMap(state -> state.nodes.values().stream())
                    .map(nodeState -> nodeState.status)
                    .collect(toList());

            // all should be AVAILABLE
            Set<NodeStatus> statuses = new HashSet<>(nodeStats);
            if (statuses.size() != 1 || !statuses.contains(NodeStatus.AVAILABLE)) {
                getHost().log(Level.INFO, "all nodes are not AVAILABLE");
                return false;
            }

            // check healthy node count
            int expectedHealthyNodeCounts = this.hosts.size() * this.hosts.size();
            if (expectedHealthyNodeCounts != nodeStats.size()) {
                getHost().log(Level.INFO, "healthy node count: expected=%d, actual=%d",
                        expectedHealthyNodeCounts, nodeStats.size());
                return false;
            }

            getHost().log(Level.INFO, "membershipUpdateTimeMicros & status are converged");
            return true;

        }, "membershipUpdateTimeMicros did not converge.");

        // check "/core/node-group/{group}/stats" "isAvailable"
        waitFor(this.timeout, () -> {
            List<Operation> statsOps = this.hosts.stream()
                    .map(host -> UriUtils.buildStatsUri(host, nodeGroupPath))
                    .map(Operation::createGet)
                    .collect(toList());

            List<ServiceStats> responses = sender.sendAndWait(statsOps, ServiceStats.class);
            boolean isStatAvailable = responses.stream()
                    .map(this::isServiceStatAvailable)
                    .allMatch(Boolean::booleanValue);

            if (!isStatAvailable) {
                getHost().log(Level.INFO, "Not all %s stat is available", nodeGroupPath);
            }
            return isStatAvailable;
        }, () -> String.format("node-group stats indicate %s is not available.", nodeGroupPath));

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
    public void waitForFactoryServiceAvailable(String factoryServicePath) {

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

        }, () -> String.format("%s factory service didn't become avaialble.", factoryServicePath));

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
