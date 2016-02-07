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

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest.ForwardingOption;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

/**
 * Uses consistent hashing to assign a client specified key to one
 * of the nodes in the node group. This service is associated with a specific node group
 */
public class ConsistentHashingNodeSelectorService extends StatelessService implements
        NodeSelectorService {

    public static final String STAT_NAME_OP_DELAY_MEMBERSHIP_UNSTABLE_COUNT = "opDelayDueToMembershipUnstableCount";
    public static final String STAT_NAME_SYNCHRONIZATION_COUNT = "synchronizationCount";

    private ConcurrentSkipListMap<String, byte[]> hashedNodeIds = new ConcurrentSkipListMap<>();
    private ConcurrentLinkedQueue<SelectAndForwardRequest> pendingRequests = new ConcurrentLinkedQueue<>();

    // Cached node group state. Refreshed during maintenance
    private NodeGroupState cachedGroupState;

    // Cached initial state. This service has "soft" state: Its configured on start and then its state is immutable.
    // If the service host restarts, all state is lost, by design.
    // Note: This is not a recommended pattern! Regular services must not use instanced fields
    private NodeSelectorState cachedState;

    private NodeSelectorReplicationService replicationUtility;

    private volatile boolean isSynchronizationRequired;
    private boolean isNodeGroupConverged;

    public ConsistentHashingNodeSelectorService() {
        super(NodeSelectorState.class);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    @Override
    public void handleStart(Operation start) {
        NodeSelectorState state = null;
        if (!start.hasBody()) {
            state = new NodeSelectorState();
            state.nodeGroupLink = ServiceUriPaths.DEFAULT_NODE_GROUP;
        } else {
            state = start.getBody(NodeSelectorState.class);
        }

        state.documentSelfLink = getSelfLink();
        state.documentKind = Utils.buildKind(NodeSelectorState.class);
        state.documentOwner = getHost().getId();
        this.cachedState = state;
        this.replicationUtility = new NodeSelectorReplicationService(this);
        startHelperServices(start);
    }

    private void startHelperServices(Operation op) {
        AtomicInteger remaining = new AtomicInteger(4);
        CompletionHandler h = (o, e) -> {
            if (e != null) {
                op.fail(e);
                return;
            }
            if (remaining.decrementAndGet() != 0) {
                return;
            }
            op.complete();
        };

        Operation subscribeToNodeGroup = Operation.createPost(
                UriUtils.buildSubscriptionUri(getHost(), this.cachedState.nodeGroupLink))
                .setCompletion(h)
                .setReferer(getUri());
        getHost().startSubscriptionService(subscribeToNodeGroup, handleNodeGroupNotification());

        // we subscribe to avoid GETs on node group state, per operation, but we need to have the initial
        // node group state, before service is available.
        sendRequest(Operation.createGet(this, this.cachedState.nodeGroupLink).setCompletion(
                (o, e) -> {
                    if (e == null) {
                        this.cachedGroupState = o.getBody(NodeGroupState.class);
                    } else {
                        logSevere(e);
                    }
                    h.handle(o, e);
                }));

        Operation startSynchPost = Operation.createPost(
                UriUtils.extendUri(getUri(), ServiceUriPaths.SERVICE_URI_SUFFIX_SYNCHRONIZATION))
                .setCompletion(h);
        Operation startForwardingPost = Operation.createPost(
                UriUtils.extendUri(getUri(), ServiceUriPaths.SERVICE_URI_SUFFIX_FORWARDING))
                .setCompletion(h);

        getHost().startService(startSynchPost, new NodeSelectorSynchronizationService(this));
        getHost().startService(startForwardingPost, new NodeSelectorForwardingService(this));
    }

    private Consumer<Operation> handleNodeGroupNotification() {
        return (notifyOp) -> {
            notifyOp.complete();
            if (notifyOp.getAction() != Action.PATCH) {
                return;
            }

            NodeGroupState ngs = notifyOp.getBody(NodeGroupState.class);
            if (ngs.nodes == null || ngs.nodes.isEmpty()) {
                return;
            }

            NodeGroupState previous = this.cachedGroupState;
            this.cachedGroupState = ngs;
            if (previous == null) {
                return;
            }
            NodeState pSelf = previous.nodes.get(getHost().getId());
            NodeState nSelf = ngs.nodes.get(getHost().getId());
            if (nSelf.membershipQuorum != pSelf.membershipQuorum) {
                logInfo("Quorum changed, before: %d, after:%d", pSelf.membershipQuorum,
                        nSelf.membershipQuorum);
            }

            this.isNodeGroupConverged = false;
            this.isSynchronizationRequired = true;
        };
    }

    @Override
    public void handleRequest(Operation op) {
        if (op.getAction() == Action.GET) {
            op.setBody(this.cachedState).complete();
            return;
        }

        if (op.getAction() == Action.DELETE) {
            super.handleRequest(op);
            return;
        }

        if (op.getAction() != Action.POST) {
            getHost().failRequestActionNotSupported(op);
            return;
        }

        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        SelectAndForwardRequest body = op.getBody(SelectAndForwardRequest.class);
        if (body.key == null && body.targetPath == null) {
            op.fail(new IllegalArgumentException("key or targetPath is required"));
            return;
        }

        selectAndForward(op, body);
    }

    /**
     *  Infrastructure use only. Called by service host to determine the node group this selector is
     *  associated with.
     *
     *  If selectors become indexed services, this will need to be removed and the
     *  service host should do a asynchronous query or GET to retrieve the selector state. Since this
     *  is not an API a service author can call (they have no access to this instance), the change will
     *  be transparent to DCP users.
     */
    public String getNodeGroup() {
        return this.cachedState.nodeGroupLink;
    }

    /**
     *  Infrastructure use only
     */
    public void selectAndForward(Operation op, SelectAndForwardRequest body) {
        selectAndForward(body, op, this.cachedGroupState, null);
    }

    /**
     * Uses the squared difference between the key and the server id of each member node to select a
     * node. Both the key and the nodes are hashed
     */
    private void selectAndForward(SelectAndForwardRequest body, Operation op,
            NodeGroupState localState,
            MessageDigest digest) {

        String keyValue = body.key != null ? body.key : body.targetPath;
        SelectOwnerResponse response = new SelectOwnerResponse();
        response.key = keyValue;
        body.associatedOp = op;

        if (queueRequestIfMembershipInFlux(localState, body)) {
            return;
        }

        if (this.cachedState.replicationFactor == null && body.options != null
                && body.options.contains(ForwardingOption.BROADCAST)) {
            response.selectedNodes = localState.nodes.values();
            if (body.options.contains(ForwardingOption.REPLICATE)) {
                replicateRequest(op, body, response);
                return;
            }
            broadcast(op, body, response);
            return;
        }

        int quorum = localState.nodes.get(getHost().getId()).membershipQuorum;
        int availableNodes = localState.nodes.size();
        SortedMap<BigInteger, NodeState> closestNodes =
                selectNodes(op, keyValue, localState, digest, quorum, availableNodes);

        NodeState closest = closestNodes.get(closestNodes.firstKey());
        response.ownerNodeId = closest.id;
        response.isLocalHostOwner = response.ownerNodeId.equals(getHost().getId());
        response.ownerNodeReference = UriUtils.buildUri(closest.groupReference, "");
        response.selectedNodes = closestNodes.values();

        if (body.targetPath == null) {
            op.setBodyNoCloning(response).complete();
            return;
        }

        if (body.options != null && body.options.contains(ForwardingOption.BROADCAST)) {
            if (body.options.contains(ForwardingOption.REPLICATE)) {
                replicateRequest(op, body, response);
            } else {
                broadcast(op, body, response);
            }
            return;
        }

        // If targetPath != null, we need to forward the operation.
        URI remoteService = UriUtils.buildUri(response.ownerNodeReference.getScheme(),
                response.ownerNodeReference.getHost(),
                response.ownerNodeReference.getPort(),
                body.targetPath, body.targetQuery);

        Operation fwdOp = op.clone().setCompletion(
                (o, e) -> {
                    op.transferResponseHeadersFrom(o).setStatusCode(o.getStatusCode())
                            .setBodyNoCloning(o.getBodyRaw());
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    op.complete();
                });
        getHost().getClient().send(fwdOp.setUri(remoteService));
    }

    private SortedMap<BigInteger, NodeState> selectNodes(Operation op, String keyValue,
            NodeGroupState localState, MessageDigest digest, int quorum,
            int availableNodes) {
        SortedMap<BigInteger, NodeState> closestNodes = new TreeMap<>();
        Set<Integer> quorums = new HashSet<>();
        long neighbourCount = 1;
        if (this.cachedState.replicationFactor != null) {
            neighbourCount = this.cachedState.replicationFactor;
        }

        if (digest == null) {
            digest = Utils.createDigest();
        }

        byte[] key;
        try {
            key = digest.digest(keyValue.getBytes(Utils.CHARSET));
        } catch (UnsupportedEncodingException e) {
            op.fail(e);
            return null;
        }

        BigInteger keyInteger = new BigInteger(key);
        for (NodeState m : localState.nodes.values()) {
            if (NodeState.isUnAvailable(m)) {
                availableNodes--;
                continue;
            }

            quorum = Math.max(m.membershipQuorum, quorum);
            quorums.add(quorum);
            byte[] hashedNodeId;
            try {
                hashedNodeId = this.hashedNodeIds.get(m.id);
                if (hashedNodeId == null) {
                    digest.reset();
                    hashedNodeId = digest.digest(m.id.getBytes(Utils.CHARSET));
                    this.hashedNodeIds.put(m.id, hashedNodeId);
                }
            } catch (UnsupportedEncodingException e) {
                op.fail(e);
                return closestNodes;
            }
            BigInteger nodeIdInteger = new BigInteger(hashedNodeId);
            BigInteger distance = nodeIdInteger.subtract(keyInteger);
            distance = distance.multiply(distance);
            closestNodes.put(distance, m);
            if (closestNodes.size() > neighbourCount) {
                // keep sorted map with only the N closest neighbors to the key
                closestNodes.remove(closestNodes.lastKey());
            }
        }

        if (quorums.size() > 1) {
            op.fail(new IllegalStateException("Available nodes: "
                    + availableNodes + ", different quorums: " + quorums));
            return closestNodes;
        }

        if (availableNodes < quorum) {
            op.fail(new IllegalStateException("Available nodes: "
                    + availableNodes + ", quorum:" + quorum));
            return closestNodes;
        }

        return closestNodes;
    }

    private void broadcast(Operation op, SelectAndForwardRequest req,
            SelectOwnerResponse selectRsp) {

        Collection<NodeState> members = selectRsp.selectedNodes;
        AtomicInteger remaining = new AtomicInteger(members.size());
        NodeGroupBroadcastResponse rsp = new NodeGroupBroadcastResponse();

        if (remaining.get() == 0) {
            op.setBody(rsp).complete();
            return;
        }

        rsp.membershipQuorum = this.cachedGroupState.nodes.get(getHost().getId()).membershipQuorum;

        AtomicInteger availableNodeCount = new AtomicInteger();
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                ServiceErrorResponse errorRsp = Utils.toServiceErrorResponse(e);
                rsp.failures.put(o.getUri(), errorRsp);
            } else if (o != null && o.hasBody()) {
                rsp.jsonResponses.put(o.getUri(), Utils.toJson(o.getBodyRaw()));
            }

            if (remaining.decrementAndGet() != 0) {
                return;
            }
            rsp.nodeCount = this.cachedGroupState.nodes.size();
            rsp.availableNodeCount = availableNodeCount.get();
            op.setBodyNoCloning(rsp).complete();
        };

        for (NodeState m : members) {
            boolean skipNode = false;
            if (req.options.contains(ForwardingOption.EXCLUDE_ENTRY_NODE)
                    && m.id.equals(getHost().getId())) {
                skipNode = true;
            }

            skipNode = NodeState.isUnAvailable(m) | skipNode;

            if (skipNode) {
                c.handle(null, null);
                continue;
            }

            URI remoteService = UriUtils.buildUri(m.groupReference.getScheme(),
                    m.groupReference.getHost(),
                    m.groupReference.getPort(),
                    req.targetPath, req.targetQuery);

            // create a operation for the equivalent service instance on the
            // remote node
            Operation remoteOp = Operation.createPost(remoteService)
                    .transferRequestHeadersFrom(op)
                    .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
                    .setAction(op.getAction())
                    .setCompletion(c)
                    .setReferer(op.getReferer())
                    .setExpiration(op.getExpirationMicrosUtc())
                    .setBody(op.getBodyRaw());

            rsp.receivers.add(remoteService);
            rsp.selectedNodes.put(m.id, m.groupReference);
            availableNodeCount.incrementAndGet();
            getHost().sendRequest(remoteOp);
        }
    }

    private void replicateRequest(Operation op, SelectAndForwardRequest body,
            SelectOwnerResponse response) {
        if (this.cachedGroupState == null) {
            op.fail(null);
        }
        this.replicationUtility.replicateUpdate(this.cachedGroupState, op, body, response);
    }

    private boolean queueRequestIfMembershipInFlux(NodeGroupState localState,
            SelectAndForwardRequest body) {

        Operation op = body.associatedOp;
        if (op.getExpirationMicrosUtc() < Utils.getNowMicrosUtc()) {
            // operation has expired
            op.fail(new TimeoutException(String.format(
                    "Operation already expired, will not queue. Exp:%d, now:%d",
                    op.getExpirationMicrosUtc(), Utils.getNowMicrosUtc())));
            return true;
        }

        if (NodeGroupUtils.isMembershipSettled(getHost(), getHost()
                .getMaintenanceIntervalMicros(), localState)) {
            return false;
        }

        adjustStat(STAT_NAME_OP_DELAY_MEMBERSHIP_UNSTABLE_COUNT, 1);

        this.pendingRequests.add(body);
        return true;
    }

    /**
     * Invoked by parent during its maintenance interval
     *
     * @param maintOp
     * @param localState
     */
    public void handleMaintenance(Operation maintOp) {
        performPendingRequestMaintenance();
        checkAndScheduleSynchronization();
        maintOp.complete();
    }

    private void performPendingRequestMaintenance() {
        if (this.pendingRequests.isEmpty()) {
            return;
        }

        MessageDigest digest = Utils.createDigest();

        while (!this.pendingRequests.isEmpty()) {
            SelectAndForwardRequest req = this.pendingRequests.poll();
            if (req == null) {
                break;
            }
            if (getHost().isStopping()) {
                req.associatedOp.fail(new CancellationException());
                continue;
            }
            selectAndForward(req, req.associatedOp, this.cachedGroupState, digest);
        }

    }

    private void checkAndScheduleSynchronization() {
        if (getHost().isStopping()) {
            return;
        }

        if (!getHost().isPeerSynchronizationEnabled()
                || !this.isSynchronizationRequired) {
            return;
        }

        if (!NodeGroupUtils.isMembershipSettled(getHost(), getHost().getMaintenanceIntervalMicros(),
                this.cachedGroupState)) {
            return;
        }

        if (!this.isNodeGroupConverged) {
            checkConvergence();
            return;
        }

        this.isSynchronizationRequired = false;
        logInfo("Scheduling synchronization of replicated services due to node group change");
        adjustStat(STAT_NAME_SYNCHRONIZATION_COUNT, 1);
        getHost().scheduleNodeGroupChangeMaintenance(getSelfLink());
    }

    private void checkConvergence() {

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                logSevere(e);
                return;
            }

            NodeGroupState ngs = o.getBody(NodeGroupState.class);
            long membershipUpdate = ngs.membershipUpdateTimeMicros;
            this.cachedGroupState = ngs;
            Operation op = Operation.createPost(null)
                    .setReferer(getUri())
                    .setExpiration(Utils.getNowMicrosUtc() + getHost().getOperationTimeoutMicros());
            NodeGroupUtils
                    .checkConvergence(
                            getHost(),
                            ngs,
                            op.setCompletion((o1, e1) -> {
                                if (e1 != null) {
                                    logWarning("Failed convergence check, will retry: %s",
                                            e1.getMessage());
                                    return;
                                }

                                if (!NodeGroupUtils.hasSynchronizationQuorum(getHost(),
                                        this.cachedGroupState)) {
                                    logInfo("Synchronization quorum not met");
                                    return;
                                }

                                // if node group changed since we kicked of this check, we need to wait for
                                // newer convergence completions
                                this.isNodeGroupConverged = membershipUpdate == this.cachedGroupState.membershipUpdateTimeMicros;
                            }));
        };

        sendRequest(Operation.createGet(this, this.cachedState.nodeGroupLink).setCompletion(c));
    }

    @Override
    public Service getUtilityService(String uriPath) {
        if (uriPath.endsWith(ServiceHost.SERVICE_URI_SUFFIX_REPLICATION)) {
            // update utility with latest set of peers
            return this.replicationUtility;
        } else if (uriPath.endsWith(ServiceHost.SERVICE_URI_SUFFIX_STATS)) {
            return super.getUtilityService(uriPath);
        }
        return null;
    }
}
