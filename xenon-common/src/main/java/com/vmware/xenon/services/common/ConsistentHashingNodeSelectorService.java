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

import java.net.URI;
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.vmware.xenon.common.FNVHash;
import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest.ForwardingOption;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceConfigUpdateRequest;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupService.UpdateQuorumRequest;

/**
 * Uses consistent hashing to assign a client specified key to one
 * of the nodes in the node group. This service is associated with a specific node group
 */
public class ConsistentHashingNodeSelectorService extends StatelessService implements
        NodeSelectorService {

    private long operationQueueLimit = Service.OPERATION_QUEUE_DEFAULT_LIMIT;
    private AtomicLong pendingOperationCount = new AtomicLong();
    private ConcurrentLinkedQueue<SelectAndForwardRequest> pendingRequestQueue = new ConcurrentLinkedQueue<>();

    // Cached node group state. Refreshed during maintenance
    private NodeGroupState cachedGroupState;

    // Cached initial state. This service has "soft" state: Its configured on start and then its state is immutable.
    // If the service host restarts, all state is lost, by design.
    // Note: This is not a recommended pattern! Regular services must not use instanced fields
    private NodeSelectorState cachedState;

    private NodeSelectorReplicationService replicationUtility;

    private volatile boolean isSynchronizationRequired;
    private boolean isNodeGroupConverged;
    private int synchQuorumWarningCount;

    private static final class ClosestNNeighbours extends TreeMap<Long, NodeState> {
        private static final long serialVersionUID = 0L;

        private final int maxN;

        public ClosestNNeighbours(int maxN) {
            super(Long::compare);
            this.maxN = maxN;
        }

        @Override
        public NodeState put(Long key, NodeState value) {
            if (size() < this.maxN) {
                return super.put(key, value);
            } else {
                // only attempt to write if new key can displace one of the top N entries
                if (comparator().compare(key, this.lastKey()) <= 0) {
                    NodeState old = super.put(key, value);
                    if (old == null) {
                        // sth. was added, remove last
                        this.remove(this.lastKey());
                    }

                    return old;
                }

                return null;
            }
        }
    }

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

        getHost().getClient().setConnectionLimitPerTag(
                ServiceClient.CONNECTION_TAG_REPLICATION,
                NodeSelectorService.REPLICATION_TAG_CONNECTION_LIMIT);

        getHost().getClient().setConnectionLimitPerTag(
                ServiceClient.CONNECTION_TAG_FORWARDING,
                FORWARDING_TAG_CONNECTION_LIMIT);

        state.documentSelfLink = getSelfLink();
        state.documentKind = Utils.buildKind(NodeSelectorState.class);
        state.documentOwner = getHost().getId();
        this.cachedState = state;
        this.replicationUtility = new NodeSelectorReplicationService(this);
        startHelperServices(start);
    }

    private void startHelperServices(Operation op) {
        allocateUtilityService();

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
                        NodeGroupState ngs = o.getBody(NodeGroupState.class);
                        updateCachedNodeGroupState(ngs, null);
                    } else if (!getHost().isStopping()) {
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
            NodeGroupState ngs = null;
            if (notifyOp.getAction() == Action.PATCH) {
                UpdateQuorumRequest bd = notifyOp.getBody(UpdateQuorumRequest.class);
                if (UpdateQuorumRequest.KIND.equals(bd.kind)) {
                    updateCachedNodeGroupState(null, bd);
                    return;
                }
            } else if (notifyOp.getAction() != Action.POST) {
                return;
            }

            ngs = notifyOp.getBody(NodeGroupState.class);
            if (ngs.nodes == null || ngs.nodes.isEmpty()) {
                return;
            }
            updateCachedNodeGroupState(ngs, null);
        };
    }

    @Override
    public void authorizeRequest(Operation op) {
        if (op.getAction() != Action.POST && op.getAction() != Action.GET) {
            super.authorizeRequest(op);
            return;
        }

        // Authorize selection requests, they have no side effects other than CPU usage (and
        // back pressure can be used to throttle them). Forwarding requests will have
        // authorization applied on them as part of their target service processing
        op.complete();
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

    @Override
    public void handleConfigurationRequest(Operation op) {
        if (op.getAction() == Action.PATCH && op.hasBody()) {
            ServiceConfigUpdateRequest body = op.getBody(ServiceConfigUpdateRequest.class);
            if (body.operationQueueLimit != null) {
                this.operationQueueLimit = body.operationQueueLimit;
            }
        }
        if (op.getAction() == Action.GET) {
            ServiceConfiguration cfg = new ServiceConfiguration();
            cfg.options = getOptions();
            cfg.maintenanceIntervalMicros = getMaintenanceIntervalMicros();
            cfg.epoch = 0;
            cfg.operationQueueLimit = (int) this.operationQueueLimit;
            op.setBodyNoCloning(cfg).complete();
            return;
        }
        super.handleConfigurationRequest(op);
    }

    /**
     *  Infrastructure use only. Called by service host to determine the node group this selector is
     *  associated with.
     *
     *  If selectors become indexed services, this will need to be removed and the
     *  service host should do a asynchronous query or GET to retrieve the selector state. Since this
     *  is not an API a service author can call (they have no access to this instance), the change will
     *  be transparent to runtime users.
     */
    @Override
    public String getNodeGroupPath() {
        return this.cachedState.nodeGroupLink;
    }

    /**
     *  Infrastructure use only
     */
    @Override
    public void selectAndForward(Operation op, SelectAndForwardRequest body) {
        selectAndForward(body, op, this.cachedGroupState);
    }

    /**
     * Uses the squared difference between the key and the server id of each member node to select a
     * node. Both the key and the nodes are hashed
     */
    private void selectAndForward(SelectAndForwardRequest body, Operation op,
            NodeGroupState localState) {

        String keyValue = body.key != null ? body.key : body.targetPath;
        SelectOwnerResponse response = new SelectOwnerResponse();
        response.key = keyValue;
        body.associatedOp = op;

        if (queueRequestIfNodeGroupIsUnavailable(localState, body)) {
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

        // select nodes and update response
        selectNodes(op, response, localState);

        if (body.targetPath == null) {
            op.setBodyNoCloning(response).complete();
            return;
        }

        if (body.options != null && body.options.contains(ForwardingOption.BROADCAST)) {
            if (body.options.contains(ForwardingOption.REPLICATE)) {
                if (op.getAction() == Action.DELETE) {
                    response.selectedNodes = localState.nodes.values();
                }
                replicateRequest(op, body, response);
            } else {
                broadcast(op, body, response);
            }
            return;
        }

        // If targetPath != null, we need to forward the operation.
        URI remoteService = UriUtils.buildServiceUri(response.ownerNodeGroupReference.getScheme(),
                response.ownerNodeGroupReference.getHost(),
                response.ownerNodeGroupReference.getPort(),
                body.targetPath, body.targetQuery, null);

        Operation fwdOp = op.clone()
                .setCompletion(
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

    private void selectNodes(Operation op,
            SelectOwnerResponse response,
            NodeGroupState localState) {
        NodeState self = localState.nodes.get(getHost().getId());
        int quorum = this.cachedState.membershipQuorum;
        int availableNodes = localState.nodes.size();

        if (availableNodes == 1) {
            response.ownerNodeId = self.id;
            response.isLocalHostOwner = true;
            response.ownerNodeGroupReference = self.groupReference;
            response.selectedNodes = localState.nodes.values();
            response.membershipUpdateTimeMicros = localState.membershipUpdateTimeMicros;
            response.availableNodeCount = 1;
            return;
        }

        int neighbourCount = 1;
        if (this.cachedState.replicationFactor != null) {
            neighbourCount = this.cachedState.replicationFactor.intValue();
        }

        ClosestNNeighbours closestNodes = new ClosestNNeighbours(neighbourCount);

        long keyHash = FNVHash.compute(response.key);
        for (NodeState m : localState.nodes.values()) {
            if (NodeState.isUnAvailable(m)) {
                availableNodes--;
                continue;
            }

            response.availableNodeCount++;

            long distance = m.getNodeIdHash() - keyHash;
            distance *= distance;
            // We assume first key (smallest) will be one with closest distance. The hashing
            // function can return negative numbers however, so a distance of zero (closest) will
            // not be the first key. Take the absolute value to cover that case and create a logical
            // ring
            distance = Math.abs(distance);
            closestNodes.put(distance, m);
        }

        if (availableNodes < quorum) {
            op.fail(new IllegalStateException("Available nodes: " + availableNodes + ", quorum:" + quorum));
            return;
        }

        NodeState closest = closestNodes.firstEntry().getValue();
        response.ownerNodeId = closest.id;
        response.isLocalHostOwner = response.ownerNodeId.equals(getHost().getId());
        response.ownerNodeGroupReference = closest.groupReference;
        response.selectedNodes = closestNodes.values();
        response.membershipUpdateTimeMicros = localState.membershipUpdateTimeMicros;
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

        rsp.membershipQuorum = this.cachedState.membershipQuorum;

        AtomicInteger availableNodeCount = new AtomicInteger();
        CompletionHandler c = (o, e) -> {
            // add failure or success response to the appropriate, concurrent map
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
                    .transferRefererFrom(op)
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

    /**
     * Returns a value indicating whether request was queued. True means request is queued
     * and will be processed once the node group is available
     */
    private boolean queueRequestIfNodeGroupIsUnavailable(NodeGroupState localState,
            SelectAndForwardRequest body) {

        Operation op = body.associatedOp;
        if (getHost().isStopping()) {
            op.fail(new CancellationException("host is stopping"));
            return true;
        }

        if (op.getExpirationMicrosUtc() < Utils.getSystemNowMicrosUtc()) {
            // operation has expired
            op.fail(new CancellationException(String.format(
                    "Operation already expired, will not queue. Exp:%d, now:%d",
                    op.getExpirationMicrosUtc(), Utils.getSystemNowMicrosUtc())));
            return true;
        }

        if (!NodeSelectorState.isAvailable(this.cachedState)) {
            synchronized (this.cachedState) {
                NodeSelectorState.updateStatus(getHost(), localState, this.cachedState);
            }
        }

        if (NodeSelectorState.isAvailable(this.cachedState)) {
            return false;
        }

        // approximate check for queue limit (not atomic)
        if (this.operationQueueLimit <= this.pendingOperationCount.get()) {
            adjustStat(STAT_NAME_LIMIT_EXCEEDED_FAILED_REQUEST_COUNT, 1);
            this.getHost().failRequestLimitExceeded(op);
            return true;
        }

        adjustStat(STAT_NAME_QUEUED_REQUEST_COUNT, 1);

        body.associatedOp = null;
        body = Utils.clone(body);
        body.associatedOp = op;

        this.pendingOperationCount.incrementAndGet();
        this.pendingRequestQueue.add(body);
        return true;
    }

    /**
     * Invoked by parent during its maintenance interval
     *
     * @param maintOp
     */
    @Override
    public void handleMaintenance(Operation maintOp) {
        performPendingRequestMaintenance();
        if (checkAndScheduleSynchronization(this.cachedGroupState.membershipUpdateTimeMicros,
                maintOp)) {
            return;
        }
        maintOp.complete();
    }

    private void performPendingRequestMaintenance() {
        if (this.pendingRequestQueue.isEmpty()) {
            return;
        }

        while (!this.pendingRequestQueue.isEmpty()) {
            if (!NodeSelectorState.isAvailable(this.cachedState)) {
                // update status in case group state changed
                NodeSelectorState.updateStatus(getHost(), this.cachedGroupState, this.cachedState);
                // Optimization: if the node group is not ready do not evaluate each
                // request. We check for availability in the selectAndForward method as well.
                return;
            }

            SelectAndForwardRequest req = this.pendingRequestQueue.poll();
            if (req == null) {
                break;
            }

            this.pendingOperationCount.decrementAndGet();

            if (getHost().isStopping()) {
                req.associatedOp.fail(new CancellationException());
                continue;
            }
            selectAndForward(req, req.associatedOp, this.cachedGroupState);
        }

    }

    private boolean checkAndScheduleSynchronization(long membershipUpdateMicros,
            Operation maintOp) {
        if (getHost().isStopping()) {
            return false;
        }

        if (!this.isSynchronizationRequired) {
            // this boolean is set to false on notifications for node group changes
            return false;
        }

        if (!NodeGroupUtils.isMembershipSettled(getHost(), getHost().getMaintenanceIntervalMicros(),
                this.cachedGroupState)) {
            checkConvergence(membershipUpdateMicros, maintOp);
            return true;
        }

        if (!this.isNodeGroupConverged) {
            checkConvergence(membershipUpdateMicros, maintOp);
            return true;
        }

        if (!getHost().isPeerSynchronizationEnabled()
                || !this.isSynchronizationRequired) {
            return false;
        }

        this.isSynchronizationRequired = false;
        logInfo("Scheduling synchronization (%d nodes)", this.cachedGroupState.nodes.size());
        adjustStat(STAT_NAME_SYNCHRONIZATION_COUNT, 1);
        getHost().scheduleNodeGroupChangeMaintenance(getSelfLink());
        return false;
    }

    private void checkConvergence(long membershipUpdateMicros, Operation maintOp) {

        CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (!getHost().isStopping()) {
                    logSevere(e);
                }
                maintOp.complete();
                return;
            }

            final int quorumWarningsBeforeQuiet = 10;
            if (!o.hasBody()) {
                logWarning("Missing node group state");
                maintOp.complete();
                return;
            }
            NodeGroupState ngs = o.getBody(NodeGroupState.class);
            updateCachedNodeGroupState(ngs, null);
            Operation op = Operation.createPost(null)
                    .setReferer(getUri())
                    .setExpiration(
                            Utils.fromNowMicrosUtc(getHost().getOperationTimeoutMicros()));
            NodeGroupUtils
                    .checkConvergence(
                            getHost(),
                            ngs,
                            op.setCompletion((o1, e1) -> {
                                if (e1 != null) {
                                    logWarning("Failed convergence check, will retry: %s",
                                            e1.getMessage());
                                    maintOp.complete();
                                    return;
                                }

                                if (!NodeGroupUtils.hasMembershipQuorum(getHost(),
                                        this.cachedGroupState)) {
                                    if (this.synchQuorumWarningCount < quorumWarningsBeforeQuiet) {
                                        logWarning("Synchronization quorum not met");
                                    } else if (this.synchQuorumWarningCount == quorumWarningsBeforeQuiet) {
                                        logWarning("Synchronization quorum not met, warning will be silenced");
                                    }
                                    this.synchQuorumWarningCount++;
                                    maintOp.complete();
                                    return;
                                }

                                // if node group changed since we kicked of this check, we need to wait for
                                // newer convergence completions
                                synchronized (this.cachedState) {
                                    this.isNodeGroupConverged = membershipUpdateMicros == this.cachedGroupState.membershipUpdateTimeMicros;
                                    if (this.isNodeGroupConverged) {
                                        this.synchQuorumWarningCount = 0;
                                    }
                                }
                                maintOp.complete();
                            }));
        };

        sendRequest(Operation.createGet(this, this.cachedState.nodeGroupLink).setCompletion(c));
    }

    private void updateCachedNodeGroupState(NodeGroupState ngs, UpdateQuorumRequest quorumUpdate) {
        if (ngs != null) {
            NodeGroupState currentState = this.cachedGroupState;
            boolean isAvailable = NodeSelectorState.isAvailable(getHost(), ngs);
            boolean isCurrentlyAvailable = currentState != null
                    && NodeSelectorState.isAvailable(getHost(), currentState);
            boolean logMsg = isAvailable != isCurrentlyAvailable
                    || (currentState != null && currentState.nodes.size() != ngs.nodes.size());
            if (currentState != null && logMsg) {
                logInfo("Node count: %d, available: %s, update time: %d (%d)",
                        ngs.nodes.size(),
                        isAvailable,
                        ngs.membershipUpdateTimeMicros, ngs.localMembershipUpdateTimeMicros);
            }
        } else if (quorumUpdate.membershipQuorum != null) {
            logInfo("Quorum update: %d", quorumUpdate.membershipQuorum);
        }

        long now = Utils.getNowMicrosUtc();
        synchronized (this.cachedState) {
            this.cachedState.status = NodeSelectorState.Status.UNAVAILABLE;
            if (quorumUpdate != null) {
                this.cachedState.documentUpdateTimeMicros = now;
                if (quorumUpdate.membershipQuorum != null) {
                    this.cachedState.membershipQuorum = quorumUpdate.membershipQuorum;
                }
                if (this.cachedGroupState != null) {
                    if (quorumUpdate.membershipQuorum != null) {
                        this.cachedGroupState.nodes.get(
                                getHost().getId()).membershipQuorum = quorumUpdate.membershipQuorum;
                    }
                    if (quorumUpdate.locationQuorum != null) {
                        this.cachedGroupState.nodes.get(
                                getHost().getId()).locationQuorum = quorumUpdate.locationQuorum;
                    }
                }
                return;
            }

            if (this.cachedGroupState == null) {
                this.cachedGroupState = ngs;
            }

            if (this.cachedGroupState.documentUpdateTimeMicros <= ngs.documentUpdateTimeMicros) {
                NodeSelectorState.updateStatus(getHost(), ngs, this.cachedState);
                this.cachedState.documentUpdateTimeMicros = now;
                this.cachedState.membershipUpdateTimeMicros = ngs.membershipUpdateTimeMicros;
                this.cachedGroupState = ngs;
                // every time we update cached state, request convergence check
                this.isNodeGroupConverged = false;
                this.isSynchronizationRequired = true;
            } else {
                return;
            }
        }
    }

    @Override
    public Service getUtilityService(String uriPath) {
        if (uriPath.endsWith(ServiceHost.SERVICE_URI_SUFFIX_REPLICATION)) {
            // update utility with latest set of peers
            return this.replicationUtility;
        } else {
            return super.getUtilityService(uriPath);
        }
    }
}
