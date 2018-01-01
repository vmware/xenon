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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
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
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupChange;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupService.UpdateQuorumRequest;
import com.vmware.xenon.services.common.ShardsManagementService.CreateOrGetShardInfoRequest;
import com.vmware.xenon.services.common.ShardsManagementService.ShardInfo;
import com.vmware.xenon.services.common.ShardsManagementService.ShardInfoResponse;
import com.vmware.xenon.services.common.ShardsManagementService.ShardsManagementServiceState;

/**
 * Uses consistent hashing to assign a client specified key to one
 * of the nodes in the node group. This service is associated with a specific node group
 */
public class ConsistentHashingNodeSelectorService extends StatelessService implements
        NodeSelectorService {

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

    private long operationQueueLimit = Service.OPERATION_QUEUE_DEFAULT_LIMIT;
    private AtomicLong pendingOperationCount = new AtomicLong();
    private ConcurrentLinkedQueue<SelectAndForwardRequest> pendingRequestQueue = new ConcurrentLinkedQueue<>();

    // Cached node group state. Refreshed during maintenance
    private NodeGroupState cachedGroupState;

    // Cached initial state. This service has "soft" state: Its configured on start and then its state is immutable.
    // If the service host restarts, all state is lost, by design.
    // Note: This is not a recommended pattern! Regular services must not use instanced fields
    private NodeSelectorState cachedState;

    // Sharding-related data structures and properties are cached here for quick access.
    // The authorotative state is kept in ShardsManagementService.
    private Map<String, ShardInfo> cachedShardIdToInfoMap;
    private int maxShards;
    private boolean allowShardsSharing;
    private Object cachedShardsMapLock = new Object();

    private NodeSelectorReplicationService replicationUtility;

    private volatile boolean isSynchronizationRequired;
    private volatile boolean isSetFactoriesAvailabilityRequired;
    private boolean isNodeGroupConverged;
    private int synchQuorumWarningCount;

    public ConsistentHashingNodeSelectorService() {
        super(NodeSelectorState.class);
        super.toggleOption(ServiceOption.CORE, true);
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
                ServiceClient.CONNECTION_TAG_SYNCHRONIZATION,
                NodeSelectorService.SYNCHRONIZATION_TAG_CONNECTION_LIMIT);

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

        int remainingInitalCount = isShardingEnabled() ? 5 : 4;
        AtomicInteger remaining = new AtomicInteger(remainingInitalCount);
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

        if (isShardingEnabled()) {
            Operation startShardsManagementFactoryPost = Operation.createPost(
                    UriUtils.extendUri(getUri(), ShardsManagementService.FACTORY_LINK_SUFFIX))
                    .setCompletion(h);
            getHost().startService(startShardsManagementFactoryPost, ShardsManagementService.createFactory());
        }
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
            // this.cachedState might be stale if NodeSelector status is UNAVAILABLE.
            // Status gets actively updated if we are going from AVAILABLE TO UNAVAILABLE status.
            // But transition to AVAILABLE is lazy so we update it on GET request.
            if (!NodeSelectorState.isAvailable(this.cachedState)) {
                synchronized (this.cachedState) {
                    NodeSelectorState.updateStatus(getHost(), this.cachedGroupState, this.cachedState);
                }
            }
            op.setBody(this.cachedState).complete();
            return;
        }

        if (op.getAction() == Action.DELETE) {
            super.handleRequest(op);
            return;
        }

        // update to node selector state
        if (op.getAction() == Action.PATCH) {
            super.handleRequest(op);
            return;
        }

        if (op.getAction() != Action.POST) {
            Operation.failActionNotSupported(op);
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
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.fail(new IllegalArgumentException("Body is required"));
            return;
        }
        ServiceDocument s = patch.getBody(ServiceDocument.class);
        if (s.documentKind == null) {
            patch.fail(new IllegalArgumentException("Kind is required"));
            return;
        }
        if (UpdateReplicationQuorumRequest.KIND.equals(s.documentKind)) {
            updateReplicationQuorum(patch, patch.getBody(UpdateReplicationQuorumRequest.class));
            return;
        }
        patch.fail(new IllegalArgumentException("Unexpected request kind " + s.documentKind));
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
            ServiceConfiguration cfg = Utils.buildServiceConfig(new ServiceConfiguration(), this);
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
     * Infrastructure use only
     *
     * This method uses cached {@link NodeGroupState}; therefore, caller needs to make sure the
     * nodegroup state is stable before calling this method.
     */
    @Override
    public SelectOwnerResponse findOwnerNode(String path) {
        return selectNodes(path, this.cachedGroupState, isShardingEnabled(), null);
    }

    private void selectAndForward(SelectAndForwardRequest forwardRequest, Operation op, NodeGroupState localState) {

        String keyValue = forwardRequest.key != null ? forwardRequest.key : forwardRequest.targetPath;
        forwardRequest.associatedOp = op;

        if (queueRequestIfNodeGroupIsUnavailable(localState, forwardRequest)) {
            return;
        }

        boolean isShardingEnabled = isShardingEnabled();
        boolean replicateOrBroadcast = forwardRequest.options != null
                && forwardRequest.options.contains(ForwardingOption.BROADCAST);

        if (isShardingEnabled && !isExemptFromSharding(keyValue)) {
            String shardKeyValue = getShardKeyValue(op, keyValue);
            if (shardKeyValue == null) {
                op.fail(new IllegalStateException("could not find sharding key"));
                return;
            }

            getShardInfo(shardKeyValue, (shardInfo,  e) -> {
                if (e != null) {
                    op.fail(e);
                    return;
                }

                SelectOwnerResponse response = selectNodes(keyValue, localState, true, shardInfo.shardNodes);
                selectAndforward(forwardRequest, op, localState, replicateOrBroadcast, response);
            }, op.getExpirationMicrosUtc());

            return;
        }

        if (isCustomNodeSelector()) {
            // TODO: remove this block once custom node selectors are deprecated
            SelectOwnerResponse response = selectNodes(keyValue, localState, false, null);
            selectAndforward(forwardRequest, op, localState, replicateOrBroadcast, response);
            return;
        }

        if (replicateOrBroadcast) {
            SelectOwnerResponse response = new SelectOwnerResponse();
            response.key = keyValue;
            response.selectedNodes = localState.nodes.values();
            replicateOrBroadcast(forwardRequest, op, localState, response);
            return;
        }

        SelectOwnerResponse response = selectNodes(keyValue, localState, false, null);
        selectAndforward(forwardRequest, op, localState, replicateOrBroadcast, response);
    }

    private void selectAndforward(SelectAndForwardRequest forwardRequest, Operation op,
            NodeGroupState localState, boolean replicateOrBroadcast, SelectOwnerResponse response) {
        int quorum = this.cachedState.membershipQuorum;
        int availableNodeCount = response.availableNodeCount;
        if (availableNodeCount < quorum) {
            op.fail(new IllegalStateException("Available nodes: " + availableNodeCount + ", quorum:" + quorum));
            return;
        }

        if (forwardRequest.targetPath == null) {
            // this is a request for ownership selection only
            op.setBodyNoCloning(response).complete();
            return;
        }

        if (replicateOrBroadcast) {
            replicateOrBroadcast(forwardRequest, op, localState, response);
            return;
        }

        forward(forwardRequest, op, response);
    }

    private void replicateOrBroadcast(SelectAndForwardRequest forwardRequest, Operation op,
            NodeGroupState localState, SelectOwnerResponse response) {
        if (forwardRequest.options.contains(ForwardingOption.REPLICATE)) {
            if (this.cachedState.replicationFactor != null && op.getAction() == Action.DELETE) {
                response.selectedNodes = localState.nodes.values();
            }
            replicateRequest(op, forwardRequest, response);
        } else {
            broadcast(op, forwardRequest, response);
        }
    }

    private void forward(SelectAndForwardRequest forwardRequest, Operation op,
            SelectOwnerResponse response) {
        URI remoteService = UriUtils.buildServiceUri(response.ownerNodeGroupReference.getScheme(),
                response.ownerNodeGroupReference.getHost(),
                response.ownerNodeGroupReference.getPort(),
                forwardRequest.targetPath, forwardRequest.targetQuery, null);

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

    private SelectOwnerResponse selectNodes(String keyValue, NodeGroupState localState,
            boolean isShardingEnabled, Collection<String> candidateNodes) {
        NodeState self = localState.nodes.get(getHost().getId());
        SelectOwnerResponse response = new SelectOwnerResponse();
        response.key = keyValue;

        if (localState.nodes.size() == 1) {
            response.ownerNodeId = self.id;
            response.isLocalHostOwner = true;
            response.ownerNodeGroupReference = self.groupReference;
            response.selectedNodes = localState.nodes.values();
            response.membershipUpdateTimeMicros = localState.membershipUpdateTimeMicros;
            response.availableNodeCount = 1;
            return response;
        }

        int neighbourCount = 1;
        if (this.cachedState.replicationFactor != null) {
            neighbourCount = this.cachedState.replicationFactor.intValue();
        }

        ClosestNNeighbours closestNodes = new ClosestNNeighbours(neighbourCount);

        long keyHash = FNVHash.compute(response.key);
        Collection<String> nodeIds = isShardingEnabled ? candidateNodes : localState.nodes.keySet();
        for (String nodeId : nodeIds) {
            NodeState m = localState.nodes.get(nodeId);
            if (NodeState.isUnAvailable(m)) {
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

        NodeState closest = closestNodes.firstEntry().getValue();
        response.ownerNodeId = closest.id;
        response.isLocalHostOwner = response.ownerNodeId.equals(getHost().getId());
        response.ownerNodeGroupReference = closest.groupReference;
        response.selectedNodes = closestNodes.values();
        response.membershipUpdateTimeMicros = localState.membershipUpdateTimeMicros;

        return response;
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
                errorRsp.statusCode = o.getStatusCode();
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
            return;
        }
        this.replicationUtility.replicateUpdate(this.cachedGroupState, op, body, response, this.cachedState.replicationQuorum);
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
            Operation.failLimitExceeded(op,
                    ServiceErrorResponse.ERROR_CODE_SERVICE_QUEUE_LIMIT_EXCEEDED,
                    "pendingRequestQueue on " + getSelfLink());
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
                req.associatedOp.fail(new CancellationException("Host is stopping"));
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

        if (!this.isSynchronizationRequired && !this.isSetFactoriesAvailabilityRequired) {
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

        if (!getHost().isPeerSynchronizationEnabled()) {
            return false;
        }

        if (this.isSynchronizationRequired) {
            this.isSynchronizationRequired = false;
            logInfo("Scheduling synchronization (%d nodes)", this.cachedGroupState.nodes.size());
            adjustStat(STAT_NAME_SYNCHRONIZATION_COUNT, 1);
            getHost().scheduleNodeGroupChangeMaintenance(getSelfLink());
        }

        if (this.isSetFactoriesAvailabilityRequired) {
            this.isSetFactoriesAvailabilityRequired = false;
            logInfo("Setting factories availability on owner node");
            getHost().setFactoriesAvailabilityIfOwner(true);
        }

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
                // We skip synchronization in case of PEER_UNAVAILABLE because we will triggered synchronization
                // when that node will get EXPIRED. PEER_UNAVAILABLE indicates that the node just become unavailable
                // and will be expired after 5 minutes if it does not come back online within that time period.
                if (ngs.lastChanges != null &&
                        ngs.lastChanges.size() == 1 &&
                        (ngs.lastChanges.contains(NodeGroupChange.PEER_UNAVAILABLE) ||
                                ngs.lastChanges.contains(NodeGroupChange.PEER_EXPIRED))) {
                    this.isSynchronizationRequired = false;
                    if (ngs.lastChanges.contains(NodeGroupChange.PEER_EXPIRED)) {
                        // synchronization is not required, but we need to set factories' availability
                        // on hosts that own them because ownership has changed, and clients might
                        // dependent on up-to-date factory availability
                        this.isSetFactoriesAvailabilityRequired = true;
                    }
                }
            }
        }
    }

    @Override
    public void updateReplicationQuorum(Operation op, UpdateReplicationQuorumRequest r) {
        if (r.replicationQuorum == null) {
            op.fail(new IllegalArgumentException("replication quorum is required"));
            return;
        }
        int replicationQuorum = r.replicationQuorum;
        int replicationFactor = this.cachedState.replicationFactor != null ?
                this.cachedState.replicationFactor.intValue() : this.cachedGroupState.nodes.size();
        if (replicationQuorum > replicationFactor) {
            String errorMsg = String.format(
                    "replicationQuorum %d > replicationFactor %d", replicationQuorum, replicationFactor);
            op.fail(new IllegalArgumentException(errorMsg));
            return;
        }
        // broadcast
        logInfo("replicationQuorum update from %s to %d", this.cachedState.replicationQuorum,
                replicationQuorum);
        this.cachedState.replicationQuorum = replicationQuorum;
        if (!r.isGroupUpdate) {
            op.complete();
            return;
        }
        r.isGroupUpdate = false;
        AtomicInteger pending = new AtomicInteger(this.cachedGroupState.nodes.size());
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                // fail the original update request if one peer update failed
                op.fail(e);
                return;
            }
            int p = pending.decrementAndGet();
            if (p != 0) {
                return;
            }
            op.complete();
        };
        for (NodeState node : this.cachedGroupState.nodes.values()) {
            if (!NodeState.isAvailable(node, getHost().getId(), true)) {
                c.handle(null, null);
                continue;
            }
            URI peerNodeSelectorService = UriUtils.buildUri(node.groupReference.getScheme(),
                    node.groupReference.getHost(),
                    node.groupReference.getPort(),
                    getSelfLink(), null);
            Operation p = Operation
                    .createPatch(peerNodeSelectorService)
                    .setBody(r)
                    .setCompletion(c);
            sendRequest(p);
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

    private boolean isShardingEnabled() {
        // we limit sharding to the default node selector configured with a
        // non-null replication factor, for now
        return this.cachedState.replicationFactor != null &&
                !isCustomNodeSelector();
    }

    private boolean isCustomNodeSelector() {
        return getSelfLink().equals(ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR) ||
                getSelfLink().equals(ServiceUriPaths.DEFAULT_3X_NODE_SELECTOR);
    }

    private boolean isExemptFromSharding(String keyValue) {
        return keyValue.startsWith(ServiceUriPaths.CORE) &&
                !keyValue.startsWith(ExampleService.FACTORY_LINK);
    }

    private String getShardKeyValue(Operation op, String defaultKeyValue) {
        URI uri = op.getUri();

        // look for shard key-value in the URI query first
        String query = uri != null ? uri.getQuery() : null;
        if (query != null) {
            Map<String, String> params = UriUtils.parseUriQueryParams(query);
            String keyValue = params.get(Operation.SHARDING_KEY_QUERY_PARAM);
            if (keyValue != null && !keyValue.isEmpty()) {
                return keyValue;
            }
        }

        // TODO: look for shard key-value in a body field with PropertyUsageOption.SHARDING_KEY

        // If shard key-value was not found, fall back to default
        return defaultKeyValue;
    }

    private void getShardInfo(String shardKeyValue, BiConsumer<ShardInfo, Throwable> handler, long expiration) {
        Map<String, ShardInfo> shardIdToInfoMap;
        synchronized (this.cachedShardsMapLock) {
            shardIdToInfoMap = this.cachedShardIdToInfoMap;
        }

        if (shardIdToInfoMap == null) {
            // no local sharding info - initializing
            initShardingInfo(e -> {
                if (e != null) {
                    handler.accept(null, e);
                    return;
                }

                // now that local sharding info exists, we can safely invoke ourselves
                // recursively
                getShardInfo(shardKeyValue, handler, expiration);
            }, expiration);
            return;
        }

        String shardId = ShardsManagementService.getShardIdFromKeyValue(shardKeyValue,
                this.allowShardsSharing, this.maxShards);
        ShardInfo shardInfo = shardIdToInfoMap.get(shardId);
        if (shardInfo == null) {
            // shard info is missing from our cached map -
            // we will request the shard manager for the info, potentially creating
            // a new shard if it doesn't exist
            URI shardsManagementFactoryUri = UriUtils.extendUri(getUri(),
                    ShardsManagementService.FACTORY_LINK_SUFFIX);
            URI shardsManagerUri = UriUtils.extendUri(shardsManagementFactoryUri,
                    ShardsManagementService.CHILD_SELFLINK_SUFFIX);
            CreateOrGetShardInfoRequest createOrGetShardRequest = new CreateOrGetShardInfoRequest();
            createOrGetShardRequest.kind = CreateOrGetShardInfoRequest.KIND;
            createOrGetShardRequest.shardKeyValue = shardKeyValue;
            Operation patch = Operation.createPatch(shardsManagerUri)
                    .setExpiration(expiration)
                    .setBody(createOrGetShardRequest)
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            handler.accept(null, e);
                            return;
                        }

                        ShardInfoResponse res = o.getBody(ShardInfoResponse.class);
                        shardIdToInfoMap.put(shardId, res.shardInfo);
                        handler.accept(res.shardInfo, null);
                    });
            // TODO: add and use majority quorum here
            //patch.addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
            //        Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);
            sendRequest(patch);
            return;
        }

        handler.accept(shardInfo, null);
    }

    private void initShardingInfo(Consumer<Throwable> h, long expiration) {
        URI shardsManagementFactoryUri = UriUtils.extendUri(getUri(),
                ShardsManagementService.FACTORY_LINK_SUFFIX);

        ShardsManagementServiceState state = new ShardsManagementServiceState();
        state.documentSelfLink = ShardsManagementService.CHILD_SELFLINK_SUFFIX;
        state.nodeGroupLink = this.cachedState.nodeGroupLink;
        state.replicationFactor = this.cachedState.replicationFactor;
        Operation post = Operation.createPost(shardsManagementFactoryUri)
                .setExpiration(expiration)
                .setBody(state)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        h.accept(e);
                        return;
                    }

                    ShardsManagementServiceState res = o.getBody(ShardsManagementServiceState.class);
                    synchronized (this.cachedShardsMapLock) {
                        this.maxShards = res.maxShards;
                        this.allowShardsSharing = res.allowShardsSharing;
                        this.cachedShardIdToInfoMap = new ConcurrentSkipListMap<>();
                    }
                    h.accept(null);
                });
        // it's OK if multiple POST requests arrive at target service concurrently -
        // it has ServiceOption.IDEMPOTENT_POST and will initialize once
        sendRequest(post);
    }
}
