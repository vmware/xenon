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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.vmware.xenon.common.FNVHash;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.XenonConfiguration;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupService.UpdateQuorumRequest;

/**
 * This service logically partitions a 'key-value' namespace into units, called
 * "shards", where each unit (shard) is mapped to a collection of nodes.
 *
 * It is used by a NodeSelector with a non-null replicationFactor to allocate
 * shards (choosing the subset of nodes from the nodegroup that a shard will
 * consist of), so that it can choose an owner and/or replication targets from
 * a given shard's nodes.
 *
 * A shard is a replication boundary - a replicated update is propagated to
 * each node member of the shard.
 *
 * A shard is also an owner selection boundary - an owner of a document is
 * selected from the shard's node members.
 */
public class ShardsManagementService extends StatefulService {
    public static final String FACTORY_LINK = ServiceUriPaths.SHARDS_MANAGEMENT_FACTORY;

    /**
     * Default maximum number of shards in a single nodegroup.
     * The actual maximum can be overridden using configuration, however:
     * It must remain constant for a given deployment.
     */
    public static final int DEFAULT_MAX_SHARDS = 100;

    /**
     * Calculates a shard id from the specified keyValue.
     * Intended to be used by both this service and by clients that cache shard info.
     */
    public static String getShardIdFromKeyValue(String shardKeyValue, boolean allowShardSharing,
            int maxShards) {
        if (!allowShardSharing) {
            return shardKeyValue;
        }

        int hash = Math.abs(FNVHash.compute32(shardKeyValue));
        int shardId = hash % maxShards;

        return String.valueOf(shardId);
    }

    public static class ShardInfo {
        public ShardInfo(String shardId, Set<String> shardNodes) {
            this.shardId = shardId;
            this.shardNodes = shardNodes;
        }

        public String shardId;
        public Set<String> shardNodes;
    }

    public static class ShardsManagementServiceState extends ServiceDocument {
        /**
         * Marks whether the service has been initialized
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public boolean initialized;

        /**
         * The nodegroup that this service is relevant for.
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public String nodeGroupLink;

        /**
         * Determines how many nodes need to receive a copy of a change.
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public int replicationFactor;

        /**
         * Maximum number of shards that can be created.
         * Cannot change post-deployment.
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public int maxShards;

        /**
         * Controls whether different key-values can map to the same shard, and whether
         * different shards can share hosts (these might be separated in the future).
         * <code>true</code> by default, which allows mapping different key-values to
         * the same shard, and allows different shards to share hosts. If set to <code>
         * false</code>, each key-value will map to its own shard and there will be no
         * host sharing among shards.
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public boolean allowShardsSharing;

        /**
         * Maps shard ids to shard info
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public Map<String, ShardInfo> shardIdToInfoMap;

        /**
         * Maps shard count to node ids, to assist in quick determination of least-loaded
         * (fewest number of shards) nodes during shard's nodes allocation.
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public SortedMap<Integer, Set<String>> shardCountToNodeIdsMap;

        /**
         * The nodegroup's node ids.
         */
        @PropertyOptions(indexing = ServiceDocumentDescription.PropertyIndexingOption.STORE_ONLY)
        public Set<String> nodeIds;
    }

    public static class CreateOrGetShardInfoRequest {
        public static final String KIND = Utils.buildKind(CreateOrGetShardInfoRequest.class);

        public CreateOrGetShardInfoRequest(String shardKeyValue) {
            this.kind = KIND;
            this.shardKeyValue = shardKeyValue;
        }

        public String kind;
        public String shardKeyValue;
    }

    public static class ShardInfoResponse {
        public ShardInfoResponse(String shardKeyValue, ShardInfo shardInfo) {
            this.shardKeyValue = shardKeyValue;
            this.shardInfo = shardInfo;
        }

        public String shardKeyValue;
        public ShardInfo shardInfo;
    }

    public static class AddNodesRequest {
        public static final String KIND = Utils.buildKind(AddNodesRequest.class);

        public AddNodesRequest(Set<String> nodes) {
            this.kind = KIND;
            this.nodes = nodes;
        }

        public String kind;
        public Set<String> nodes;
    }

    public static class AddNodesResponse {
        // deliberately empty for performance reasons
    }

    public static Service createFactory() {
        return FactoryService.createIdempotent(ShardsManagementService.class);
    }

    private NodeGroupState cachedNodeGroupState;
    private Object cachedNodeGroupStateLock = new Object();

    public ShardsManagementService() {
        super(ShardsManagementServiceState.class);
        toggleOption(ServiceOption.CORE, true);
        toggleOption(ServiceOption.PERSISTENCE, true);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        init(startPost);
    }

    @Override
    public void handlePut(Operation put) {
        if (!put.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
            Operation.failActionNotSupported(put);
            return;
        }

        init(put);
    }

    private void init(Operation op) {
        ShardsManagementServiceState body = op.hasBody() ? op.getBody(ShardsManagementServiceState.class) : null;
        if (!validateState(op, body)) {
            return;
        }

        // TODO: use Long.MAX_VALUE once overflow bug in ServiceResourceTracker is fixed
        setCacheClearDelayMicros(TimeUnit.DAYS.toMicros(365 * 10));

        // we initialize once, whether through a POST or a PUT
        ShardsManagementServiceState state = getState(op);
        if (state == null || !state.initialized) {
            if (state == null) {
                state = new ShardsManagementServiceState();
            }
            state.initialized = true;

            // nodeGroupLink and replicationFactor are provided by the client
            state.nodeGroupLink = body.nodeGroupLink;
            state.replicationFactor = body.replicationFactor;

            // the rest of the fields are self-calculated
            state.maxShards = getMaxShards();
            state.allowShardsSharing = getAllowShardSharing();
            state.shardIdToInfoMap = new HashMap<>();
            state.shardCountToNodeIdsMap = new TreeMap<>();
            state.nodeIds = new HashSet<>();
        }

        op.setBody(state);

        // subscribe to nodegroup changes and get nodegroup state
        AtomicInteger remaining = new AtomicInteger(2);
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
                UriUtils.buildSubscriptionUri(getHost(), state.nodeGroupLink))
                .setCompletion(h)
                .setReferer(getUri());
        getHost().startSubscriptionService(subscribeToNodeGroup, handleNodeGroupNotification());
        sendRequest(Operation.createGet(this, state.nodeGroupLink).setCompletion(
                (o, e) -> {
                    if (e == null) {
                        NodeGroupState ngs = o.getBody(NodeGroupState.class);
                        updateCachedNodeGroupState(ngs);
                    } else if (!getHost().isStopping()) {
                        logSevere(e);
                    }
                    h.handle(o, e);
                }));
    }

    private boolean validateState(Operation op, ShardsManagementServiceState state) {
        String errMsg = null;
        if (state == null) {
            errMsg = "expecting non-empty state";
        } else if (state.nodeGroupLink == null || state.nodeGroupLink.isEmpty()) {
            errMsg = "expecting non-empty nodeGroupLink";
        } else if (state.replicationFactor <= 0) {
            errMsg = "expecting a positive replication factor";
        }

        if (errMsg != null) {
            op.fail(new IllegalArgumentException(errMsg));
            return false;
        }

        return true;
    }

    @Override
    public void handlePatch(Operation patch) {
        ShardsManagementServiceState currentState = getState(patch);

        CreateOrGetShardInfoRequest createOrGetShardInfoRequestBody =
                patch.getBody(CreateOrGetShardInfoRequest.class);
        if (CreateOrGetShardInfoRequest.KIND.equals(createOrGetShardInfoRequestBody.kind)) {
            handleCreateOrGetShardInfo(patch, createOrGetShardInfoRequestBody, currentState);
            return;
        }

        AddNodesRequest addNodesRequestBody = patch.getBody(AddNodesRequest.class);
        if (AddNodesRequest.KIND.equals(addNodesRequestBody.kind)) {
            handleAddNodes(patch, addNodesRequestBody, currentState);
            return;
        }

        String errorMsg = "unsupported request kind";
        patch.fail(Operation.STATUS_CODE_BAD_REQUEST, new IllegalArgumentException(errorMsg), errorMsg);
    }

    private void handleCreateOrGetShardInfo(Operation patch, CreateOrGetShardInfoRequest body,
            ShardsManagementServiceState currentState) {
        String shardKeyValue = body.shardKeyValue;
        String shardId = getShardIdFromKeyValue(shardKeyValue, currentState.allowShardsSharing,
                currentState.maxShards);
        ShardInfo shardInfo = currentState.shardIdToInfoMap.get(shardId);

        if (shardInfo != null) {
            ShardInfoResponse responseBody = new ShardInfoResponse(shardKeyValue, shardInfo);
            patch.setBody(responseBody);
            patch.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE);
            patch.complete();
            return;
        }

        shardInfo = allocateNodesForNewShard(shardId, currentState);
        if (shardInfo == null) {
            patch.fail(new IllegalStateException("Failed to allocate nodes for new shard"));
            return;
        }

        ShardInfoResponse responseBody = new ShardInfoResponse(shardKeyValue, shardInfo);
        patch.setBody(responseBody);
        setState(patch, currentState);
        patch.complete();
    }

    private void handleAddNodes(Operation patch, AddNodesRequest body, ShardsManagementServiceState currentState) {
        Set<String> nodesWithZeroShards = currentState.shardCountToNodeIdsMap.computeIfAbsent(
                0, k -> new HashSet<>());

        for (String nodeId : body.nodes) {
            boolean added = currentState.nodeIds.add(nodeId);
            if (added) {
                nodesWithZeroShards.add(nodeId);
            }
        }

        AddNodesResponse responseBody = new AddNodesResponse();
        patch.setBody(responseBody);
        setState(patch, currentState);
        patch.complete();
    }

    private ShardInfo allocateNodesForNewShard(String shardId, ShardsManagementServiceState currentState) {
        int nodesRequired = currentState.replicationFactor;
        boolean allowShardSharing = currentState.allowShardsSharing;
        List<String> allocatedNodes = new ArrayList<>(nodesRequired);
        List<Integer> allocatedNodesShardCounts = new ArrayList<>(nodesRequired);

        // we try to allocate nodes with minimum number of existing shards
        SEARCH_NODES:
        for (Entry<Integer, Set<String>> e : currentState.shardCountToNodeIdsMap.entrySet()) {
            Integer shardCount = e.getKey();

            if (!allowShardSharing && shardCount > 0) {
                // nodes in this entry already host shards and the policy states no
                // shard sharing - break
                break SEARCH_NODES;
            }

            for (String nodeId : e.getValue()) {
                NodeState nodeState = this.cachedNodeGroupState.nodes.get(nodeId);
                if (nodeState == null || NodeState.isUnAvailable(nodeState)) {
                    // we only allocate available nodes
                    continue;
                }

                allocatedNodes.add(nodeId);
                allocatedNodesShardCounts.add(shardCount);
                if (allocatedNodes.size() == nodesRequired) {
                    break SEARCH_NODES;
                }
            }
        }

        if (allocatedNodes.size() < nodesRequired) {
            // failed to allocate enough nodes
            return null;
        }

        // successfully allocated nodes for new shard - update state maps
        ShardInfo shardInfo = new ShardInfo(shardId, new HashSet<>(allocatedNodes));
        currentState.shardIdToInfoMap.put(shardId, shardInfo);
        for (int i = 0; i < allocatedNodes.size(); i++) {
            String nodeId = allocatedNodes.get(i);
            Integer prevShardCount = allocatedNodesShardCounts.get(i);

            Set<String> nodesPerPrevShardCount = currentState.shardCountToNodeIdsMap.get(prevShardCount);
            nodesPerPrevShardCount.remove(nodeId);
            if (nodesPerPrevShardCount.isEmpty()) {
                currentState.shardCountToNodeIdsMap.remove(prevShardCount);
            }

            Integer nextShardCount = Integer.valueOf(prevShardCount + 1);
            Set<String> nodesPerNextShardCount = currentState.shardCountToNodeIdsMap.computeIfAbsent(
                    nextShardCount, k -> new HashSet<>());
            nodesPerNextShardCount.add(nodeId);
        }

        return shardInfo;
    }

    private Consumer<Operation> handleNodeGroupNotification() {
        return (notifyOp) -> {
            notifyOp.complete();
            if (notifyOp.getAction() == Action.PATCH) {
                UpdateQuorumRequest bd = notifyOp.getBody(UpdateQuorumRequest.class);
                if (UpdateQuorumRequest.KIND.equals(bd.kind)) {
                    return;
                }
            } else if (notifyOp.getAction() != Action.POST) {
                return;
            }

            NodeGroupState ngs = notifyOp.getBody(NodeGroupState.class);
            if (ngs.nodes == null || ngs.nodes.isEmpty()) {
                return;
            }
            updateCachedNodeGroupState(ngs);
        };
    }

    private void updateCachedNodeGroupState(NodeGroupState ngs) {
        boolean updated = false;

        synchronized (this.cachedNodeGroupStateLock) {
            if (this.cachedNodeGroupState == null ||
                    this.cachedNodeGroupState.documentUpdateTimeMicros <= ngs.documentUpdateTimeMicros) {
                this.cachedNodeGroupState = ngs;
                updated = true;
            }
        }

        if (!updated) {
            return;
        }

        // send a self-patch with nodegroup nodes - the receiving side will determine
        // whether each node is a new one
        Set<String> nodes = new HashSet<>(ngs.nodes.keySet());
        AddNodesRequest addNodesRequest = new AddNodesRequest(nodes);
        Operation selfPatch = Operation.createPatch(getUri())
                .setBody(addNodesRequest);
        sendRequest(selfPatch);
    }

    private int getMaxShards() {
        return XenonConfiguration.integer(
                ShardsManagementService.class,
                "MAX_SHARDS",
                DEFAULT_MAX_SHARDS);
    }

    private boolean getAllowShardSharing() {
        return XenonConfiguration.bool(
                ShardsManagementService.class,
                "ALLOW_SHARD_SHARING",
                true);
    }
}