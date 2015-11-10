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

package com.vmware.dcp.services.common;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.NodeState.NodeOption;
import com.vmware.dcp.services.common.NodeState.NodeStatus;

/**
 * Service for maintaining a list of nodes through the use of a membership gossip layer. New nodes
 * are added to a group through POST
 */
public class NodeGroupService extends StatefulService {

    private static final String STAT_NAME_REFERER_SEGMENT = ".referer.";

    private enum NodeGroupChange {
        PEER_ADDED, PEER_STATUS_CHANGE, SELF_CHANGE
    }

    public static class CheckConvergenceRequest {
        public static final String KIND = Utils.buildKind(CheckConvergenceRequest.class);

        public long membershipUpdateTimeMicros;

        public static CheckConvergenceRequest create(long membershipUpdateTime) {
            CheckConvergenceRequest r = new CheckConvergenceRequest();
            r.membershipUpdateTimeMicros = membershipUpdateTime;
            r.kind = KIND;
            return r;
        }

        public String kind;
    }

    public static class CheckConvergenceResponse {
        public boolean isConverged;
    }

    public static class JoinPeerRequest {
        public static final String KIND = Utils.buildKind(JoinPeerRequest.class);

        public static JoinPeerRequest create(URI peerToJoin, Integer synchQuorum) {
            JoinPeerRequest r = new JoinPeerRequest();
            r.memberGroupReference = peerToJoin;
            r.synchQuorum = synchQuorum;
            r.kind = KIND;
            return r;
        }

        /**
         * Member of the group we wish to join through
         */
        public URI memberGroupReference;

        /**
         * Optional node join options. If specified the node state representing the local node
         * will be updated with these options. Further, these options determine join behavior.
         */
        public EnumSet<NodeOption> localNodeOptions;

        /**
         * Minimum number of nodes to enumeration, after join, for synchronization to start
         */
        public Integer synchQuorum;

        public String kind;
    }

    public static class UpdateQuorumRequest {
        public static final String KIND = Utils.buildKind(UpdateQuorumRequest.class);

        public static UpdateQuorumRequest create(boolean isGroupUpdate, int quorum) {
            UpdateQuorumRequest r = new UpdateQuorumRequest();
            r.isGroupUpdate = isGroupUpdate;
            r.membershipQuorum = quorum;
            r.kind = KIND;
            return r;
        }

        public boolean isGroupUpdate;
        public int membershipQuorum;
        public String kind;
    }

    public static class NodeGroupConfig {
        public static final long DEFAULT_NODE_REMOVAL_DELAY_MICROS = TimeUnit.HOURS.toMicros(1);
        public long nodeRemovalDelayMicros = DEFAULT_NODE_REMOVAL_DELAY_MICROS;

        /**
         * Number of maintenance intervals after last update to group membership before owner
         * selection and replication requests should be processed.
         */
        public long stableGroupMaintenanceIntervalCount = 5;
    }

    public static class NodeGroupState extends ServiceDocument {
        public NodeGroupConfig config;
        public Map<String, NodeState> nodes = new ConcurrentSkipListMap<>();
        public long membershipUpdateTimeMicros;
    }

    public static final int MIN_PEER_GOSSIP_COUNT = 10;

    public static final String STAT_NAME_RESTARTING_SERVICES_COUNT = "restartingServicesCount";
    public static final String STAT_NAME_RESTARTING_SERVICES_FAILURE_COUNT = "restartingServicesFailureCount";

    public NodeGroupService() {
        super(NodeGroupState.class);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        NodeGroupState initState = null;
        if (startPost.hasBody()) {
            initState = startPost.getBody(NodeGroupState.class);
        } else {
            initState = new NodeGroupState();
        }

        initState.documentOwner = getHost().getId();

        if (initState.config == null) {
            initState.config = new NodeGroupConfig();
        }

        NodeState self = initState.nodes.get(getHost().getId());
        self = buildLocalNodeState(self);

        if (!validateNodeOptions(startPost, self.options)) {
            return;
        }

        initState.nodes.put(self.id, self);
        startPost.setBody(initState).complete();
    }

    @Override
    public void handleGet(Operation get) {
        NodeGroupState state = getState(get);
        get.setBodyNoCloning(state).complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        NodeGroupState body = getBody(patch);
        if (body == null) {
            patch.fail(new IllegalArgumentException("body of type NodeGroupState is required"));
            return;
        }

        NodeGroupState localState = getState(patch);

        if (body.config == null && body.nodes.isEmpty()) {
            UpdateQuorumRequest bd = patch.getBody(UpdateQuorumRequest.class);
            if (UpdateQuorumRequest.KIND.equals(bd.kind)) {
                handleUpdateQuorumPatch(patch, localState);
                return;
            }
            patch.fail(new IllegalArgumentException("nodes or config are required"));
            return;
        }

        if (body.config != null && body.nodes.isEmpty()) {
            localState.config = body.config;
            patch.complete();
            return;
        }

        adjustStat(patch.getAction() + STAT_NAME_REFERER_SEGMENT + body.documentOwner, 1);
        EnumSet<NodeGroupChange> changes = EnumSet.noneOf(NodeGroupChange.class);
        mergeRemoteAndLocalMembership(
                localState,
                body,
                changes);
        patch.setNotificationDisabled(changes.isEmpty());

        localState.documentOwner = getHost().getId();
        NodeState localNodeState = localState.nodes.get(getHost().getId());
        localNodeState.groupReference = UriUtils.buildPublicUri(getHost(), getSelfLink());

        patch.setBody(localState).complete();

        if (localState.nodes.size() < Math.max(localNodeState.membershipQuorum,
                localNodeState.synchQuorum)) {
            return;
        }

        if (!NodeGroupUtils.isMembershipSettled(getHost(), getHost().getMaintenanceIntervalMicros(),
                localState)) {
            return;
        }

        if (localNodeState.status == NodeStatus.AVAILABLE) {
            return;
        }

        localNodeState.status = NodeStatus.AVAILABLE;
        this.sendAvailableSelfPatch(localNodeState);
    }

    private void handleUpdateQuorumPatch(Operation patch,
            NodeGroupState localState) {
        UpdateQuorumRequest bd = patch.getBody(UpdateQuorumRequest.class);
        NodeState self = localState.nodes.get(getHost().getId());
        logInfo("Updating self quorum from %d to %d, isGroupUpdate:%s",
                self.membershipQuorum, bd.membershipQuorum, bd.isGroupUpdate);

        self.membershipQuorum = bd.membershipQuorum;
        self.documentVersion++;
        self.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        localState.membershipUpdateTimeMicros = self.documentUpdateTimeMicros;

        if (!bd.isGroupUpdate) {
            patch.setBodyNoCloning(localState).complete();
            return;
        }

        // TODO use a three phase consensus algorithm to update quorum similar
        // to the steady state replication consensus.

        // Issue N requests to update quorum to all member of the group. If they
        // do not all succeed the request, then the operation fails and some peers
        // will be left with a quorum level different than the others. That is
        // acceptable. The replication logic, can reject a peer if its quorum level
        // is not set at the same level as the owner. The client of this request can
        // also retry...

        bd.isGroupUpdate = false;

        int failureThreshold = (localState.nodes.size() - 1) / 2;
        AtomicInteger pending = new AtomicInteger(localState.nodes.size());
        AtomicInteger failures = new AtomicInteger();
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                logWarning("Node %s failed quorum update: %s", o.getUri(), e.toString());
                failures.incrementAndGet();
            }

            int p = pending.decrementAndGet();
            if (p != 0) {
                return;
            }
            if (failures.get() > failureThreshold) {
                patch.fail(new IllegalStateException("Majority of nodes failed request"));
            } else {
                patch.setBodyNoCloning(localState).complete();
            }
        };

        for (NodeState node : localState.nodes.values()) {
            if (!NodeState.isAvailable(node, getHost().getId(), true)) {
                c.handle(null, null);
                continue;
            }
            node.membershipQuorum = bd.membershipQuorum;
            node.documentVersion++;
            node.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
            Operation p = Operation
                    .createPatch(node.groupReference)
                    .setBody(bd)
                    .setCompletion(c);
            sendRequest(p);
        }
    }

    /**
     * Handles a POST to either join this service to a group using a peer existing member
     * (JoinPeerRequest as the body) or add a new local monitor service to track the state of a
     * remote peer
     *
     * @param post
     */
    @Override
    public void handlePost(Operation post) {
        if (!post.hasBody()) {
            post.fail(new IllegalArgumentException("body is required"));
            return;
        }

        CheckConvergenceRequest cr = post.getBody(CheckConvergenceRequest.class);
        if (CheckConvergenceRequest.KIND.equals(cr.kind)) {
            handleCheckConvergencePost(post, cr);
            return;
        }

        JoinPeerRequest joinBody = post.getBody(JoinPeerRequest.class);
        if (joinBody != null && joinBody.memberGroupReference != null) {
            handleJoinPost(joinBody, post, getState(post), null);
            return;
        }

        NodeState body = post.getBody(NodeState.class);
        if (body.id == null) {
            post.fail(new IllegalArgumentException("id is required"));
            return;
        }

        adjustStat(post.getAction() + STAT_NAME_REFERER_SEGMENT + body.id, 1);

        boolean isLocalNode = body.id.equals(getHost().getId());

        if (body.groupReference == null) {
            post.fail(new IllegalArgumentException("groupReference is required"));
            return;
        }

        if (isLocalNode) {
            // this is a node instance representing us
            buildLocalNodeState(body);
        } else {
            body.documentSelfLink = UriUtils.buildUriPath(getSelfLink(), body.id);
        }

        NodeGroupState localState = getState(post);
        localState.nodes.put(body.id, body);

        post.setBody(body).complete();
    }

    private void handleCheckConvergencePost(Operation post, CheckConvergenceRequest body) {
        NodeGroupState localState = getState(post);
        CheckConvergenceResponse rsp = new CheckConvergenceResponse();
        rsp.isConverged = localState.membershipUpdateTimeMicros == body.membershipUpdateTimeMicros;
        post.setBody(rsp).complete();
        return;
    }

    private void handleJoinPost(JoinPeerRequest joinBody,
            Operation joinOp,
            NodeGroupState localState,
            NodeGroupState remotePeerState) {

        if (UriUtils.isHostEqual(getHost(), joinBody.memberGroupReference)) {
            logInfo("Skipping self join");
            // we tried joining ourself, abort;
            joinOp.complete();
            return;
        }

        NodeState self = localState.nodes.get(getHost().getId());

        if (joinOp != null) {
            self.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
            self.documentVersion++;

            // at a minimum we need 2 nodes to synch: self plus the node we are joining
            self.synchQuorum = 2;

            if (joinBody.synchQuorum != null) {
                self.synchQuorum = Math.max(self.synchQuorum, joinBody.synchQuorum);
            }

            if (joinBody.localNodeOptions != null) {
                if (!validateNodeOptions(joinOp, joinBody.localNodeOptions)) {
                    return;
                }
                self.options = joinBody.localNodeOptions;
            }

            localState.membershipUpdateTimeMicros = self.documentUpdateTimeMicros;

            // complete the join POST, continue with state merge
            joinOp.complete();
        }

        // this method is two pass
        // First pass get the remote peer state
        // Second pass, insert self

        if (remotePeerState == null) {
            // Pass 1, get existing member state
            sendRequest(Operation.createGet(joinBody.memberGroupReference)
                    .setCompletion(
                            (o, e) -> {
                                if (e != null) {
                                    logWarning("Failure getting peer %s state:%s", o.getUri(),
                                            e.toString());
                                    return;
                                }

                                NodeGroupState remoteState = getBody(o);
                                handleJoinPost(joinBody, null, localState, remoteState);
                            }));
            return;
        }

        // Pass 2, merge remote group state with ours, send self to peer
        sendRequest(Operation.createPatch(getUri()).setBody(remotePeerState));

        logInfo("Synch quorum: %d. Sending POST to insert self (%s) to peer %s",
                self.synchQuorum,
                self.groupReference,
                joinBody.memberGroupReference);

        Operation insertSelfToPeer = Operation
                .createPost(joinBody.memberGroupReference)
                .setBody(self)
                .setCompletion(
                        (o, e) -> {
                            if (e != null) {
                                logSevere("Insert POST to %s failed", o.getUri());
                                return;
                            }
                            // we will restart services to synchronize with peers on the next
                            // maintenance interval with a stable group membership
                    });
        sendRequest(insertSelfToPeer);
    }

    private boolean validateNodeOptions(Operation joinOp, EnumSet<NodeOption> options) {
        if (options.isEmpty()) {
            joinOp.fail(new IllegalArgumentException("at least one option must be specified"));
            return false;
        }

        if (options.contains(NodeOption.OBSERVER) && options.contains(NodeOption.PEER)) {
            joinOp.fail(new IllegalArgumentException(
                    String.format("%s and %s are mutually exclusive",
                            NodeOption.OBSERVER, NodeOption.PEER)));
            return false;
        }

        return true;
    }

    private void sendAvailableSelfPatch(NodeState local) {
        // mark self as available by issuing self PATCH
        NodeGroupState body = new NodeGroupState();
        body.config = null;
        body.documentOwner = getHost().getId();
        body.documentSelfLink = UriUtils.buildUriPath(
                getSelfLink(), body.documentOwner);
        local.status = NodeStatus.AVAILABLE;
        body.nodes.put(local.id, local);

        sendRequest(Operation.createPatch(getUri()).setBody(
                body));
    }

    private NodeState buildLocalNodeState(NodeState body) {
        if (body == null) {
            body = new NodeState();
        }
        body.id = getHost().getId();
        body.status = NodeStatus.SYNCHRONIZING;
        body.groupReference = UriUtils.buildPublicUri(getHost(), getSelfLink());
        body.documentSelfLink = UriUtils.buildUriPath(getSelfLink(), body.id);
        body.documentKind = Utils.buildKind(NodeState.class);
        body.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        return body;
    }

    @Override
    public void handleMaintenance(Operation op) {
        sendRequest(Operation.createGet(getUri())
                .setCompletion((o, e) -> performGroupMaintenance(op, o, e)));
    }

    private void performGroupMaintenance(Operation maint, Operation get, Throwable getEx) {
        // we ignore any body associated with the PUT

        if (getEx != null) {
            logWarning("Failure getting state: %s", getEx.toString());
            maint.complete();
            return;
        }

        if (!get.hasBody()) {
            maint.complete();
            return;
        }

        NodeGroupState localState = get.getBody(NodeGroupState.class);

        if (localState == null || localState.nodes == null) {
            maint.complete();
            return;
        }

        if (localState.nodes.size() <= 1) {
            maint.complete();
            return;
        }

        if (getHost().isStopping()) {
            maint.complete();
            return;
        }

        // probe a fixed, random selection of our peers, giving them our view of the group and
        // getting back theirs

        // probe log 10 of peers (exclude self)
        int peersToProbe = (int) Math.log10(localState.nodes.size() - 1);
        // probe at least N peers
        peersToProbe = Math.max(peersToProbe, MIN_PEER_GOSSIP_COUNT);
        // probe at most total number of peers
        peersToProbe = Math.min(localState.nodes.size() - 1, peersToProbe);

        AtomicInteger remaining = new AtomicInteger(peersToProbe);
        NodeState[] randomizedPeers = shuffleGroupMembers(localState);
        NodeState localNode = localState.nodes.get(getHost().getId());
        localNode.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
        localNode.groupReference = UriUtils.buildPublicUri(getHost(), getSelfLink());
        localState.documentOwner = getHost().getId();

        NodeGroupState patchBody = new NodeGroupState();
        patchBody.documentOwner = getHost().getId();
        patchBody.documentUpdateTimeMicros = Utils.getNowMicrosUtc();

        int probeCount = 0;
        for (NodeState peer : randomizedPeers) {
            if (peer == null) {
                continue;
            }

            if (peer.id.equals(getHost().getId())) {
                continue;
            }

            NodeState remotePeer = peer;
            URI peerUri = peer.groupReference;
            // send a gossip PATCH to the peer, with our state

            // perform a health check to N randomly selected peers
            // 1) We issue a PATCH to a peer, with the body set to our view of the group
            // 2a) if the peer is healthy, they will merge our state with theirs and return
            // the merged state in the response. We will then update our state and mark the
            // peer AVAILABLE. We just update peer node, we don't currently merge their state
            // 2b) if the PATCH failed, we mark the PEER it UNAVAILABLE

            CompletionHandler ch = (o, e) -> handleGossipPatchCompletion(maint, o, e, localState,
                    patchBody,
                    remaining, remotePeer);
            Operation patch = Operation
                    .createPatch(peerUri)
                    .setBody(localState)
                    .setRetryCount(0)
                    .setExpiration(
                            Utils.getNowMicrosUtc() + getHost().getOperationTimeoutMicros() / 2)
                    .forceRemote()
                    .setCompletion(ch);

            if (peer.groupReference.equals(localNode.groupReference)
                    && peer.status != NodeStatus.REPLACED) {
                // If we just detected this is a peer node that used to listen on our address,
                // but its obviously no longer around, mark it as REPLACED and do not send PATCH
                peer.status = NodeStatus.REPLACED;
                peer.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
                peer.documentVersion++;
                ch.handle(null, null);
            } else {
                sendRequest(patch);
            }

            // only probe N peers
            if (++probeCount >= peersToProbe) {
                break;
            }
        }

        if (probeCount == 0) {
            maint.complete();
        }
    }

    public void handleGossipPatchCompletion(Operation maint, Operation patch, Throwable e,
            NodeGroupState localState, NodeGroupState patchBody, AtomicInteger remaining,
            NodeState remotePeer) {

        try {
            if (patch == null) {
                return;
            }

            long updateTime = localState.membershipUpdateTimeMicros;
            if (e != null) {
                updateTime = remotePeer.status != NodeStatus.UNAVAILABLE ? Utils.getNowMicrosUtc()
                        : updateTime;

                if (remotePeer.status != NodeStatus.UNAVAILABLE) {
                    remotePeer.documentUpdateTimeMicros = Utils.getNowMicrosUtc();
                    remotePeer.documentVersion++;
                }
                remotePeer.status = NodeStatus.UNAVAILABLE;
            } else {
                NodeGroupState peerState = getBody(patch);
                if (peerState.documentOwner.equals(remotePeer.id)) {
                    NodeState remotePeerStateFromRsp = peerState.nodes.get(remotePeer.id);
                    if (remotePeerStateFromRsp.documentVersion > remotePeer.documentVersion) {
                        remotePeer = remotePeerStateFromRsp;
                    }
                } else if (remotePeer.status != NodeStatus.REPLACED) {
                    logWarning("Peer address %s has changed to id %s from %s",
                            patch.getUri(),
                            peerState.documentOwner,
                            remotePeer.id);
                    remotePeer.status = NodeStatus.REPLACED;
                    remotePeer.documentVersion++;
                    updateTime = Utils.getNowMicrosUtc();
                }
                updateTime = Math.max(updateTime, peerState.membershipUpdateTimeMicros);
            }

            synchronized (patchBody) {
                patchBody.nodes.put(remotePeer.id, remotePeer);
                patchBody.membershipUpdateTimeMicros = Math.max(updateTime,
                        patchBody.membershipUpdateTimeMicros);
            }

        } finally {
            int r = remaining.decrementAndGet();
            if (r != 0) {
                return;
            }

            // to merge updated state, issue a self PATCH. It contains NodeState entries for every
            // peer node we just talked to
            sendRequest(Operation.createPatch(getUri())
                    .setBody(patchBody));

            maint.complete();
        }
    }

    /**
     * Merges current node group state with state that came through a PATCH.
     *
     * PATCH requests are sent from
     *
     * 1) local service to itself, after it has communicated with a peer, during maintenance.
     *
     * 2) A remote peer when its probing this local service, during its maintenance cycle
     *
     * The key invariants that should not be violated, guaranteeing forward evolution of state even
     * if nodes only talk to a small portion of their peers:
     *
     * - When a status changes, the change is accepted if the remote version is higher
     *
     * - A local node is the only node that can change its own node entry status, for a PATCH that it
     * receives.
     *
     * - A node should never increment the version of a node entry, for other nodes, unless that node
     * entry is marked UNAVAILABLE
     *
     * - When a status changes during gossip version must be incremented - Versions always move forward
     */
    private void mergeRemoteAndLocalMembership(
            NodeGroupState localState,
            NodeGroupState remotePeerState,
            EnumSet<NodeGroupChange> changes) {
        if (localState == null) {
            return;
        }

        boolean isSelfPatch = remotePeerState.documentOwner.equals(getHost().getId());
        long now = Utils.getNowMicrosUtc();

        NodeState selfEntry = localState.nodes.get(getHost().getId());

        for (NodeState remoteNodeEntry : remotePeerState.nodes.values()) {

            NodeState l = localState.nodes.get(remoteNodeEntry.id);
            boolean isLocalNode = remoteNodeEntry.id.equals(getHost().getId());

            if (!isSelfPatch && isLocalNode) {
                if (remoteNodeEntry.status != l.status) {
                    logWarning("Peer %s is reporting us as %s, current status: %s",
                            remotePeerState.documentOwner, remoteNodeEntry.status, l.status);
                    if (remoteNodeEntry.documentVersion > l.documentVersion) {
                        // increment local version to re-enforce we are alive and well
                        l.documentVersion = remoteNodeEntry.documentVersion;
                        l.documentUpdateTimeMicros = now;
                        changes.add(NodeGroupChange.SELF_CHANGE);
                    }
                }
                // local instance of node group service is the only one that can update its own
                // status
                continue;
            }

            if (l == null) {
                boolean hasExpired = remoteNodeEntry.documentExpirationTimeMicros > 0
                        && remoteNodeEntry.documentExpirationTimeMicros < now;
                if (hasExpired || NodeState.isUnAvailable(remoteNodeEntry)) {
                    continue;
                }
                if (!isLocalNode) {
                    logInfo("Adding new peer %s (%s), status %s", remoteNodeEntry.id,
                            remoteNodeEntry.groupReference, remoteNodeEntry.status);
                }
                // we found a new peer, through the gossip PATCH. Add to our state
                localState.nodes.put(remoteNodeEntry.id, remoteNodeEntry);
                changes.add(NodeGroupChange.PEER_ADDED);
                continue;
            }

            boolean needsUpdate = l.status != remoteNodeEntry.status;
            if (needsUpdate) {
                changes.add(NodeGroupChange.PEER_STATUS_CHANGE);
            }

            if (isSelfPatch && isLocalNode && needsUpdate) {
                // we sent a self PATCH to update our status. Move our version forward;
                remoteNodeEntry.documentVersion = Math.max(remoteNodeEntry.documentVersion,
                        l.documentVersion) + 1;
            }

            // versions move forward only, ignore stale nodes
            if (remoteNodeEntry.documentVersion < l.documentVersion) {
                logInfo("v:%d - q:%d, v:%d - q:%d , %s - %s (local:%s %d)", l.documentVersion,
                        l.membershipQuorum,
                        remoteNodeEntry.documentVersion, remoteNodeEntry.membershipQuorum,
                        l.id,
                        remotePeerState.documentOwner,
                        getHost().getId(),
                        selfEntry.documentVersion);
                continue;
            }

            if (remoteNodeEntry.documentVersion == l.documentVersion && needsUpdate) {
                // pick update with most recent time, even if that is prone to drift and jitter
                // between nodes
                if (remoteNodeEntry.documentUpdateTimeMicros < l.documentUpdateTimeMicros) {
                    logWarning(
                            "Ignoring update for %s from peer %s. Local status: %s, remote status: %s",
                            remoteNodeEntry.id, remotePeerState.documentOwner, l.status,
                            remoteNodeEntry.status);
                    continue;
                }
            }

            if (remoteNodeEntry.status == NodeStatus.UNAVAILABLE
                    && l.documentExpirationTimeMicros == 0
                    && remoteNodeEntry.documentExpirationTimeMicros == 0) {
                remoteNodeEntry.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                        + localState.config.nodeRemovalDelayMicros;
                logInfo("Set expiration at %d for unavailable node %s(%s)",
                        remoteNodeEntry.documentExpirationTimeMicros,
                        remoteNodeEntry.id,
                        remoteNodeEntry.groupReference);
                changes.add(NodeGroupChange.PEER_STATUS_CHANGE);
                needsUpdate = true;
            }

            if (remoteNodeEntry.status == NodeStatus.UNAVAILABLE && needsUpdate) {
                // nodes increment their own entry version, except, if they are unavailable
                remoteNodeEntry.documentVersion++;
            }

            localState.nodes.put(remoteNodeEntry.id, remoteNodeEntry);
        }

        List<String> missingNodes = new ArrayList<>();
        for (NodeState l : localState.nodes.values()) {
            NodeState r = remotePeerState.nodes.get(l.id);
            if (!NodeState.isUnAvailable(l) || l.id.equals(getHost().getId())) {
                continue;
            }

            long expirationMicros = l.documentExpirationTimeMicros;
            if (r != null) {
                expirationMicros = Math.max(l.documentExpirationTimeMicros,
                        r.documentExpirationTimeMicros);
            }

            if (expirationMicros > 0 && now > expirationMicros) {
                changes.add(NodeGroupChange.PEER_STATUS_CHANGE);
                logInfo("Removing expired unavailable node %s(%s)", l.id, l.groupReference);
                missingNodes.add(l.id);
            }
        }

        for (String id : missingNodes) {
            localState.nodes.remove(id);
        }

        boolean isModified = !changes.isEmpty();
        localState.membershipUpdateTimeMicros = Math.max(
                remotePeerState.membershipUpdateTimeMicros,
                isModified ? now : localState.membershipUpdateTimeMicros);
        if (isModified) {
            logInfo("State updated, merge with %s, self %s, %d",
                    remotePeerState.documentOwner,
                    localState.documentOwner,
                    localState.membershipUpdateTimeMicros);
        }
    }

    public NodeState[] shuffleGroupMembers(NodeGroupState localState) {
        // randomize the list of peers and place them in array. Then pick the first N peers to
        // probe. We can probably come up with a single pass, probabilistic approach
        // but this works for now and is relatively cheap even for groups with thousands of members
        NodeState[] randomizedPeers = new NodeState[localState.nodes.size()];
        localState.nodes.values().toArray(randomizedPeers);
        int index;
        NodeState t;
        Random random = new Random();
        for (int i = randomizedPeers.length - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            t = randomizedPeers[index];
            randomizedPeers[index] = randomizedPeers[i];
            randomizedPeers[i] = t;
        }
        return randomizedPeers;
    }

    NodeGroupState getBody(Operation o) {
        if (!o.hasBody()) {
            return new NodeGroupState();
        }
        NodeGroupState rsp = o.getBody(NodeGroupState.class);
        if (rsp != null) {
            if (rsp.nodes == null) {
                rsp.nodes = new HashMap<>();
            }
        }
        return rsp;
    }
}
