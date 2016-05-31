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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.OperationOption;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocument.DocumentRelationship;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

public class NodeSelectorSynchronizationService extends StatelessService {

    public static final String PROPERTY_NAME_SYNCHRONIZATION_LOGGING = Utils.PROPERTY_NAME_PREFIX
            + "NodeSelectorSynchronizationService.isDetailedLoggingEnabled";

    public static class NodeGroupSynchronizationState extends ServiceDocument {
        public Set<String> inConflictLinks = new HashSet<>();
    }

    public static class SynchronizePeersRequest {
        public static final String KIND = Utils.buildKind(SynchronizePeersRequest.class);

        public static SynchronizePeersRequest create() {
            SynchronizePeersRequest r = new SynchronizePeersRequest();
            r.kind = KIND;
            return r;
        }

        public ServiceDocument state;
        public ServiceDocumentDescription stateDescription;
        public EnumSet<ServiceOption> options;
        public String factoryLink;
        public boolean wasOwner;
        public boolean isOwner;
        public URI ownerNodeReference;
        public String ownerNodeId;
        public String kind;
    }

    private Service parent;

    private boolean isDetailedLoggingEnabled = Boolean
            .getBoolean(PROPERTY_NAME_SYNCHRONIZATION_LOGGING);

    public NodeSelectorSynchronizationService(Service parent) {
        super(NodeGroupSynchronizationState.class);
        super.toggleOption(ServiceOption.UTILITY, true);
        this.parent = parent;
    }

    @Override
    public void handleStart(Operation startPost) {
        startPost.complete();
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handleRequest(Operation post) {
        if (post.getAction() != Action.POST) {
            post.fail(new IllegalArgumentException("Action not supported"));
            return;
        }

        if (!post.hasBody()) {
            post.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        SynchronizePeersRequest body = post.getBody(SynchronizePeersRequest.class);
        if (body.kind == null) {
            post.fail(new IllegalArgumentException("kind is required"));
            return;
        }
        if (body.kind.equals(SynchronizePeersRequest.KIND)) {
            handleSynchronizeRequest(post, body);
            return;
        }

        post.fail(new IllegalArgumentException("kind is not supported: " + body.kind));
    }

    private void handleSynchronizeRequest(Operation post, SynchronizePeersRequest body) {
        if (body.state == null) {
            post.fail(new IllegalArgumentException("state is required"));
            return;
        }

        if (body.state.documentSelfLink == null) {
            post.fail(new IllegalArgumentException("state.documentSelfLink is required"));
            return;
        }

        if (body.options == null || body.options.isEmpty()) {
            post.fail(new IllegalArgumentException("options is required"));
            return;
        }

        if (body.factoryLink == null || body.factoryLink.isEmpty()) {
            post.fail(new IllegalArgumentException("factoryLink is required"));
            return;
        }

        // we are going to broadcast a query (GET) to all peers, that should return
        // a document with the specified self link

        URI localQueryUri = UriUtils.buildDocumentQueryUri(
                getHost(),
                body.state.documentSelfLink,
                false,
                true,
                body.options);

        Operation remoteGet = Operation.createGet(localQueryUri)
                .setReferer(getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        post.fail(e);
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody(NodeGroupBroadcastResponse.class);
                    handleBroadcastGetCompletion(rsp, post, body);
                });

        getHost().broadcastRequest(this.parent.getSelfLink(), body.state.documentSelfLink, true,
                remoteGet);
    }

    private void handleBroadcastGetCompletion(NodeGroupBroadcastResponse rsp, Operation post,
            SynchronizePeersRequest request) {

        if (rsp.failures.size() > 0 && rsp.jsonResponses.isEmpty()) {
            post.fail(new IllegalStateException("Failures received: " + Utils.toJsonHtml(rsp)));
            return;
        }

        ServiceDocument bestPeerRsp = null;
        boolean incrementEpoch = false;

        TreeMap<Long, List<ServiceDocument>> syncRspsPerEpoch = new TreeMap<>();
        Map<URI, ServiceDocument> peerStates = new HashMap<>();

        for (Entry<URI, String> e : rsp.jsonResponses.entrySet()) {
            ServiceDocument peerState = Utils.fromJson(e.getValue(),
                    request.state.getClass());

            if (peerState.documentSelfLink == null
                    || !peerState.documentSelfLink.equals(request.state.documentSelfLink)) {
                logWarning("Invalid state from peer %s: %s", e.getKey(), e.getValue());
                peerStates.put(e.getKey(), new ServiceDocument());
                continue;
            }

            peerStates.put(e.getKey(), peerState);

            if (peerState.documentEpoch == null) {
                peerState.documentEpoch = 0L;
            }
            List<ServiceDocument> statesForEpoch = syncRspsPerEpoch.get(peerState.documentEpoch);
            if (statesForEpoch == null) {
                statesForEpoch = new ArrayList<>();
                syncRspsPerEpoch.put(peerState.documentEpoch, statesForEpoch);
            }
            statesForEpoch.add(peerState);

            if (!request.ownerNodeId.equals(peerState.documentOwner)) {
                incrementEpoch = true;
            }
        }

        // As part of synchronization we need to detect what peer services do not have the best state.
        // A peer might have been missing this service instance, or it might be unavailable. Either way,
        // when we attempt to broadcast the best state, we will use this map to determine if a peer
        // should receive the best state we found.
        for (URI remotePeerService : rsp.receivers) {
            if (peerStates.containsKey(remotePeerService)) {
                continue;
            }
            if (this.isDetailedLoggingEnabled) {
                logInfo("No peer response for %s from %s", request.state.documentSelfLink,
                        remotePeerService);
            }
            peerStates.put(remotePeerService, new ServiceDocument());
        }

        if (!syncRspsPerEpoch.isEmpty()) {
            List<ServiceDocument> statesForHighestEpoch = syncRspsPerEpoch.get(syncRspsPerEpoch
                    .lastKey());
            long maxVersion = Long.MIN_VALUE;
            for (ServiceDocument peerState : statesForHighestEpoch) {
                if (peerState.documentVersion > maxVersion) {
                    bestPeerRsp = peerState;
                    maxVersion = peerState.documentVersion;
                }
            }
        }

        if (bestPeerRsp != null && bestPeerRsp.documentEpoch == null) {
            bestPeerRsp.documentEpoch = 0L;
        }

        if (request.state.documentEpoch == null) {
            request.state.documentEpoch = 0L;
        }

        EnumSet<DocumentRelationship> results = EnumSet.noneOf(DocumentRelationship.class);
        if (bestPeerRsp == null) {
            results.add(DocumentRelationship.PREFERRED);
        } else if (request.state.documentEpoch.compareTo(bestPeerRsp.documentEpoch) > 0) {
            // Local state is of higher epoch than all peers
            results.add(DocumentRelationship.PREFERRED);
        } else if (request.state.documentEpoch.equals(bestPeerRsp.documentEpoch)) {
            // compare local state against peers only if they are in the same epoch
            results = ServiceDocument.compare(request.state,
                    bestPeerRsp, request.stateDescription, Utils.getTimeComparisonEpsilonMicros());
        }

        if (results.contains(DocumentRelationship.IN_CONFLICT)) {
            markServiceInConflict(request.state, bestPeerRsp);
            // if we detect conflict, we will synchronize local service with selected peer state
        } else if (results.contains(DocumentRelationship.PREFERRED)) {
            // the local state is preferred
            bestPeerRsp = request.state;
        }

        if (bestPeerRsp == null) {
            bestPeerRsp = request.state;
        }

        if (bestPeerRsp.documentSelfLink == null
                || bestPeerRsp.documentVersion < 0
                || bestPeerRsp.documentEpoch == null
                || bestPeerRsp.documentEpoch < 0) {
            post.fail(new IllegalStateException(
                    "Chosen state has invalid epoch or version: " + Utils.toJson(bestPeerRsp)));
            return;
        }

        if (this.isDetailedLoggingEnabled) {
            logInfo("Using best peer state %s", Utils.toJson(bestPeerRsp));
        }

        bestPeerRsp.documentOwner = request.ownerNodeId;

        broadcastBestState(rsp.selectedNodes, peerStates, post, request, bestPeerRsp,
                incrementEpoch);
    }

    private void broadcastBestState(Map<String, URI> selectedNodes,
            Map<URI, ServiceDocument> peerStates,
            Operation post, SynchronizePeersRequest request,
            ServiceDocument bestPeerRsp,
            boolean incrementEpoch) {
        try {
            post.setBodyNoCloning(null);
            if (peerStates.isEmpty()) {
                if (this.isDetailedLoggingEnabled) {
                    logInfo("(isOwner: %s) No peers available for %s", request.isOwner,
                            bestPeerRsp.documentSelfLink);
                }
                post.complete();
                return;
            }

            final ServiceDocument bestState = bestPeerRsp;
            boolean isServiceDeleted = Action.DELETE.toString().equals(
                    bestPeerRsp.documentUpdateAction);

            if (isServiceDeleted) {
                // The caller relies on the body being present to determine if a peer had a better
                // state than local state. If the body is not present, it assumes all is OK, and proceeds
                // with either service start, or completing pending requests. If we detect the state is
                // deleted, we must set the body, so the caller aborts start, thus converging with peers
                // that already have stopped this service
                post.setBodyNoCloning(bestPeerRsp);
            }

            if (peerStates.isEmpty()) {
                post.complete();
                return;
            }

            AtomicInteger remaining = new AtomicInteger(peerStates.size());
            CompletionHandler c = ((o, e) -> {
                int r = remaining.decrementAndGet();
                if (e != null) {
                    logWarning("Peer update to %s:%d for %s failed with %s, remaining %d",
                            o.getUri().getHost(), o.getUri().getPort(),
                            bestPeerRsp.documentSelfLink,
                            e.toString(), r);
                }
                if (r != 0) {
                    return;
                }

                // if we are not the owner, request has already completed, before we even sent
                // the requests
                if (request.isOwner) {
                    post.complete();
                }
            });

            if (incrementEpoch) {
                logInfo("Incrementing epoch from %d to %d for %s", bestPeerRsp.documentEpoch,
                        bestPeerRsp.documentEpoch + 1, bestPeerRsp.documentSelfLink);
                bestPeerRsp.documentEpoch += 1;
                bestPeerRsp.documentVersion++;
            }

            post.setBody(bestPeerRsp);

            // if we are not the owner, complete operation, to avoid deadlock:
            // if we send a POST which converts to a PUT, on the actual owner of the service, it will
            // attempt to synchronize with this host, but since we are blocked waiting for it, we deadlock
            if (!request.isOwner) {
                post.complete();
            }

            ServiceDocument clonedState = Utils.clone(bestPeerRsp);
            for (Entry<URI, ServiceDocument> entry : peerStates.entrySet()) {
                URI peer = entry.getKey();
                ServiceDocument peerState = entry.getValue();

                Operation peerOp = prepareSynchPostRequest(post, request, bestState,
                        isServiceDeleted, c, clonedState, peer);

                if (!incrementEpoch
                        && bestPeerRsp.getClass().equals(peerState.getClass())
                        && ServiceDocument.equals(request.stateDescription, bestPeerRsp, peerState)) {
                    skipSynchOrStartServiceOnPeer(peerOp, peerState.documentSelfLink);
                    continue;
                }

                if (this.isDetailedLoggingEnabled) {
                    logInfo("(isOwner: %s)(remaining: %d) (last action: %s) Sending %s with best state for %s to %s (e:%d, v:%d)",
                            request.isOwner,
                            remaining.get(),
                            clonedState.documentUpdateAction,
                            peerOp.getAction(),
                            clonedState.documentSelfLink,
                            peerOp.getUri(),
                            clonedState.documentEpoch,
                            clonedState.documentVersion);
                }
                sendRequest(peerOp);
            }
        } catch (Throwable e) {
            logSevere(e);
            post.fail(e);
        }
    }

    /**
     * The service state on the peer node is identical to best state. We should
     * skip sending a synchronization POST, if the service is already started
     */
    private void skipSynchOrStartServiceOnPeer(Operation peerOp, String link) {
        Operation checkGet = Operation.createGet(UriUtils.buildUri(peerOp.getUri(), link))
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_FORWARDING)
                .setConnectionSharing(true)
                .setExpiration(Utils.getNowMicrosUtc() + TimeUnit.SECONDS.toMicros(2))
                .setCompletion((o, e) -> {
                    if (e == null) {
                        if (this.isDetailedLoggingEnabled) {
                            logInfo("Skipping %s , state identical with best state", o.getUri());
                        }
                        peerOp.complete();
                        return;
                    }
                    // service does not seem to exist, issue POST to start it
                    sendRequest(peerOp);
                });
        sendRequest(checkGet);
    }

    private Operation prepareSynchPostRequest(Operation post, SynchronizePeersRequest request,
            final ServiceDocument bestState, boolean isServiceDeleted, CompletionHandler c,
            ServiceDocument clonedState, URI peer) {
        URI targetFactoryUri = UriUtils.buildUri(peer, request.factoryLink);
        Operation peerOp = Operation.createPost(targetFactoryUri)
                .transferRefererFrom(post).setExpiration(post.getExpirationMicrosUtc())
                .setCompletion(c);

        peerOp.toggleOption(OperationOption.CONNECTION_SHARING, true);

        // Mark it as replicated so the remote factories do not try to replicate it again
        peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED);

        // Request a version check to prevent restarting/recreating a service that might
        // have been deleted
        peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);

        // indicate this is a synchronization request.
        peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH);

        peerOp.addRequestHeader(Operation.REPLICATION_PHASE_HEADER,
                Operation.REPLICATION_PHASE_COMMIT);

        //  must get started on peers, regardless if the index has it. Since only one node is doing
        // synchronization, its responsible for starting the children on ALL nodes. If this is a synchronization
        // due to a node joining or leaving and some peers have already started the service, the POST will
        // automatically get converted to a PUT, if the factory is IDEMPOTENT. Otherwise, it will fail
        clonedState.documentSelfLink = bestState.documentSelfLink.replace(
                request.factoryLink, "");

        if (isServiceDeleted) {
            peerOp.setAction(Action.DELETE);
        }

        peerOp.setBody(clonedState);
        return peerOp;
    }

    private void markServiceInConflict(ServiceDocument state, ServiceDocument bestPeerRsp) {
        logWarning("State in conflict. Local: %s, Among peers: %s",
                Utils.toJsonHtml(state), Utils.toJsonHtml(bestPeerRsp));
    }

}
