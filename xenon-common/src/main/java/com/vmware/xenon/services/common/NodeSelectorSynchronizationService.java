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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
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
    public static final String STAT_NAME_EPOCH_INCREMENT_RETRY_COUNT = "epochIncrementRetryCount";

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
            bestPeerRsp = null;
        }

        if (bestPeerRsp != null && request.isOwner) {
            bestPeerRsp.documentOwner = getHost().getId();
        }

        if (bestPeerRsp != null && this.isDetailedLoggingEnabled) {
            logInfo("Using best peer state for %s (e:%d, v:%d)", bestPeerRsp.documentSelfLink,
                    bestPeerRsp.documentEpoch,
                    bestPeerRsp.documentVersion);
        }

        boolean incrementEpoch = false;

        if (bestPeerRsp == null) {
            // if the local state is preferred, there is no need to increment epoch.
            bestPeerRsp = request.state;
            if (this.isDetailedLoggingEnabled) {
                logInfo("Local is best peer state for %s (e:%d, v:%d)",
                        bestPeerRsp.documentSelfLink,
                        bestPeerRsp.documentEpoch,
                        bestPeerRsp.documentVersion);
            }
        }

        // we increment epoch only when we assume the role of owner
        if (!request.wasOwner && request.isOwner) {
            if (bestPeerRsp.documentVersion > 0) {
                // only increment epoch if this is not a document being created, on this host
                incrementEpoch = true;
            }
        }

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
            Iterator<Entry<URI, ServiceDocument>> peerStateIt = peerStates.entrySet().iterator();

            TreeMap<String, URI> peersWithService = new TreeMap<>();

            peersWithService.put(getHost().getId(), getHost().getPublicUri());
            boolean isMissingFromOwner = false;

            // build a map that lets us lookup a node id, given its URI
            Map<URI, String> uriToNodeId = new HashMap<>();
            for (Entry<String, URI> en : selectedNodes.entrySet()) {
                uriToNodeId.put(UriUtils.buildUri(en.getValue(), ""), en.getKey());
            }

            // we need to determine if a node, other than us, that became the owner for the
            // service we are trying to synchronize, does NOT have the service.
            // If it does not have the service, it can't synchronize it. If the current
            // node was the previous owner, it will assume the role of synchronizing
            while (peerStateIt.hasNext()) {
                Entry<URI, ServiceDocument> e = peerStateIt.next();
                ServiceDocument peerState = e.getValue();

                if (peerState.documentSelfLink == null) {
                    if (!request.isOwner) {
                        // Peer did not have this service and we are not the owner. Add self to a list
                        // and we will decide downstream if we should take responsibility for
                        // broadcasting state to the peer
                        URI peerUri = e.getKey();
                        boolean isPeerNewOwner = peerUri.getHost()
                                .equals(request.ownerNodeReference.getHost())
                                && peerUri.getPort() == request.ownerNodeReference.getPort();
                        if (isPeerNewOwner) {
                            isMissingFromOwner = true;
                            continue;
                        }
                    } else {
                        // We are the new or current owner so we will broadcast our state to the
                        // peer. Skip additional checks below
                        continue;
                    }
                } else {
                    // the peer has this service. Added to a sorted map so we can select the one
                    // responsible for broadcasting the state
                    URI baseUri = UriUtils.buildUri(e.getKey(), "");
                    String id = uriToNodeId.get(baseUri);
                    if (id == null) {
                        logWarning("Failure finding id for peer %s, not synchronizing!", baseUri);
                    } else {
                        peersWithService.put(id, e.getKey());
                    }
                }

                if (incrementEpoch || !request.isOwner) {
                    continue;
                }

                if (getHost().getId().equals(peerState.documentOwner)
                        && bestPeerRsp.documentEpoch.equals(peerState.documentEpoch)
                        && bestPeerRsp.documentVersion == peerState.documentVersion) {
                    // this peer has latest state and agrees we are the owner, nothing to do
                    peerStateIt.remove();
                    if (this.isDetailedLoggingEnabled) {
                        logFine("Peer %s has latest epoch, owner and version for %s skipping broadcast",
                                e.getKey(),
                                peerState.documentSelfLink);
                    }
                } else {
                    incrementEpoch = true;
                }
            }

            if (isMissingFromOwner) {
                // we sort the peers by node id, to create a deterministic way to select which
                // node is responsible for broadcasting service state to peers, if the selected
                // owner (a new node with no state) does not yet have the service.
                URI peerThatShouldAssumeOwnership = peersWithService.firstEntry().getValue();
                if (UriUtils.isHostEqual(getHost(), peerThatShouldAssumeOwnership)) {
                    request.isOwner = true;
                    incrementEpoch = true;
                    if (this.isDetailedLoggingEnabled) {
                        logFine("Broadcasting %s (epoch %d) to new owner %s\n"
                                + " Others with service:%s",
                                bestPeerRsp.documentSelfLink,
                                bestPeerRsp.documentEpoch + 1,
                                request.ownerNodeReference,
                                peersWithService);
                    }
                }
            }

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

            if (!request.isOwner) {
                post.complete();
                return;
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
                post.complete();
            });

            bestPeerRsp.documentOwner = request.ownerNodeId;
            if (incrementEpoch) {
                if (this.isDetailedLoggingEnabled) {
                    logInfo("Incrementing epoch from %d to %d for %s", bestPeerRsp.documentEpoch,
                            bestPeerRsp.documentEpoch + 1, bestPeerRsp.documentSelfLink);
                }
                bestPeerRsp.documentEpoch += 1;
                bestPeerRsp.documentVersion++;
                // we set the body to indicate we have found a better state
                post.setBody(bestPeerRsp);
            }

            ServiceDocument clonedState = Utils.clone(bestPeerRsp);
            for (Entry<URI, ServiceDocument> entry : peerStates.entrySet()) {

                URI peer = entry.getKey();

                URI targetFactoryUri = UriUtils.buildUri(peer, request.factoryLink);
                Operation peerOp = Operation.createPost(targetFactoryUri)
                        .setReferer(post.getReferer()).setExpiration(post.getExpirationMicrosUtc())
                        .setCompletion(c);

                // Mark it as replicated so the remote factories do not try to replicate it again
                peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED);
                // Request a version check to prevent restarting/recreating a service that might
                // have been deleted
                peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);

                peerOp.addRequestHeader(Operation.REPLICATION_PHASE_HEADER,
                        Operation.REPLICATION_PHASE_COMMIT);

                if (entry.getValue().documentSelfLink != null) {
                    // service exists on peer node, push latest state as a PUT
                    if (isMissingFromOwner) {
                        // skip nodes that already have the service, if we are acting as "owner"
                        c.handle(null, null);
                        continue;
                    }

                    peerOp.setAction(Action.PUT);
                    peerOp.setUri(UriUtils.buildUri(peer, bestState.documentSelfLink));
                    clonedState.documentSelfLink = bestState.documentSelfLink;
                } else {
                    // service does not exist, issue a POST to factory
                    clonedState.documentSelfLink = bestState.documentSelfLink.replace(
                            request.factoryLink, "");
                }

                if (isServiceDeleted) {
                    peerOp.setAction(Action.DELETE);
                }

                // clone body again, since for some nodes we need to post to factory, vs
                // a PUT to the service itself.
                peerOp.setBody(clonedState);

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

    private void markServiceInConflict(ServiceDocument state, ServiceDocument bestPeerRsp) {
        logWarning("State in conflict. Local: %s, Among peers: %s",
                Utils.toJsonHtml(state), Utils.toJsonHtml(bestPeerRsp));
    }

}
