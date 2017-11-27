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
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocument.DocumentRelationship;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.config.XenonConfiguration;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;


public class NodeSelectorSynchronizationService extends StatelessService {

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

        public String indexLink;
        public ServiceDocument state;
        public ServiceDocumentDescription stateDescription;
        public EnumSet<ServiceOption> options;
        public String factoryLink;
        public String kind;
    }

    private Service parent;

    private boolean isDetailedLoggingEnabled = XenonConfiguration.bool(
            NodeSelectorSynchronizationService.class,
            "isDetailedLoggingEnabled",
            false
    );

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

        boolean synchHistoricalVersions = post.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_HISTORICAL_VERSIONS);
        if (synchHistoricalVersions) {
            QueryTask.Query.Builder queryBuilder = QueryTask.Query.Builder.create()
                    .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, body.state.documentSelfLink);
            if (body.state.documentVersion > 0) {
                // fetch historical versions up to latest exclusive
                queryBuilder.addRangeClause(ServiceDocument.FIELD_NAME_VERSION,
                        NumericRange.createLongRange(0L, body.state.documentVersion, true, false));
            }
            QueryTask.Query query = queryBuilder.build();
            QueryTask task = QueryTask.Builder.createDirectTask()
                    .setQuery(query)
                    .setResultLimit((int) body.stateDescription.versionRetentionLimit)
                    .addOption(QueryOption.INCLUDE_ALL_VERSIONS)
                    .addOption(QueryOption.BROADCAST)
                    .build();
            task.documentExpirationTimeMicros = post.getExpirationMicrosUtc();
            task.indexLink = body.indexLink;

            URI localQueryTasksUri = UriUtils.buildUri(getHost(),ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
            Operation taskPost = Operation.createPost(localQueryTasksUri)
                    .setBody(task)
                    .setReferer(getUri())
                    .setCompletion((o, e) -> {
                        if (e != null) {
                            post.fail(e);
                            return;
                        }

                        QueryTask result = o.getBody(QueryTask.class);
                        handleBroadcastQueryCompletion(result, post, body);
                    });
            getHost().sendRequest(taskPost);
            return;
        }

        // we are going to broadcast a query (GET) to all peers, that should return
        // a document with the specified self link
        createAndBroadcastGetQuery(post, body, body.state.documentSelfLink);
    }

    private void handleBroadcastQueryCompletion(QueryTask result, Operation post,
            SynchronizePeersRequest request) {
        ServiceDocumentQueryResult queryResult = result.results;

        // allocate hash set to track already fetched versioned documents (as duplicates are expected)
        Set<String> synchronizedVersionedLinks = new HashSet<>(queryResult.documentCount.intValue());

        String pageLink = queryResult.nextPageLink;
        synchronizePage(pageLink, post, request, synchronizedVersionedLinks, result.documentSelfLink);
    }

    private void synchronizePage(String pageLink, Operation post, SynchronizePeersRequest request,
            Set<String> synchronizedVersionedLinks, String queryTaskLink) {
        if (pageLink == null || pageLink.isEmpty()) {
            // all versions have been synchronized - we delete the query to release resources
            Operation.createDelete(UriUtils.buildUri(getHost(), queryTaskLink)).sendWith(this);

            post.complete();
            return;
        }

        Operation getPage = Operation.createGet(UriUtils.buildUri(getHost(), pageLink))
                .setReferer(getUri()).setCompletion((o, e) -> {
                    if (e != null) {
                        post.fail(e);
                        return;
                    }

                    ServiceDocumentQueryResult page = o.getBody(QueryTask.class).results;
                    post.nestCompletion((oo, ee) -> {
                        if (ee != null) {
                            post.fail(e);
                            return;
                        }

                        synchronizePage(page.nextPageLink, post, request, synchronizedVersionedLinks,
                                queryTaskLink);
                    });

                    synchronizeSingleVersion(page.documentLinks, 0, request, post, synchronizedVersionedLinks);
                });
        getHost().sendRequest(getPage);
    }

    private void synchronizeSingleVersion(List<String> documentLinks, int documentLinkIndex,
            SynchronizePeersRequest request, Operation post,
            Set<String> synchronizedVersionedLinks) {
        if (documentLinkIndex >= documentLinks.size()) {
            post.complete();
            return;
        }

        String documentLink = documentLinks.get(documentLinkIndex);
        if (synchronizedVersionedLinks.contains(documentLink)) {
            synchronizeSingleVersion(documentLinks, documentLinkIndex + 1, request, post, synchronizedVersionedLinks);
            return;
        }

        post.nestCompletion((o, e) -> {
            if (e != null) {
                post.fail(e);
                return;
            }

            synchronizeSingleVersion(documentLinks, documentLinkIndex + 1, request, post, synchronizedVersionedLinks);
        });

        synchronizedVersionedLinks.add(documentLink);
        createAndBroadcastGetQuery(post, request, documentLink);
    }

    private void createAndBroadcastGetQuery(Operation post, SynchronizePeersRequest body, String documentLink) {
        // determine if this is a versioned link
        int queryPos = documentLink.lastIndexOf(UriUtils.URI_QUERY_CHAR);
        String version = null;
        Long versionNum = null;
        String selfLink = documentLink;
        if (queryPos >= 0) {
            String query = documentLink.substring(queryPos + 1);
            Map<String, String> params = UriUtils.parseUriQueryParams(query);
            version = params.get(ServiceDocument.FIELD_NAME_VERSION);
            selfLink = documentLink.substring(0, queryPos);
        }
        URI localQueryUri = UriUtils.buildDocumentQueryUri(
                getHost(),
                body.indexLink,
                selfLink,
                false,
                true,
                body.options);
        if (version != null) {
            localQueryUri = UriUtils.extendUriWithQuery(localQueryUri, ServiceDocument.FIELD_NAME_VERSION, version);
            try {
                versionNum = Long.parseLong(version);
            } catch (NumberFormatException ex) {
                logWarning("DocumentLink %s: Failed to parse version %s as a number: %s",
                        documentLink, version, ex.getMessage());
                post.fail(ex);
                return;
            }
        }

        final Long finalVersion = versionNum;
        Operation remoteGet = Operation.createGet(localQueryUri)
                .setReferer(getUri())
                .setConnectionSharing(true)
                .setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION)
                .setExpiration(
                        Utils.fromNowMicrosUtc(NodeGroupService.PEER_REQUEST_TIMEOUT_MICROS))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        post.fail(e);
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody(NodeGroupBroadcastResponse.class);
                    handleBroadcastGetCompletion(rsp, post, body, finalVersion);
                });

        boolean excludeThisHostFromBroadcast = version == null;
        getHost().broadcastRequest(this.parent.getSelfLink(), selfLink, excludeThisHostFromBroadcast,
                remoteGet);
    }

    private void handleBroadcastGetCompletion(NodeGroupBroadcastResponse rsp, Operation post,
            SynchronizePeersRequest request, Long version) {

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

            if (version != null && !version.equals(peerState.documentVersion)) {
                logWarning("Invalid state from peer %s: %s. Version mismatch: expected version %s, received %d",
                        e.getKey(), e.getValue(), version, peerState.documentVersion);
                post.fail(Operation.STATUS_CODE_INTERNAL_ERROR);
                return;
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
        // hasn't responded and should therefore receive the best state we found.
        Set<URI> nonRespondingPeers = new HashSet<>();
        for (URI remotePeerService : rsp.receivers) {
            if (peerStates.containsKey(remotePeerService)) {
                continue;
            }
            if (this.isDetailedLoggingEnabled) {
                logInfo("No peer response for %s from %s", request.state.documentSelfLink,
                        remotePeerService);
            }
            nonRespondingPeers.add(remotePeerService);
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

        if (version == null) {
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
        }

        if (bestPeerRsp == null && (request.state.documentVersion == -1 || version != null)) {
            // If we did not find any documents from peers for the provided self-link
            // and the owner also did not provide a valid document, then we fail
            // the request with 404 - NOT FOUND error.
            // Note, documentVersion is set to -1 by the owner in
            // ServiceSynchronizationTracker to indicate that the owner does not have
            // a document for the self-link.
            this.logWarning("Synch failed to find any documents for self-link %s (version: %s)",
                    request.state.documentSelfLink, version);
            post.fail(Operation.STATUS_CODE_NOT_FOUND);
            return;
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

        if (version == null) {
            bestPeerRsp.documentOwner = this.getHost().getId();
        }

        broadcastBestState(rsp.selectedNodes, peerStates, nonRespondingPeers, post, request, bestPeerRsp,
                version);
    }

    private void broadcastBestState(Map<String, URI> selectedNodes,
            Map<URI, ServiceDocument> peerStates,
            Set<URI> nonRespondingPeers,
            Operation post,
            SynchronizePeersRequest request,
            ServiceDocument bestPeerRsp,
            Long version) {

        try {
            post.setBodyNoCloning(null);
            if (peerStates.isEmpty() && nonRespondingPeers.isEmpty()) {
                if (this.isDetailedLoggingEnabled) {
                    logInfo(") No peers available for %s", bestPeerRsp.documentSelfLink);
                }
                post.complete();
                return;
            }

            AtomicInteger remaining = new AtomicInteger(peerStates.size() + nonRespondingPeers.size());
            CompletionHandler c = (o, e) -> {
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
            };

            post.setBody(bestPeerRsp);

            boolean isServiceDeleted = Action.DELETE.toString().equals(
                    bestPeerRsp.documentUpdateAction);

            ServiceDocument clonedState = Utils.clone(bestPeerRsp);

            // send synchPost to peers with a different version
            for (Entry<URI, ServiceDocument> entry : peerStates.entrySet()) {
                URI peer = entry.getKey();
                ServiceDocument peerState = entry.getValue();

                Operation peerOp = prepareSynchPostRequest(post, request, bestPeerRsp,
                        isServiceDeleted, c, clonedState, peer, version);

                boolean isVersionSame = ServiceDocument
                        .compare(bestPeerRsp, peerState, request.stateDescription, Utils.getTimeComparisonEpsilonMicros())
                        .contains(DocumentRelationship.EQUAL_VERSION);

                if (isVersionSame
                        && bestPeerRsp.getClass().equals(peerState.getClass())
                        && ServiceDocument.equals(request.stateDescription, bestPeerRsp, peerState)) {
                    peerOp.complete();
                    continue;
                }

                sendSynchPost(peerOp, clonedState, remaining);
            }

            // send synchPost to non-responding peers
            for (URI peer : nonRespondingPeers) {
                Operation peerOp = prepareSynchPostRequest(post, request, bestPeerRsp,
                        isServiceDeleted, c, clonedState, peer, version);
                sendSynchPost(peerOp, clonedState, remaining);
            }
        } catch (Exception e) {
            logSevere(e);
            post.fail(e);
        }
    }

    private void sendSynchPost(Operation peerOp, ServiceDocument clonedState, AtomicInteger remaining) {
        if (this.isDetailedLoggingEnabled) {
            logInfo("(remaining: %d) (last action: %s) Sending %s with best state for %s to %s (e:%d, v:%d)",
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

    private Operation prepareSynchPostRequest(Operation post, SynchronizePeersRequest request,
            final ServiceDocument bestState, boolean isServiceDeleted, CompletionHandler c,
            ServiceDocument clonedState, URI peer, Long version) {
        URI targetFactoryUri = UriUtils.buildUri(peer, request.factoryLink);
        Operation peerOp = Operation.createPost(targetFactoryUri)
                .transferRefererFrom(post)
                .setCompletion(c);

        peerOp.setRetryCount(0);

        peerOp.setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION);

        // Mark it as replicated so the remote factories do not try to replicate it again
        peerOp.setFromReplication(true);
        peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED);

        // Request a version check to prevent restarting/recreating a service that might
        // have been deleted
        peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_VERSION_CHECK);

        // indicate this is a synchronization request.
        peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_PEER);

        if (version != null) {
            // indicate this is a synchronization request of a specific version
            peerOp.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_VERSION);
        }

        peerOp.addRequestHeader(Operation.REPLICATION_PHASE_HEADER,
                Operation.REPLICATION_PHASE_COMMIT);

        // must get started on peers, regardless if the index has it. Since only one node is doing
        // synchronization, its responsible for starting the children on ALL nodes. If this is a synchronization
        // due to a node joining or leaving and some peers have already started the service, the POST will
        // automatically get converted to a PUT, if the factory is IDEMPOTENT. Otherwise, it will fail
        clonedState.documentSelfLink = UriUtils.getLastPathSegment(bestState.documentSelfLink);

        if (isServiceDeleted && version == null) {
            peerOp.setAction(Action.DELETE);

            // If this is a delete request, let's reset the uri to
            // the actual documentSelfLink, both on the operation
            // and the ServiceDocument.
            peerOp.setUri(UriUtils.buildUri(peer, bestState.documentSelfLink));
            clonedState.documentSelfLink = bestState.documentSelfLink;
        }

        peerOp.setBody(clonedState);
        return peerOp;
    }

    private void markServiceInConflict(ServiceDocument state, ServiceDocument bestPeerRsp) {
        logWarning("State in conflict. Local: %s, Among peers: %s",
                Utils.toJsonHtml(state), Utils.toJsonHtml(bestPeerRsp));
    }
}
