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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState.NodeOption;

public class NodeSelectorReplicationService extends StatelessService {

    public static final String PROPERTY_NAME_REPLICA_NOT_FOUND_TIMEOUT_MICROS = Utils.PROPERTY_NAME_PREFIX
            + "NodeSelectorReplicationService.replicaTimeoutMicros";

    public static final int BINARY_SERIALIZATION = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + "NodeSelectorReplicationService.BINARY_SERIALIZATION",
            1);

    private Service parent;
    private Map<String, Integer> nodeCountPerLocation;
    private Map<URI, String> locationPerNodeURI;
    private long peerTimeoutMicros;

    private String nodeGroupLink;

    public NodeSelectorReplicationService(Service parent) {
        this.parent = parent;
        super.setHost(parent.getHost());
        super.setSelfLink(UriUtils.buildUriPath(parent.getSelfLink(),
                ServiceHost.SERVICE_URI_SUFFIX_REPLICATION));
        if (parent.getHost().getLocation() != null) {
            this.nodeCountPerLocation = new ConcurrentHashMap<>();
            this.locationPerNodeURI = new ConcurrentHashMap<>();
        }
        this.peerTimeoutMicros = Long.getLong(
                PROPERTY_NAME_REPLICA_NOT_FOUND_TIMEOUT_MICROS,
                NodeGroupService.PEER_REQUEST_TIMEOUT_MICROS);
        super.setProcessingStage(ProcessingStage.AVAILABLE);
    }

    /**
     * Issues updates to peer nodes, after a local update has been accepted
     */
    void replicateUpdate(NodeGroupState localState,
            Operation outboundOp, SelectAndForwardRequest req, SelectOwnerResponse rsp) {

        int memberCount = localState.nodes.size();
        NodeState selfNode = localState.nodes.get(getHost().getId());

        if (req.serviceOptions.contains(ServiceOption.OWNER_SELECTION)
                && selfNode.membershipQuorum > memberCount) {
            outboundOp.fail(new IllegalStateException("Not enough peers: " + memberCount));
            return;
        }

        if (memberCount == 1) {
            outboundOp.complete();
            return;
        }

        // The eligible count can be less than the member count if the parent node selector has
        // a smaller replication factor than group size. We need to use the replication factor
        // as the upper bound for calculating success and failure thresholds
        Collection<NodeState> selectedNodes = rsp.selectedNodes;
        int eligibleMemberCount = selectedNodes.size();

        // location is usually null, unless set explicitly
        String location = getHost().getLocation();
        NodeSelectorReplicationContext context = new NodeSelectorReplicationContext(
                location, selectedNodes, outboundOp);
        context.locationThreshold = selfNode.locationQuorum;

        // success threshold is determined based on the following precedence:
        // 1. request replication quorum header (if exists)
        // 2. group membership quorum (in case of OWNER_SELECTION)
        // 3. at least one remote node (in case one exists)
        String rplQuorumValue = outboundOp
                .getRequestHeaderAsIs(Operation.REPLICATION_QUORUM_HEADER);
        if (rplQuorumValue != null) {
            // replicate using success threshold based on request quorum header
            try {
                if (Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL.equals(rplQuorumValue)) {
                    context.successThreshold = eligibleMemberCount;
                } else {
                    context.successThreshold = Integer.parseInt(rplQuorumValue);
                }
                if (context.successThreshold > eligibleMemberCount) {
                    String errorMsg = String.format(
                            "Requested quorum %d is larger than member count %d",
                            context.successThreshold, eligibleMemberCount);
                    throw new IllegalArgumentException(errorMsg);
                }
            } catch (Exception e) {
                outboundOp.setRetryCount(0).fail(e);
                return;
            }

            context.failureThreshold = (eligibleMemberCount - context.successThreshold) + 1;
            replicateUpdateToNodes(context);
            return;
        }

        if (req.serviceOptions.contains(ServiceOption.OWNER_SELECTION)) {
            // replicate using group membership quorum
            if (location == null) {
                context.successThreshold = Math.min(eligibleMemberCount, selfNode.membershipQuorum);
                context.failureThreshold = (eligibleMemberCount - context.successThreshold) + 1;
            } else {
                int localNodeCount = getNodeCountInLocation(location, selectedNodes);
                if (selfNode.locationQuorum == 1) {
                    context.successThreshold = Math.min(localNodeCount, selfNode.membershipQuorum);
                    context.failureThreshold = (localNodeCount - context.successThreshold) + 1;
                }
            }
            replicateUpdateToNodes(context);
            return;
        }

        // When quorum is not required, succeed when we replicate to at least one remote node,
        // or, if only local node is available, succeed immediately.
        context.successThreshold = Math.min(2, eligibleMemberCount - 1);
        context.failureThreshold = (eligibleMemberCount - context.successThreshold) + 1;

        replicateUpdateToNodes(context);
    }

    /**
     * Returns the number of nodes in the specified location
     */
    private int getNodeCountInLocation(String location,
            Collection<NodeState> nodes) {
        // try cached value first
        Integer count = this.nodeCountPerLocation.get(location);
        if (count != null) {
            return count;
        }

        // fill cache maps
        count = (int) nodes.stream()
                .filter(ns -> Objects.equals(location,
                        ns.customProperties.get(NodeState.PROPERTY_NAME_LOCATION)))
                .peek(ns -> this.locationPerNodeURI.put(ns.groupReference, location))
                .count();
        this.nodeCountPerLocation.put(location, count);
        return count;
    }

    private void updateLocation(NodeState node) {
        this.nodeGroupLink = node.groupReference.getPath();
        this.locationPerNodeURI.computeIfAbsent(node.groupReference,
                (u) -> node.customProperties.get(NodeState.PROPERTY_NAME_LOCATION));
    }

    private String getLocation(Operation remotePeerResponse) {
        URI remotePeerService = remotePeerResponse.getUri();
        URI remoteNodeUri = UriUtils.buildServiceUri(remotePeerService.getScheme(),
                remotePeerService.getHost(),
                remotePeerService.getPort(), this.nodeGroupLink, null, null);

        return this.locationPerNodeURI.get(remoteNodeUri);
    }

    private void replicateUpdateToNodes(NodeSelectorReplicationContext context) {
        Operation update = createReplicationRequest(context.parentOp, null);
        update.setCompletion((o, e) -> handleReplicationCompletion(context, o, e));

        // trigger completion once, for self node, since its part of our accounting
        handleReplicationCompletion(context, null, null);

        for (NodeState m : context.nodes) {
            if (m.id.equals(this.getHost().getId())) {
                continue;
            }

            if (m.options.contains(NodeOption.OBSERVER)) {
                continue;
            }

            URI updateUri = createReplicaUri(m.groupReference, context.parentOp);
            update.setUri(updateUri);

            if (context.location != null) {
                updateLocation(m);
            }

            if (NodeState.isUnAvailable(m)) {
                int originalStatusCode = update.getStatusCode();
                update.setStatusCode(Operation.STATUS_CODE_FAILURE_THRESHOLD);
                handleReplicationCompletion(
                        context, update, new IllegalStateException("node is not available"));

                // set status code to its original value, since we're using a
                // shared update operation object, and the completion handler
                // checks the status code.
                update.setStatusCode(originalStatusCode);
                continue;
            }

            this.getHost().getClient().send(update);
        }
    }

    private static Operation createReplicationRequest(Operation outboundOp, URI remoteUri) {
        Operation update = Operation.createPost(remoteUri)
                .setAction(outboundOp.getAction())
                .setRetryCount(1)
                .forceRemote()
                .setExpiration(outboundOp.getExpirationMicrosUtc())
                .transferRefererFrom(outboundOp);

        String pragmaHeader = outboundOp.getRequestHeaderAsIs(Operation.PRAGMA_HEADER);
        if (pragmaHeader != null && !Operation.PRAGMA_DIRECTIVE_FORWARDED.equals(pragmaHeader)) {
            update.addRequestHeader(Operation.PRAGMA_HEADER, pragmaHeader);
            update.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED);
        }

        String commitHeader = outboundOp.getRequestHeaderAsIs(Operation.REPLICATION_PHASE_HEADER);
        if (commitHeader != null) {
            update.addRequestHeader(Operation.REPLICATION_PHASE_HEADER, commitHeader);
        }

        Utils.encodeAndTransferLinkedStateToBody(outboundOp, update, BINARY_SERIALIZATION == 1);

        update.setFromReplication(true);
        update.setConnectionTag(ServiceClient.CONNECTION_TAG_REPLICATION);

        if (NodeSelectorService.REPLICATION_OPERATION_OPTION != null) {
            update.toggleOption(NodeSelectorService.REPLICATION_OPERATION_OPTION, true);
        }

        if (update.getCookies() != null) {
            update.getCookies().clear();
        }

        return update;
    }

    private URI createReplicaUri(URI remoteHost, Operation outboundOp) {
        return UriUtils.buildServiceUri(remoteHost.getScheme(),
                remoteHost.getHost(), remoteHost.getPort(),
                outboundOp.getUri().getPath(), outboundOp.getUri().getQuery(), null);
    }

    private void handleReplicationCompletion(
            NodeSelectorReplicationContext context, Operation o, Throwable e) {
        String remoteLocation = context.location;
        if (context.location != null) {
            if (o != null) {
                remoteLocation = getLocation(o);
            }
            // if location is set and locationQuorum == 1 we require completion
            // from nodes in the same location. All other responses are ignored
            if (context.locationThreshold == 1
                    && !context.location.equals(remoteLocation)) {
                return;
            }
            // locationQuorum is greater than one, so proceed with regular completion processing
        }

        if (e == null && o != null
                && o.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
            e = new IllegalStateException("Request failed: " + o.toString());
        }

        if (e != null && handleServiceNotFoundOnReplica(context, o)) {
            return;
        }

        context.checkAndCompleteOperation(getHost(), e, o, remoteLocation);
    }

    private boolean handleServiceNotFoundOnReplica(NodeSelectorReplicationContext context, Operation o) {
        Operation op = context.parentOp;

        // A replica would report a service-not-found error only for
        // update requests.
        if (!op.isUpdate()) {
            return false;
        }

        // We expect a body with ServiceErrorResponse when the failure is because
        // of service-not-found error.
        if (o == null || !o.hasBody() || !o.getContentType().equals(Operation.MEDIA_TYPE_APPLICATION_JSON)) {
            return false;
        }

        ServiceErrorResponse rsp = o.getBody(ServiceErrorResponse.class);
        if (rsp == null ||
                rsp.getErrorCode() != ServiceErrorResponse.ERROR_CODE_SERVICE_NOT_FOUND_ON_REPLICA) {
            return false;
        }

        // We avoid retrying if the service is not started on the replica even after
        // retrying a few times.
        if (Utils.beforeNow(context.startTimeMicros + this.peerTimeoutMicros)) {
            logWarning("Service %s not found on replica. Giving up on %s replication request ...",
                    o.getUri().getPath(), op.getAction());
            return false;
        }

        // The remote replica does not have the service in AVAILABLE stage. This could be
        // because of out-of-order replication requests of a POST and PUT/PATCH.
        // We will just retry for that specific replica.
        URI remoteUri = createReplicaUri(o.getUri(), op);
        Operation update = createReplicationRequest(op, remoteUri);
        update.setCompletion((innerOp, innerEx) ->
                this.handleReplicationCompletion(context, innerOp, innerEx));

        this.getHost().scheduleCore(() -> {
            logWarning("Service %s not found on replica. Retrying %s replication request ...",
                    o.getUri().getPath(), op.getAction());
            this.getHost().getClient().send(update);
        }, this.getHost().getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);

        return true;
    }

    @Override
    public void sendRequest(Operation op) {
        this.parent.sendRequest(op);
    }

    @Override
    public ServiceHost getHost() {
        return this.parent.getHost();
    }
}
