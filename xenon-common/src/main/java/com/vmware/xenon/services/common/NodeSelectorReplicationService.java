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
import java.util.Objects;
import java.util.stream.Collectors;

import com.vmware.xenon.common.NodeSelectorService;
import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceClient;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeState.NodeOption;

public class NodeSelectorReplicationService extends StatelessService {

    public static final int BINARY_SERIALIZATION = Integer.getInteger(
            Utils.PROPERTY_NAME_PREFIX
                    + "NodeSelectorReplicationService.BINARY_SERIALIZATION",
            1);

    private Service parent;

    public NodeSelectorReplicationService(Service parent) {
        this.parent = parent;
        super.setHost(parent.getHost());
        super.setSelfLink(UriUtils.buildUriPath(parent.getSelfLink(),
                ServiceHost.SERVICE_URI_SUFFIX_REPLICATION));
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
        int eligibleMemberCount = rsp.selectedNodes.size();
        Collection<NodeState> selectedNodes = rsp.selectedNodes;

        // When quorum is not required, succeed when we replicate to at least one remote node,
        // or, if only local node is available, succeed immediately.
        int successThreshold = Math.min(2, eligibleMemberCount - 1);

        String rplQuorumValue = outboundOp.getRequestHeader(Operation.REPLICATION_QUORUM_HEADER);
        if (rplQuorumValue == null && !req.serviceOptions.contains(ServiceOption.OWNER_SELECTION)) {
            // replicate using 'default' success threshold
            replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold);
            return;
        }

        if (rplQuorumValue != null) {
            // replicate using success threshold based on request quorum header
            try {
                if (Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL.equals(rplQuorumValue)) {
                    successThreshold = eligibleMemberCount;
                } else {
                    successThreshold = Integer.parseInt(rplQuorumValue);
                }
                if (successThreshold > eligibleMemberCount) {
                    String errorMsg = String.format(
                            "Requested quorum %d is larger than member count %d",
                            successThreshold, eligibleMemberCount);
                    throw new IllegalArgumentException(errorMsg);
                }
                outboundOp.getRequestHeaders().remove(Operation.REPLICATION_QUORUM_HEADER);
            } catch (Throwable e) {
                outboundOp.setRetryCount(0).fail(e);
                return;
            }

            replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold);
            return;
        }

        // replicate using OWNER_SELECTION
        String location = selfNode.customProperties.get(NodeState.PROPERTY_NAME_LOCATION);
        if (location != null) {
            // replicate using quorum among nodes in the same location
            int localNodes = getNodesInLocation(location, selectedNodes);
            int quorum = (localNodes / 2) + 1;
            successThreshold = Math.min(eligibleMemberCount, quorum);
            logInfo("replicating %s (op id %d) to %d nodes with location %s quorum %d",
                    outboundOp.getAction(), outboundOp.getId(), selectedNodes.size(), location,
                    successThreshold);
            replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold, location,
                    localNodes);
            return;
        }

        // no location - replicate using membership quorum
        successThreshold = Math.min(eligibleMemberCount, selfNode.membershipQuorum);
        replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold);
    }

    /**
     * Returns the number of nodes in the specified location
     */
    private int getNodesInLocation(String location,
            Collection<NodeState> nodes) {
        return (int) nodes.stream()
                .filter(ns -> Objects.equals(location,
                        ns.customProperties.get(NodeState.PROPERTY_NAME_LOCATION)))
                .peek(ns -> logInfo("Node in location %s: %s (groupReference: %s)", location,
                        ns.id, ns.groupReference))
                .count();
    }

    /**
     * Returns true if the specified response is from one of the specified nodes
     * in the spcified location
     */
    private boolean isReponseFromLocation(Operation remotePeerResponse, String location,
            Collection<NodeState> nodes) {
        if (remotePeerResponse == null) {
            logInfo("isResponseFromLocation remotePeerService: null, location: %s, rc: true",
                    location);
            return true;
        }

        URI remotePeerService = remotePeerResponse.getUri();
        boolean rc = !nodes.stream()
                .filter(ns -> Objects.equals(location,
                        ns.customProperties.get(NodeState.PROPERTY_NAME_LOCATION)))
                .filter(ns -> ns.groupReference.getHost().equals(remotePeerService.getHost())
                        && ns.groupReference.getPort() == remotePeerService.getPort())
                .collect(Collectors.toList()).isEmpty();
        logInfo("%s remotePeerService: %s (op id: %d), location: %s, rc: %b",
                remotePeerResponse.getAction(), remotePeerService, remotePeerResponse.getId(),
                location, rc);
        return rc;
    }

    private void replicateUpdateToNodes(Operation outboundOp,
            Collection<NodeState> nodes,
            int successThreshold) {
        replicateUpdateToNodes(outboundOp, nodes, successThreshold, null, 0);
    }

    /**
     * Issues update to specified nodes and completes/fails the original
     * operation based on the specified success threshold. If location
     * is not null, then successThreshold applies to nodes within the same
     * location as the owner.
     */
    private void replicateUpdateToNodes(Operation outboundOp,
            Collection<NodeState> nodes,
            int successThreshold,
            String location,
            int nodesInLocation) {
        int eligibleMemberCount = nodes.size();
        final int successThresholdFinal = successThreshold;
        boolean locationQuorum = location != null;
        final int failureThresholdFinal = locationQuorum ? nodesInLocation - successThreshold
                : eligibleMemberCount - successThreshold;
        int[] completionCounts = new int[4];

        CompletionHandler c = (o, e) -> {
            if (e == null && o != null
                    && o.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                e = new IllegalStateException("Request failed: " + o.toString());
            }

            int sCount = completionCounts[0];
            int fCount = completionCounts[1];
            int sLocationCount = completionCounts[2];
            int fLocationCount = completionCounts[3];
            synchronized (outboundOp) {
                if (e != null) {
                    completionCounts[1] = completionCounts[1] + 1;
                    fCount = completionCounts[1];
                    if (locationQuorum && isReponseFromLocation(o, location, nodes)) {
                        completionCounts[3] = completionCounts[3] + 1;
                        fLocationCount = completionCounts[3];
                    }
                } else {
                    completionCounts[0] = completionCounts[0] + 1;
                    sCount = completionCounts[0];
                    if (locationQuorum && isReponseFromLocation(o, location, nodes)) {
                        completionCounts[2] = completionCounts[2] + 1;
                        sLocationCount = completionCounts[2];
                    }
                }
            }

            if (e != null && o != null) {
                logWarning("Replication request to %s failed with %d, %s",
                        o.getUri(), o.getStatusCode(), e.getMessage());
                // Preserve the status code from latest failure. We do not have a mechanism
                // to report different failure codes, per operation.
                outboundOp.setStatusCode(o.getStatusCode());
            }

            if ((!locationQuorum && sCount == successThresholdFinal)
                    || (locationQuorum && sLocationCount == successThresholdFinal)) {
                outboundOp.setStatusCode(Operation.STATUS_CODE_OK).complete();
                return;
            }

            if (fCount == 0) {
                return;
            }

            if (!locationQuorum && (fCount > failureThresholdFinal
                    || ((fCount + sCount) == eligibleMemberCount)) ||
                    (locationQuorum && (fLocationCount > failureThresholdFinal
                            || ((fLocationCount + sLocationCount) == nodesInLocation)))) {
                String error = String
                        .format("%s to %s failed. Success: %d,  Fail: %d, quorum: %d, failure threshold: %d",
                                outboundOp.getAction(),
                                outboundOp.getUri().getPath(),
                                locationQuorum ? sLocationCount : sCount,
                                locationQuorum ? fLocationCount : fCount,
                                successThresholdFinal,
                                failureThresholdFinal);
                logWarning("%s", error);
                outboundOp.fail(new IllegalStateException(error));
            }
        };

        String path = outboundOp.getUri().getPath();
        String query = outboundOp.getUri().getQuery();

        Operation update = Operation.createPost(null)
                .setAction(outboundOp.getAction())
                .setRetryCount(1)
                .setExpiration(outboundOp.getExpirationMicrosUtc())
                .transferRefererFrom(outboundOp);
        update.setCompletion(c);

        String pragmaHeader = outboundOp.getRequestHeader(Operation.PRAGMA_HEADER);
        if (pragmaHeader != null && !Operation.PRAGMA_DIRECTIVE_FORWARDED.equals(pragmaHeader)) {
            update.addRequestHeader(Operation.PRAGMA_HEADER, pragmaHeader);
            update.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED);
        }

        String commitHeader = outboundOp.getRequestHeader(Operation.REPLICATION_PHASE_HEADER);
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

        ServiceClient cl = getHost().getClient();
        String selfId = getHost().getId();

        // trigger completion once, for self node, since its part of our accounting
        c.handle(null, null);

        for (NodeState m : nodes) {
            if (m.id.equals(selfId)) {
                continue;
            }

            if (m.options.contains(NodeOption.OBSERVER)) {
                continue;
            }

            try {
                URI remoteHost = m.groupReference;
                URI remotePeerService = new URI(remoteHost.getScheme(),
                        null, remoteHost.getHost(), remoteHost.getPort(),
                        path, query, null);
                update.setUri(remotePeerService);
            } catch (Throwable e1) {
            }

            if (NodeState.isUnAvailable(m)) {
                update.setStatusCode(Operation.STATUS_CODE_FAILURE_THRESHOLD);
                c.handle(update, new IllegalStateException("node is not available"));
                continue;
            }

            logInfo("Node %s: Replicating %s (original op id: %d, replicated op id: %d) to node %s",
                    selfId, update.getAction(), outboundOp.getId(), update.getId(), m.id);
            cl.send(update);
        }
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
