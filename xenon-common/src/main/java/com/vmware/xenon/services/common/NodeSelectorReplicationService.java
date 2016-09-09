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
        Collection<NodeState> selectedNodes = rsp.selectedNodes;
        int eligibleMemberCount = selectedNodes.size();

        // location is usually null, unless set explicitly
        String location = getHost().getLocation();

        // success threshold is determined based on the following precedence:
        // 1. request replication quorum header (if exists)
        // 2. group membership quorum (in case of OWNER_SELECTION)
        // 3. at least one remote node (in case one exists)
        int successThreshold;
        int failureThreshold;

        String rplQuorumValue = outboundOp.getRequestHeader(Operation.REPLICATION_QUORUM_HEADER);
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

            failureThreshold = (eligibleMemberCount - successThreshold) + 1;
            replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold, failureThreshold,
                    location);
            return;
        }

        if (req.serviceOptions.contains(ServiceOption.OWNER_SELECTION)) {
            // replicate using group membership quorum
            if (location == null) {
                successThreshold = Math.min(eligibleMemberCount, selfNode.membershipQuorum);
                failureThreshold = (eligibleMemberCount - successThreshold) + 1;
            } else {
                int localNodeCount = getNodeCountInLocation(location, selectedNodes);
                successThreshold = Math.min(localNodeCount, selfNode.membershipQuorum);
                failureThreshold = (localNodeCount - successThreshold) + 1;
            }
            replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold, failureThreshold,
                    location);
            return;
        }

        // When quorum is not required, succeed when we replicate to at least one remote node,
        // or, if only local node is available, succeed immediately.
        successThreshold = Math.min(2, eligibleMemberCount - 1);
        failureThreshold = (eligibleMemberCount - successThreshold) + 1;
        replicateUpdateToNodes(outboundOp, selectedNodes, successThreshold, failureThreshold,
                location);
    }

    /**
     * Returns the number of nodes in the specified location
     */
    private int getNodeCountInLocation(String location,
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
     * in the specified location
     */
    private boolean isResponseFromLocation(Operation remotePeerResponse, String location,
            Collection<NodeState> nodes) {
        if (remotePeerResponse == null) {
            return true;
        }

        URI remotePeerService = remotePeerResponse.getUri();
        return !nodes.stream()
                .filter(ns -> Objects.equals(location,
                        ns.customProperties.get(NodeState.PROPERTY_NAME_LOCATION)))
                .filter(ns -> ns.groupReference.getHost().equals(remotePeerService.getHost())
                        && ns.groupReference.getPort() == remotePeerService.getPort())
                .collect(Collectors.toList()).isEmpty();
    }

    private void replicateUpdateToNodes(Operation outboundOp,
            Collection<NodeState> nodes,
            int successThreshold,
            int failureThreshold,
            String location) {
        final int successThresholdFinal = successThreshold;
        final int failureThresholdFinal = failureThreshold;
        // Index 0 - success count
        // index 1 - failure count
        // index 2 - most recent failure status
        int[] countsAndStatus = new int[3];

        CompletionHandler c = (o, e) -> {
            // if location is set we require success from nodes in the same location;
            // all other responses are ignored for success calculation purposes
            if (location != null && !isResponseFromLocation(o, location, nodes)) {
                return;
            }

            if (e == null && o != null
                    && o.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                e = new IllegalStateException("Request failed: " + o.toString());
            }

            int sCount = countsAndStatus[0];
            int fCount = countsAndStatus[1];
            boolean completeWithSuccess = false;
            boolean completeWithFailure = false;
            synchronized (outboundOp) {
                if (e != null) {
                    countsAndStatus[1] = countsAndStatus[1] + 1;
                    fCount = countsAndStatus[1];
                } else {
                    countsAndStatus[0] = countsAndStatus[0] + 1;
                    sCount = countsAndStatus[0];
                }
                // to avoid a AtomicBoolean and additional allocations, and make the completion
                // check atomic, do the count checks inside the synchronized block
                completeWithSuccess = sCount == successThresholdFinal;
                completeWithFailure = fCount == failureThresholdFinal;
            }

            if (completeWithSuccess) {
                // this code must only be called once.
                outboundOp.setStatusCode(Operation.STATUS_CODE_OK).complete();
                return;
            }

            if (e != null && o != null) {
                logWarning("(Original id: %d) Replication request to %s failed with %d, %s",
                        outboundOp.getId(),
                        o.getUri(), o.getStatusCode(), e.getMessage());
                countsAndStatus[2] = o.getStatusCode();
            }

            if (completeWithFailure) {
                String error = String
                        .format("(Original id: %d) %s to %s failed. Success: %d,  Fail: %d, quorum: %d, failure threshold: %d",
                                outboundOp.getId(),
                                outboundOp.getAction(),
                                outboundOp.getUri().getPath(),
                                sCount,
                                fCount,
                                successThresholdFinal,
                                failureThresholdFinal);
                logWarning("%s", error);
                outboundOp.setStatusCode(countsAndStatus[2]).fail(new IllegalStateException(error));
            }
        };

        String path = outboundOp.getUri().getPath();
        String query = outboundOp.getUri().getQuery();

        Operation update = Operation.createPost(null)
                .setAction(outboundOp.getAction())
                .setCompletion(c)
                .setRetryCount(1)
                .setExpiration(outboundOp.getExpirationMicrosUtc())
                .transferRefererFrom(outboundOp);

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
                int originalStatusCode = update.getStatusCode();
                update.setStatusCode(Operation.STATUS_CODE_FAILURE_THRESHOLD);
                c.handle(update, new IllegalStateException("node is not available"));
                // set status code to its original value, since we're using a
                // shared update operation object, and the completion handler
                // checks the status code.
                update.setStatusCode(originalStatusCode);
                continue;
            }

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
