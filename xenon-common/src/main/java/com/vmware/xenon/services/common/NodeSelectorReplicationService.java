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

    private Service parent;

    public NodeSelectorReplicationService(Service parent) {
        this.parent = parent;
        super.setHost(parent.getHost());
        super.setSelfLink(UriUtils.buildUriPath(parent.getSelfLink(),
                ServiceHost.SERVICE_URI_SUFFIX_REPLICATION));
        super.setProcessingStage(ProcessingStage.AVAILABLE);
    }


    /**
     * Issues updates to peer nodes, after a local update has been accepted. If the service support
     * OWNER_SELECTION the replication message is the Propose message in the consensus work flow.
     * @param localState
     * @param outboundOp
     * @param req
     * @param rsp
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

        int[] completionCounts = new int[2];

        // The eligible count can be less than the member count if the parent node selector has
        // a smaller replication factor than group size. We need to use the replication factor
        // as the upper bound for calculating success and failure thresholds
        int eligibleMemberCount = rsp.selectedNodes.size();

        // When quorum is not required, succeed when we replicate to at least one remote node,
        // or, if only local node is available, succeed immediately.
        int successThreshold = Math.min(2, eligibleMemberCount - 1);
        int failureThreshold = eligibleMemberCount - successThreshold;

        if (req.serviceOptions.contains(ServiceOption.OWNER_SELECTION)) {
            successThreshold = Math.min(eligibleMemberCount, selfNode.membershipQuorum);
            failureThreshold = eligibleMemberCount - successThreshold;
        }

        String rplQuorumValue = outboundOp.getRequestHeader(Operation.REPLICATION_QUORUM_HEADER);
        if (rplQuorumValue != null) {
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
                failureThreshold = eligibleMemberCount - successThreshold;
                outboundOp.getRequestHeaders().remove(Operation.REPLICATION_QUORUM_HEADER);
            } catch (Throwable e) {
                outboundOp.fail(e);
                return;
            }
        }

        final int successThresholdFinal = successThreshold;
        final int failureThresholdFinal = failureThreshold;

        CompletionHandler c = (o, e) -> {
            if (e == null && o != null
                    && o.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                e = new IllegalStateException("Request failed: " + o.toString());
            }

            int sCount = completionCounts[0];
            int fCount = completionCounts[1];
            synchronized (outboundOp) {
                if (e != null) {
                    completionCounts[1] = completionCounts[1] + 1;
                    fCount = completionCounts[1];
                } else {
                    completionCounts[0] = completionCounts[0] + 1;
                    sCount = completionCounts[0];
                }
            }

            if (sCount == successThresholdFinal) {
                outboundOp.complete();
                return;
            }

            if (fCount == 0) {
                return;
            }

            if (fCount > failureThresholdFinal || ((fCount + sCount) == memberCount)) {
                String error = String
                        .format("%s to %s failed. Success: %d,  Fail: %d, quorum: %d, threshold: %d",
                                outboundOp.getAction(),
                                outboundOp.getUri().getPath(),
                                sCount,
                                fCount,
                                selfNode.membershipQuorum,
                                failureThresholdFinal);
                logWarning("%s", error);
                outboundOp.fail(new IllegalStateException(error));
            }
        };

        String jsonBody = Utils.toJson(req.linkedState);
        String path = outboundOp.getUri().getPath();
        String query = outboundOp.getUri().getQuery();

        Operation update = Operation.createPost(null)
                .setAction(outboundOp.getAction())
                .setBodyNoCloning(jsonBody)
                .setCompletion(c)
                .setRetryCount(1)
                .setExpiration(outboundOp.getExpirationMicrosUtc())
                .transferRefererFrom(outboundOp);

        update.setFromReplication(true);
        update.setConnectionSharing(true);

        // Only use replication tag on HTTP + HTTP/2. On HTTPS, we want to use the default HTTP1.1
        // connection tag, which allows for a lot more concurrent connections. This will change
        // once we have TLS/APLN support for HTTP/2
        if (selfNode.groupReference.getScheme().equals(UriUtils.HTTP_SCHEME)) {
            update.setConnectionTag(ServiceClient.CONNECTION_TAG_REPLICATION);
        }

        if (update.getCookies() != null) {
            update.getCookies().clear();
        }

        ServiceClient cl = getHost().getClient();
        String selfId = getHost().getId();

        // trigger completion once, for self node, since its part of our accounting
        c.handle(null, null);

        for (NodeState m : rsp.selectedNodes) {
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
                c.handle(update, new IllegalStateException("node is not available"));
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
