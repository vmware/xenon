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
import java.util.concurrent.atomic.AtomicInteger;

import com.vmware.xenon.common.NodeSelectorService.SelectAndForwardRequest;
import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

public class NodeSelectorReplicationService extends StatelessService {

    private Service parent;

    public NodeSelectorReplicationService(Service parent) {
        this.parent = parent;
        super.setHost(parent.getHost());
        super.setSelfLink(UriUtils.buildUriPath(parent.getSelfLink(),
                ServiceHost.SERVICE_URI_SUFFIX_REPLICATION));
        super.setProcessingStage(ProcessingStage.AVAILABLE);
    }

    @Override
    public void handleRequest(Operation op) {
        final String header = Operation.REPLICATION_TARGET_HEADER;
        String path = op.getRequestHeader(header);
        if (path == null) {
            op.fail(new IllegalArgumentException(header + " is required"));
            return;
        }

        handleRemoteUpdate(op.setUri(UriUtils.buildUri(getHost(), path,
                op.getUri().getQuery())));
    }

    /**
     * Issues updates to peer nodes, after a local update has been accepted. If the service support
     * OWNER_SELECTION the replication message is the Propose message in the consensus work flow.
     *
     * @param localState
     * @param localNodeOptions
     * @param outboundOp
     */
    void replicateUpdate(NodeGroupState localState,
            Operation outboundOp, SelectAndForwardRequest req, SelectOwnerResponse rsp) {

        Collection<NodeState> members = localState.nodes.values();
        NodeState localNode = localState.nodes.get(getHost().getId());
        AtomicInteger successCount = new AtomicInteger(0);

        if (members.size() == 1) {
            if (options.contains(ServiceOption.EAGER_CONSISTENCY)
                    && localNode.membershipQuorum > 1) {
                outboundOp.fail(new IllegalStateException("No available peers: " + members.size()));
            } else {
                outboundOp.complete();
            }
            return;
        }

        AtomicInteger requestsSent = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        int failureThreshold = members.size() - localNode.membershipQuorum;
        int successThreshold = Math.max(2, localNode.membershipQuorum);

        CompletionHandler c = (o, e) -> {
            if (e == null && o != null
                    && o.getStatusCode() >= Operation.STATUS_CODE_FAILURE_THRESHOLD) {
                e = new IllegalStateException("Request failed: " + o.toString());
            }
            int sCount = 0;
            if (e != null) {
                logInfo("Request failed for %s: %s", o.getUri(), e.toString());
                failureCount.incrementAndGet();
            } else {
                sCount = successCount.incrementAndGet();
            }

            if (sCount == successThreshold) {
                outboundOp.complete();
                return;
            }

            if (failureCount.get() > 0) {
                String error = String.format(
                        "request %d failed. Fail count: %d,  sent count: %d, quorum: %d",
                        outboundOp.getId(),
                        failureCount.get(),
                        requestsSent.get(),
                        localNode.membershipQuorum);
                logWarning("%s", error);
                if (failureCount.get() >= failureThreshold) {
                    outboundOp.fail(new IllegalStateException(error));
                }
            }
        };

        String body = Utils.toJson(req.linkedState);
        String phase = outboundOp.getRequestHeaders().get(Operation.REPLICATION_PHASE_HEADER);
        Operation update = Operation
                .createPost(null)
                .setAction(outboundOp.getAction())
                .setBodyNoCloning(body)
                .setCompletion(c)
                .setRetryCount(1)
                .setExpiration(outboundOp.getExpirationMicrosUtc())
                .addRequestHeader(Operation.REPLICATION_TARGET_HEADER,
                        outboundOp.getUri().getPath())
                .transferRequestHeadersFrom(outboundOp)
                .setReferer(outboundOp.getReferer());

        if (phase != null) {
            update.addRequestHeader(Operation.REPLICATION_PHASE_HEADER, phase);
        }

        for (NodeState m : rsp.selectedNodes) {
            if (NodeState.isUnAvailable(m) || m.id.equals(getHost().getId())) {
                c.handle(null, null);
                continue;
            }

            URI remoteGroupReplicationService = UriUtils.buildUri(m.groupReference.getScheme(),
                    m.groupReference.getHost(), m.groupReference.getPort(), getSelfLink(),
                    outboundOp.getUri().getQuery());
            update.setUri(remoteGroupReplicationService);
            requestsSent.incrementAndGet();

            getHost().getClient().send(update);
        }
    }

    private void handleRemoteUpdate(Operation op) {
        CompletionHandler c = (o, e) -> {
            if (e != null) {
                op.setStatusCode(o.getStatusCode()).fail(e);
                return;
            }

            op.setBody(null).complete();
        };

        op.nestCompletion(c);
        // Use the client directly so the referrer is not overwritten by the
        // super.sendRequest() method
        getHost().getClient().send(op);
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
