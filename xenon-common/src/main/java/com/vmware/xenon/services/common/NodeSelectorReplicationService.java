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

        if (options.contains(ServiceOption.ENFORCE_QUORUM)
                && localNode.membershipQuorum > members.size()) {
            outboundOp.fail(new IllegalStateException("Not enough peers: " + members.size()));
            return;
        }

        if (members.size() == 1) {
            outboundOp.complete();
            return;
        }

        AtomicInteger sentCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();

        // When quorum is not required, succeed when we replicate to at least one remote node,
        // or, if only local node is available, succeed immediately.
        int successThreshold = Math.min(2, members.size() - 1);
        int failureThreshold = members.size();

        if (options.contains(ServiceOption.ENFORCE_QUORUM)) {
            failureThreshold = members.size() - localNode.membershipQuorum;
            successThreshold = Math.max(2, localNode.membershipQuorum);
        }

        final int successThresholdFinal = successThreshold;
        final int failureThresholdFinal = failureThreshold;

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

            if (sCount == successThresholdFinal) {
                outboundOp.complete();
                return;
            }

            if (failureCount.get() > 0) {
                String error = String.format(
                        "request %d failed. Fail count: %d,  sent count: %d, quorum: %d",
                        outboundOp.getId(),
                        failureCount.get(),
                        sentCount.get(),
                        localNode.membershipQuorum);
                logWarning("%s", error);
                if (failureCount.get() >= failureThresholdFinal || failureCount.get() >= sentCount.get()) {
                    outboundOp.fail(new IllegalStateException(error));
                }
            }
        };

        Operation update = Operation.createPost(null)
                .setAction(outboundOp.getAction())
                .setBodyNoCloning(req.linkedState)
                .setCompletion(c)
                .setRetryCount(1)
                .setExpiration(outboundOp.getExpirationMicrosUtc())
                .transferRequestHeadersFrom(outboundOp)
                .removePragmaDirective(Operation.PRAGMA_DIRECTIVE_FORWARDED)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_REPLICATED)
                .setReferer(outboundOp.getReferer());

        if (update.getCookies() != null && !update.getCookies().isEmpty()) {
            update.getCookies().clear();
        }

        for (NodeState m : rsp.selectedNodes) {
            if (m.id.equals(getHost().getId())) {
                c.handle(null, null);
                continue;
            }

            if (m.options.contains(NodeOption.OBSERVER)) {
                continue;
            }

            try {
                URI remotePeerService = new URI(m.groupReference.getScheme(),
                        null, m.groupReference.getHost(), m.groupReference.getPort(),
                        outboundOp.getUri().getPath(), outboundOp.getUri().getQuery(), null);
                update.setUri(remotePeerService);
            } catch (Throwable e1) {
            }

            if (NodeState.isUnAvailable(m)) {
                c.handle(update, new IllegalStateException("node is not available"));
                continue;
            }
            sentCount.incrementAndGet();
            getHost().getClient().send(update);
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
