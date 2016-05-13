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
import java.util.List;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.CheckConvergenceRequest;
import com.vmware.xenon.services.common.NodeGroupService.CheckConvergenceResponse;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

public class NodeGroupUtils {

    /**
     * Issues a broadcast GET to service/available on all nodes and returns success if at least one
     * service replied with status OK
     */
    public static void checkServiceAvailability(CompletionHandler ch, Service s) {
        checkServiceAvailability(ch, s.getHost(), s.getSelfLink(), s.getPeerNodeSelectorPath());
    }

    /**
     * Issues a broadcast GET to service/available on all nodes and returns success if at least one
     * service replied with status OK
     */
    public static void checkServiceAvailability(CompletionHandler ch, ServiceHost host,
            String link, String selectorPath) {
        if (link == null) {
            throw new IllegalArgumentException("link is required");
        }

        URI service = UriUtils.buildUri(host, link);
        checkServiceAvailability(ch, host, service, selectorPath);
    }

    /**
     * Issues a broadcast GET to service/available on all nodes and returns success if at least one
     * service replied with status OK
     */
    public static void checkServiceAvailability(CompletionHandler ch, ServiceHost host,
            URI service,
            String selectorPath) {
        URI available = UriUtils.buildAvailableUri(service);

        if (selectorPath == null) {
            throw new IllegalArgumentException("selectorPath is required");
        }
        // we are in multiple node mode, create a broadcast URI since replicated
        // factories will only be marked available on one node, the owner for the factory
        available = UriUtils.buildBroadcastRequestUri(available, selectorPath, service.getPath());

        Operation get = Operation.createGet(available).setCompletion((o, e) -> {
            if (e != null) {
                // the broadcast request itself failed
                ch.handle(null, e);
                return;
            }

            NodeGroupBroadcastResponse rsp = o.getBody(NodeGroupBroadcastResponse.class);
            // we expect at least one node to not return failure, when its factory is ready
            if (rsp.failures.size() < rsp.availableNodeCount) {
                ch.handle(o, null);
                return;
            }

            ch.handle(o, new IllegalStateException("All services on all nodes not available"));
        });
        host.sendRequest(get.setReferer(host.getPublicUri()));
    }

    /**
     * Issues a convergence request to the node group service on all peers and returns success
     * if all nodes confirm that they are converged (in terms of last membership update).
     * This method should be used when the supplied host is not part of the node group
     */
    public static void checkConvergenceFromAnyHost(ServiceHost host, NodeGroupState ngs,
            Operation parentOp) {
        checkConvergenceAcrossPeers(host, ngs, parentOp);
    }

    /**
     * Issues a convergence request to the node group service on all peers and returns success
     * if all nodes confirm that they are converged (in terms of last membership update).
     *
     * It is expected the supplied host is listed as a peer in node group state
     */
    public static void checkConvergence(ServiceHost host, NodeGroupState ngs, Operation parentOp) {
        NodeState self = ngs.nodes.get(host.getId());
        if (self == null) {
            parentOp.fail(new IllegalStateException("Self node is required"));
            return;
        }

        if (self.membershipQuorum == 1 && ngs.nodes.size() == 1) {
            parentOp.complete();
            return;
        }
        checkConvergenceAcrossPeers(host, ngs, parentOp);
    }

    private static void checkConvergenceAcrossPeers(ServiceHost host, NodeGroupState ngs,
            Operation parentOp) {
        JoinedCompletionHandler joinedCompletion = (ops, failures) -> {
            if (failures != null) {
                parentOp.fail(new IllegalStateException("At least one peer failed convergence"));
                return;
            }

            for (Operation peerOp : ops.values()) {
                CheckConvergenceResponse r = peerOp.getBody(CheckConvergenceResponse.class);
                if (!r.isConverged) {
                    String error = String.format("Peer %s is not converged", peerOp.getUri());
                    parentOp.fail(new IllegalStateException(error));
                    return;
                }
            }

            parentOp.complete();
        };

        List<Operation> ops = new ArrayList<>();
        for (NodeState ns : ngs.nodes.values()) {
            if (NodeState.isUnAvailable(ns)) {
                continue;
            }
            CheckConvergenceRequest peerReq = CheckConvergenceRequest
                    .create(ngs.membershipUpdateTimeMicros);
            Operation peerOp = Operation.createPost(ns.groupReference)
                    .transferRefererFrom(parentOp)
                    .setExpiration(parentOp.getExpirationMicrosUtc())
                    .setBodyNoCloning(peerReq);
            ops.add(peerOp);
        }

        if (ops.isEmpty()) {
            parentOp.fail(new IllegalStateException("no available nodes"));
            return;
        }

        OperationJoin.create(ops).setCompletion(joinedCompletion).sendWith(host);
    }

    public static void checkConvergence(ServiceHost host, URI nodegroupReference, Operation parentOp) {
        Operation.createGet(nodegroupReference)
                .transferRefererFrom(parentOp)
                .setCompletion((o, t) -> {
                    if (t != null) {
                        parentOp.fail(t);
                        return;
                    }
                    NodeGroupState ngs = o.getBody(NodeGroupState.class);
                    checkConvergenceAcrossPeers(host, ngs, parentOp);
                }).sendWith(host);
    }

    /**
     * Given a node group state, looks at the time the node group state changed. If
     * the group was updated within a certain number of maintenance interval, we
     * consider it unstable. The interval count can be set through a PATCH to
     * core/node-groups/<group>/config.
     *
     * See {@code NodeGroupConfig}
     */
    public static boolean isMembershipSettled(ServiceHost host, long maintIntervalMicros,
            NodeGroupState localState) {
        NodeState selfNode = localState.nodes.get(host.getId());
        if (selfNode == null) {
            return false;
        }

        if (localState.nodes.size() == 1 && selfNode.membershipQuorum == 1) {
            // single, stand-alone node, skip update time check
            return true;
        }

        long threshold = localState.membershipUpdateTimeMicros
                + localState.config.stableGroupMaintenanceIntervalCount
                        * maintIntervalMicros;

        if (Utils.getNowMicrosUtc() - threshold < 0) {
            return false;
        }
        return true;
    }

    /**
     * Given a node group state, and the value of NodeState.membershipQuorum for the
     * local host, it determines if enough nodes are available for the group as a whole,
     * to be considered available
     */
    public static boolean hasMembershipQuorum(ServiceHost host, NodeGroupState ngs) {
        NodeState selfNode = ngs.nodes.get(host.getId());
        if (selfNode == null) {
            return false;
        }

        int availableNodeCount = 0;
        if (ngs.nodes.size() == 1) {
            availableNodeCount = 1;
        } else {
            for (NodeState ns : ngs.nodes.values()) {
                if (!NodeState.isAvailable(ns, selfNode.id, false)) {
                    continue;
                }
                availableNodeCount++;
            }
        }

        if (availableNodeCount < selfNode.membershipQuorum) {
            return false;
        }
        return true;
    }

    /**
     * Evaluates current node group state and returns true if requests should be process,
     * forwarded, etc
     * The conditions are:
     *
     * 1) The node group membership should have been stable (gossip did not produce
     * changes) for a specific period
     *
     * 2) The number of node group members in available stage is >= membershipQuorum
     */
    public static boolean isNodeGroupAvailable(ServiceHost host, NodeGroupState localState) {
        // we invoke the isMembershipSettled first because it has low overhead, does not walk all
        // nodes in the group
        if (NodeGroupUtils.isMembershipSettled(host, host
                .getMaintenanceIntervalMicros(), localState)
                && NodeGroupUtils.hasMembershipQuorum(host, localState)) {
            return true;
        }
        return false;
    }

}