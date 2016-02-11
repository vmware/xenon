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

import java.util.ArrayList;
import java.util.List;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.OperationJoin.JoinedCompletionHandler;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.CheckConvergenceRequest;
import com.vmware.xenon.services.common.NodeGroupService.CheckConvergenceResponse;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;

public class NodeGroupUtils {

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
                    .setReferer(parentOp.getReferer())
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