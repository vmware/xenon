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

    static boolean isMembershipSettled(ServiceHost host, long maintIntervalMicros,
            NodeGroupState localState) {
        NodeState selfNode = localState.nodes.get(host.getId());
        if (selfNode == null) {
            return false;
        }

        long threshold = localState.membershipUpdateTimeMicros
                + localState.config.stableGroupMaintenanceIntervalCount
                        * maintIntervalMicros;

        if (Utils.getNowMicrosUtc() - threshold < 0) {
            return false;
        }
        return true;
    }
}