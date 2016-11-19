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

package com.vmware.xenon.common;


import java.util.EnumSet;

import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.NodeGroupUtils;


public class NodeSelectorState extends ServiceDocument {

    public enum Status {
        UNAVAILABLE,
        PAUSED,
        AVAILABLE
    }

    public static final EnumSet<Status> UNAVAILABLE = EnumSet.of(Status.UNAVAILABLE);
    public static final EnumSet<Status> PAUSED = EnumSet.of(Status.PAUSED, Status.AVAILABLE);
    public static final EnumSet<Status> AVAILABLE = EnumSet.of(Status.AVAILABLE);
    public static final EnumSet<Status> PAUSED_UNAVAILABLE = EnumSet.of(Status.PAUSED, Status.UNAVAILABLE);

    /**
     * Calculates the status of the Node Selector by inspecting the NodeGroup and local
     * 'pause'/'resume' status.
     */
    public static Status calculateStatus(ServiceHost host, NodeGroupState groupState) {
        boolean isAvailable = NodeGroupUtils.isNodeGroupAvailable(host, groupState);
        return (isAvailable ? Status.AVAILABLE : Status.UNAVAILABLE);
    }

    public static void updateStatus(ServiceHost host,
            NodeGroupState groupState, NodeSelectorState ns) {
        ns.nodeSelectorStatus = calculateStatus(host, groupState);
    }

    public static boolean isAvailable(NodeSelectorState ns) {
        return AVAILABLE.contains(ns.nodeSelectorStatus);
    }

    public static boolean isAvailable(ServiceHost host, NodeGroupState groupState) {
        return AVAILABLE.contains(calculateStatus(host, groupState));
    }

    public String nodeGroupLink;
    public Long replicationFactor;
    public int membershipQuorum;
    public long membershipUpdateTimeMicros;
    public Status nodeSelectorStatus;
}