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
    public static Status calculateStatus(ServiceHost host, NodeGroupState localState) {
        boolean isAvailable = NodeGroupUtils.isNodeGroupAvailable(host, localState);
        return (isAvailable ? Status.AVAILABLE : Status.UNAVAILABLE);
    }

    public static void updateStatus(NodeSelectorState ns, ServiceHost host, NodeGroupState localState) {
        ns.nodeSelectorStatus = calculateStatus(host, localState);
    }

    public static boolean isAvailable(NodeSelectorState ns) {
        return AVAILABLE.contains(ns.nodeSelectorStatus);
    }

    public static boolean isAvailable(ServiceHost host, NodeGroupState localstate) {
        return AVAILABLE.contains(calculateStatus(host, localstate));
    }

    public String nodeGroupLink;
    public Long replicationFactor;
    public int membershipQuorum;
    public long membershipUpdateTimeMicros;
    public Status nodeSelectorStatus;
}