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

public class NodeSelectorState extends ServiceDocument {

    public enum GroupStatus {
        /*
         * NodeGroup is unavailable
         */
        UNAVAILABLE,

        /*
         * NodeGroup is paused (queuing but not taking requests; otherwise active & alive)
         */
        PAUSED,

        /*
         * NodeGroup is fully available
         */
        RUNNING,
        ;

        // IS_RUNNING means that it is actively taking and routing requests
        public static final EnumSet<GroupStatus> IS_RUNNING = EnumSet
                .of(GroupStatus.RUNNING);

        // IS_ONLINE means that it is up, alive, has quorum, etc and can handle (but not necessarily process) requests
        public static final EnumSet<GroupStatus> IS_ONLINE = EnumSet
                .of(GroupStatus.PAUSED, GroupStatus.RUNNING);

        // GROUPSTATUS_QUEUEING means that it is up and queueing requests
        public static final EnumSet<GroupStatus> IS_BUSY = EnumSet
                .of(GroupStatus.PAUSED, GroupStatus.UNAVAILABLE);
    }

    public String nodeGroupLink;
    public Long replicationFactor;
    public int membershipQuorum;
    public long membershipUpdateTimeMicros;
    public GroupStatus nodeSelectorGroupStatus;
}