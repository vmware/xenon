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

    public enum Status {
        /*
         * NodeGroup is unavailable
         */
        UNAVAILABLE_STATUS,

        /*
         * NodeGroup is paused (queuing but not taking requests; otherwise active & alive)
         */
        PAUSED_STATUS,

        /*
         * NodeGroup is fully available
         */
        RUNNING_STATUS,
        ;

        // IS_RUNNING means that it is actively taking and routing requests (old isAvailable)
        public static final EnumSet<Status> IS_RUNNING = EnumSet
                .of(Status.RUNNING_STATUS);

        // IS_ONLINE means that it is up, alive, has quorum, etc and can handle (but not necessarily process) requests
        public static final EnumSet<Status> IS_ONLINE = EnumSet
                .of(Status.PAUSED_STATUS, Status.RUNNING_STATUS);

        // IS_BUSY means that it's PAUSED, or UNAVAILBLE
        public static final EnumSet<Status> IS_BUSY = EnumSet
                .of(Status.PAUSED_STATUS, Status.UNAVAILABLE_STATUS);
    }

    public String nodeGroupLink;
    public Long replicationFactor;
    public int membershipQuorum;
    public long membershipUpdateTimeMicros;
    public Status nodeSelectorGroupStatus;
}