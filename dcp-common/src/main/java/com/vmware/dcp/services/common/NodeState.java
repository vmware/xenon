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

package com.vmware.dcp.services.common;

import java.net.URI;
import java.util.EnumSet;

import com.vmware.dcp.common.ServiceDocument;

public class NodeState extends ServiceDocument {
    public enum NodeStatus {
        /**
         * Node status is unknown
         */
        UNKNOWN,

        /**
         * Node is not available and is not responding to membership update requests
         */
        UNAVAILABLE,

        /**
         * Node is marked as healthy, it has successfully reported membership during the last update
         * interval
         */
        AVAILABLE,

        /**
         * Node is healthy but synchronizing state with its peers.
         */
        SYNCHRONIZING,

        /**
         * Node has been replaced by a new node, listening on the same IP address and port and is no
         * longer active
         */
        REPLACED
    }

    public enum NodeOption {
        PEER, OBSERVER
    }

    public static final EnumSet<NodeOption> DEFAULT_OPTIONS = EnumSet.of(NodeOption.PEER);

    /**
     * Public URI to this node group service
     */
    public URI groupReference;

    /**
     * Current node status
     */
    public NodeStatus status = NodeStatus.UNKNOWN;

    /**
     * Set of options that guide node behavior in the node group
     */
    public EnumSet<NodeOption> options = DEFAULT_OPTIONS;

    /**
     * Node unique identifier, should match {@code ServiceHost} identifier
     */
    public String id;

    /**
     * Minimum number of available nodes required to accept a proposal for replicated updates to
     * succeed
     */
    public int membershipQuorum = 1;

    /**
     * Minimum number of available nodes, including self, before synchronization with other nodes starts
     */
    public int synchQuorum = 1;

    public static boolean isUnAvailable(NodeState ns) {
        return ns.status == NodeStatus.UNAVAILABLE || ns.status == NodeStatus.REPLACED
                || ns.options.contains(NodeOption.OBSERVER);
    }

    public static boolean isAvailable(NodeState m, String hostId, boolean excludeThisHost) {
        if (m.status != NodeStatus.AVAILABLE) {
            return false;
        }
        if (excludeThisHost && m.id.equals(hostId)) {
            return false;
        }
        return true;
    }

}