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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.vmware.xenon.common.FNVHash;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.Utils;

public class NodeState extends ServiceDocument {
    public static final String PROPERTY_NAME_MEMBERSHIP_QUORUM = Utils.PROPERTY_NAME_PREFIX
            + "NodeState.membershipQuorum";
    public static final String PROPERTY_NAME_LOCATION = Utils.PROPERTY_NAME_PREFIX
            + "NodeState.location";

    private transient  long nodeIdHash;

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
     * Minimum number of available nodes required for consensus operations
     * and synchronization
     */
    public int membershipQuorum;

    /**
     * Bag of additional properties, like {@link #PROPERTY_NAME_LOCATION}
     */
    public Map<String, String> customProperties = new HashMap<>();

    public static boolean isUnAvailable(NodeState ns) {
        return isUnAvailable(ns, NodeOption.OBSERVER);
    }

    public static boolean isUnAvailable(NodeState ns, NodeOption excludeNodeOption) {
        boolean unAvailable = ns.status == NodeStatus.UNAVAILABLE ||
                ns.status == NodeStatus.REPLACED;
        if (excludeNodeOption != null) {
            unAvailable |= ns.options.contains(excludeNodeOption);
        }
        return unAvailable;
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

    long getNodeIdHash() {
        // just like String::hashCode
        long h = this.nodeIdHash;
        if (h == 0) {
            h = FNVHash.compute(this.id);
            this.nodeIdHash = h;
        }

        return h;
    }
}
