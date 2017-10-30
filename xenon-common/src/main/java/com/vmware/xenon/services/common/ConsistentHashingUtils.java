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

import java.util.Collection;
import java.util.TreeMap;

import com.vmware.xenon.common.FNVHash;

/**
 * Uses consistent hashing to assign a client specified key to one
 * of the nodes in the node group. This service is associated with a specific node group
 */
public class ConsistentHashingUtils {
    public static final class ClosestNNeighbours extends TreeMap<Long, NodeState> {
        private static final long serialVersionUID = 0L;

        private final int maxN;

        ClosestNNeighbours(int maxN) {
            super(Long::compare);
            this.maxN = maxN;
        }

        @Override
        public NodeState put(Long key, NodeState value) {
            if (size() < this.maxN) {
                return super.put(key, value);
            } else {
                // only attempt to write if new key can displace one of the top N entries
                if (comparator().compare(key, this.lastKey()) <= 0) {
                    NodeState old = super.put(key, value);
                    if (old == null) {
                        // sth. was added, remove last
                        this.remove(this.lastKey());
                    }

                    return old;
                }

                return null;
            }
        }
    }

    private ConsistentHashingUtils() {

    }

    static ClosestNNeighbours getClosestNNeighbours(String key, Collection<NodeState> nodes) {
        ClosestNNeighbours closestNodes = new ClosestNNeighbours(nodes.size());

        long keyHash = FNVHash.compute(key);
        for (NodeState m : nodes) {
            if (NodeState.isUnAvailable(m)) {
                continue;
            }

            long distance = m.getNodeIdHash() - keyHash;
            distance *= distance;
            // We assume first key (smallest) will be one with closest distance. The hashing
            // function can return negative numbers however, so a distance of zero (closest) will
            // not be the first key. Take the absolute value to cover that case and create a logical
            // ring
            distance = Math.abs(distance);
            closestNodes.put(distance, m);
        }
        return closestNodes;
    }

    public static NodeState getOwner(String key, Collection<NodeState> nodes) {
        return getClosestNNeighbours(key, nodes).firstEntry().getValue();
    }
}
