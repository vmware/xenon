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

import java.util.ArrayDeque;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

public class RoundRobinOperationQueue {

    public static final int INITIAL_CAPACITY = 256;

    public static RoundRobinOperationQueue create() {
        return new RoundRobinOperationQueue();
    }

    private NavigableMap<String, Queue<Operation>> queues = new ConcurrentSkipListMap<>();
    private String activeKey = "";

    /**
     * Queue the operation on the queue associated with the key
     */
    public boolean offer(String key, Operation op) {
        if (key == null || op == null) {
            throw new IllegalArgumentException("key and operation are required");
        }
        Queue<Operation> q = this.queues.computeIfAbsent(key, (k) -> {
            return new ArrayDeque<>(INITIAL_CAPACITY);
        });
        if (!q.offer(op)) {
            op.setStatusCode(Operation.STATUS_CODE_UNAVAILABLE);
            op.fail(new CancellationException("queue limit exceeded"));
            return false;
        }

        return true;
    }

    /**
     * Determines the active queue, and polls it for an operation. If no operation
     * is found the queue is removed from the map of queues and the methods proceeds
     * to check the next queue, until an operation is found.
     * If all queues are empty and no operation was found, the method returns null
     */
    public Operation poll() {
        while (!this.queues.isEmpty()) {
            Entry<String, Queue<Operation>> nextActive = this.queues
                    .higherEntry(this.activeKey);
            if (nextActive == null) {
                nextActive = this.queues.firstEntry();
            }
            this.activeKey = nextActive.getKey();
            Operation op = nextActive.getValue().poll();
            if (op != null) {
                return op;
            }
            // queue is empty, remove from active map
            this.queues.remove(nextActive.getKey());
        }
        Logger.getAnonymousLogger()
                .warning("No available operations found across all query queues");
        return null;
    }

    public boolean isEmpty() {
        return this.queues.isEmpty();
    }
}