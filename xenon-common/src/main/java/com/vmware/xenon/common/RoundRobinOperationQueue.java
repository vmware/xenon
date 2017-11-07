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

import static java.lang.String.format;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListMap;

public final class RoundRobinOperationQueue {

    private static final int INITIAL_CAPACITY = 256;

    private final NavigableMap<String, Queue<Operation>> queues = new ConcurrentSkipListMap<>();

    private String activeKey = "";

    private String description = "";

    private final int limit;

    private int totalCount;

    public RoundRobinOperationQueue(String description, int limit) {
        this.description = description;
        this.limit = limit;
    }

    /**
     * Queue the operation on the queue associated with the key
     */
    public synchronized boolean offer(String key, Operation op) {
        if (key == null || op == null) {
            throw new IllegalArgumentException(format("key and operation are required (%s)", this.description));
        }

        if (this.totalCount >= this.limit) {
            op.setStatusCode(Operation.STATUS_CODE_UNAVAILABLE);
            op.fail(new CancellationException(format("Limit for queue %s exceeded: %d", this.description, this.limit)));
            return false;
        }

        Queue<Operation> q = this.queues.computeIfAbsent(key, this::makeQueue);
        q.offer(op);
        this.totalCount++;

        return true;
    }

    private Queue<Operation> makeQueue(String key) {
        return new ArrayDeque<>(INITIAL_CAPACITY);
    }

    /**
     * Determines the active queue, and polls it for an operation. If no operation
     * is found the queue is removed from the map of queues and the methods proceeds
     * to check the next queue, until an operation is found.
     * If all queues are empty and no operation was found, the method returns null
     */
    public synchronized Operation poll() {
        while (!this.queues.isEmpty()) {
            Entry<String, Queue<Operation>> nextActive = this.queues.higherEntry(this.activeKey);
            if (nextActive == null) {
                nextActive = this.queues.firstEntry();
            }
            this.activeKey = nextActive.getKey();
            Operation op = nextActive.getValue().poll();
            if (op != null) {
                this.totalCount--;
                return op;
            } else {
                this.queues.remove(nextActive.getKey());
            }
        }

        return null;
    }

    public boolean isEmpty() {
        return this.queues.isEmpty();
    }

    public Map<String, Integer> sizesByKey() {
        Map<String, Integer> sizes = new HashMap<>();
        synchronized (this) {
            for (Entry<String, Queue<Operation>> queueEntry : this.queues.entrySet()) {
                sizes.put(queueEntry.getKey(), queueEntry.getValue().size());
            }
        }

        return sizes;
    }
}
