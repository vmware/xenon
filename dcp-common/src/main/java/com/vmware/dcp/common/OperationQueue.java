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

package com.vmware.dcp.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Queue implementation customized for the needs of a service. Depending on creation options
 * it will act as a limited capacity {@code Deque} with either FIFO or LIFO behavior.
 * The queue is not thread safe and should be used within a synchronized context
 */
class OperationQueue {

    public static OperationQueue createFifo(int limit) {
        OperationQueue opDeque = new OperationQueue();
        opDeque.limit = limit;
        return opDeque;
    }

    public static OperationQueue createLifo(int limit) {
        OperationQueue opDeque = new OperationQueue();
        opDeque.limit = -limit;
        return opDeque;
    }

    /**
     * Maximum number of elements in the queue. The choice of FIFO or LIFO is encoded in the limit:
     * A positive limit indicates FIFO, a negative limit indicates LIFO
     */
    private int limit;

    private int elementCount;

    /**
     * Underlying storage for the operation queue. The choice of data structure is subject to
     * change but any changes will not be visible to the consumers of OperationQueue
     */
    private Deque<Operation> store = new ConcurrentLinkedDeque<>();

    private OperationQueue() {
    }

    public int getLimit() {
        return Math.abs(this.limit);
    }

    public void setLimit(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be greater than zero");
        }
        // we do not drain the queue if its beyond the limit, new operation will just
        // fail to enqueue until the queue depth drops below the limit
        this.limit = this.limit < 0 ? -limit : limit;
    }

    public boolean isEmpty() {
        return this.store.isEmpty();
    }

    /**
     * Adds an element to the queue if the limit has not been reached.
     * If the queue is configured to evict queued operations, the operation
     * will be added and the either the newest or oldest queued operation will
     * be failed with {@code CancellationException}
     */
    public boolean offer(Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("op is required");
        }

        // we use non restricted queue so we do not check for offer failures

        if (this.limit < 0) {
            // LIFO queue
            if (this.elementCount >= -this.limit) {
                return false;
            }

            this.store.offerFirst(op);
            this.elementCount++;
            return true;
        } else {
            // FIFO queue
            if (this.elementCount >= this.limit) {
                return false;
            }

            this.store.offerLast(op);
            this.elementCount++;
            return true;
        }
    }

    /**
     * Retrieves and removes an operation. The operation is removed from the head of the
     * queue if the queue is configured as FIFO, otherwise its removed from the tail
     */
    public Operation poll() {
        Operation op = this.store.poll();
        if (op == null) {
            return null;
        }
        this.elementCount--;
        if (this.elementCount < 0) {
            throw new IllegalStateException("elementCount is negative");
        }
        return op;
    }

    Collection<Operation> toCollection() {
        ArrayList<Operation> clone = new ArrayList<>(this.elementCount);
        for (Operation op : this.store) {
            clone.add(op);
        }
        return clone;
    }

    public void clear() {
        this.store.clear();
    }
}