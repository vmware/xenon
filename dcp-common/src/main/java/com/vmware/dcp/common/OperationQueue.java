/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

import java.util.ArrayDeque;
import java.util.EnumSet;

/**
 * Queue implementation customized for the needs of a service. Depending on creation options
 * it will act as a limited capacity {@code Deque} with either FIFO or LIFO behavior.
 * The queue is not thread safe and should be sued within a synchronized context
 */
class OperationQueue {

    public enum Option {
        LIFO, FIFO, EVICT
    }

    public static OperationQueue create(int limit, EnumSet<Option> options) {
        OperationQueue opDeque = new OperationQueue();
        opDeque.limit = limit;
        opDeque.options = options;
        return opDeque;
    }

    private int limit;

    private EnumSet<Option> options;

    private ArrayDeque<Operation> store = new ArrayDeque<>(1);

    private OperationQueue() {
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    EnumSet<Option> getOptions() {
        return this.options;
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

        if (store.size() >= limit) {
            return false;
        }

        return store.offer(op);
    }

    /**
     * Retrieves and removes an operation. The operation is removed from the head of the
     * queue if the queue is configured as FIFO, otherwise its removed from the tail
     */
    public Operation poll() {
        return store.poll();
    }

}