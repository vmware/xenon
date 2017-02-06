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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

public class OperationPriorityQueue {

    public static OperationPriorityQueue create(int limit) {
        OperationPriorityQueue queue = new OperationPriorityQueue();
        queue.limit = limit;
        return queue;
    }

    public static class OperationComparator implements Serializable, Comparator<Operation> {

        private static final long serialVersionUID = 0L;

        @Override
        public int compare(Operation op1, Operation op2) {
            ServiceDocument sd1 = op1.getLinkedState();
            ServiceDocument sd2 = op2.getLinkedState();

            if (sd1.documentEpoch < sd2.documentEpoch) {
                return -1;
            } else if (sd1.documentEpoch > sd2.documentEpoch) {
                return 1;
            }

            if (sd1.documentVersion < sd2.documentVersion) {
                return -1;
            } else if (sd1.documentVersion > sd2.documentVersion) {
                return -1;
            }

            return 0;
        }
    }

    private static final OperationComparator COMPARATOR = new OperationComparator();

    /**
     * Maximum number of elements in the queue.
     */
    private int limit;

    /**
     * Number of elements currently in the queue.
     */
    private int elementCount;

    /**
     * Underlying storage for the operation queue.
     */
    private PriorityQueue<Operation> store = new PriorityQueue<>(COMPARATOR);

    public void clear() {
        this.store.clear();
        this.elementCount = 0;
    }

    public boolean isEmpty() {
        return this.store.isEmpty();
    }

    public boolean offer(Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("op is required");
        }

        if (op.getLinkedState() == null) {
            throw new IllegalArgumentException("op must have linked state");
        }

        if (this.elementCount >= this.limit) {
            return false;
        }

        this.store.offer(op);
        this.elementCount++;
        return true;
    }

    public Operation peek() {
        return this.store.peek();
    }

    public boolean remove(Operation op) {
        if (op == null) {
            throw new IllegalArgumentException("op is required");
        }

        boolean result = this.store.remove(op);
        if (result) {
            this.elementCount--;
            if (this.elementCount < 0) {
                throw new IllegalStateException("elementCount is negative");
            }
        }

        return result;
    }

    public Collection<Operation> toCollection() {
        ArrayList<Operation> clone = new ArrayList<>(this.elementCount);
        clone.addAll(this.store);
        return clone;
    }
}
