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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

public class TestOperationQueue {

    public int count = 10000;

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }

    @Test
    public void fifoOfferAndPoll() {
        offerAndPoll(true);
    }

    @Test
    public void lifoOfferAndPoll() {
        offerAndPoll(false);
    }

    private void offerAndPoll(boolean isFifo) {
        OperationQueue q = createQueue(isFifo);

        try {
            q.offer(null);
            throw new IllegalStateException("null offer should have failed");
        } catch (IllegalArgumentException e) {

        }

        assertEquals(this.count, q.getLimit());

        assertTrue(q.isEmpty());

        List<Operation> ops = new ArrayList<>();

        for (int i = 0; i < this.count; i++) {
            Operation op = Operation.createPost(null);
            ops.add(op);
            q.offer(op);
        }

        assertTrue(!q.isEmpty());

        // verify operations beyond limit are not queued
        assertTrue(false == q.offer(Operation.createGet(null)));

        // increase limit
        q.setLimit(this.count + 1);
        assertEquals(this.count + 1, q.getLimit());
        // add an operation
        Operation lastOp = Operation.createGet(null);
        assertTrue(q.offer(lastOp));

        if (!isFifo) {
            // dequeue most recent, over the initial limit operation
            assertEquals(lastOp, q.poll());
        }

        // dequeue all operations, make sure they exist in our external list, in the expected order
        for (int i = 0; i < ops.size(); i++) {
            Operation op = null;
            if (isFifo) {
                op = ops.get(i);
            } else {
                op = ops.get(this.count - 1 - i);
            }
            Operation qOp = q.poll();

            if (qOp.getId() != op.getId()) {
                throw new IllegalStateException("unexpected operation from queue");
            }
        }

        if (isFifo) {
            // finally, dequeue most recent, over the initial limit operation
            assertEquals(lastOp, q.poll());
            // verify no more operations remain
            assertTrue(q.poll() == null);
        }
    }

    private OperationQueue createQueue(boolean isFifo) {
        OperationQueue q = isFifo ? OperationQueue.createFifo(this.count)
                : OperationQueue.createLifo(this.count);
        return q;
    }

    @Test
    public void toCollection() {
        OperationQueue q = OperationQueue.createFifo(this.count);
        final String pragma = UUID.randomUUID().toString();
        for (int i = 0; i < this.count; i++) {
            Operation op = Operation.createPost(null).addPragmaDirective(pragma);
            q.offer(op);
        }

        Collection<Operation> ops = q.toCollection();
        assertTrue(ops.size() == this.count);
        assertTrue(!q.isEmpty());
        for (Operation op : ops) {
            assertEquals(pragma, op.getRequestHeader(Operation.PRAGMA_HEADER));
        }

        q.clear();
        assertTrue(q.isEmpty());
    }

}
