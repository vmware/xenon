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
    public void offerAndPollWithDefaults() {
        OperationQueue q = OperationQueue.create(this.count, null);

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


        // dequeue all operations, make sure they exist in our external list, in the expected order
        for (Operation op : ops) {
            Operation qOp = q.poll();

            if (qOp.getId() != op.getId()) {
                throw new IllegalStateException("unexpected operation from queue");
            }
        }

        // finally, dequeue most recent, over the initial limit operation
        assertEquals(lastOp, q.poll());
        // verify no more operations remain
        assertTrue(q.poll() == null);
    }

    @Test
    public void toCollection() {
        OperationQueue q = OperationQueue.create(this.count, null);
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
