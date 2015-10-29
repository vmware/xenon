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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestOperationQueue {

    public int count = 1000;

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

        List<Operation> ops = new ArrayList<>();

        for (int i = 0; i < this.count; i++) {
            Operation op = Operation.createPost(null);
            ops.add(op);
            q.offer(op);
        }

        // verify operations beyond limit are not queued
        assertTrue(false == q.offer(Operation.createGet(null)));

        // dequeue all operations, make sure they exist in our external list, in the expected order
        for (Operation op : ops) {
            Operation qOp = q.poll();
            if (qOp.getId() != op.getId()) {
                throw new IllegalStateException("unexpected operation from queue");
            }
        }

        // verify no more operations remain
        assertTrue(q.poll() == null);
    }
}
