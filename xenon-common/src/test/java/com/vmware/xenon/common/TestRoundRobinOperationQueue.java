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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

public class TestRoundRobinOperationQueue {

    public int count = 10000;

    public int keyCount = 10;

    public int iterationCount = 3;

    @Before
    public void setUp() {
        CommandLineArgumentParser.parseFromProperties(this);
    }

    @Test
    public void offerAndPollSameKey() {
        RoundRobinOperationQueue q = new RoundRobinOperationQueue("test", this.count);

        try {
            q.offer(null, Operation.createGet(null));
            fail("null key offer should have failed");
        } catch (IllegalArgumentException e) {

        }

        try {
            q.offer("", null);
            fail("null op offer should have failed");
        } catch (IllegalArgumentException e) {

        }

        List<Operation> ops = new ArrayList<>();

        for (int i = 0; i < this.count; i++) {
            Operation op = Operation.createPost(null);
            ops.add(op);
            q.offer("", op);
        }

        assertTrue(!q.isEmpty());

        // dequeue all operations, make sure they exist in our external list, in the expected order
        for (int i = 0; i < ops.size(); i++) {
            Operation op = ops.get(i);
            Operation qOp = q.poll();

            if (qOp.getId() != op.getId()) {
                fail("unexpected operation from queue");
            }
        }

    }

    @Test
    public void offerAndPollMultipleKeys() {
        int limit = this.iterationCount * this.keyCount * this.count;
        RoundRobinOperationQueue q = new RoundRobinOperationQueue("test",
                limit);
        Map<String, List<Operation>> ops = new HashMap<>();
        Map<Long, String> opToKey = new HashMap<>();

        for (int iteration = 0; iteration < this.iterationCount; iteration++) {
            double pollDuration = 0;
            double offerDuration = 0;
            for (int k = 0; k < this.keyCount; k++) {
                List<Operation> perKeyOps = new ArrayList<>();
                String key = k + "";
                ops.put(k + "", perKeyOps);
                for (int i = 0; i < this.count; i++) {
                    Operation op = Operation.createPost(null);
                    perKeyOps.add(op);
                    opToKey.put(op.getId(), key);
                    long s = System.nanoTime();
                    q.offer(key, op);
                    long e = System.nanoTime();
                    offerDuration += (e - s);
                }
            }

            assertTrue(!q.isEmpty());

            // dequeue all operations, make sure they exist in our external list, in the expected order
            String previousKey = null;
            for (int i = 0; i < ops.size(); i++) {
                long s = System.nanoTime();
                Operation qOp = q.poll();
                long e = System.nanoTime();
                pollDuration += (e - s);
                String key = opToKey.get(qOp.getId());
                assertTrue(!key.equals(previousKey));
                assertTrue(ops.containsKey(key));
                previousKey = key;
            }

            logThroughput("poll", pollDuration);
            logThroughput("offer", offerDuration);
        }
    }

    private void logThroughput(String msg, double durationNanos) {
        durationNanos /= (double) TimeUnit.SECONDS.toNanos(1);
        double thpt = (double) this.count / (durationNanos);
        Logger.getAnonymousLogger()
                .info(String.format("%s throughput: %f, count: %d", msg, thpt, this.count));
    }

    @Test
    public void testLimitDifferentKeys() {
        RoundRobinOperationQueue q = new RoundRobinOperationQueue("sayonara", 2);
        assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));
        assertTrue(q.offer("k2", Operation.createGet(URI.create("/test"))));
        Operation excess = Operation.createGet(URI.create("/test"));
        assertFalse(q.offer("k3", excess));
        assertEquals(503, excess.getStatusCode());
        assertTrue(excess.getErrorResponseBody().message.contains("sayonara"));
    }

    @Test
    public void testLimitSameKey() {
        RoundRobinOperationQueue q = new RoundRobinOperationQueue("sayonara", 2);
        assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));
        assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));

        Operation excess = Operation.createGet(URI.create("/test"));
        assertFalse(q.offer("k1", excess));
        assertEquals(503, excess.getStatusCode());
        assertTrue(excess.getErrorResponseBody().message.contains("sayonara"));
    }

    @Test
    public void testTotalRestoredAfterRemove() {
        RoundRobinOperationQueue q = new RoundRobinOperationQueue("q", 2);

        for (int i = 0; i < 20; i++) {
            assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));
            assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));

            assertNotNull(q.poll());
            assertNotNull(q.poll());
        }

        assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));
        assertTrue(q.offer("k1", Operation.createGet(URI.create("/test"))));
    }
}
