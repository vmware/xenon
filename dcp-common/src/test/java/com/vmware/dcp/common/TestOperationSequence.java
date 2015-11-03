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

import java.util.EnumSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.services.common.MinimalTestService;

public class TestOperationSequence extends BasicReusableHostTestCase {
    private List<Service> services;

    @Before
    public void prepare() throws Throwable {
        this.services = initServices();
    }

    @Test
    public void testOperationsSequenceNext() throws Throwable {
        Operation op1 = createServiceOperation(this.services.get(0));
        Operation op2 = createServiceOperation(this.services.get(1));

        MinimalTestServiceState state = new MinimalTestServiceState();
        state.id = "1";
        Operation op3 = Operation.createPatch(this.services.get(2).getUri())
                .setBody(state)
                .setReferer(host.getUri());

        // the uri and the body will be set based on the result of the first set of operations.
        Operation op4 = Operation.createPatch(null)
                .setBody(null)
                .setReferer(host.getUri());
        Operation op5 = createServiceOperation(this.services.get(1));
        Operation op6 = createServiceOperation(this.services.get(2));

        // the uri and the body will be set based on the result of the second set of operations.
        Operation op7 = Operation.createPatch(null)
                .setBody(null)
                .setReferer(host.getUri());
        Operation op8 = createServiceOperation(this.services.get(1));
        Operation op9 = createServiceOperation(this.services.get(2));

        host.testStart(1);
        OperationSequence
                .create(op1, op2, op3)// initial joined operations
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        host.failIteration(exc.values().iterator().next());
                    } else {
                        assertEquals(3, ops.values().size());
                        Operation o3 = ops.get(op3.getId());

                        op4.setUri(o3.getUri());
                        MinimalTestServiceState body = o3
                                .getBody(MinimalTestServiceState.class);
                        body.id = String.valueOf(Integer.parseInt(body.id) + 1);
                        op4.setBody(body);
                    }
                })
                .next(op4, op5, op6) // next in sequence joined operations
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        host.failIteration(exc.values().iterator().next());
                    } else {
                        assertEquals(6, ops.values().size());

                        assertTrue(ops.containsKey(op1.getId()));
                        assertTrue(ops.containsKey(op2.getId()));
                        assertTrue(ops.containsKey(op3.getId()));
                        assertTrue(ops.containsKey(op4.getId()));
                        assertTrue(ops.containsKey(op5.getId()));
                        assertTrue(ops.containsKey(op6.getId()));

                        Operation o4 = ops.get(op4.getId());
                        assertEquals(o4.getUri(), ops.get(op3.getId()).getUri());

                        MinimalTestServiceState body = o4
                                .getBody(MinimalTestServiceState.class);
                        int currentCount = Integer.parseInt(body.id);
                        assertEquals(2, currentCount);
                        body.id = String.valueOf(currentCount + 1);

                        op7.setUri(o4.getUri()).setBody(body);
                    }
                })
                .next(op7, op8, op9) // third level in the sequence joined operations
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        host.failIteration(exc.values().iterator().next());
                    } else {

                        assertTrue(ops.containsKey(op1.getId()));
                        assertTrue(ops.containsKey(op2.getId()));
                        assertTrue(ops.containsKey(op3.getId()));
                        assertTrue(ops.containsKey(op4.getId()));
                        assertTrue(ops.containsKey(op5.getId()));
                        assertTrue(ops.containsKey(op6.getId()));
                        assertTrue(ops.containsKey(op7.getId()));
                        assertTrue(ops.containsKey(op8.getId()));
                        assertTrue(ops.containsKey(op9.getId()));

                        Operation o7 = ops.get(op7.getId());
                        MinimalTestServiceState body = o7
                                .getBody(MinimalTestServiceState.class);
                        int currentCount = Integer.parseInt(body.id);
                        assertEquals(3, currentCount);

                        assertEquals(9, ops.values().size());
                        host.completeIteration();
                    }
                })
                .sendWith(host);
        host.testWait();
    }

    @Test
    public void testJoinSequenceNext() throws Throwable {
        Operation op1 = createServiceOperation(this.services.get(0));
        Operation op2 = createServiceOperation(this.services.get(1));

        MinimalTestServiceState state = new MinimalTestServiceState();
        state.id = "1";
        Operation op3 = Operation.createPatch(this.services.get(2).getUri())
                .setBody(state)
                .setReferer(host.getUri());

        MinimalTestServiceState state2 = new MinimalTestServiceState();
        state2.id = "2";
        // the uri and the body will be set based on the result of the first set of operations.
        Operation op4 = Operation.createPatch(this.services.get(2).getUri())
                .setBody(state2)
                .setReferer(host.getUri());
        Operation op5 = createServiceOperation(this.services.get(1));
        Operation op6 = createServiceOperation(this.services.get(2));

        MinimalTestServiceState state3 = new MinimalTestServiceState();
        state3.id = "3";
        // the uri and the body will be set based on the result of the second set of operations.
        Operation op7 = Operation.createPatch(this.services.get(2).getUri())
                .setBody(state3)
                .setReferer(host.getUri());
        Operation op8 = createServiceOperation(this.services.get(0));
        Operation op9 = createServiceOperation(this.services.get(1));

        host.testStart(1);
        OperationSequence
                .create(OperationJoin.create(op1, op2, op3),
                        OperationJoin.create(op4, op5, op6),
                        OperationJoin.create(op7, op8, op9))
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        host.failIteration(exc.values().iterator().next());
                    } else {
                        assertTrue(ops.containsKey(op1.getId()));
                        assertTrue(ops.containsKey(op2.getId()));
                        assertTrue(ops.containsKey(op3.getId()));
                        assertTrue(ops.containsKey(op4.getId()));
                        assertTrue(ops.containsKey(op5.getId()));
                        assertTrue(ops.containsKey(op6.getId()));
                        assertTrue(ops.containsKey(op7.getId()));
                        assertTrue(ops.containsKey(op8.getId()));
                        assertTrue(ops.containsKey(op9.getId()));

                        Operation o7 = ops.get(op7.getId());
                        MinimalTestServiceState body = o7
                                .getBody(MinimalTestServiceState.class);
                        int currentCount = Integer.parseInt(body.id);
                        assertEquals(3, currentCount);

                        assertEquals(9, ops.values().size());
                        host.completeIteration();
                    }
                })
                .sendWith(host);
        host.testWait();

        host.testStart(1);
        OperationSequence
                .create(OperationJoin.create(op1, op2, op3))
                .next(OperationJoin.create(op4, op5, op6),
                        OperationJoin.create(op7, op8, op9))
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        host.failIteration(exc.values().iterator().next());
                    } else {

                        try {
                            assertTrue(ops.containsKey(op1.getId()));
                            assertTrue(ops.containsKey(op2.getId()));
                            assertTrue(ops.containsKey(op3.getId()));
                            assertTrue(ops.containsKey(op4.getId()));
                            assertTrue(ops.containsKey(op5.getId()));
                            assertTrue(ops.containsKey(op6.getId()));
                            assertTrue(ops.containsKey(op7.getId()));
                            assertTrue(ops.containsKey(op8.getId()));
                            assertTrue(ops.containsKey(op9.getId()));

                            Operation o7 = ops.get(op7.getId());
                            MinimalTestServiceState body = o7
                                    .getBody(MinimalTestServiceState.class);
                            int currentCount = Integer.parseInt(body.id);
                            assertEquals(3, currentCount);

                            assertEquals(9, ops.values().size());
                            host.completeIteration();
                        } catch (Throwable e) {
                            this.host.failIteration(e);
                        }
                    }
                })
                .sendWith(host);
        host.testWait();
    }

    @Test
    public void testJoinAfterWithFailure() throws Throwable {
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = null; // expect validation error.
        Operation op1 = Operation.createPatch(this.services.get(0).getUri()).setBody(body)
                .setReferer(host.getUri()).setCompletion(null);
        Operation op2 = createServiceOperation(this.services.get(1)).setCompletion(null);
        Operation op3 = createServiceOperation(this.services.get(2)).setCompletion(null);

        Operation op4 = Operation.createPatch(this.services.get(0).getUri()).setBody(body)
                .setReferer(host.getUri()).setCompletion(null);
        Operation op5 = createServiceOperation(this.services.get(1)).setCompletion(null);
        Operation op6 = createServiceOperation(this.services.get(2)).setCompletion(null);

        host.testStart(2);
        OperationSequence.create(op1, op2, op3)// initial joined operations
                .setCompletion((ops, exc) -> {
                    try {
                        assertEquals(1, exc.values().size());
                    } catch (Throwable e) {
                        this.host.failIteration(e);
                        return;
                    }
                    if (exc != null) {
                        exc.clear();
                        host.completeIteration();
                    }
                })
                .next(op4, op5, op6)// expected one exception on this level
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        try {
                            assertEquals(1, exc.values().size());
                            assertEquals(6, ops.values().size());
                        } catch (Throwable e) {
                            this.host.failIteration(e);
                            return;
                        }
                        host.completeIteration();
                    } else {
                        host.failIteration(new IllegalStateException("Expected exception"));
                    }
                })
                .sendWith(host);
        host.testWait();

        host.testStart(1);
        OperationSequence.create(op1, op2, op3)
                .next(op4, op5, op6)
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        try {
                            assertEquals(2, exc.values().size());
                            assertEquals(6, ops.values().size());
                        } catch (Throwable e) {
                            host.failIteration(e);
                            return;
                        }
                        host.completeIteration();
                    } else {
                        host.failIteration(new IllegalStateException("Expected exception"));
                    }
                })
                .sendWith(host);
        host.testWait();
    }

    private List<Service> initServices() throws Throwable {
        int numberOfServices = 3;
        return host.doThroughputServiceStart(numberOfServices,
                MinimalTestService.class, host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);
    }

    private Operation createServiceOperation(Service s) {
        return Operation.createGet(s.getUri())
                .setReferer(host.getUri())
                .forceRemote();
    }

}
