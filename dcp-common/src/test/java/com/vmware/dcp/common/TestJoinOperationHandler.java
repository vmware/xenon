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
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.services.common.MinimalTestService;

public class TestJoinOperationHandler extends BasicReportTestCase {
    private List<Service> services;

    @Before
    public void prepare() throws Throwable {
        this.services = initServices();
    }

    @Test
    public void testJoin() throws Throwable {
        Operation op1 = createServiceOperation(this.services.get(0));
        Operation op2 = createServiceOperation(this.services.get(1));
        Operation op3 = createServiceOperation(this.services.get(2));

        CompletionHandler completion = (o, e) -> {
            try {
                host.completeIteration();
            } catch (Throwable ex) {
                host.failIteration(ex);
            }
        };
        op1.setCompletion(completion);
        op2.setCompletion(completion);
        op3.setCompletion(completion);

        host.testStart(3);
        OperationJoin.create(op1, op2, op3).sendWith(host);
        host.testWait();

        // test join with operations not send with a client, for example, startService

        Operation startPostOne = Operation.createPost(UriUtils.buildUri(this.host, UUID
                .randomUUID().toString()));
        Operation startPostTwo = Operation.createPost(UriUtils.buildUri(this.host, UUID
                .randomUUID().toString()));

        this.host.testStart(1);
        OperationJoin.create(startPostOne, startPostTwo).setCompletion((ops, exs) -> {
            if (exs != null && !exs.isEmpty()) {
                this.host.failIteration(new IllegalStateException("Post failed"));
                return;
            }

            this.host.completeIteration();
        });

        this.host.startService(startPostOne, new MinimalTestService());
        this.host.startService(startPostTwo, new MinimalTestService());

        this.host.testWait();
    }

    @Test
    public void testOnJoinCompletion() throws Throwable {
        doJoinCompletion(false);
        doJoinCompletion(true);
    }

    private void doJoinCompletion(boolean forceRemote) throws Throwable {
        Operation op1 = createServiceOperation(this.services.get(0));
        Operation op2 = createServiceOperation(this.services.get(1));
        Operation op3 = createServiceOperation(this.services.get(2));

        if (forceRemote) {
            op1.forceRemote();
            op2.forceRemote();
            op3.forceRemote();
        }

        host.testStart(3);
        OperationJoin
                .create(op1, op2, op3)
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        host.failIteration(exc.values().iterator().next());
                    } else {
                        host.completeIteration();
                        host.completeIteration();
                        host.completeIteration();
                    }
                }).sendWith(host);
        host.testWait();
    }

    @Test
    public void testJoinCollectionOfOps() throws Throwable {
        Operation op1 = createServiceOperation(this.services.get(0));
        Operation op2 = createServiceOperation(this.services.get(1));
        Operation op3 = createServiceOperation(this.services.get(2));

        host.testStart(1);
        OperationJoin
                .create(Arrays.asList(op1, op2, op3))
                .setCompletion((os, es) -> {
                    if (es != null && !es.isEmpty()) {
                        host.failIteration(es.values().iterator().next());
                        return;
                    }

                    try {
                        assertNotNull(os.get(op1.getId()).getBody(MinimalTestServiceState.class));
                        assertNotNull(os.get(op2.getId()).getBody(MinimalTestServiceState.class));
                        assertNotNull(os.get(op3.getId()).getBody(MinimalTestServiceState.class));
                        host.completeIteration();
                    } catch (Throwable ex) {
                        host.failIteration(ex);
                    }
                })
                .sendWith(host);
        host.testWait();
    }

    @Test
    public void testJoinStreamOfOps() throws Throwable {
        Operation op1 = createServiceOperation(this.services.get(0));
        Operation op2 = createServiceOperation(this.services.get(1));
        Operation op3 = createServiceOperation(this.services.get(2));

        host.testStart(1);
        OperationJoin
                .create(Stream.of(op1, op2, op3))
                .setCompletion((os, es) -> {
                    if (es != null && !es.isEmpty()) {
                        host.failIteration(es.values().iterator().next());
                        return;
                    }

                    try {
                        assertNotNull(os.get(op1.getId()).getBody(MinimalTestServiceState.class));
                        assertNotNull(os.get(op2.getId()).getBody(MinimalTestServiceState.class));
                        assertNotNull(os.get(op3.getId()).getBody(MinimalTestServiceState.class));
                        host.completeIteration();
                    } catch (Throwable ex) {
                        host.failIteration(ex);
                    }
                })
                .sendWith(host);
        host.testWait();
    }

    @Test
    public void testOnJoinCompletionError() throws Throwable {
        CompletionHandler perOpCompletion = (o, e) -> host.completeIteration();

        Operation op1 = createServiceOperation(this.services.get(0))
                .setCompletion(perOpCompletion);

        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = null; // expect validation error.
        Operation op2 = Operation.createPatch(this.services.get(1).getUri())
                .setBody(body)
                .setReferer(host.getUri())
                .forceRemote().setCompletion(perOpCompletion);

        Operation op3 = createServiceOperation(this.services.get(2)).setCompletion(perOpCompletion);

        // we expect per operation completion to be called, and the joined handler to be called
        host.testStart(6);
        OperationJoin
                .create(op1, op2, op3)
                .setCompletion((ops, exc) -> {
                    if (exc != null) {
                        assertEquals(1, exc.size());
                        host.completeIteration();
                        host.completeIteration();
                        host.completeIteration();
                    } else {
                        host.failIteration(new IllegalStateException("Expected exception"));
                    }
                }).sendWith(host);
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
                .setCompletion(host.getCompletion());
    }
}
