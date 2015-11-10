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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.test.MinimalTestServiceState;
import com.vmware.dcp.services.common.MinimalTestService;

public class TestOperationJoin extends BasicReportTestCase {
    private List<Service> services;
    private final int numberOfServices = 3;

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

    private class JoinWithBatchParams {
        Collection<Operation> ops;
        int batchSize;

        JoinWithBatchParams(Collection<Operation> ops, int batchSize) {
            this.ops = ops;
            this.batchSize = batchSize;
        }
    }

    @Test
    public void testJoinWithBatchOnServiceClient() throws Throwable {
        testJoinWithBatch((params) ->
                OperationJoin.create(params.ops).sendWith(this.host.getClient(), params.batchSize));
    }

    @Test
    public void testJoinWithBatchOnService() throws Throwable {
        testJoinWithBatch((params) ->
                OperationJoin.create(params.ops).sendWith(this.services.get(0), params.batchSize));
    }

    @Test
    public void testJoinWithBatchOnHost() throws Throwable {
        testJoinWithBatch((params) ->
                OperationJoin.create(params.ops).sendWith(this.host, params.batchSize));
    }

    public void testJoinWithBatch(Consumer<JoinWithBatchParams> createJoinOperation) throws Throwable {
        for (int numberOfOperations = 1; numberOfOperations < 5; numberOfOperations++) {
            for (int batchSize = 1; batchSize < numberOfOperations; batchSize++) {
                Collection<Operation> ops = getOperations(
                        numberOfOperations,
                        this.services.get(0),
                        (o, e) -> {
                            try {
                                host.completeIteration();
                            } catch (Throwable ex) {
                                host.failIteration(ex);
                            }
                        });
                host.testStart(numberOfOperations);
                createJoinOperation.accept(new JoinWithBatchParams(ops, batchSize));
                host.testWait();
            }
        }
    }

    @Test
    public void testJoinWithBatchSize() throws Throwable {
        Service testService = new MinimalTestService();
        testService = this.host.startServiceAndWait(testService, UUID.randomUUID().toString(), null);
        // Using queue limit feature of Service to only take batch size of requests.
        // If batching is not done right by OperationJoin then test service will not
        // accept extra operations and this test will fail.
        int limit = 40;
        this.host.setOperationQueueLimit(testService.getUri(), limit);
        AtomicInteger cancelledOpCount = new AtomicInteger();
        int count = 100;
        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();
        body.id = MinimalTestService.STRING_MARKER_DELAY_COMPLETION;
        Collection<Operation> ops1 = getOperations(count, testService, (o, e) -> {
            if (e != null) {
                if (o.getStatusCode() != Operation.STATUS_CODE_UNAVAILABLE) {
                    this.host.failIteration(
                            new IllegalStateException("unexpected status code"));
                    return;
                }
                String retrySeconds = o.getResponseHeader(Operation.RETRY_AFTER_HEADER);
                if (retrySeconds == null || Integer.parseInt(retrySeconds) < 1) {
                    this.host.failIteration(
                            new IllegalStateException("missing or unexpected retry-after"));
                    return;
                }

                cancelledOpCount.incrementAndGet();
                this.host.completeIteration();
                return;
            }

            this.host.completeIteration();
        });

        this.host.testStart(count);
        OperationJoin.create(ops1).sendWith(host, limit - 1);
        this.host.testWait();
    }

    private Collection<Operation> getOperations(int n, Service service, CompletionHandler handler) {
        Collection<Operation> ops = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Operation op = createServiceOperation(service);
            op.setCompletion(handler);
            ops.add(op);
        }

        return ops;
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
        return host.doThroughputServiceStart(this.numberOfServices,
                MinimalTestService.class, host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);
    }

    private Operation createServiceOperation(Service s) {
        MinimalTestServiceState body = (MinimalTestServiceState) this.host.buildMinimalTestState();
        body.id = MinimalTestService.STRING_MARKER_DELAY_COMPLETION;

        return Operation.createPatch(s.getUri())
                .setBody(body)
                .setReferer(this.host.getUri());
    }
}
