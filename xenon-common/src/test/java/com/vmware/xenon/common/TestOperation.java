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

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.MinimalTestService;

public class TestOperation extends BasicReusableHostTestCase {
    private List<Service> services;

    @Test
    public void addRemovePragma() {
        Operation op = Operation.createGet(this.host.getUri());
        op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED);
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED));

        op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED));
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK));

        // add a pragma that already exists
        op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED));
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK));

        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED));
        assertTrue(!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK));

        // attempt to remove that does not exist
        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
        assertTrue(op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_CREATED));
        assertTrue(!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK));
    }

    @Test
    public void setterValidation() {
        Operation op = Operation.createGet(this.host.getUri());

        Runnable r = () -> {
            op.setRetryCount(Short.MAX_VALUE * 2);
        };
        verifyArgumentException(r);

        r = () -> {
            op.setRetryCount(-10);
        };
        verifyArgumentException(r);

        r = () -> {
            op.addHeader("sadfauisydf", false);
        };
        verifyArgumentException(r);

        r = () -> {
            op.addHeader("", false);
        };
        verifyArgumentException(r);

        r = () -> {
            op.addHeader(null, false);
        };
        verifyArgumentException(r);
    }

    private void verifyArgumentException(Runnable r) {
        try {
            r.run();
            throw new IllegalStateException("Should have failed");
        } catch (IllegalArgumentException e) {
            return;
        }
    }

    @Test
    public void addRemoveHeaders() {
        Operation op = Operation.createGet(this.host.getUri());
        String ctMixed = "Content-Type";
        String ctLower = "content-type";
        String ctValue = UUID.randomUUID().toString() + "AAAAbbbb";
        op.addRequestHeader(ctMixed, ctValue);
        String ctV = op.getRequestHeader(ctLower);
        assertEquals(ctValue, ctV);
        op.addRequestHeader(ctLower, ctValue);
        ctV = op.getRequestHeader(ctLower);
        assertEquals(ctValue, ctV);
        op.addResponseHeader(ctMixed, ctValue);
        ctV = op.getResponseHeader(ctLower);
        assertEquals(ctValue, ctV);
        op.addResponseHeader(ctLower, ctValue);
        ctV = op.getResponseHeader(ctLower);
        assertEquals(ctValue, ctV);
    }

    @Test
    public void operationDoubleCompletion() throws Throwable {
        AtomicInteger completionCount = new AtomicInteger();
        CompletionHandler c = (o, e) -> {
            completionCount.incrementAndGet();
        };

        int count = 100;
        this.host.toggleNegativeTestMode(true);
        this.host.testStart(count);
        Operation op = Operation.createGet(this.host.getUri()).setCompletion(c);
        for (int i = 0; i < count; i++) {
            this.host.run(() -> {
                op.complete();
                op.fail(new Exception());
                try {
                    Thread.sleep(1);
                } catch (Exception e1) {
                }
                this.host.completeIteration();
            });
        }
        this.host.testWait();
        this.host.toggleNegativeTestMode(false);
        assertTrue(completionCount.get() == 1);
    }

    @Test
    public void operationWithContextId() throws Throwable {
        this.services = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        // issue a patch to verify contextId received in the services matches the one set
        // by the client on the Operation
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_HAS_CONTEXT_ID;
        body.stringValue = "request-id";

        this.host.testStart(1);
        this.host.send(Operation
                .createPatch(this.services.get(0).getUri())
                .forceRemote()
                .setBody(body)
                .setContextId(body.stringValue)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                }));
        this.host.testWait();
    }

    @Test
    public void operationMultiStageFlowWithContextId() throws Throwable {
        String contextId = UUID.randomUUID().toString();
        int opCount = Utils.DEFAULT_THREAD_COUNT * 2;
        AtomicInteger pending = new AtomicInteger(opCount);

        CompletionHandler stageTwoCh = (o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }
            String contextIdActual = OperationContext.getContextId();
            if (!contextId.equals(contextIdActual)) {
                this.host.failIteration(new IllegalStateException("context id not set"));
                return;
            }
            this.host.completeIteration();
        };

        CompletionHandler stageOneCh = (o, e) -> {
            if (e != null) {
                this.host.failIteration(e);
                return;
            }
            String contextIdActual = OperationContext.getContextId();
            if (!contextId.equals(contextIdActual)) {
                this.host.failIteration(new IllegalStateException("context id not set"));
                return;
            }
            int r = pending.decrementAndGet();
            if (r != 0) {
                return;
            }

            // now send some new "child" operations, and expect the ID to flow
            Operation childOp = Operation.createGet(o.getUri())
                    .setCompletion(stageTwoCh);
            this.host.send(childOp);
        };

        // send N parallel requests, that will all complete in parallel, and should have the
        // same context id. When they all complete (using a join like stageOne completion above)
        // we will send another operation, and expect it to carry the proper contextId
        this.host.testStart(1);
        for (int i = 0; i < opCount; i++) {
            Operation op = Operation
                    .createGet(UriUtils.buildUri(this.host, ExampleService.FACTORY_LINK))
                    .setCompletion(stageOneCh)
                    .setContextId(contextId);
            this.host.send(op);
        }
        this.host.testWait();
    }

    @Test
    public void operationWithoutContextId() throws Throwable {
        this.services = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        // issue a patch request to verify the contextId is 'null'
        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_HAS_CONTEXT_ID;

        this.host.testStart(1);
        this.host.send(Operation
                .createPatch(this.services.get(0).getUri())
                .forceRemote()
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.completeIteration();
                        return;
                    }

                    this.host.failIteration(new IllegalStateException(
                            "Request should have failed due to missing contextId"));
                }));
        this.host.testWait();
    }

    @Test
    public void testSendWithOnHost() throws Throwable {
        testSendWith((o) -> o.sendWith(this.host));
    }

    @Test
    public void testSendWithOnService() throws Throwable {
        testSendWith((o) -> o.sendWith(this.services.get(0)));
    }

    @Test
    public void testSendWithOnServiceClient() throws Throwable {
        testSendWith((o) -> o.sendWith(this.host.getClient()));
    }

    public void testSendWith(Consumer<Operation> sendOperation) throws Throwable {
        this.services = this.host.doThroughputServiceStart(1,
                MinimalTestService.class,
                this.host.buildMinimalTestState(),
                EnumSet.noneOf(Service.ServiceOption.class), null);

        MinimalTestServiceState body = new MinimalTestServiceState();
        body.id = MinimalTestService.STRING_MARKER_HAS_CONTEXT_ID;
        body.stringValue = "request-id";

        this.host.testStart(1);
        sendOperation.accept(Operation
                .createPatch(this.services.get(0).getUri())
                .forceRemote()
                .setBody(body)
                .setContextId(body.stringValue)
                .setReferer(this.host.getReferer())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        this.host.failIteration(e);
                        return;
                    }
                    this.host.completeIteration();
                }));
        this.host.testWait();
    }
}
