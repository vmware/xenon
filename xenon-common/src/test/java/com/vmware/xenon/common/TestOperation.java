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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.vmware.xenon.common.Operation.STATUS_CODE_NOT_MODIFIED;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.SerializedOperation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.common.test.TestRequestSender.FailureResponse;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.MinimalTestService;
import com.vmware.xenon.services.common.ServiceUriPaths;

public class TestOperation extends BasicReusableHostTestCase {

    public static class NotModifiedOwnerSelectedService extends StatefulService {

        public static final String FACTORY_LINK = ServiceUriPaths.CORE
                + "/tests/304-stateful-ownerselected";

        public static class State extends ServiceDocument {
            public String name;
        }

        public NotModifiedOwnerSelectedService() {
            super(NotModifiedOwnerSelectedService.State.class);
            toggleOption(ServiceOption.PERSISTENCE, true);
            toggleOption(ServiceOption.REPLICATION, true);
            toggleOption(ServiceOption.OWNER_SELECTION, true);
        }

        @Override
        public void handleGet(Operation get) {
            completeWith304(get);
        }

        @Override
        public void handlePatch(Operation patch) {
            // this is not defined behavior on http-304 spec
            completeWith304(patch);
        }

        @Override
        public void handlePut(Operation put) {
            // this is not defined behavior on http-304 spec
            completeWith304(put);
        }

        private void completeWith304(Operation op) {
            NotModifiedOwnerSelectedService.State bogusBody = new NotModifiedOwnerSelectedService.State();
            bogusBody.name = "should not be a body";
            op.setBody(bogusBody);
            op.setStatusCode(STATUS_CODE_NOT_MODIFIED);
            op.complete();
        }
    }

    public static class NotModifiedNonOwnerSelectedService extends NotModifiedOwnerSelectedService {

        public static final String FACTORY_LINK = ServiceUriPaths.CORE
                + "/tests/304-stateful-non-owner";

        public NotModifiedNonOwnerSelectedService() {
            super();
            toggleOption(ServiceOption.OWNER_SELECTION, false);
        }
    }

    public static class NotModifiedStatelessService extends StatelessService {

        public static final String SELF_LINK = ServiceUriPaths.CORE + "/tests/304-stateless";

        @Override
        public void handleGet(Operation get) {
            completeWith304(get);
        }

        @Override
        public void handlePatch(Operation patch) {
            // this is not defined behavior on http-304 spec
            completeWith304(patch);
        }

        @Override
        public void handlePut(Operation put) {
            // this is not defined behavior on http-304 spec
            completeWith304(put);
        }

        private void completeWith304(Operation op) {
            op.setBody("should not be a body");
            op.setStatusCode(STATUS_CODE_NOT_MODIFIED);
            op.complete();
        }
    }

    public static class ConflictStatelessService extends StatelessService {

        public static final String SELF_LINK = ServiceUriPaths.CORE + "/tests/409-stateless";

        @Override
        public void handleGet(Operation get) {
            get.fail(409, new RuntimeException("test exception"), "{\"message\":\"failure-body\"}");
        }
    }

    private List<Service> services;

    @Test
    public void create() throws Throwable {
        String link = ExampleService.FACTORY_LINK;
        Service s = this.host.startServiceAndWait(new MinimalTestService(),
                UUID.randomUUID().toString(), null);

        Action a = Action.POST;
        Operation op = Operation.createPost(this.host, link);
        verifyOp(link, a, op);
        op = Operation.createPost(s, link);
        verifyOp(link, a, op);
        op = Operation.createPost(s.getUri());
        verifyOp(s.getSelfLink(), a, op);

        a = Action.PATCH;
        op = Operation.createPatch(this.host, link);
        verifyOp(link, a, op);
        op = Operation.createPatch(s, link);
        verifyOp(link, a, op);
        op = Operation.createPatch(s.getUri());
        verifyOp(s.getSelfLink(), a, op);

        a = Action.PUT;
        op = Operation.createPut(this.host, link);
        verifyOp(link, a, op);
        op = Operation.createPut(s, link);
        verifyOp(link, a, op);
        op = Operation.createPut(s.getUri());
        verifyOp(s.getSelfLink(), a, op);

        a = Action.DELETE;
        op = Operation.createDelete(this.host, link);
        verifyOp(link, a, op);
        op = Operation.createDelete(s, link);
        verifyOp(link, a, op);
        op = Operation.createDelete(s.getUri());
        verifyOp(s.getSelfLink(), a, op);

        a = Action.GET;
        op = Operation.createGet(this.host, link);
        verifyOp(link, a, op);
        op = Operation.createGet(s, link);
        verifyOp(link, a, op);
        op = Operation.createGet(s.getUri());
        verifyOp(s.getSelfLink(), a, op);

        a = Action.OPTIONS;
        op = Operation.createOptions(this.host, link);
        verifyOp(link, a, op);
        op = Operation.createOptions(s, link);
        verifyOp(link, a, op);
        op = Operation.createOptions(s.getUri());
        verifyOp(s.getSelfLink(), a, op);
    }

    private void verifyOp(String link, Action a, Operation op) {
        assertEquals(a, op.getAction());
        assertEquals(link, op.getUri().getPath());
    }

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

        // repeat on fresh op, no remote context allocated
        op = Operation.createGet(this.host.getUri());
        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK);
        assertTrue(!op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_INDEX_CHECK));

        // check overlapping directives
        op = Operation.createGet(this.host.getUri());
        op.addPragmaDirective("x-abc");
        assertTrue(op.hasPragmaDirective("x-abc"));
        assertTrue(!op.hasPragmaDirective("x-ab"));
        op.addPragmaDirective("x-123");
        assertTrue(op.hasPragmaDirective("x-abc"));
        assertTrue(!op.hasPragmaDirective("x-ab"));
        assertTrue(op.hasPragmaDirective("x-123"));
        assertTrue(!op.hasPragmaDirective("x-12"));
    }

    @Test
    public void defaultFailureCompletion() {
        Operation getToNowhere = getOperationFailure();
        // we are just making no exceptions are thrown in the context of the sendRequest call
        this.host.sendRequest(getToNowhere);

    }

    @Test
    public void nestCompletion() throws Throwable {
        TestContext ctx = testCreate(1);
        Operation op = Operation.createGet(this.host.getUri()).setCompletion(ctx.getCompletion());
        op.nestCompletion((o) -> {
            // complete original operation, triggering test completion
            op.complete();
        });
        op.complete();
        ctx.await();

        ctx = testCreate(1);
        Operation opWithFail = Operation.createGet(this.host.getUri()).setCompletion(
                ctx.getExpectedFailureCompletion());
        opWithFail.nestCompletion((o, e) -> {
            if (e != null) {
                // the fail() below is triggered due to the fail() right before ctx.await(),
                // and it should result in the original completion being triggered
                opWithFail.fail(e);
                return;
            }
            // complete original operation, triggering test completion
            opWithFail.complete();
        });
        opWithFail.fail(new IllegalStateException("induced failure"));
        ctx.await();

        ctx = testCreate(1);
        Operation opWithFailImplicitNest = Operation.createGet(this.host.getUri()).setCompletion(
                ctx.getExpectedFailureCompletion());
        opWithFailImplicitNest.nestCompletion((o) -> {
            // we should never execute the line below, since we fail the operation, in the code
            // below, right before ctx.await()
            opWithFailImplicitNest
                    .fail(new IllegalStateException("nested completion should have been skipped"));
        });
        opWithFailImplicitNest.fail(new IllegalStateException("induced failure"));
        ctx.await();

        // clone the operation before failing so its the *cloned* instance that is passed to the
        // nested completion
        ctx = testCreate(1);
        Operation opWithFailImplicitNestAndClone = Operation.createGet(this.host.getUri())
                .setCompletion(
                        ctx.getExpectedFailureCompletion());
        opWithFailImplicitNestAndClone.nestCompletion((o) -> {
            opWithFailImplicitNestAndClone
                    .fail(new IllegalStateException("nested completion should have been skipped"));
        });
        Operation clone = opWithFailImplicitNestAndClone.clone();
        clone.fail(new IllegalStateException("induced failure"));
        ctx.await();
    }

    @Test
    public void nestCompletionOrder() throws Throwable {
        List<Integer> list = new ArrayList<>();
        Operation op = Operation.createGet(this.host.getUri());
        op.setCompletion((o, e) -> list.add(0));
        op.nestCompletion((o, e) -> {
            list.add(1);
            o.complete();
            list.add(10);
        });
        op.nestCompletion((o, e) -> {
            list.add(2);
            o.complete();
            list.add(20);
        });
        op.complete();

        assertArrayEquals(new Integer[] { 2, 1, 0, 10, 20 },
                list.toArray(new Integer[list.size()]));
    }

    @Test
    public void nestCompletionWithEmptyCompletionHandler() throws Throwable {
        List<Integer> list = new ArrayList<>();
        Operation op = Operation.createGet(this.host.getUri());
        // not calling setCompletion()
        op.nestCompletion((o, e) -> {
            list.add(1);
            o.complete();
            list.add(10);
        });
        op.nestCompletion((o, e) -> {
            list.add(2);
            o.complete();
            list.add(20);
        });
        op.complete();

        assertArrayEquals(new Integer[] { 2, 1, 10, 20 }, list.toArray(new Integer[list.size()]));
    }

    @Test
    public void appendCompletionCheckOrderAndOperationIdentity() throws Throwable {

        List<Integer> list = new ArrayList<>();
        Operation op = Operation.createGet(this.host.getUri());
        op.setCompletion((o, e) -> {
            list.add(1);
            op.complete();
            list.add(10);
        });
        op.appendCompletion((o, e) -> {
            list.add(2);
            assertSame(op, o);
            op.complete();
            list.add(20);
        });
        op.appendCompletion((o, e) -> {
            list.add(3);
            assertSame(op, o);
            op.complete();
            list.add(30);
        });
        op.complete();

        assertArrayEquals(new Integer[] { 1, 2, 3, 30, 20, 10 },
                list.toArray(new Integer[list.size()]));
    }

    @Test
    public void appendCompletionCheckOrderAndExceptionIdentity() throws Throwable {
        Exception ex1 = new RuntimeException();
        Exception ex2 = new RuntimeException();
        Exception ex3 = new RuntimeException();

        List<Integer> list = new ArrayList<>();
        Operation op = Operation.createGet(this.host.getUri());
        op.setCompletion((o, e) -> {
            list.add(1);
            assertSame(op, o);
            assertSame(ex1, e);
            op.fail(ex2);
            list.add(10);
        });
        op.appendCompletion((o, e) -> {
            list.add(2);
            assertSame(op, o);
            assertSame(ex2, e);
            op.fail(ex3);
            list.add(20);
        });
        op.appendCompletion((o, e) -> {
            list.add(3);
            assertSame(op, o);
            assertSame(ex3, e);
        });

        op.fail(ex1);

        assertArrayEquals(new Integer[] { 1, 2, 3, 20, 10 },
                list.toArray(new Integer[list.size()]));
    }

    @Test
    public void appendCompletionNoComplete() throws Throwable {

        List<Integer> list = new ArrayList<>();
        Operation op = Operation.createGet(this.host.getUri());
        op.setCompletion((o, e) -> {
            list.add(1);
            o.complete();
            list.add(10);
        });
        op.appendCompletion((o, e) -> {
            list.add(2);
            // DO NOT CALL complete()
        });
        op.appendCompletion((o, e) -> {
            fail("Should not be called");
        });
        op.complete();

        assertArrayEquals(new Integer[] { 1, 2, 10 }, list.toArray(new Integer[list.size()]));
    }

    @Test
    public void appendCompletionWithEmptyCompletionHandler() throws Throwable {
        List<Integer> list = new ArrayList<>();
        Operation op = Operation.createGet(this.host.getUri());
        // not calling setCompletion()
        op.appendCompletion((o, e) -> {
            list.add(1);
            o.complete();
            list.add(10);
        });
        op.appendCompletion((o, e) -> {
            list.add(2);
            o.complete();
            list.add(20);
        });
        op.complete();

        assertArrayEquals(new Integer[] { 1, 2, 20, 10 }, list.toArray(new Integer[list.size()]));
    }

    @Test
    public void completion() throws Throwable {
        boolean[] isSuccessHandlerCalled = new boolean[] { false };
        boolean[] isFailureHandlerCalled = new boolean[] { false };
        Consumer<Operation> successHandler = op -> isSuccessHandlerCalled[0] = true;
        CompletionHandler failureHandler = (op, e) -> isFailureHandlerCalled[0] = true;

        Operation successOp = getOperationSuccess().setCompletion(successHandler, failureHandler);
        wrapCompletionHandlerWithCompleteIteration(successOp);

        this.host.sendAndWait(successOp);
        assertTrue("op success should call success handler", isSuccessHandlerCalled[0]);
        assertFalse("op success should NOT call success handler", isFailureHandlerCalled[0]);

        // reset the flags
        isSuccessHandlerCalled[0] = false;
        isFailureHandlerCalled[0] = false;

        Operation failureOp = getOperationFailure().setCompletion(successHandler, failureHandler);
        wrapCompletionHandlerWithCompleteIteration(failureOp);

        this.host.sendAndWait(failureOp);
        assertFalse("op failure should NOT call success handler", isSuccessHandlerCalled[0]);
        assertTrue("op failure should call success handler", isFailureHandlerCalled[0]);
    }

    private Operation getOperationSuccess() {
        return Operation.createGet(this.host, ExampleService.FACTORY_LINK)
                .setReferer(this.host.getUri());
    }

    private Operation getOperationFailure() {
        return Operation.createGet(UriUtils.buildUri(this.host, "/somethingnotvalid"))
                .setReferer(this.host.getUri());
    }

    private void wrapCompletionHandlerWithCompleteIteration(Operation operation) {
        CompletionHandler ch = operation.getCompletion();
        operation.setCompletion((op, e) -> {
            ch.handle(op, e);
            this.host.completeIteration();
        });
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
        op.addRequestHeader("req", " \r\n 123 \r\n ");
        assertEquals(op.getRequestHeader("req"), "123");
        op.addRequestHeader("req", "123\r\n");
        assertEquals(op.getRequestHeader("req"), "123");
        op.addRequestHeader("req", "\r\n123");
        assertEquals(op.getRequestHeader("req"), "123");
        op.addRequestHeader("req", "\r\n123\r\n456");
        assertEquals(op.getRequestHeader("req"), "123456");
        op.addRequestHeader("req", "\r\n");
        assertEquals(op.getRequestHeader("req"), "");
        op.addResponseHeader("res", " - \r\n\r\n - 123 - \r\n\r\n - ");
        assertEquals(op.getResponseHeader("res"), "-  - 123 -  -");
    }

    @Test
    public void getHeaders() {
        Operation op = Operation.createGet(this.host.getUri());

        // check request header
        op.getRequestHeaders().put("foo-request", "FOO-REQUEST");
        assertEquals("FOO-REQUEST", op.getRequestHeader("foo-request"));
        assertEquals("FOO-REQUEST", op.getRequestHeaders().get("foo-request"));

        op.addRequestHeader("bar-request", "BAR-REQUEST");
        assertEquals("BAR-REQUEST", op.getRequestHeader("bar-request"));
        assertEquals("BAR-REQUEST", op.getRequestHeaders().get("bar-request"));

        // check response header
        op.getResponseHeaders().put("foo-response", "FOO-RESPONSE");
        assertEquals("FOO-RESPONSE", op.getResponseHeader("foo-response"));
        assertEquals("FOO-RESPONSE", op.getResponseHeaders().get("foo-response"));

        op.addResponseHeader("bar-response", "BAR-RESPONSE");
        assertEquals("BAR-RESPONSE", op.getResponseHeader("bar-response"));
        assertEquals("BAR-RESPONSE", op.getResponseHeaders().get("bar-response"));
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

    @Test
    public void testSerializedOperation() throws Throwable {
        String link = ExampleService.FACTORY_LINK;
        String contextId = UUID.randomUUID().toString();
        String transactionId = UUID.randomUUID().toString();
        Operation op = Operation
                .createPost(UriUtils.buildUri(this.host, link, "someQuery", "someUserInfo"))
                .setBody("body")
                .setReferer(this.host.getReferer())
                .setContextId(contextId)
                .setTransactionId(transactionId)
                .setStatusCode(Operation.STATUS_CODE_OK);

        SerializedOperation sop = SerializedOperation.create(op);
        verifyOp(op, sop);
    }

    @Test
    public void testErrorCodes() throws Throwable {
        ServiceErrorResponse rsp = ServiceErrorResponse.create(
                new IllegalArgumentException(), Operation.STATUS_CODE_BAD_REQUEST);
        rsp.setErrorCode(123123);
        assertEquals(rsp.getErrorCode(), 123123);
        rsp.setInternalErrorCode(0x81234567);
        assertEquals(rsp.getErrorCode(), 0x81234567);
    }

    @Test
    public void testFailureCodes() throws Throwable {
        Operation op = Operation.createGet(this.host.getUri());
        Operation.failActionNotSupported(op);
        ServiceErrorResponse rsp = op.getErrorResponseBody();
        assertEquals(op.getStatusCode(), Operation.STATUS_CODE_BAD_METHOD);
        assertNotNull(rsp);
        assertEquals(rsp.statusCode, Operation.STATUS_CODE_BAD_METHOD);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonXenonErrorCode() throws Throwable {
        ServiceErrorResponse rsp = ServiceErrorResponse.create(
                new IllegalArgumentException(), Operation.STATUS_CODE_BAD_REQUEST);
        rsp.setErrorCode(0x81234567);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testXenonErrorCode() throws Throwable {
        ServiceErrorResponse rsp = ServiceErrorResponse.create(
                new IllegalArgumentException(), Operation.STATUS_CODE_BAD_REQUEST);
        rsp.setInternalErrorCode(123123);
    }

    private void verifyOp(Operation op, SerializedOperation sop) {
        assertEquals(op.getAction(), sop.action);
        assertEquals(op.getUri().getHost(), sop.host);
        assertEquals(op.getUri().getPort(), sop.port);
        assertEquals(op.getUri().getPath(), sop.path);
        assertEquals(op.getUri().getQuery(), sop.query);
        assertEquals(op.getId(), sop.id.longValue());
        assertEquals(op.getReferer(), sop.referer);
        assertEquals(op.getBodyRaw(), sop.jsonBody);
        assertEquals(op.getStatusCode(), sop.statusCode);
        assertEquals(op.getOptions(), sop.options);
        assertEquals(op.getContextId(), sop.contextId);
        assertEquals(op.getTransactionId(), sop.transactionId);
        assertEquals(op.getUri().getUserInfo(), sop.userInfo);
        assertEquals(SerializedOperation.KIND, sop.documentKind);
        assertEquals(op.getExpirationMicrosUtc(), sop.documentExpirationTimeMicros);
    }

    @Test
    public void testIsNotification() throws Throwable {
        // making sure we only do a full directive match to 'xn-nt' (PRAGMA_DIRECTIVE_NOTIFICATION)
        Operation op = Operation.createGet(null);
        assertFalse(op.isNotification());

        op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SKIPPED_NOTIFICATIONS);
        assertFalse(op.isNotification());

        op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NOTIFICATION);
        assertTrue(op.isNotification());

        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_SKIPPED_NOTIFICATIONS);
        assertTrue(op.isNotification());

        op.removePragmaDirective(Operation.PRAGMA_DIRECTIVE_NOTIFICATION);
        assertFalse(op.isNotification());
    }

    @Test
    public void conflictResponsePropagated() throws Throwable {

        this.host.startService(new ConflictStatelessService());
        this.host.waitForServiceAvailable(ConflictStatelessService.SELF_LINK);

        /**
         * Simulates the use case where a service needs to call a remote endpoint.
         * Normally, by using the sendWithDeferredResult() the caller loses the original
         * response body and status code in case of failure because the DeferredResult
         * does not propagate it back. By overriding the sendWithDeferredResult() however,
         * we are able to propagate the original failure body back to the caller.
         */
        class PropagatingErrorContextService extends StatelessService {

            public static final String SELF_LINK = ServiceUriPaths.CORE
                    + "/tests/propagating-error-context";

            public PropagatingErrorContextService() {
                toggleOption(ServiceOption.WRAP_ERROR_RESPONSE, true);
            }

            @Override
            public void handleGet(Operation get) {
                Operation xenonToRemote = Operation.createGet(this, ConflictStatelessService.SELF_LINK);
                xenonToRemote.forceRemote();

                sendWithDeferredResult(xenonToRemote)
                        .whenCompleteNotify(get);
            }
        }

        PropagatingErrorContextService service = new PropagatingErrorContextService();
        this.host.startService(service);
        this.host.waitForServiceAvailable(PropagatingErrorContextService.SELF_LINK);

        FailureResponse response = this.sender.sendAndWaitFailure(
                Operation.createGet(this.host, PropagatingErrorContextService.SELF_LINK));
        assertEquals(409, response.op.getStatusCode());
        assertEquals("failure-body", response.op.getErrorResponseBody().message);
    }

    @Test
    public void contextAttributesPropagated() throws Throwable {

        this.host.startService(new ConflictStatelessService());
        this.host.waitForServiceAvailable(ConflictStatelessService.SELF_LINK);

        /**
         * Simulates the use case where the user passes arbitrary context attributes through the
         * OperationContext TL and they get properly propagated through the asynchronous callbacks.
         */
        class PropagatingContextAttributesService extends StatelessService {

            public static final String SELF_LINK = ServiceUriPaths.CORE
                    + "/tests/propagating-context-attributes";

            @Override
            public void handleGet(Operation get) {
                OperationContext.setAttribute("test-key", "test-value");
                Operation xenonToRemote = Operation.createGet(this, ConflictStatelessService.SELF_LINK);
                xenonToRemote.forceRemote();

                // Use mutex to guarantee that the OperationContext is properly cleaned up
                // before the whenComplete callback is executed. Even in this case, the TL
                // attribute should be propagated to the callback.
                Semaphore mutex = new Semaphore(1);
                try {
                    mutex.acquire();

                    OperationContext.setAttribute("test-key", "test-value");
                    sendWithDeferredResult(xenonToRemote)
                            .whenComplete((__, e) -> {
                                try {
                                    mutex.acquire();
                                    assertNotNull(OperationContext.getAttribute("test-key"));
                                    assertEquals("test-value",
                                            OperationContext.getAttribute("test-key"));
                                } catch (InterruptedException ee) {
                                    fail("Failed while waiting for mutex: " + ee.getMessage());
                                } finally {
                                    mutex.release();
                                    get.complete();
                                }
                            });
                } catch (InterruptedException e) {
                    fail("Failed while waiting for mutex: " + e.getMessage());
                } finally {
                    OperationContext.removeAttribute("test-key");
                    assertNull(OperationContext.getAttribute("test-key"));
                    mutex.release();
                }
            }
        }

        PropagatingContextAttributesService service = new PropagatingContextAttributesService();
        this.host.startService(service);
        this.host.waitForServiceAvailable(PropagatingContextAttributesService.SELF_LINK);

        this.sender.sendAndWait(
                Operation.createGet(this.host, PropagatingContextAttributesService.SELF_LINK));
    }

    @Test
    public void notModifiedResponse() throws Throwable {
        this.host.startFactory(new NotModifiedOwnerSelectedService());
        this.host.startFactory(new NotModifiedNonOwnerSelectedService());
        this.host.startService(new NotModifiedStatelessService());
        this.host.waitForServiceAvailable(NotModifiedOwnerSelectedService.FACTORY_LINK,
                NotModifiedNonOwnerSelectedService.FACTORY_LINK,
                NotModifiedStatelessService.SELF_LINK);

        String ownerServicePath = UriUtils
                .buildUriPath(NotModifiedOwnerSelectedService.FACTORY_LINK, "/foo");
        String nonOwnerServicePath = UriUtils
                .buildUriPath(NotModifiedNonOwnerSelectedService.FACTORY_LINK, "/foo");

        NotModifiedOwnerSelectedService.State ownerServiceState = new NotModifiedOwnerSelectedService.State();
        ownerServiceState.name = "initial-owner-name";
        ownerServiceState.documentSelfLink = ownerServicePath;

        NotModifiedOwnerSelectedService.State nonOwnerServiceState = new NotModifiedOwnerSelectedService.State();
        nonOwnerServiceState.name = "initial-non-owner-name";
        nonOwnerServiceState.documentSelfLink = nonOwnerServicePath;

        Operation ownerPost = Operation
                .createPost(this.host, NotModifiedOwnerSelectedService.FACTORY_LINK)
                .setBody(ownerServiceState);
        this.sender.sendAndWait(ownerPost);

        Operation nonOwnerPost = Operation
                .createPost(this.host, NotModifiedNonOwnerSelectedService.FACTORY_LINK)
                .setBody(nonOwnerServiceState);
        this.sender.sendAndWait(nonOwnerPost);

        // check for GET
        performAndVerify304Response(ownerServicePath, Action.GET, false);
        performAndVerify304Response(ownerServicePath, Action.GET, true);
        performAndVerify304Response(nonOwnerServicePath, Action.GET, false);
        performAndVerify304Response(nonOwnerServicePath, Action.GET, true);
        performAndVerify304Response(NotModifiedStatelessService.SELF_LINK, Action.GET, false);
        performAndVerify304Response(NotModifiedStatelessService.SELF_LINK, Action.GET, true);

        // check for PUT and PATCH
        // (HTTP 304 behavior for PUT and PATCH are not specified. Currently, xenon applies same behavior for PUT/PATCH)

        // check owner service
        performAndVerify304Response(ownerServicePath, Action.PUT, false);
        performAndVerify304Response(ownerServicePath, Action.PUT, true);
        performAndVerify304Response(ownerServicePath, Action.PATCH, false);
        performAndVerify304Response(ownerServicePath, Action.PATCH, true);

        // check non-owner service
        performAndVerify304Response(nonOwnerServicePath, Action.PUT, false);
        performAndVerify304Response(nonOwnerServicePath, Action.PUT, true);
        performAndVerify304Response(nonOwnerServicePath, Action.PATCH, false);
        performAndVerify304Response(nonOwnerServicePath, Action.PATCH, true);

        // check stateless service
        performAndVerify304Response(NotModifiedStatelessService.SELF_LINK, Action.PUT, false);
        performAndVerify304Response(NotModifiedStatelessService.SELF_LINK, Action.PUT, true);
        performAndVerify304Response(NotModifiedStatelessService.SELF_LINK, Action.PATCH, false);
        performAndVerify304Response(NotModifiedStatelessService.SELF_LINK, Action.PATCH, true);
    }

    private void performAndVerify304Response(String servicePath, Action action, boolean isRemote) {
        NotModifiedOwnerSelectedService.State dummyBody = new NotModifiedOwnerSelectedService.State();
        Operation op = Operation.createGet(this.host, servicePath).setBody(dummyBody);
        op.setAction(action);
        if (isRemote) {
            op.forceRemote();
        }
        Operation resultOp = this.sender.sendAndWait(op);
        assertNull(resultOp.getBodyRaw());
        assertEquals(isRemote, resultOp.isRemote());
        assertEquals(STATUS_CODE_NOT_MODIFIED, resultOp.getStatusCode());
    }

}
