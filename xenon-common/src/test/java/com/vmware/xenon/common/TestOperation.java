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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.Operation.SerializedOperation;
import com.vmware.xenon.common.Service.Action;
import com.vmware.xenon.common.test.MinimalTestServiceState;
import com.vmware.xenon.common.test.TestContext;
import com.vmware.xenon.services.common.ExampleService;
import com.vmware.xenon.services.common.MinimalTestService;

public class TestOperation extends BasicReusableHostTestCase {
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

        assertArrayEquals(new Integer[]{2, 1, 0, 10, 20}, list.toArray(new Integer[list.size()]));
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

        assertArrayEquals(new Integer[]{2, 1, 10, 20}, list.toArray(new Integer[list.size()]));
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

        assertArrayEquals(new Integer[]{1, 2, 3, 30, 20, 10}, list.toArray(new Integer[list.size()]));
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

        assertArrayEquals(new Integer[]{1, 2, 3, 20, 10}, list.toArray(new Integer[list.size()]));
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

        assertArrayEquals(new Integer[]{1, 2, 10}, list.toArray(new Integer[list.size()]));
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

        assertArrayEquals(new Integer[]{1, 2, 20, 10}, list.toArray(new Integer[list.size()]));
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
        rsp.setXenonErrorCode(0x81234567);
        assertEquals(rsp.getErrorCode(), 0x81234567);
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
        rsp.setXenonErrorCode(123123);
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
        assertEquals(op.options, sop.options);
        assertEquals(op.getContextId(), sop.contextId);
        assertEquals(op.getTransactionId(), sop.transactionId);
        assertEquals(op.getUri().getUserInfo(), sop.userInfo);
        assertEquals(SerializedOperation.KIND, sop.documentKind);
        assertEquals(op.getExpirationMicrosUtc(), sop.documentExpirationTimeMicros);
    }
}
