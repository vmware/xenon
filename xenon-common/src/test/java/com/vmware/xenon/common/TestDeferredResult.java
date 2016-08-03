/*
 * Copyright (c) 2014-2016 VMware, Inc. All Rights Reserved.
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

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.vmware.xenon.common.test.TestContext;

public class TestDeferredResult {
    private <T> Supplier<T> exceptionSupplier() {
        return exceptionSupplier("Not ready!");
    }

    private <T> Supplier<T> exceptionSupplier(String message) {
        return () -> {
            throw new RuntimeException(message);
        };
    }

    public static class TestException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public TestException() {
            super();
        }

        public TestException(String message) {
            super(message);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //
        }
    }

    private <T> BiConsumer<T, ? super Throwable> getExpectedExceptionCompletion(TestContext ctx) {
        return getExpectedExceptionCompletion(ctx, true);
    }

    private <T> BiConsumer<T, ? super Throwable> getExpectedExceptionCompletion(TestContext ctx, boolean wrapped) {
        return (ignore, ex) -> {
            try {
                Assert.assertNotNull(ex);
                if (wrapped) {
                    Assert.assertEquals(CompletionException.class, ex.getClass());
                    Assert.assertNotNull(ex.getCause());
                    ex = ex.getCause();
                }
                Assert.assertEquals(TestException.class, ex.getClass());
                ctx.completeIteration();
            } catch (Throwable e) {
                ctx.failIteration(e);
            }
        };
    }

    private void runAfter(long millis, Runnable action) {
        new Thread(() -> {
            sleep(millis);
            action.run();
        }).start();
    }

    private void runAfterAndComplete(long millis, Runnable action, TestContext ctx) {
        runAfter(millis, () -> {
            action.run();
            ctx.completeIteration();
        });
    }

    private void runAfter(TestContext ctx, Runnable action) {
        runAfter(ctx, 0, action);
    }

    private void runAfter(TestContext ctx, long additionalDelay, Runnable action) {
        Thread t = new Thread(() -> {
            ctx.await();
            if (additionalDelay > 0) {
                sleep(additionalDelay);
            }
            action.run();
        });
        t.setDaemon(true);
        t.start();
    }

    @Test
    public void testThenApply() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> result = original
                .thenApply(i -> i + 1);
        result.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.complete(1));
        ctx.await();
        Assert.assertEquals(2, result.getNow(0).intValue());
    }

    @Test
    public void testThenCompose() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<String> result = DeferredResult.completed(12345)
                .thenCompose(ignore -> {
                    DeferredResult<String> nested = new DeferredResult<>();
                    runAfter(10, () -> nested.complete("foo"));
                    return nested;
                });
        result.whenComplete(ctx.getCompletionDeferred());
        ctx.await();
        Assert.assertEquals("foo", result.getNow("bar"));
    }

    @Test
    public void testThenComposeNestedStageException() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult.completed(12345)
            .thenCompose(ignore -> {
                DeferredResult<String> nested = new DeferredResult<>();
                runAfter(10, () -> nested.fail(new TestException()));
                return nested;
            })
            .whenComplete(getExpectedExceptionCompletion(ctx));
        ctx.await();
    }

    @Test
    public void testThenAccept() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        original
            .thenAccept(i -> {
                invocations.incrementAndGet();
                Assert.assertEquals(1, i.intValue());
            })
            .whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.complete(1));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testThenAcceptCanThrowException() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        original
            .thenAccept(i -> {
                throw new TestException();
            })
            .whenComplete(getExpectedExceptionCompletion(ctx));
        runAfter(10, () -> original.complete(1));
        ctx.await();
    }

    @Test
    public void testThenRun() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        original
            .thenRun(() -> {
                invocations.incrementAndGet();
            })
            .whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.complete(1));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testThenCombine() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> other = new DeferredResult<>();
        DeferredResult<Integer> result = original
                .thenCombine(other, (x, y) -> x + y);
        result.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.complete(1));
        runAfter(20, () -> other.complete(2));
        ctx.await();
        Assert.assertEquals(3, result.getNow(0).intValue());
    }

    private void verifyThenAcceptBoth(long waitOriginal, long waitOther) throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> other = new DeferredResult<>();
        AtomicInteger invocations = new AtomicInteger();
        original
            .thenAcceptBoth(other, (x, y) -> {
                Assert.assertEquals(1, x.intValue());
                Assert.assertEquals(2, y.intValue());
                invocations.incrementAndGet();
            })
            .whenComplete(ctx.getCompletionDeferred());
        runAfter(waitOriginal, () -> original.complete(1));
        runAfter(waitOther, () -> other.complete(2));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testThenAcceptBoth() throws Throwable {
        verifyThenAcceptBoth(1, 20);
        verifyThenAcceptBoth(20, 1);
    }

    private void verifyRunAfterBoth(long waitOriginal, long waitOther) throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> other = new DeferredResult<>();
        AtomicInteger invocations = new AtomicInteger();
        original
            .runAfterBoth(other, () -> {
                Assert.assertEquals(1, original.getNow(0).intValue());
                Assert.assertEquals(2, other.getNow(0).intValue());
                invocations.incrementAndGet();
            })
            .whenComplete(ctx.getCompletionDeferred());
        runAfter(waitOriginal, () -> original.complete(1));
        runAfter(waitOther, () -> other.complete(2));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testRunAfterBoth() throws Throwable {
        verifyRunAfterBoth(1, 100);
        verifyRunAfterBoth(100, 1);
    }

    private void verifyApplyToEither(long wait, boolean originalFirst) throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> other = new DeferredResult<>();
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> result = original
                .applyToEither(other, x -> {
                    invocations.incrementAndGet();
                    return x + 1;
                });
        result.whenComplete(ctx.getCompletionDeferred());
        int originalValue = 10;
        int otherValue = 20;
        TestContext synchCtx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        if (originalFirst) {
            runAfterAndComplete(wait, () -> original.complete(originalValue), synchCtx);
            runAfter(synchCtx, () -> other.complete(otherValue));
        } else {
            runAfter(synchCtx, () -> original.complete(originalValue));
            runAfterAndComplete(wait, () -> other.complete(otherValue), synchCtx);
        }
        ctx.await();
        Assert.assertEquals(1, invocations.get());
        int expected = (originalFirst ? originalValue : otherValue) + 1;
        Assert.assertEquals(expected, result.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testApplyToEither() throws Throwable {
        verifyApplyToEither(10, true);
        verifyApplyToEither(10, false);
    }

    private void verifyAcceptEither(long wait, boolean originalFirst) throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> other = new DeferredResult<>();
        AtomicInteger invocations = new AtomicInteger();
        int originalValue = 10;
        int otherValue = 20;
        int expected = (originalFirst ? originalValue : otherValue);
        original
            .acceptEither(other, (x) -> {
                Assert.assertEquals(expected, x.intValue());
                invocations.incrementAndGet();
            })
            .whenComplete(ctx.getCompletionDeferred());
        TestContext synchCtx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        if (originalFirst) {
            runAfterAndComplete(wait, () -> original.complete(originalValue), synchCtx);
            runAfter(synchCtx, wait, () -> other.complete(otherValue));
        } else {
            runAfter(synchCtx, wait, () -> original.complete(originalValue));
            runAfterAndComplete(wait, () -> other.complete(otherValue), synchCtx);
        }
        ctx.await();
        Assert.assertFalse(originalFirst ? other.isDone() : original.isDone());
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testAcceptEither() throws Throwable {
        verifyAcceptEither(100, true);
        verifyAcceptEither(100, false);
    }

    private void verifyRunAfterEither(long wait, boolean originalFirst) throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> other = new DeferredResult<>();
        AtomicInteger invocations = new AtomicInteger();
        int originalValue = 10;
        int otherValue = 20;
        int absentValue = 0;
        int expectedOriginal = (originalFirst ? originalValue : absentValue);
        int expectedOther = (originalFirst ? absentValue : otherValue);
        original
            .runAfterEither(other, () -> {
                Assert.assertEquals(expectedOriginal, original.getNow(absentValue).intValue());
                Assert.assertEquals(expectedOther, other.getNow(absentValue).intValue());
                invocations.incrementAndGet();
            })
            .whenComplete(ctx.getCompletionDeferred());
        TestContext synchCtx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        if (originalFirst) {
            runAfterAndComplete(wait, () -> original.complete(originalValue), synchCtx);
            runAfter(synchCtx, wait, () -> other.complete(otherValue));
        } else {
            runAfter(synchCtx, wait, () -> original.complete(originalValue));
            runAfterAndComplete(wait, () -> other.complete(otherValue), synchCtx);
        }
        ctx.await();
        Assert.assertFalse(originalFirst ? other.isDone() : original.isDone());
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testRunAfterEither() throws Throwable {
        verifyRunAfterEither(100, true);
        verifyRunAfterEither(100, false);
    }

    @Test
    public void testExceptionally() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        int recoverValue = 5;
        DeferredResult<Integer> result = original
                .exceptionally(ex -> {
                    Assert.assertNotNull(ex);
                    Assert.assertEquals(TestException.class, ex.getClass());
                    invocations.incrementAndGet();
                    return recoverValue;
                })
                .thenApply(x -> x + 1);
        result.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.fail(new TestException()));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
        Assert.assertEquals(recoverValue + 1, result.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testExceptionallyNoException() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        int recoverValue = 5;
        DeferredResult<Integer> result = original
                .exceptionally(ex -> {
                    invocations.incrementAndGet();
                    return recoverValue;
                })
                .thenApply(x -> x + 1);
        result.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.complete(1));
        ctx.await();
        Assert.assertEquals(0, invocations.get());
        Assert.assertEquals(2, result.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testExceptionallyRethrow() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        original
            .exceptionally(ex -> {
                Assert.assertNotNull(ex);
                Assert.assertEquals(RuntimeException.class, ex.getClass());
                invocations.incrementAndGet();
                throw new TestException();
            })
            .whenComplete(getExpectedExceptionCompletion(ctx));
        runAfter(10, () -> original.fail(new RuntimeException()));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testWhenCompleteWithException() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        original
            .whenComplete((result, ex) -> {
                invocations.incrementAndGet();
                // We throw RTE, but the original exception is propagated
                throw new RuntimeException();
            })
            .whenComplete(getExpectedExceptionCompletion(ctx));
        runAfter(10, () -> original.fail(new TestException()));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
    }

    @Test
    public void testHandle() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> result = original
                .handle((x, ex) -> {
                    Assert.assertNull(ex);
                    invocations.incrementAndGet();
                    return x + 1;
                });
        result.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.complete(1));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
        Assert.assertEquals(2, result.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testHandleException() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        int recoveredValue = 5;
        DeferredResult<Integer> original = new DeferredResult<>();
        DeferredResult<Integer> result = original
                .handle((x, ex) -> {
                    Assert.assertNotNull(ex);
                    Assert.assertEquals(TestException.class, ex.getClass());
                    invocations.incrementAndGet();
                    return recoveredValue;
                })
                .thenApply(x -> x + 1);
        result.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> original.fail(new TestException()));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
        Assert.assertEquals(recoveredValue + 1, result.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testHandleRethrow() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        AtomicInteger postExceptionInvocations = new AtomicInteger();
        DeferredResult<Integer> original = new DeferredResult<>();
        original
            .handle((x, ex) -> {
                Assert.assertNotNull(ex);
                Assert.assertEquals(RuntimeException.class, ex.getClass());
                invocations.incrementAndGet();
                throw new TestException();
            })
            .thenRun(() -> {
                postExceptionInvocations.incrementAndGet();
            })
            .whenComplete(getExpectedExceptionCompletion(ctx));
        runAfter(10, () -> original.fail(new RuntimeException()));
        ctx.await();
        Assert.assertEquals(1, invocations.get());
        Assert.assertEquals(0, postExceptionInvocations.get());
    }

    @Test
    public void testGetNowWithValue() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        int defaultValue = 0;
        int value = 10;
        DeferredResult<Integer> deferredResult = new DeferredResult<>();
        deferredResult.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> deferredResult.complete(value));
        Assert.assertEquals(defaultValue, deferredResult.getNow(defaultValue).intValue());
        ctx.await();
        Assert.assertEquals(value, deferredResult.getNow(defaultValue).intValue());
    }

    @Test
    public void testGetNowWithValueFailed() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        int defaultValue = 0;
        DeferredResult<Integer> deferredResult = new DeferredResult<>();
        deferredResult.whenComplete(getExpectedExceptionCompletion(ctx, false));
        runAfter(10, () -> deferredResult.fail(new TestException()));
        Assert.assertEquals(defaultValue, deferredResult.getNow(defaultValue).intValue());
        ctx.await();
        try {
            deferredResult.getNow(defaultValue);
            Assert.fail();
        } catch (CompletionException e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertEquals(TestException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testGetNow() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        int value = 10;
        DeferredResult<Integer> deferredResult = new DeferredResult<>();
        deferredResult.whenComplete(ctx.getCompletionDeferred());
        runAfter(10, () -> deferredResult.complete(value));
        try {
            deferredResult.getNow(exceptionSupplier("expected"));
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals("expected", e.getMessage());
        }
        ctx.await();
        Assert.assertEquals(value, deferredResult.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testGetNowFailed() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<Integer> deferredResult = new DeferredResult<>();
        deferredResult.whenComplete(getExpectedExceptionCompletion(ctx, false));
        runAfter(10, () -> deferredResult.fail(new TestException()));
        try {
            deferredResult.getNow(exceptionSupplier("expected"));
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals("expected", e.getMessage());
        }
        ctx.await();
        try {
            deferredResult.getNow(exceptionSupplier());
            Assert.fail();
        } catch (CompletionException e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertEquals(TestException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testExecutionAfterException() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        AtomicInteger invocations = new AtomicInteger();
        DeferredResult<Integer> other = DeferredResult.completed(1);
        DeferredResult<Integer> deferredResult = new DeferredResult<>();
        deferredResult
            .thenApply(x -> {
                invocations.incrementAndGet();
                return x;
            })
            .thenAccept(x -> {
                invocations.incrementAndGet();
            })
            .thenCompose(x -> {
                invocations.incrementAndGet();
                return DeferredResult.completed(x);
            })
            .thenRun(() -> {
                invocations.incrementAndGet();
            })
            .thenAcceptBoth(other, (x, y) -> {
                invocations.incrementAndGet();
            })
            .thenCombine(other, (x, y) -> {
                invocations.incrementAndGet();
                return x;
            })
            .runAfterBoth(other, () -> {
                invocations.incrementAndGet();
            })
            .whenComplete(getExpectedExceptionCompletion(ctx));
        runAfter(10, () -> deferredResult.fail(new TestException()));
        ctx.await();
        Assert.assertEquals(0, invocations.get());
    }

    @Test
    public void testAllOf() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        int count = 10;
        List<DeferredResult<Integer>> deferredResults = IntStream.range(0, count)
                .mapToObj(ignore -> new DeferredResult<Integer>())
                .collect(Collectors.toList());
        DeferredResult<List<Integer>> deferredResult = DeferredResult.allOf(deferredResults);
        deferredResult.whenComplete(ctx.getCompletionDeferred());
        IntStream.range(0, count).forEach(i -> {
            runAfter((long) (Math.random() * 100), () -> deferredResults.get(i).complete(i));
        });
        ctx.await();
        List<Integer> result = deferredResult.getNow(exceptionSupplier());
        IntStream.range(0, count).forEach(i -> {
            Assert.assertEquals(i, result.get(i).intValue());
        });
    }

    @Test
    public void testAllOfOneFail() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        int count = 10;
        List<DeferredResult<Integer>> deferredResults = IntStream.range(0, count)
                .mapToObj(ignore -> new DeferredResult<Integer>())
                .collect(Collectors.toList());
        DeferredResult<List<Integer>> deferredResult = DeferredResult.allOf(deferredResults);
        deferredResult.whenComplete(getExpectedExceptionCompletion(ctx));
        IntStream.range(0, count).forEach(i -> {
            if (i == 5) {
                deferredResults.get(i).fail(new TestException());
            } else {
                runAfter((long) (Math.random() * 100), () -> deferredResults.get(i).complete(i));
            }
        });
        ctx.await();
        try {
            deferredResult.getNow(exceptionSupplier());
            Assert.fail();
        } catch (CompletionException e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertEquals(TestException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testAnyOf() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        int count = 10;
        List<DeferredResult<Integer>> deferredResults = IntStream.range(0, count)
                .mapToObj(ignore -> new DeferredResult<Integer>())
                .collect(Collectors.toList());
        DeferredResult<Integer> deferredResult = DeferredResult.anyOf(deferredResults);
        deferredResult.whenComplete(ctx.getCompletionDeferred());
        int fastest = 7;
        TestContext synchCtx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        IntStream.range(0, count).forEach(i -> {
            if (i == fastest) {
                runAfterAndComplete(1, () -> deferredResults.get(i).complete(i), synchCtx);
            } else {
                runAfter(synchCtx, () -> deferredResults.get(i).complete(i));
            }
        });
        ctx.await();
        Assert.assertEquals(fastest, deferredResult.getNow(exceptionSupplier()).intValue());
    }

    @Test
    public void testGetNowWithSlowWhenComplete() throws Throwable {
        TestContext ctx = TestContext.create(1, TimeUnit.SECONDS.toMicros(1));
        DeferredResult<String> original = new DeferredResult<>();
        DeferredResult<String> result = original
                .whenComplete((ignore, error) -> {
                    if (error != null) {
                        ctx.fail(error.getCause());
                    } else {
                        ctx.complete();
                    }
                    sleep(500);
                });
        runAfter(1, () -> original.complete("foo"));
        ctx.await();
        Assert.assertEquals("bar", result.getNow("bar"));
    }
}
