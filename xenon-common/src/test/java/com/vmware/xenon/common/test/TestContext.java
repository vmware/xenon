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

package com.vmware.xenon.common.test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.test.VerificationHost.WaitHandler;

/**
 * Test context used for synchronous tests. Provides an isolated version of the
 * {@code VerificationHost} testStart and testWait methods and allows nesting
 */
public class TestContext {

    public static final Duration DEFAULT_WAIT_DURATION = Duration.ofSeconds(30);

    private CountDownLatch latch;

    private Duration interval = Duration.ofSeconds(1);

    private Duration duration;

    private volatile Throwable error;

    private boolean started;

    private int initialCount;

    /**
     * Consider using {@link #TestContext(int, Duration)}
     * This method exists for backward compatibility, and may be deprecated/deleted in future.
     */
    public static TestContext create(int count, long expIntervalMicros) {
        return new TestContext(count, Duration.of(expIntervalMicros, ChronoUnit.MICROS));
    }

    public static void waitFor(Duration waitDuration, WaitHandler wh) {
        waitFor(waitDuration, wh, () -> "waitFor timed out");
    }

    public static void waitFor(Duration waitDuration, WaitHandler wh, String timeoutMessage) {
        waitFor(waitDuration, wh, () -> timeoutMessage);
    }

    public static void waitFor(Duration waitDuration, WaitHandler wh, Supplier<String> timeoutMessageSupplier) {
        TestContext waitContext = new TestContext(1, waitDuration);

        try {
            waitContext.await(() -> {
                if (wh.isReady()) {
                    waitContext.complete();
                }
            });
        } catch (Exception ex) {
            Exception exceptionToThrow;
            if (ex instanceof TimeoutException) {
                // add original exception as suppressed exception to provide more context
                String timeoutMessage = timeoutMessageSupplier.get();
                exceptionToThrow = new TimeoutException(timeoutMessage);
            } else {
                String msg = "waitFor check logic raised exception";
                exceptionToThrow = new IllegalStateException(msg);
            }
            exceptionToThrow.addSuppressed(ex);
            throw ExceptionTestUtils.throwAsUnchecked(exceptionToThrow);
        }
    }


    public TestContext(int count, Duration duration) {
        this.latch = new CountDownLatch(count);
        this.duration = duration;
        this.initialCount = count;
    }

    public void complete() {
        this.started = true;
        this.latch.countDown();
    }

    public void fail(Throwable e) {
        this.started = true;
        this.error = e;
        this.latch.countDown();
    }

    public void setCount(int count) {
        if (this.started) {
            throw new RuntimeException(String.format(
                    "%s has already started. count=%d", getClass().getSimpleName(), this.latch.getCount()));
        }
        this.latch = new CountDownLatch(count);
    }

    public void setTimeout(Duration duration) {
        if (this.started) {
            throw new RuntimeException(String.format(
                    "%s has already started. count=%d", getClass().getSimpleName(), this.latch.getCount()));
        }
        this.duration = duration;
    }

    public void setCheckInterval(Duration interval) {
        this.interval = interval;
    }

    public void await() {
        await(() -> {
        });
    }

    public void await(ExecutableBlock beforeCheck) {

        ExceptionTestUtils.executeSafely(() -> {

            if (this.latch == null) {
                throw new IllegalStateException("This context is already used");
            }

            Instant startAt = Instant.now();
            Instant expireAt = startAt.plus(this.duration);
            long countAtAwait = this.latch.getCount();

            // keep polling latch every interval
            while (expireAt.isAfter(Instant.now())) {
                beforeCheck.execute();
                if (this.latch.await(this.interval.toNanos(), TimeUnit.NANOSECONDS)) {
                    break;
                }
            }

            Instant now = Instant.now();
            if (expireAt.isBefore(now)) {
                LocalDateTime startAtLocal = LocalDateTime.ofInstant(startAt, ZoneId.systemDefault());
                LocalDateTime expireAtLocal = LocalDateTime.ofInstant(expireAt, ZoneId.systemDefault());

                Duration actualDuration = Duration.between(startAt, now);
                String msg = String.format("%s has expired. [start=%s(%s), expire=%s(%s), " +
                                "durationGiven=%s, durationActual=%s, " +
                                "countAtInit=%d, countAtAwait=%d, countNow=%d]",
                        getClass().getSimpleName(),
                        startAt.toEpochMilli(), startAtLocal, expireAt.toEpochMilli(), expireAtLocal,
                        this.duration, actualDuration,
                        this.initialCount, countAtAwait, this.latch.getCount());
                throw new TimeoutException(msg);
            }

            // prevent this latch from being reused
            this.latch = null;

            if (this.error != null) {
                throw this.error;
            }
        });
    }

    /**
     * Consider using {@link #complete()}.
     * This method exists for backward compatibility, and may be deprecated/deleted in future.
     */
    public void completeIteration() {
        complete();
    }

    /**
     * Consider using {@link #fail(Throwable)}.
     * This method exists for backward compatibility, and may be deprecated/deleted in future.
     */
    public void failIteration(Throwable e) {
        fail(e);
    }

    public CompletionHandler getCompletion() {
        return (o, e) -> {
            if (e != null) {
                this.fail(e);
            } else {
                this.complete();
            }
        };
    }

    public <T> BiConsumer<T, ? super Throwable> getCompletionDeferred() {
        return (ignore, e) -> {
            if (e != null) {
                if (e instanceof CompletionException) {
                    e = e.getCause();
                }
                failIteration(e);
                return;
            }
            completeIteration();
        };
    }

    public CompletionHandler getExpectedFailureCompletion() {
        return (o, e) -> {
            if (e != null) {
                this.complete();
            } else {
                this.fail(new IllegalStateException("got success, expected failure"));
            }
        };
    }
}