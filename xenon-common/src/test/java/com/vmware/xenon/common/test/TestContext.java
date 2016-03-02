package com.vmware.xenon.common.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.vmware.xenon.common.Utils;
import com.vmware.xenon.common.Operation.CompletionHandler;

public class TestContext {
    private CountDownLatch latch;
    private long expiration;
    private volatile Throwable error;

    public static TestContext create(int count, long expIntervalMicros) {
        TestContext ctx = new TestContext();
        ctx.latch = new CountDownLatch(count);
        ctx.expiration = Utils.getNowMicrosUtc();
        ctx.expiration += expIntervalMicros;
        return ctx;
    }

    public void completeIteration() {
        this.latch.countDown();
    }

    public void failIteration(Throwable e) {
        this.error = e;
        this.latch.countDown();
    }

    public void await() throws Throwable {
        if (this.latch == null) {
            throw new IllegalStateException("This context is already used");
        }

        // keep polling latch every second, allows for easier debugging
        while (Utils.getNowMicrosUtc() < this.expiration) {
            if (this.latch.await(1, TimeUnit.SECONDS)) {
                break;
            }
        }

        if (this.expiration < Utils.getNowMicrosUtc()) {
            throw new TimeoutException();
        }

        // prevent this latch from being reused
        this.latch = null;

        if (this.error != null) {
            throw this.error;
        }

        return;
    }

    public CompletionHandler getCompletion() {
        return (o, e) -> {
            if (e != null) {
                this.failIteration(e);
            } else {
                this.completeIteration();
            }
        };
    }

    public CompletionHandler getExpectedFailureCompletion() {
        return (o, e) -> {
            if (e != null) {
                this.completeIteration();
            } else {
                this.failIteration(new IllegalStateException("got success, expected failure"));
            }
        };
    }
}