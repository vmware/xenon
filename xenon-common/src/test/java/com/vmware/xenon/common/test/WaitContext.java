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

package com.vmware.xenon.common.test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TODO: deprecate TestContext
 */
public class WaitContext {

    private CountDownLatch latch;

    private Duration interval = Duration.ofSeconds(1);

    private Duration duration;

    private volatile Throwable error;

    private boolean started;

    public WaitContext(int count, Duration duration) {
        this.latch = new CountDownLatch(count);
        this.duration = duration;
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
                    "%s has already started. count=%d", WaitContext.class.getSimpleName(), this.latch.getCount()));
        }
        this.latch = new CountDownLatch(count);
    }

    public void setTimeout(Duration duration) {
        if (this.started) {
            throw new RuntimeException(String.format(
                    "%s has already started. count=%d", WaitContext.class.getSimpleName(), this.latch.getCount()));
        }
        this.duration = duration;
    }

    public void setCheckInterval(Duration interval) {
        this.interval = interval;
    }

    public void await() {

        ExceptionTestUtils.executeSafely(() -> {

            if (this.latch == null) {
                throw new IllegalStateException("This context is already used");
            }

            LocalDateTime expireAt = LocalDateTime.now().plus(this.duration);
            // keep polling latch every interval
            while (expireAt.isAfter(LocalDateTime.now())) {
                if (this.latch.await(this.interval.toNanos(), TimeUnit.NANOSECONDS)) {
                    break;
                }
            }

            LocalDateTime now = LocalDateTime.now();
            if (expireAt.isBefore(now)) {
                Duration difference = Duration.between(expireAt, now);

                throw new TimeoutException(String.format(
                        "%s has expired. [duration=%s, count=%d]", WaitContext.class.getSimpleName(), difference,
                        this.latch.getCount()));
            }

            // prevent this latch from being reused
            this.latch = null;

            if (this.error != null) {
                throw this.error;
            }
        });
    }
}
