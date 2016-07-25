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

package com.vmware.xenon.test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestContext {
    private CountDownLatch latch;
    private LocalDateTime expiredAt;
    private volatile Throwable error;

    public TestContext(int count) {
        this(count, XenonTestConfig.timeoutStrategy.getTimeout());
    }

    public TestContext(int count, Duration duration) {
        this.latch = new CountDownLatch(count);
        this.expiredAt = LocalDateTime.now().plus(duration);
    }

    public void completeIteration() {
        this.latch.countDown();
    }

    public void failIteration(Throwable e) {
        this.error = e;
        this.latch.countDown();
    }

    public void await() {
        ExceptionTestUtils.executeSafely(() -> {
            if (this.latch == null) {
                throw new IllegalStateException("This context is already used");
            }

            // keep polling latch every second, allows for easier debugging
            while (this.expiredAt.isAfter(LocalDateTime.now())) {
                if (this.latch.await(1, TimeUnit.SECONDS)) {
                    break;
                }
            }

            if (this.expiredAt.isBefore(LocalDateTime.now())) {
                // TODO: include latch count info and maybe stack info into error message
                throw new TimeoutException();
            }

            // prevent this latch from being reused
            this.latch = null;

            if (this.error != null) {
                // TOOD: may include more information into error by error.addSuppressed() or wrapping error
                throw this.error;
            }
        });
    }
}
