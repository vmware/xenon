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

import com.vmware.xenon.common.Utils;

/**
 * Default timeout strategy implementation.
 *
 * Default order to resolve timeout duration is:
 * - system variable
 * - thread local
 *
 * For temporal operation with specified {@link Duration} as timeout, use {@link #withTimeout(Duration, ExecutableBlock)}.
 *
 * @see XenonTestConfig
 */
public class DefaultTimeoutStrategy implements TimeoutStrategy {

    private static final String PROPERTY_NAME_TIMEOUT_SEC = Utils.PROPERTY_NAME_PREFIX + "test.timeout";
    private static final int DEFAULT_TIMEOUT_SEC = 30;
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(Integer.getInteger(PROPERTY_NAME_TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC));

    private static final ThreadLocal<Duration> timeout = ThreadLocal.withInitial(() -> DEFAULT_TIMEOUT);

    public static void setTimeoutDuration(Duration duration) {
        timeout.set(duration);
    }

    public static Duration getTimeoutDuration() {
        return timeout.get();
    }

    public static void resetTimeoutDuration() {
        timeout.set(DEFAULT_TIMEOUT);
    }


    public static void withTimeout(Duration duration, ExecutableBlock executable) {
        if (duration == null) {
            // TODO: throw exception
        }
        Duration original = DefaultTimeoutStrategy.getTimeoutDuration();
        ExceptionTestUtils.executeSafely(() -> {
            try {
                DefaultTimeoutStrategy.setTimeoutDuration(duration);
                executable.execute();
            } finally {
                DefaultTimeoutStrategy.setTimeoutDuration(original);
            }
        });
    }

    @Override
    public Duration getTimeout() {
        return DefaultTimeoutStrategy.timeout.get();
    }
}
