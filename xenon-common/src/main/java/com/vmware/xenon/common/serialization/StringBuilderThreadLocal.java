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

package com.vmware.xenon.common.serialization;

import com.vmware.xenon.common.ServiceHost;

/**
 */
public class StringBuilderThreadLocal extends ThreadLocal<StringBuilder> {

    public static final int DEFAULT_INITIAL_CAPACITY = ServiceHost.DEFAULT_SERVICE_STATE_COST_BYTES;

    @Override
    protected StringBuilder initialValue() {
        return new StringBuilder(DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public StringBuilder get() {
        StringBuilder result = super.get();

        if (result.length() > KryoSerializers.THREAD_LOCAL_BUFFER_LIMIT_BYTES) {
            result.setLength(DEFAULT_INITIAL_CAPACITY);
            result.trimToSize();
        }

        result.setLength(0);
        return result;
    }
}
