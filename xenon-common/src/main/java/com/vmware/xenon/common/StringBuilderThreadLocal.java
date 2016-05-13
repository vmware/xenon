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

/**
 */
public class StringBuilderThreadLocal extends ThreadLocal<StringBuilder> {

    private static final int BUFFER_INITIAL_CAPACITY = 1 * 1024;

    @Override
    protected StringBuilder initialValue() {
        return new StringBuilder(BUFFER_INITIAL_CAPACITY);
    }

    @Override
    public StringBuilder get() {
        StringBuilder result = super.get();

        if (result.length() > BUFFER_INITIAL_CAPACITY * 10) {
            result = new StringBuilder(BUFFER_INITIAL_CAPACITY);
            set(result);
        } else {
            result.setLength(0);
        }

        return result;
    }
}
