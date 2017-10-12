/*
 * Copyright (c) 2017 VMware, Inc. All Rights Reserved.
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

package com.vmware.xenon.common.opentracing;

import io.opentracing.ActiveSpan;
import io.opentracing.tag.Tags;

import com.vmware.xenon.common.Operation;

public class TracingUtils {

    private TracingUtils() {}

    /**
     * Set common tags for tracing spans on an operation.
     */
    public static void setSpanTags(Operation operation, ActiveSpan span) {
        span.setTag(Tags.HTTP_METHOD.getKey(), operation.getAction().toString());
        span.setTag(Tags.HTTP_URL.getKey(), operation.getUri().toString());
    }
}