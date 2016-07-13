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

package com.vmware.xenon.common.net;

public interface ContentAware {

    boolean hasBody();

    /**
     * Deserializes the body associated with the operation, given the type.
     *
     * Note: This method is *not* idempotent. It will modify the body contents
     * so subsequent calls will not have access to the original instance. This
     * occurs only for local operations, not operations that have a serialized
     * body already attached (in the form of a JSON string).
     *
     * If idempotent behavior is desired, use {@link #getBodyRaw}
     */
    <T> T getBody(Class<T> type);

    Object getBodyRaw();

    ContentAware setBody(Object body);

    long getContentLength();

    ContentAware setContentLength(long l);

    String getContentType();

    ContentAware setContentType(String type);
}
