/*
 * Copyright (c) 2014-2019 VMware, Inc. All Rights Reserved.
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
 * An exception wrapper that holds additional context information such as
 * HTTP status code and failure body.
 * <p>
 * Can be used together with {@link DeferredResult} to propagate the failure body and
 * status code in case the sent {@link Operation} completed exceptionally.
 */
public class WrappedDeferredResultException extends Exception {

    private static final long serialVersionUID = 1L;

    public final ServiceErrorResponse failureBody;
    public final int statusCode;

    public WrappedDeferredResultException(Throwable cause, ServiceErrorResponse failureBody, int statusCode) {
        super(cause);
        this.failureBody = failureBody;
        this.statusCode = statusCode;
    }
}
