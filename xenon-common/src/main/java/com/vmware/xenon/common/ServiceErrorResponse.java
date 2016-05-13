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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Common service error response body set by the framework when an operation fails.
 * If the service author call {@code Operation.fail} and specifies a error response body,
 * it will be preserved. Service authors should derive from this class and add any
 * additional fields that communicate error details to the client.
 */
public class ServiceErrorResponse {
    public static enum ErrorDetail {
        SHOULD_RETRY
    }

    public static ServiceErrorResponse create(Throwable e, int statusCode) {
        return create(e, statusCode, null);
    }

    public static ServiceErrorResponse createWithShouldRetry(Throwable e) {
        return create(e, Operation.STATUS_CODE_FAILURE_THRESHOLD,
                EnumSet.of(ErrorDetail.SHOULD_RETRY));
    }

    public static ServiceErrorResponse create(Throwable e, int statusCode,
            EnumSet<ErrorDetail> details) {
        ServiceErrorResponse rsp = new ServiceErrorResponse();
        rsp.message = e.getLocalizedMessage();
        rsp.stackTrace = new ArrayList<>();
        for (StackTraceElement se : e.getStackTrace()) {
            rsp.stackTrace.add(se.toString());
        }

        rsp.details = details;
        rsp.statusCode = statusCode;
        return rsp;
    }

    public static final String KIND = Utils.buildKind(ServiceErrorResponse.class);
    public String message;
    public String messageId;
    public List<String> stackTrace;
    public int statusCode;
    public EnumSet<ErrorDetail> details;

    public String documentKind = KIND;
}