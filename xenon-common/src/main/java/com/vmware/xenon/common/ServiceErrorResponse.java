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

    public static final String PROPERTY_NAME_DISABLE_STACK_TRACE_COLLECTION =
            Utils.PROPERTY_NAME_PREFIX + "ServiceErrorResponse.disableStackTraceCollection";

    public static final boolean DISABLE_STACK_TRACE_COLLECTION = Boolean.getBoolean(
            PROPERTY_NAME_DISABLE_STACK_TRACE_COLLECTION);

    public static final int ERROR_CODE_INTERNAL_MASK = 0x80000000;
    public static final int ERROR_CODE_OUTDATED_SYNCH_REQUEST = 0x80000001;
    public static final int ERROR_CODE_STATE_MARKED_DELETED = 0x80000002;
    public static final int ERROR_CODE_SERVICE_ALREADY_EXISTS = 0x80000003;
    public static final int ERROR_CODE_SERVICE_NOT_FOUND_ON_REPLICA = 0x80000004;
    public static final int ERROR_CODE_OWNER_MISMATCH = 0x80000005;
    public static final int ERROR_CODE_SERVICE_PARENT_NOT_A_FACTORY = 0x80000006;
    public static final int ERROR_CODE_SERVICE_QUEUE_LIMIT_EXCEEDED = 0x80000007;
    public static final int ERROR_CODE_HOST_RATE_LIMIT_EXCEEDED = 0x80000008;
    public static final int ERROR_CODE_CLIENT_QUEUE_LIMIT_EXCEEDED = 0x80000009;

    public enum ErrorDetail {
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

        if (!DISABLE_STACK_TRACE_COLLECTION) {
            rsp.stackTrace = new ArrayList<>();
            for (StackTraceElement se : e.getStackTrace()) {
                rsp.stackTrace.add(se.toString());
            }
        }

        rsp.details = details;
        rsp.statusCode = statusCode;
        return rsp;
    }

    private static boolean isInternalErrorCode(int errorCode) {
        return (errorCode & ERROR_CODE_INTERNAL_MASK) != 0;
    }

    public static final String KIND = Utils.buildKind(ServiceErrorResponse.class);
    public String message;
    public String messageId;
    public List<String> stackTrace;
    public int statusCode;
    public EnumSet<ErrorDetail> details;

    public String documentKind = KIND;

    protected int errorCode;

    public int getErrorCode() {
        return this.errorCode;
    }

    public void setErrorCode(int errorCode) {
        if (isInternalErrorCode(errorCode)) {
            throw new IllegalArgumentException(
                    "Error code must not use internal xenon errorCode range.");
        }
        this.errorCode = errorCode;
    }

    public void setInternalErrorCode(int errorCode) {
        if (!isInternalErrorCode(errorCode)) {
            throw new IllegalArgumentException(
                    "Error code must use internal xenon errorCode range.");
        }
        this.errorCode = errorCode;
    }
}
