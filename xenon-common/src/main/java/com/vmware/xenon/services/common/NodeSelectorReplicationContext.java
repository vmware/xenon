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

package com.vmware.xenon.services.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;

public class NodeSelectorReplicationContext {
    public NodeSelectorReplicationContext(String location, Collection<NodeState> nodes, Operation op) {
        this.location = location;
        this.nodes = nodes;
        this.parentOp = op;
        this.startTimeMicros = Utils.getNowMicrosUtc();
    }

    String location;
    Collection<NodeState> nodes;
    Operation parentOp;
    int successThreshold;
    int failureThreshold;
    long startTimeMicros;
    private int successCount;
    private int failureCount;
    private int failureStatus;
    private List<Integer> failureErrorCodes;
    private Status completionStatus = Status.PENDING;

    public enum Status {
        SUCCEEDED, FAILED, PENDING
    }

    public void checkAndCompleteOperation(ServiceHost h, Throwable e, Operation o) {
        Status ct;
        String failureMsg = null;
        Operation op = this.parentOp;
        int errorCode = 0;
        boolean shouldRetry = false;

        if (e != null && o != null) {
            h.log(Level.WARNING,
                    "(Original id: %d) Replication request to %s-%s failed with %d, %s",
                    op.getId(),
                    o.getUri(), o.getAction(), o.getStatusCode(), e.getMessage());
            this.failureStatus = o.getStatusCode();
            errorCode = this.getErrorCode(o);
        }

        synchronized (this) {
            this.saveErrorCode(errorCode);
            if (this.completionStatus != Status.PENDING) {
                return;
            }
            if (e == null && ++this.successCount == this.successThreshold) {
                this.completionStatus = Status.SUCCEEDED;
            } else if (e != null && ++this.failureCount == this.failureThreshold) {
                this.completionStatus = Status.FAILED;
                shouldRetry = parentOpShouldRetry();
                failureMsg = String
                        .format("(Original id: %d) %s to %s failed. Success: %d,  Fail: %d, quorum: %d, failure threshold: %d",
                                op.getId(),
                                op.getAction(),
                                op.getUri().getPath(),
                                this.successCount,
                                this.failureCount,
                                this.successThreshold,
                                this.failureThreshold);
            }
            ct = this.completionStatus;
        }

        if (ct == Status.SUCCEEDED) {
            op.setStatusCode(Operation.STATUS_CODE_OK).complete();
            return;
        }

        if (ct == Status.FAILED) {
            h.log(Level.WARNING, "%s", failureMsg);
            op.setStatusCode(this.failureStatus);

            Exception ex = new IllegalStateException(failureMsg);
            ServiceErrorResponse sep = Utils.toServiceErrorResponse(ex);
            sep.statusCode = this.failureStatus;
            if (shouldRetry) {
                sep.details = EnumSet.of(ServiceErrorResponse.ErrorDetail.SHOULD_RETRY);
            }
            op.setBody(sep);
            op.fail(ex);
            return;
        }
    }

    private int getErrorCode(Operation o) {
        if (o.hasBody() && o.getContentType().equals(Operation.MEDIA_TYPE_APPLICATION_JSON)) {
            Object obj = o.getBodyRaw();
            if (obj instanceof ServiceErrorResponse) {
                ServiceErrorResponse rsp = (ServiceErrorResponse)obj;
                return rsp.getErrorCode();
            }
        }
        return 0;
    }

    private void saveErrorCode(int errorCode) {
        if (errorCode == 0) {
            return;
        }
        if (this.failureErrorCodes == null) {
            this.failureErrorCodes = new ArrayList<>();
        }
        this.failureErrorCodes.add(errorCode);
    }

    private boolean parentOpShouldRetry() {
        if (this.failureErrorCodes != null) {
            for (Integer errorCode : this.failureErrorCodes) {
                if (errorCode == ServiceErrorResponse.ERROR_CODE_SERVICE_NOT_FOUND_ON_REPLICA) {
                    return true;
                }
            }
        }
        return false;
    }
}
