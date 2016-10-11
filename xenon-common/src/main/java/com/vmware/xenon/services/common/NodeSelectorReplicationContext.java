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

package com.vmware.xenon.services.common;

import java.util.Collection;
import java.util.logging.Level;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceErrorResponse;
import com.vmware.xenon.common.ServiceErrorResponse.ErrorDetail;
import com.vmware.xenon.common.ServiceHost;

class NodeSelectorReplicationContext {
    public NodeSelectorReplicationContext(String location, Collection<NodeState> nodes, Operation op) {
        this.location = location;
        this.nodes = nodes;
        this.parentOp = op;
    }

    String location;
    Collection<NodeState> nodes;
    Operation parentOp;
    int successThreshold;
    int failureThreshold;
    private int successCount;
    private int failureCount;
    private ServiceErrorResponse failureBody;
    private NodeSelectorReplicationContext.Status completionStatus = Status.PENDING;

    public enum Status {
        SUCCEEDED, FAILED, PENDING
    }

    public void checkAndCompleteOperation(ServiceHost h, Throwable e, Operation o) {
        NodeSelectorReplicationContext.Status ct;
        ServiceErrorResponse finalFailureBody = null;
        Operation op = this.parentOp;

        if (e != null && o != null) {
            logAndSetFailureInfo(h, e, o, op);
        }

        synchronized (this) {
            if (this.completionStatus != Status.PENDING) {
                return;
            }
            if (e == null && ++this.successCount == this.successThreshold) {
                this.completionStatus = Status.SUCCEEDED;
            } else if (e != null && ++this.failureCount == this.failureThreshold) {
                this.completionStatus = Status.FAILED;
                finalFailureBody = this.failureBody;
                String failureMsg = String
                        .format("(Original id: %d) %s to %s failed. Success: %d,  Fail: %d, quorum: %d, failure threshold: %d",
                                op.getId(),
                                op.getAction(),
                                op.getUri().getPath(),
                                this.successCount,
                                this.failureCount,
                                this.successThreshold,
                                this.failureThreshold);
                finalFailureBody.message = failureMsg;
            }
            ct = this.completionStatus;
        }

        if (ct == Status.SUCCEEDED) {
            op.setStatusCode(Operation.STATUS_CODE_OK).complete();
            return;
        }

        if (ct == NodeSelectorReplicationContext.Status.FAILED) {
            completeWithFailure(h, finalFailureBody, op);
            return;
        }

    }

    private void logAndSetFailureInfo(ServiceHost h, Throwable e, Operation o, Operation op) {
        h.log(Level.WARNING,
                "(Original id: %d) Replication request to %s-%s failed with %d, %s",
                op.getId(),
                o.getUri(), o.getAction(), o.getStatusCode(), e.getMessage());
        synchronized (this) {
            ServiceErrorResponse errorBody = null;
            if (o.hasBody() && Operation.MEDIA_TYPE_APPLICATION_JSON.equals(o.getContentType())) {
                errorBody = o.getBody(ServiceErrorResponse.class);
            } else {
                errorBody = ServiceErrorResponse.create(e, o.getStatusCode());
            }
            if (this.failureBody == null) {
                this.failureBody = errorBody;
            } else if (errorBody.details != null && this.failureBody.details != null) {
                for (ErrorDetail ed : errorBody.details) {
                    this.failureBody.details.add(ed);
                }
            }
        }
    }

    private void completeWithFailure(ServiceHost h, ServiceErrorResponse failureBody,
            Operation op) {
        h.log(Level.WARNING, "Replication failure: %d %s %s",
                failureBody.statusCode, failureBody.message, failureBody.details);
        op.setBodyNoCloning(failureBody).fail(failureBody.statusCode);
    }
}