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

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.services.common.TransactionService.ResolutionKind;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;

/**
 * Transaction-specific "utility" service responsible for masking commit resolution asynchrony during commit phase.
 */
public class TransactionResolutionService extends StatelessService {
    public static final String RESOLUTION_SUFFIX = "/resolve";

    StatefulService parent;

    public TransactionResolutionService(StatefulService parent) {
        this.parent = parent;
    }

    @Override
    public void authorizeRequest(Operation op) {
        op.complete();
    }

    @Override
    public void handlePost(Operation op) {
        handleResolutionRequest(op);
    }

    /**
     * Wrap the request in a pub/sub pattern in order to forward to the transaction service. Body is not introspected,
     * checks should be handled by the transaction coordinator itself. Similarly, upon notification, the response is
     * simply forwarded to the client -- even if it is failure.
     *
     * TODO: Use reliable subscriptions
     */
    public void handleResolutionRequest(Operation op) {
        ResolutionRequest resolutionRequest = op.getBody(ResolutionRequest.class);
        Operation subscribeToCoordinator = Operation.createPost(
                UriUtils.buildSubscriptionUri(this.parent.getUri()))
                .setCompletion((o, e) -> {
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                    Operation operation = Operation
                            .createPatch(this.parent.getUri())
                            .setBody(resolutionRequest)
                            .setCompletion((o2, e2) -> {
                                if (e2 != null) {
                                    op.fail(e2);
                                }
                            });
                    sendRequest(operation);
                }).setReferer(getUri());

        getHost().startSubscriptionService(subscribeToCoordinator, (notifyOp) -> {
            ResolutionRequest resolve = notifyOp.getBody(ResolutionRequest.class);
            notifyOp.complete();
            if (isNotComplete(resolve.resolutionKind)) {
                return;
            }
            if ((resolve.resolutionKind == ResolutionKind.COMMITTED && resolutionRequest.resolutionKind == ResolutionKind.COMMIT) ||
                    (resolve.resolutionKind == ResolutionKind.ABORTED && resolutionRequest.resolutionKind == ResolutionKind.ABORT)) {
                op.setBodyNoCloning(notifyOp.getBodyRaw());
                op.setStatusCode(notifyOp.getStatusCode());
                op.complete();
            } else {
                String errorMsg = String.format("Resolution %s of transaction %s is different than requested", resolve.resolutionKind, this.parent.getSelfLink());
                logWarning(errorMsg);
                op.fail(new IllegalStateException(errorMsg));
            }
        });
    }

    private boolean isNotComplete(ResolutionKind kind) {
        return (kind != ResolutionKind.COMMITTED && kind != ResolutionKind.ABORTED);
    }
}
