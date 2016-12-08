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
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.TransactionService.ResolutionKind;
import com.vmware.xenon.services.common.TransactionService.ResolutionRequest;

/**
 * Transaction-specific "utility" service responsible for masking commit resolution asynchrony during commit phase.
 */
public class TransactionResolutionService extends StatelessService {
    public static final String RESOLUTION_SUFFIX = "/resolve";
    public static final int TRANSACTION_CLEAR_GRACE_PERIOD_SECONDS = 10;

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
        if (!resolutionRequest.kind.equals(ResolutionRequest.KIND)
                || (!resolutionRequest.resolutionKind.equals(ResolutionKind.COMMIT)
                        && (!resolutionRequest.resolutionKind.equals(ResolutionKind.ABORT)))) {
            op.fail(new IllegalArgumentException(
                    "Unrecognized resolution request: " + Utils.toJson(op.getBodyRaw())));
            return;
        }

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
                            .setTransactionId(null)
                            .setCompletion((o2, e2) -> {
                                if (e2 != null) {
                                    op.fail(e2);
                                    return;
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
            logInfo("Received notification: action=%s, resolution=%s",
                    notifyOp.getAction(),
                    resolve.resolutionKind);
            if ((resolve.resolutionKind.equals(ResolutionKind.COMMITTED)
                    && resolutionRequest.resolutionKind.equals(ResolutionKind.COMMIT)) ||
                    (resolve.resolutionKind.equals(ResolutionKind.ABORTED)
                            && resolutionRequest.resolutionKind.equals(ResolutionKind.ABORT))) {
                logInfo("Resolution of transaction %s is complete",
                        this.parent.getSelfLink());
                op.setBodyNoCloning(notifyOp.getBodyRaw());
                op.setStatusCode(notifyOp.getStatusCode());
                op.complete();
            } else {
                String errorMsg = String.format(
                        "Resolution %s of transaction %s is different than requested",
                        resolve.resolutionKind, this.parent.getSelfLink());
                logWarning(errorMsg);
                op.fail(new IllegalStateException(errorMsg));
            }

            // schedule transaction instance stop and transaction resolution stop to free up memory.
            // give grace period for potentially conflicting transactions to get this transaction stage.
            /*
            getHost().schedule(() -> {
                Operation.createDelete(getUri())
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                        .sendWith(this.parent);
                Operation.createDelete(this.parent.getUri())
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_NO_INDEX_UPDATE)
                        .sendWith(this);
            }, TRANSACTION_CLEAR_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS);
            */
        });
    }

    private boolean isNotComplete(ResolutionKind kind) {
        return (kind != ResolutionKind.COMMITTED && kind != ResolutionKind.ABORTED);
    }
}
