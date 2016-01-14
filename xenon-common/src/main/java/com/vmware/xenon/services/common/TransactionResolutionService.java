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

import java.net.UnknownHostException;

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
    public void handleRequest(Operation op) {
        if (op.getUri().getPath().endsWith(RESOLUTION_SUFFIX)) {
            handleResolutionRequest(op);
        } else {
            op.fail(new UnknownHostException());
        }
    }

    /**
     * Wrap the request in a pub/sub pattern in order to forward to the transaction service. Body is not introspected,
     * checks should be handled by the transaction coordinator itself. Similarly, upon notification, the response is
     * simply forwarded to the client -- even if it is failure.
     *
     * TODO: Use reliable subscriptions
     */
    public void handleResolutionRequest(Operation op) {
        Operation subscribeToCoordinator = Operation.createPost(
                UriUtils.buildSubscriptionUri(this.parent.getUri()))
                .setCompletion((o, e) -> {
                    // if we could not subscribe, let's notify the client
                    if (e != null) {
                        op.fail(e);
                        return;
                    }
                }).setReferer(getUri());
        getHost().startSubscriptionService(subscribeToCoordinator, (notifyOp) -> {
            // simply clone the raw body and status code (possible error code); forward to the client
            ResolutionRequest resolve = notifyOp.getBody(ResolutionRequest.class);
            notifyOp.complete();
            if (isNotComplete(resolve.kind)) {
                return;
            }
            op.setBodyNoCloning(notifyOp.getBodyRaw());
            op.setStatusCode(notifyOp.getStatusCode());
            op.complete();
        });

        ResolutionRequest resolve = op.getBody(ResolutionRequest.class);
        Operation operation = Operation
                .createPatch(this.parent.getUri())
                .setBody(resolve);
        sendRequest(operation);
    }

    private boolean isNotComplete(ResolutionKind kind) {
        return (kind != ResolutionKind.COMMITTED && kind != ResolutionKind.ABORTED);
    }
}
