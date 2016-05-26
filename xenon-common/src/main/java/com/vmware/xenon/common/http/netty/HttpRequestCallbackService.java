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

package com.vmware.xenon.common.http.netty;

import java.util.HashMap;
import java.util.Map;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;

/**
 * Long running service tracking pending two-phase callback operations
 */
public class HttpRequestCallbackService extends StatelessService {
    Map<Long, Operation> pendingOperations = new HashMap<>();
    private String publicUri;
    private long nextKey = 0;
    private NettyHttpServiceClient client;

    public HttpRequestCallbackService(NettyHttpServiceClient client) {
        super(ServiceDocument.class);
        this.client = client;
    }

    public String queueUntilCallback(Operation op) {

        long key = 0;
        synchronized (this.pendingOperations) {
            key = this.nextKey++;
            this.pendingOperations.put(key, op);
        }

        if (this.publicUri == null) {
            // no need to do this in a thread safe fashion, benign extra work
            this.publicUri = UriUtils.buildPublicUri(getHost(), getSelfLink()).toString();
        }
        return this.publicUri + UriUtils.URI_QUERY_CHAR + key;
    }

    @Override
    public void authorizeRequest(Operation o) {
        if (o.getAction() != Action.PATCH) {
            super.authorizeRequest(o);
            return;
        }

        // the operation will fail if the id being patched is wrong.
        o.complete();
        return;
    }

    @Override
    public void handleRequest(Operation o) {
        if (o.getAction() == Action.DELETE) {
            super.handleRequest(o);
            return;
        }

        if (o.getAction() != Action.PATCH) {
            o.fail(new IllegalArgumentException("action not supported: " + o.getAction()));
            return;
        }

        Operation request = null;
        try {
            String query = o.getUri().getQuery();
            if (query == null) {
                o.fail(new IllegalArgumentException("Missing query parameter"));
                return;
            }

            Long opId = Long.parseLong(query);
            synchronized (this.pendingOperations) {
                request = this.pendingOperations.remove(opId);
            }

            if (request == null) {
                o.fail(new IllegalArgumentException("Operation not found: " + o.getUri()));
                return;
            }

            this.client.stopTracking(request);

            String responseStatusValue = o.getRequestHeaders().remove(
                    Operation.RESPONSE_CALLBACK_STATUS_HEADER);

            if (responseStatusValue == null) {
                request.fail(new IllegalArgumentException(
                        "Missing response callback status header :" + o.toString()));
                return;
            }

            request.setBodyNoCloning(o.getBodyRaw());

            request.transferRequestHeadersToResponseHeadersFrom(o);
            request.setStatusCode(Integer.parseInt(responseStatusValue));
            // even if the status indicates failure, we just call complete since there is
            // already an error response body
            request.complete();
        } catch (Throwable e) {
            logSevere(e);
            if (request != null) {
                request.fail(e);
            }
        } finally {
            if (request != null) {
                o.complete();
            }
        }
    }
}