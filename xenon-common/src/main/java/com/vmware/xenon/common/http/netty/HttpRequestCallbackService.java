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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Long running service tracking pending two-phase callback operations
 */
public class HttpRequestCallbackService extends StatelessService {
    Map<Long, Operation> pendingOperations = new ConcurrentSkipListMap<>();
    AtomicLong nextKey = new AtomicLong();
    private String publicUri;

    public HttpRequestCallbackService() {
        super(ServiceDocument.class);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
    }

    public URI queueUntilCallback(Operation op) {
        long k = this.nextKey.incrementAndGet();
        this.pendingOperations.put(k, op);

        if (this.publicUri == null) {
            // no need to do this in a thread safe fashion, benign extra work
            this.publicUri = UriUtils.buildPublicUri(getHost(), getSelfLink()).toString();
        }

        try {
            return new URI(this.publicUri + UriUtils.URI_QUERY_CHAR + Long.toString(k));
        } catch (URISyntaxException e) {
            return null;
        }
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
            request = this.pendingOperations.remove(opId);

            if (request == null) {
                o.fail(new IllegalArgumentException("Operation not found: " + o.getUri()));
                return;
            }

            request.setBodyNoCloning(o.getBodyRaw());
            String responseStatusValue = o
                    .getRequestHeader(Operation.RESPONSE_CALLBACK_STATUS_HEADER);
            if (responseStatusValue == null) {
                request.fail(new IllegalArgumentException(
                        "Missing response callback status header :" + o.toString()));
                return;
            }

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

    @Override
    public void handleMaintenance(Operation o) {

        long now = Utils.getNowMicrosUtc();
        Iterator<Operation> it = this.pendingOperations.values().iterator();
        while (it.hasNext()) {
            Operation op = it.next();
            if (op.getExpirationMicrosUtc() < now) {
                it.remove();
                op.fail(new TimeoutException());
            } else {
                // we are making the simplifying assumption that operations have similar timeouts. Since
                // operations are a) inserted in chronological order, b) the skip list map iterator walks the keys in
                // ascending order, c) the insertion key is monotonically increasing, then the moment we reach
                // a operation that has not expired, it means all operations after have not expired either
                break;
            }
        }
        o.complete();

    }
}