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

package com.vmware.xenon.common;

import com.vmware.xenon.common.net.Request;
import com.vmware.xenon.common.net.Response;

/**
 * Sends {@link Operation}s.
 *
 * @see ServiceClient
 * @see ServiceHost
 * @see Service
 */
public interface ServiceRequestSender {
    void sendRequest(Operation op);

    /**
     * Sends an asynchronous request and returns the eventual response body as promise.
     * @param request
     * @param resultType The expected type of the response body.
     * @return
     */
    default <T> Promise<T> sendRequest(Request request, Class<T> resultType) {
        return sendRequest(request)
                .thenApply(response -> response.getBody(resultType));
    }

    /**
     * Sends an asynchronous request and returns the eventual response as promise.
     * @param response
     * @return
     */
    default Promise<Response> sendRequest(Request request) {
        DefaultPromise<Response> promise = new DefaultPromise<Response>();
        Operation operation = Operation.fromRequest(request);
        operation.nestCompletion((response, e) -> {
            if (e != null) {
                promise.fail(e);
            } else {
                promise.complete(response);
            }
        });
        sendRequest(operation);
        return promise;
    }
}
