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
 * Promise-based protocol for handling requests targeting {@link StatefulService}
 */
public interface StateAwareRequestsHandler<T extends ServiceDocument> {
    /**
     * A state initialization method. The requested initial state for the service is provided as
     * the request's body.
     * @param request
     * @return A promise to return the state to be stored for the service as result of this
     * operation and an appropriate {@link Response} to the client.
     */
    Promise<StatefulResponse<T>> handleCreate(Request request);

    /**
     * A state initialization method. The requested initial state for the service is provided as
     * the request's body after the service has been started or restarted.
     * @param request
     * @return A promise to return the state to be stored for the service as result of this
     * operation and an appropriate {@link Response} to the client.
     */
    Promise<StatefulResponse<T>> handleStart(Request request);

    /**
     * A state query method.
     * @param state The current state of the service
     * @param request
     * @return A promise to return {@link Response} based on the current state of the service and
     * additional parameters contained in the request.
     */
    Promise<Response> handleGet(T state, Request request);

    /**
     * A state query method.
     * @param state The current state of the service
     * @param request
     * @return A promise to return {@link Response} based on the current state of the service and
     * additional parameters contained in the request.
     */
    Promise<Response> handlePost(T state, Request request);

    /**
     * A state transformation method. The requested replacement state is provided as the request's
     * body. After validating the state transition the handler returns the new state to be stored
     * and an appropriate response for the client.
     * @param state The current state associated with the service.
     * @param request
     * @return A promise to return the new validated state and a {@link Response} for the client
     */
    Promise<StatefulResponse<T>> handlePut(T state, Request request);

    /**
     * A state transformation method. The requested modifications of the state are provided as the
     * request's body. Common pattern is to provide an instance of the service document with values
     * for the fields that have to be modified.
     * @param state
     * @param request
     * @return A promise to return the new validated state and a {@link Response} for the client.
     */
    Promise<StatefulResponse<T>> handlePatch(T state, Request request);

    /**
     * A state destroy method.
     * @param state The current state of the service.
     * @param request
     * @return A promise to return appropriate response for the client based on whether the task
     * was accepted and carried on.
     */
    Promise<Response> handleDelete(T state, Request request);

    /**
     * A request to take the service down.
     * @param state
     * @param request
     * @return A promise to return appropriate response.
     */
    Promise<Response> handleStop(T state, Request request);
}
