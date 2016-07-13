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
 * Implementation of {@link StateAwareRequestsHandler} standardizing the protocol for modifying
 * the state of the service, while hiding the details of the transport layer.
 */
public class SimpleStateAwareRequestsHandler<T extends ServiceDocument>
        extends StandardStateAwareRequestsHandler<T> {

    /**
     * Constructor
     * @param stateType
     */
    public SimpleStateAwareRequestsHandler(Class<T> stateType) {
        super(stateType);
    }

    @Override
    public final Promise<Response> handleGet(T state, Request request) {
        return handleGet(state).thenApply(transformedState -> {
            Response response = request.createResponse();
            response.setBody(transformedState);
            return response;
        });
    }

    /**
     * The handler can respond with a modified version of the associated state.
     * @param state The current state
     * @return The promise to return a potentially modified copy of the service's state
     */
    public Promise<T> handleGet(T state) {
        return DefaultPromise.completed(state);
    }

    @Override
    public final Promise<Response> handlePost(T state, Request request) {
        return handlePost(state).thenApply(transformedState -> {
            Response response = request.createResponse();
            response.setBody(transformedState);
            return response;
        });
    }

    /**
     * The handler can respond with a modified version of the associated state.
     * @param state The current state
     * @return The promise to return a potentially modified copy of the service's state
     */
    public Promise<T> handlePost(T state) {
        throw new NotSupportedException();
    }

    @Override
    public final Promise<StatefulResponse<T>> handlePut(T state, Request request) {
        T newState = request.getBody(getStateType());
        return handlePut(state, newState).thenApply(request::createResponse);
    }

    /**
     * The handler should perform necessary validations and eventually return the accepted state.
     * @param state The current state of the service
     * @param replacementState The state to replace with
     * @return Promise to return the new state
     */
    public Promise<T> handlePut(T state, T newState) {
        return DefaultPromise.completed(newState);
    }

    @Override
    public final Promise<StatefulResponse<T>> handlePatch(T state, Request request) {
        T patch = request.getBody(getStateType());
        return handlePatch(state, patch).thenApply(request::createResponse);
    }

    /**
     * The handler should apply the patch to the current state and eventually return the patched
     * state.
     * @param state The current state of the service
     * @param patch The patch to apply to the current state
     * @return Promise to return the patched state
     */
    public Promise<T> handlePatch(T state, T patch) {
        throw new NotSupportedException();
    }

    @Override
    public final Promise<StatefulResponse<T>> handleCreate(Request request) {
        T initialState = request.getBody(getStateType());
        return handleCreate(initialState).thenApply(request::createResponse);
    }

    /**
     * The handler can validate and change the provided initial state and should eventually return
     * it to the host.
     * @param initialState The initial state for this service
     * @return Promise to return the actual state to be stored
     */
    public Promise<T> handleCreate(T initialState) {
        return DefaultPromise.completed(initialState);
    }

    @Override
    public final Promise<StatefulResponse<T>> handleStart(Request request) {
        T initialState = request.getBody(getStateType());
        return handleStart(initialState).thenApply(request::createResponse);
    }

    /**
     * The handler can validate and modify if needed the initial state before it becomes available
     * and should eventually return it.
     * @param initialState The initial state for this service
     * @return Promise to return the actual state
     */
    public Promise<T> handleStart(T initialState) {
        return DefaultPromise.completed(initialState);
    }

    @Override
    public final Promise<Response> handleDelete(T state, Request request) {
        return handleDelete(state).thenApply(ignore -> {
            return request.createResponse();
        });
    }

    /**
     * The handler should validate the delete operation, perform additional cleanup and eventually
     * signal that the operation is completed.
     * @param state The state associated with the service
     * @return Promise to complete the delete operation
     */
    public EmptyPromise handleDelete(T state) {
        return EmptyPromise.completed();
    }

    @Override
    public final Promise<Response> handleStop(T state, Request request) {
        return handleStop(state).thenApply(ignore -> {
            return request.createResponse();
        });
    }

    /**
     * The handler can perform clean-up tasks before it becomes unavailable.
     * @param state The current state of the service
     * @return Promise to complete the stop operation
     */
    public EmptyPromise handleStop(T state) {
        return EmptyPromise.completed();
    }
}
