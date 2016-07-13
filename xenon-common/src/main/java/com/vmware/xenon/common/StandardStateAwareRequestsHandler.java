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

import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;

import com.vmware.xenon.common.net.Request;
import com.vmware.xenon.common.net.Response;

/**
 * Default implementation of the {@link StateAwareRequestsHandler} building on top of the
 * {@link StatefulService}
 */
public class StandardStateAwareRequestsHandler<T extends ServiceDocument>
        extends StatefulService
        implements StateAwareRequestsHandler<T> {

    public StandardStateAwareRequestsHandler(Class<T> stateType) {
        super(stateType);
    }

    /**
     * If the provided failure is {@link CompletionException}, the return value is the
     * failure's cause, otherwise it is the failure itself.
     * @param failure
     * @return
     */
    private Throwable unwrapCompletionException(Throwable failure) {
        return failure instanceof CompletionException ? failure.getCause() : failure;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getStateType() {
        return (Class<T>) super.getStateType();
    }

    /**
     * Returns a consumer that can be used to conclude a state mutation request
     * @param op The operation representing the request
     * @return
     */
    protected BiConsumer<StatefulResponse<T>, Throwable> stateMutationCompletionHandler(Operation op) {
        return (r, failure) -> {
            if (failure != null) {
                failOperation(failure, op);
            } else {
                op.applyResponse(r.getResponse());
                setState(op, r.getState());
                op.complete();
            }
        };
    }

    /**
     * Returns a consumer that can be used to conclude a request.
     * @param op
     * @return
     */
    protected BiConsumer<Response, Throwable> completionHandler(Operation op) {
        return (response, failure) -> {
            if (failure != null) {
                failOperation(failure, op);
            } else {
                op.applyResponse(response);
                op.complete();
            }
        };
    }

    /**
     * Signals that the operation has failed with the provided failure.
     * @param failure
     * @param op
     */
    protected void failOperation(Throwable failure, Operation op) {
        failure = unwrapCompletionException(failure);
        if (failure instanceof NotSupportedException) {
            op.fail(Operation.STATUS_CODE_BAD_METHOD);
        } else {
            op.fail(failure);
        }
    }

    @Override
    public final void handleCreate(Operation post) {
        handleCreate((Request) post).whenComplete(stateMutationCompletionHandler(post));
    }

    @Override
    public Promise<StatefulResponse<T>> handleCreate(Request request) {
        T initialState = request.getBody(getStateType());
        return DefaultPromise.completed(request.createResponse(initialState));
    }

    @Override
    public final void handleStart(Operation post) {
        handleStart((Request) post).whenComplete(stateMutationCompletionHandler(post));
    }

    @Override
    public Promise<StatefulResponse<T>> handleStart(Request request) {
        T initialState = request.getBody(getStateType());
        return DefaultPromise.completed(request.createResponse(initialState));
    }

    @Override
    public final void handleGet(Operation get) {
        T state = getState(get);
        handleGet(state, get).whenComplete(completionHandler(get));
    }

    @Override
    public Promise<Response> handleGet(T state, Request request) {
        Response response = request.createResponse();
        response.setBody(state);
        return DefaultPromise.completed(response);
    }

    @Override
    public final void handlePost(Operation post) {
        T state = getState(post);
        handlePost(state, post).whenComplete(completionHandler(post));
    }

    @Override
    public Promise<Response> handlePost(T state, Request request) {
        return DefaultPromise.failed(new NotSupportedException());
    }

    @Override
    public final void handlePut(Operation put) {
        T state = getState(put);
        handlePut(state, put).whenComplete(stateMutationCompletionHandler(put));
    }

    @Override
    public Promise<StatefulResponse<T>> handlePut(T state, Request request) {
        T newState = request.getBody(getStateType());
        return DefaultPromise.completed(request.createResponse(newState));
    }

    @Override
    public final void handlePatch(Operation patch) {
        T state = getState(patch);
        handlePatch(state, patch).whenComplete(stateMutationCompletionHandler(patch));
    }

    @Override
    public Promise<StatefulResponse<T>> handlePatch(T state, Request request) {
        return DefaultPromise.failed(new NotSupportedException());
    }

    @Override
    public final void handleDelete(Operation delete) {
        T state = getState(delete);
        handleDelete(state, delete).whenComplete(completionHandler(delete));
    }

    @Override
    public Promise<Response> handleDelete(T state, Request request) {
        return DefaultPromise.completed(request.createResponse());
    }

    @Override
    public final void handleStop(Operation delete) {
        T state = getState(delete);
        handleStop(state, delete).whenComplete(completionHandler(delete));
    }

    @Override
    public Promise<Response> handleStop(T state, Request request) {
        return DefaultPromise.completed(request.createResponse());
    }

}
