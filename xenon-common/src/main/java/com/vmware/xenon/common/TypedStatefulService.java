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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Statically typed {@link StatefulService} which provides Promise-enabled versions of the handle*
 * methods.
 * @param <T> the {@link ServiceDocument} implementation to use as state for this service.
 */
public class TypedStatefulService<T extends ServiceDocument> extends StatefulService {
    /**
     * Signals that an operation is not supported.
     */
    private static class NotSupportedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        NotSupportedException() {
            super();
        }
    }

    /**
     * Used to signal that a promise without an actual payload is completed.
     * The actual enum value should be ignored. In case of error, the {@link CompletableFuture}
     * should be completed with exception.
     */
    public static enum Completion {
        /**
         * The task has completed. Use completeExceptionally to signal failure.
         */
        Done
    }

    /**
     * Constructor
     * @param stateType The implementation of {@link ServiceDocument} to use as state for this
     *                  service.
     */
    public TypedStatefulService(Class<T> stateType) {
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

    @Override
    public final void handleCreate(Operation post) {
        @SuppressWarnings("unchecked")
        T initialState = (T) post.getBody(getStateType());
        handleCreate(initialState).whenComplete((newState, failure) -> {
            if (failure != null) {
                post.fail(unwrapCompletionException(failure));
            } else {
                post.setBody(newState).complete();
            }
        });
    }

    /**
     * The service can validate and change the provided state and should eventually return
     * it to the host.
     * @param initialState The initial state for this service
     * @return Promise to return the actual state for the service
     */
    public CompletableFuture<T> handleCreate(T initialState) {
        return CompletableFuture.completedFuture(initialState);
    }

    @Override
    public final void handleStart(Operation post) {
        @SuppressWarnings("unchecked")
        T state = (T) post.getBody(getStateType());
        handleStart(state).whenComplete((newState, failure) -> {
            if (failure != null) {
                post.fail(unwrapCompletionException(failure));
            } else {
                post.setBody(newState).complete();
            }
        });
    }

    /**
     * The service can validate and modify if needed the initial state before it becomes available
     * and should eventually return it.
     * @param initialState The initial state for this service
     * @return Promise to return the actual state for the service
     */
    public CompletableFuture<T> handleStart(T initialState) {
        return CompletableFuture.completedFuture(initialState);
    }

    @Override
    public final void handleDelete(Operation delete) {
        T state = getState(delete);
        handleDelete(state).whenComplete((ignore, failure) -> {
            if (failure != null) {
                delete.fail(unwrapCompletionException(failure));
            } else {
                delete.complete();
            }
        });
    }

    /**
     * The service should validate the delete operation, perform additional cleanup and
     * eventually complete the promise it returns.
     * @param state The state associated with the service
     * @return Promise to complete the delete operation
     */
    public CompletableFuture<Completion> handleDelete(T state) {
        return CompletableFuture.completedFuture(Completion.Done);
    }

    @Override
    public final void handleStop(Operation delete) {
        T state = getState(delete);
        handleStop(state).whenComplete((ignore, failure) -> {
            if (failure != null) {
                delete.fail(unwrapCompletionException(failure));
            } else {
                delete.complete();
            }
        });
    }

    /**
     * The service can perform clean-up tasks before it becomes unavailable.
     * @param state The current state of the service
     * @return Promise to complete the stop operation
     */
    public CompletableFuture<Completion> handleStop(T state) {
        return CompletableFuture.completedFuture(Completion.Done);
    }

    @Override
    public final void handlePatch(Operation patch) {
        T state = getState(patch);
        @SuppressWarnings("unchecked")
        T patchToApply = (T) patch.getBody(getStateType());
        handlePatch(state, patchToApply).whenComplete((newState, failure) -> {
            if (failure != null) {
                Throwable unwrapped = unwrapCompletionException(failure);
                if (unwrapped instanceof NotSupportedException) {
                    super.handlePatch(patch);
                } else {
                    patch.fail(unwrapped);
                }
            } else {
                setState(patch, newState);
                patch.complete();
            }
        });
    }

    /**
     * The service should apply the patch to the current state and eventually return the patched
     * state.
     * @param state The current state of the service
     * @param patch The patch to apply to the current state
     * @return Promise to return the patched state
     */
    public CompletableFuture<T> handlePatch(T state, T patch) {
        CompletableFuture<T> promise = new CompletableFuture<T>();
        promise.completeExceptionally(new NotSupportedException());
        return promise;
    }

    @Override
    public final void handlePut(Operation put) {
        T state = getState(put);
        @SuppressWarnings("unchecked")
        T replacementState = (T) put.getBody(getStateType());
        handlePut(state, replacementState).whenComplete((newState, failure) -> {
            if (failure != null) {
                put.fail(unwrapCompletionException(failure));
            } else {
                setState(put, newState);
                put.complete();
            }
        });
    }

    /**
     * The service should perform necessary validations and eventually return the new state.
     * @param state The current state of the service
     * @param replacementState The state to replace with
     * @return Promise to return the new state
     */
    public CompletableFuture<T> handlePut(T state, T replacementState) {
        return CompletableFuture.completedFuture(replacementState);
    }

    @Override
    public final void handleGet(Operation get) {
        T state = getState(get);
        handleGet(state).whenComplete((response, failure) -> {
            if (failure != null) {
                get.fail(unwrapCompletionException(failure));
            } else {
                get.setBody(response).complete();
            }
        });
    }

    /**
     * The service can modify the state with which it responds here and eventually return it.
     * @param state The current state
     * @return The promise to return a potentially modified copy of the service's state
     */
    public CompletableFuture<T> handleGet(T state) {
        return CompletableFuture.completedFuture(state);
    }
}
