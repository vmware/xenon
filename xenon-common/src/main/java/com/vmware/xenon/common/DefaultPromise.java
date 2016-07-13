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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Default implementation of {@link Promise} encapsulating {@link CompletableFuture}
 * @param <T>
 */
public class DefaultPromise<T> implements Promise<T> {
    private final CompletableFuture<T> completableFuture;

    /**
     * Creates a new {@link Promise}
     */
    public DefaultPromise() {
        this(new CompletableFuture<>());
    }

    /**
     * Creates a new {@link Promise} by wrapping the provided {@link CompletableFuture}
     * @param completionStage
     */
    public DefaultPromise(CompletableFuture<T> completableFuture) {
        this.completableFuture = completableFuture;
    }

    /**
     * Wraps the provided {@link CompletableFuture} into {@link Promise}
     * @param completionStage
     * @return
     */
    protected <U> Promise<U> wrap(CompletableFuture<U> completableFuture) {
        return new DefaultPromise<>(completableFuture);
    }

    @Override
    public <U> Promise<U> thenApply(Function<? super T, ? extends U> fn) {
        return wrap(this.completableFuture.thenApply(fn));
    }

    @Override
    public <U> Promise<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return wrap(this.completableFuture.thenApplyAsync(fn, executor));
    }

    @Override
    public Promise<Void> thenAccept(Consumer<? super T> action) {
        return wrap(this.completableFuture.thenAccept(action));
    }

    @Override
    public Promise<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return wrap(this.completableFuture.thenAcceptAsync(action, executor));
    }

    @Override
    public Promise<Void> thenRun(Runnable action) {
        return wrap(this.completableFuture.thenRun(action));
    }

    @Override
    public Promise<Void> thenRunAsync(Runnable action, Executor executor) {
        return wrap(this.completableFuture.thenRunAsync(action, executor));
    }

    @Override
    public <U, V> Promise<V> thenCombine(Promise<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn) {
        return wrap(this.completableFuture.thenCombine(other.toCompletionStage(), fn));
    }

    @Override
    public <U, V> Promise<V> thenCombineAsync(Promise<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return wrap(this.completableFuture.thenCombineAsync(other.toCompletionStage(), fn,
                executor));
    }

    @Override
    public <U> Promise<Void> thenAcceptBoth(Promise<? extends U> other,
            BiConsumer<? super T, ? super U> action) {
        return wrap(this.completableFuture.thenAcceptBoth(other.toCompletionStage(), action));
    }

    @Override
    public <U> Promise<Void> thenAcceptBothAsync(Promise<? extends U> other,
            BiConsumer<? super T, ? super U> action, Executor executor) {
        return wrap(this.completableFuture.thenAcceptBothAsync(other.toCompletionStage(), action,
                executor));
    }

    @Override
    public Promise<Void> runAfterBoth(Promise<?> other, Runnable action) {
        return wrap(this.completableFuture.runAfterBoth(other.toCompletionStage(), action));
    }

    @Override
    public Promise<Void> runAfterBothAsync(Promise<?> other, Runnable action, Executor executor) {
        return wrap(this.completableFuture.runAfterBothAsync(other.toCompletionStage(), action,
                executor));
    }

    @Override
    public <U> Promise<U> applyToEither(Promise<? extends T> other, Function<? super T, U> fn) {
        return wrap(this.completableFuture.applyToEither(other.toCompletionStage(), fn));
    }

    @Override
    public <U> Promise<U> applyToEitherAsync(Promise<? extends T> other, Function<? super T, U> fn,
            Executor executor) {
        return wrap(this.completableFuture.applyToEitherAsync(other.toCompletionStage(), fn,
                executor));
    }

    @Override
    public Promise<Void> acceptEither(Promise<? extends T> other, Consumer<? super T> action) {
        return wrap(this.completableFuture.acceptEither(other.toCompletionStage(), action));
    }

    @Override
    public Promise<Void> acceptEitherAsync(Promise<? extends T> other, Consumer<? super T> action,
            Executor executor) {
        return wrap(this.completableFuture.acceptEitherAsync(other.toCompletionStage(), action,
                executor));
    }

    @Override
    public Promise<Void> runAfterEither(Promise<?> other, Runnable action) {
        return wrap(this.completableFuture.runAfterEither(other.toCompletionStage(), action));
    }

    @Override
    public Promise<Void> runAfterEitherAsync(Promise<?> other, Runnable action, Executor executor) {
        return wrap(this.completableFuture.runAfterEitherAsync(other.toCompletionStage(), action,
                executor));
    }

    @Override
    public <U> Promise<U> thenCompose(Function<? super T, ? extends Promise<U>> fn) {
        return wrap(this.completableFuture.thenCompose(fn.andThen(p -> p.toCompletionStage())));
    }

    @Override
    public <U> Promise<U> thenComposeAsync(Function<? super T, ? extends Promise<U>> fn,
            Executor executor) {
        return wrap(this.completableFuture.thenComposeAsync(fn.andThen(p -> p.toCompletionStage()),
                executor));
    }

    @Override
    public Promise<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return wrap(this.completableFuture.exceptionally(fn));
    }

    @Override
    public Promise<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(this.completableFuture.whenComplete(action));
    }

    @Override
    public Promise<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action,
            Executor executor) {
        return wrap(this.completableFuture.whenCompleteAsync(action, executor));
    }

    @Override
    public <U> Promise<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(this.completableFuture.handle(fn));
    }

    @Override
    public <U> Promise<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn,
            Executor executor) {
        return wrap(this.completableFuture.handleAsync(fn, executor));
    }

    @Override
    public CompletionStage<T> toCompletionStage() {
        return this.completableFuture;
    }

    @Override
    public boolean complete(T value) {
        return this.completableFuture.complete(value);
    }

    @Override
    public boolean fail(Throwable ex) {
        return this.completableFuture.completeExceptionally(ex);
    }

    /**
     * Constructs an already fulfilled {@link Promise} with the provided value.
     * @param value
     * @return
     */
    public static <U> Promise<U> completed(U value) {
        DefaultPromise<U> promise = new DefaultPromise<>();
        promise.complete(value);
        return promise;
    }

    /**
     * Constructs a failed {@link Promise} with the provided exception.
     * @param ex
     * @return
     */
    public static <U> Promise<U> failed(Throwable ex) {
        DefaultPromise<U> promise = new DefaultPromise<>();
        promise.fail(ex);
        return promise;
    }
}
