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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This is a condensed version of {@link CompletionStage}, excluding the methods
 * that implicitly use a global executor.
 * The methods/descriptions are copied from {@link CompletionStage}
 * @param <T>
 */
public interface Promise<T> {
    /**
     * @see CompletionStage#thenApply(Function)
     */
    public <U> Promise<U> thenApply(Function<? super T,? extends U> fn);

    /**
     * @see CompletionStage#thenApplyAsync(Function, Executor)
     */
    public <U> Promise<U> thenApplyAsync(Function<? super T,? extends U> fn, Executor executor);

    /**
     * @see CompletionStage#thenAccept(Consumer)
     */
    public Promise<Void> thenAccept(Consumer<? super T> action);

    /**
     * @see CompletionStage#thenAcceptAsync(Consumer, Executor)
     */
    public Promise<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor);
    /**
     * @see CompletionStage#thenRun(Runnable)
     */
    public Promise<Void> thenRun(Runnable action);

    /**
     * @see CompletionStage#thenRunAsync(Runnable, Executor)
     */
    public Promise<Void> thenRunAsync(Runnable action, Executor executor);

    /**
     * @see CompletionStage#thenCombine(CompletionStage, BiFunction)
     */
    public <U,V> Promise<V> thenCombine(Promise<? extends U> other,
            BiFunction<? super T,? super U,? extends V> fn);

    /**
     * @see CompletionStage#thenCombineAsync(CompletionStage, BiFunction, Executor)
     */
    public <U,V> Promise<V> thenCombineAsync(Promise<? extends U> other,
            BiFunction<? super T,? super U,? extends V> fn, Executor executor);

    /**
     * @see CompletionStage#thenAcceptBoth(CompletionStage, BiConsumer)
     */
    public <U> Promise<Void> thenAcceptBoth(Promise<? extends U> other,
            BiConsumer<? super T, ? super U> action);

    /**
     * @see CompletionStage#thenAcceptBothAsync(CompletionStage, BiConsumer, Executor)
     */
    public <U> Promise<Void> thenAcceptBothAsync(Promise<? extends U> other,
            BiConsumer<? super T, ? super U> action, Executor executor);

    /**
     * @see CompletionStage#runAfterBoth(CompletionStage, Runnable)
     */
    public Promise<Void> runAfterBoth(Promise<?> other, Runnable action);

    /**
     * @see CompletionStage#runAfterBothAsync(CompletionStage, Runnable, Executor)
     */
    public Promise<Void> runAfterBothAsync(Promise<?> other, Runnable action, Executor executor);

    /**
     * @see CompletionStage#applyToEither(CompletionStage, Function)
     */
    public <U> Promise<U> applyToEither(Promise<? extends T> other, Function<? super T, U> fn);

    /**
     * @see CompletionStage#applyToEitherAsync(CompletionStage, Function, Executor)
     */
    public <U> Promise<U> applyToEitherAsync(Promise<? extends T> other, Function<? super T, U> fn,
            Executor executor);

    /**
     * @see CompletionStage#acceptEither(CompletionStage, Consumer)
     */
    public Promise<Void> acceptEither(Promise<? extends T> other, Consumer<? super T> action);

    /**
     * @see CompletionStage#acceptEitherAsync(CompletionStage, Consumer, Executor)
     */
    public Promise<Void> acceptEitherAsync(Promise<? extends T> other, Consumer<? super T> action,
            Executor executor);

    /**
     * @see CompletionStage#runAfterEither(CompletionStage, Runnable)
     */
    public Promise<Void> runAfterEither(Promise<?> other, Runnable action);

    /**
     * @see CompletionStage#runAfterEitherAsync(CompletionStage, Runnable, Executor)
     */
    public Promise<Void> runAfterEitherAsync(Promise<?> other, Runnable action,
            Executor executor);

    /**
     * @see CompletionStage#thenCompose(Function)
     */
    public <U> Promise<U> thenCompose(Function<? super T, ? extends Promise<U>> fn);

    /**
     * @see CompletionStage#thenComposeAsync(Function, Executor)
     */
    public <U> Promise<U> thenComposeAsync(Function<? super T, ? extends Promise<U>> fn,
            Executor executor);

    /**
     * @see CompletionStage#exceptionally(Function)
     */
    public Promise<T> exceptionally(Function<Throwable, ? extends T> fn);

    /**
     * @see CompletionStage#whenComplete(BiConsumer)
     */
    public Promise<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

    /**
     * @see CompletionStage#whenCompleteAsync(BiConsumer, Executor)
     */
    public Promise<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action,
            Executor executor);

    /**
     * @see CompletionStage#handle(BiFunction)
     */
    public <U> Promise<U> handle(BiFunction<? super T, Throwable, ? extends U> fn);

    /**
     * @see CompletionStage#handleAsync(BiFunction, Executor)
     */
    public <U> Promise<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn,
            Executor executor);

    /**
     * Converts this {@link Promise} to {@link CompletionStage}
     * @return
     */
    CompletionStage<T> toCompletionStage();

    /**
     * Explicitly fulfills the promise with the supplied value.
     * @param value
     * @return
     */
    boolean complete(T value);

    /**
     * Signals that the promise has failed with the exception.
     * @param t
     * @return
     */
    boolean fail(Throwable t);
}