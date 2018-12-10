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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.vmware.xenon.common.config.XenonConfiguration;

/**
 * This is an implementation of a condensed version of {@link CompletionStage},
 * excluding the methods that implicitly use a global executor. This class
 * encapsulates {@link CompletableFuture}
 * @param <T>
 */
public final class DeferredResult<T> {
    private static final Logger LOG = Logger.getLogger(DeferredResult.class.getName());
    private static boolean IS_UNCAUGHT_EXCEPTION_LOGGING_ENABLED = XenonConfiguration.bool(
            DeferredResult.class,
            "isUncaughtExceptionLoggingEnabled", false);

    private final CompletableFuture<T> completableFuture;
    private boolean isLastInChain;

    /**
     * Constructs an already realized {@link DeferredResult} with the provided
     * value.
     * @param value
     * @return
     */
    public static <U> DeferredResult<U> completed(U value) {
        DeferredResult<U> deferred = new DeferredResult<>();
        deferred.complete(value);
        return deferred;
    }

    /**
     * Constructs a failed {@link DeferredResult} with the provided exception.
     * @param ex
     * @return
     */
    public static <U> DeferredResult<U> failed(Throwable ex) {
        DeferredResult<U> deferred = new DeferredResult<>();
        deferred.fail(ex);
        return deferred;
    }

    /**
     * @see CompletableFuture#allOf(CompletableFuture...)
     * @param deferredResults
     * @return
     */
    public static DeferredResult<Void> allOf(DeferredResult<?>... deferredResults) {
        CompletableFuture<?>[] cfs = Arrays.stream(deferredResults)
                .map(d -> d.completableFuture)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture<?>[deferredResults.length]);
        CompletableFuture<Void> cf = CompletableFuture.allOf(cfs);
        return new DeferredResult<>(cf);
    }

    /**
     * Similar to CompletableFuture#allOf(CompletableFuture...) operating on
     * deferred results with the same value type. The result is the deferred
     * list of the values of the individual instances.
     * @param deferredResults
     * @return
     */
    public static <U> DeferredResult<List<U>> allOf(List<DeferredResult<U>> deferredResults) {
        List<CompletableFuture<U>> futures = deferredResults
                .stream()
                .map(d -> d.completableFuture)
                .collect(Collectors.toList());
        CompletableFuture<List<U>> cf = CompletableFuture
                .allOf(futures.toArray(new CompletableFuture<?>[deferredResults.size()]))
                .thenApply(ignore -> {
                    // Here all the futures are completed *successfully* which
                    // means that based on the CompletableFuture contract:
                    // 1. getNow() will return the actual value!
                    // 2. there are no exceptions
                    List<U> results = futures
                            .stream()
                            .map(f -> f.getNow(null))
                            .collect(Collectors.toList());
                    return results;
                });
        return new DeferredResult<>(cf);
    }

    /**
     * @see CompletableFuture#anyOf(CompletableFuture...)
     * @param deferredResults
     * @return
     */
    public static DeferredResult<?> anyOf(DeferredResult<?>... deferredResults) {
        CompletableFuture<?>[] cfs = Arrays.stream(deferredResults)
                .map(d -> d.completableFuture)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture<?>[deferredResults.length]);
        CompletableFuture<?> cf = CompletableFuture.anyOf(cfs);
        return new DeferredResult<>(cf);
    }

    /**
     * Similar to CompletableFuture#anyOf(CompletableFuture...) operating on
     * deferred results with the value type.
     * @param deferredResults
     * @return
     */
    public static <U> DeferredResult<U> anyOf(List<DeferredResult<U>> deferredResults) {
        List<CompletableFuture<U>> futures = deferredResults
                .stream()
                .map(d -> d.completableFuture)
                .collect(Collectors.toList());
        @SuppressWarnings("unchecked")
        CompletableFuture<U> cf = CompletableFuture
                .anyOf(futures.toArray(new CompletableFuture<?>[deferredResults.size()]))
                .thenApply(obj -> (U) obj);
        return new DeferredResult<>(cf);
    }

    /**
     * Creates a new incomplete {@link DeferredResult}
     */
    public DeferredResult() {
        this(new CompletableFuture<>());
    }

    /**
     * Creates a new {@link DeferredResult} by wrapping the provided {@link CompletableFuture}
     * @param completionStage
     */
    public DeferredResult(CompletableFuture<T> completableFuture) {
        this.completableFuture = completableFuture;
        this.isLastInChain = true;
    }

    /**
     * Wraps the provided {@link CompletableFuture} into {@link DeferredResult}
     * @param completionStage
     * @return
     */
    protected <U> DeferredResult<U> wrap(CompletableFuture<U> completableFuture) {
        this.isLastInChain = false;
        return handleExceptionLogging(new DeferredResult<>(completableFuture));
    }

    /**
     * Adds additional exception handler to the completable future, if
     * {@link DeferredResult#IS_LOGGING_UNCAUGHT_EXCEPTIONS_ENABLED} is true and in case the handled exception is
     * the last invocation in the chain, logs the exception and the initiator of the deferred result so that it
     * can be properly handled..
     * @param dr
     * @return
     */
    private <U> DeferredResult<U> handleExceptionLogging(DeferredResult<U> dr) {
        if (!IS_UNCAUGHT_EXCEPTION_LOGGING_ENABLED) {
            return dr;
        }

        String stackTrace = Utils.toString(new Exception());

        dr.completableFuture.exceptionally(e -> {
            CompletionException ex = e instanceof CompletionException ? (CompletionException) e
                    : new CompletionException(e);
            if (dr.isLastInChain) {
                Utils.log(LOG, null, getClass().getSimpleName(), Level.WARNING, () -> String
                        .format("Exception in DeferredResult chain was not handled. Initiator: %s\nException: %s",
                                stackTrace, Utils.toString(ex.getCause())));
            }
            throw ex;
        });

        return dr;
    }

    /**
     * @see CompletionStage#thenApply(Function)
     */
    public <U> DeferredResult<U> thenApply(Function<? super T, ? extends U> fn) {
        return wrap(this.completableFuture.thenApply(fn));
    }

    /**
     * @see CompletionStage#thenApplyAsync(Function, Executor)
     */
    public <U> DeferredResult<U> thenApplyAsync(Function<? super T, ? extends U> fn,
            Executor executor) {
        return wrap(this.completableFuture.thenApplyAsync(fn, executor));
    }

    /**
     * @see CompletionStage#thenAccept(Consumer)
     */
    public DeferredResult<Void> thenAccept(Consumer<? super T> action) {
        return wrap(this.completableFuture.thenAccept(action));
    }

    /**
     * @see CompletionStage#thenAcceptAsync(Consumer, Executor)
     */
    public DeferredResult<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return wrap(this.completableFuture.thenAcceptAsync(action, executor));
    }

    /**
     * @see CompletionStage#thenRun(Runnable)
     */
    public DeferredResult<Void> thenRun(Runnable action) {
        return wrap(this.completableFuture.thenRun(action));
    }

    /**
     * @see CompletionStage#thenRunAsync(Runnable, Executor)
     */
    public DeferredResult<Void> thenRunAsync(Runnable action, Executor executor) {
        return wrap(this.completableFuture.thenRunAsync(action, executor));
    }

    /**
     * @see CompletionStage#thenCombine(CompletionStage, BiFunction)
     */
    public <U, V> DeferredResult<V> thenCombine(DeferredResult<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn) {
        return wrap(this.completableFuture.thenCombine(other.toCompletionStage(), fn));
    }

    /**
     * @see CompletionStage#thenCombineAsync(CompletionStage, BiFunction, Executor)
     */
    public <U, V> DeferredResult<V> thenCombineAsync(DeferredResult<? extends U> other,
            BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return wrap(this.completableFuture.thenCombineAsync(other.toCompletionStage(), fn,
                executor));
    }

    /**
     * @see CompletionStage#thenAcceptBoth(CompletionStage, BiConsumer)
     */
    public <U> DeferredResult<Void> thenAcceptBoth(DeferredResult<? extends U> other,
            BiConsumer<? super T, ? super U> action) {
        return wrap(this.completableFuture.thenAcceptBoth(other.toCompletionStage(), action));
    }

    /**
     * @see CompletionStage#thenAcceptBothAsync(CompletionStage, BiConsumer, Executor)
     */
    public <U> DeferredResult<Void> thenAcceptBothAsync(DeferredResult<? extends U> other,
            BiConsumer<? super T, ? super U> action, Executor executor) {
        return wrap(this.completableFuture.thenAcceptBothAsync(other.toCompletionStage(), action,
                executor));
    }

    /**
     * @see CompletionStage#runAfterBoth(CompletionStage, Runnable)
     */
    public DeferredResult<Void> runAfterBoth(DeferredResult<?> other, Runnable action) {
        return wrap(this.completableFuture.runAfterBoth(other.toCompletionStage(), action));
    }

    /**
     * @see CompletionStage#runAfterBothAsync(CompletionStage, Runnable, Executor)
     */
    public DeferredResult<Void> runAfterBothAsync(DeferredResult<?> other, Runnable action,
            Executor executor) {
        return wrap(this.completableFuture.runAfterBothAsync(other.toCompletionStage(), action,
                executor));
    }

    /**
     * @see CompletionStage#applyToEither(CompletionStage, Function)
     */
    public <U> DeferredResult<U> applyToEither(DeferredResult<? extends T> other,
            Function<? super T, U> fn) {
        return wrap(this.completableFuture.applyToEither(other.toCompletionStage(), fn));
    }

    /**
     * @see CompletionStage#applyToEitherAsync(CompletionStage, Function, Executor)
     */
    public <U> DeferredResult<U> applyToEitherAsync(DeferredResult<? extends T> other,
            Function<? super T, U> fn,
            Executor executor) {
        return wrap(this.completableFuture.applyToEitherAsync(other.toCompletionStage(), fn,
                executor));
    }

    /**
     * @see CompletionStage#acceptEither(CompletionStage, Consumer)
     */
    public DeferredResult<Void> acceptEither(DeferredResult<? extends T> other,
            Consumer<? super T> action) {
        return wrap(this.completableFuture.acceptEither(other.toCompletionStage(), action));
    }

    /**
     * @see CompletionStage#acceptEitherAsync(CompletionStage, Consumer, Executor)
     */
    public DeferredResult<Void> acceptEitherAsync(DeferredResult<? extends T> other,
            Consumer<? super T> action,
            Executor executor) {
        return wrap(this.completableFuture.acceptEitherAsync(other.toCompletionStage(), action,
                executor));
    }

    /**
     * @see CompletionStage#runAfterEither(CompletionStage, Runnable)
     */
    public DeferredResult<Void> runAfterEither(DeferredResult<?> other, Runnable action) {
        return wrap(this.completableFuture.runAfterEither(other.toCompletionStage(), action));
    }

    /**
     * @see CompletionStage#runAfterEitherAsync(CompletionStage, Runnable, Executor)
     */
    public DeferredResult<Void> runAfterEitherAsync(DeferredResult<?> other, Runnable action,
            Executor executor) {
        return wrap(this.completableFuture.runAfterEitherAsync(other.toCompletionStage(), action,
                executor));
    }

    /**
     * @see CompletionStage#thenCompose(Function)
     */
    public <U> DeferredResult<U> thenCompose(Function<? super T, ? extends DeferredResult<U>> fn) {
        return wrap(this.completableFuture.thenCompose(fn.andThen(p -> p.toCompletionStage())));
    }

    /**
     * @see CompletionStage#thenComposeAsync(Function, Executor)
     */
    public <U> DeferredResult<U> thenComposeAsync(
            Function<? super T, ? extends DeferredResult<U>> fn,
            Executor executor) {
        return wrap(this.completableFuture.thenComposeAsync(fn.andThen(p -> p.toCompletionStage()),
                executor));
    }

    /**
     * @see CompletionStage#exceptionally(Function)
     */
    public DeferredResult<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return wrap(this.completableFuture.exceptionally(fn));
    }

    /**
     * @see CompletionStage#whenComplete(BiConsumer)
     */
    public DeferredResult<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(this.completableFuture.whenComplete(action));
    }

    /**
     * @see CompletionStage#whenCompleteAsync(BiConsumer, Executor)
     */
    public DeferredResult<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action,
            Executor executor) {
        return wrap(this.completableFuture.whenCompleteAsync(action, executor));
    }

    /**
     * @see CompletionStage#handle(BiFunction)
     */
    public <U> DeferredResult<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(this.completableFuture.handle(fn));
    }

    /**
     * @see CompletionStage#handleAsync(BiFunction, Executor)
     */
    public <U> DeferredResult<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn,
            Executor executor) {
        return wrap(this.completableFuture.handleAsync(fn, executor));
    }

    /**
     * @see CompletableFuture#getNow(Object)
     * @param valueIfAbsent
     * @return
     */
    public T getNow(T valueIfAbsent) {
        return this.completableFuture.getNow(valueIfAbsent);
    }

    /**
     * @throws CompletionException if the execution encountered exception.
     * @return The value of this DeferredResult if it is already completed else
     * uses the provided valueSupplierIfAbsent to generate the returned value.
     */
    public T getNow(Supplier<? extends T> valueSupplierIfAbsent) {
        if (this.completableFuture.isDone()) {
            return this.completableFuture.join();
        }
        return valueSupplierIfAbsent.get();
    }

    /**
     * @see CompletableFuture#isDone()
     * @return
     */
    public boolean isDone() {
        return this.completableFuture.isDone();
    }

    /**
     * Converts this {@link DeferredResult} to {@link CompletionStage}
     * @return
     */
    public CompletionStage<T> toCompletionStage() {
        return this.completableFuture;
    }

    /**
     * Explicitly signals the deferred realization is complete with the supplied value.
     * @param value
     * @return
     */
    public boolean complete(T value) {
        return this.completableFuture.complete(value);
    }

    /**
     * Signals that the deferred realization has failed with the exception.
     * @param ex
     * @return
     */
    public boolean fail(Throwable ex) {
        return this.completableFuture.completeExceptionally(ex);
    }

    /**
     * Has the same semantics as {@link #whenComplete(BiConsumer)} but notifies
     * the provided operation that the stage is completed.
     * @param operation
     * @return
     */
    public DeferredResult<T> whenCompleteNotify(Operation operation) {
        return wrap(this.completableFuture.whenComplete((ignore, ex) -> {
            if (ex != null) {
                if (ex instanceof CompletionException) {
                    ex = ex.getCause();
                }
                operation.fail(ex);
            } else {
                operation.complete();
            }
        }));
    }
}
