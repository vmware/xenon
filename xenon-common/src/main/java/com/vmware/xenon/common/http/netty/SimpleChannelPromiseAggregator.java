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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.EventExecutor;

/**
 * Provides the ability to associate the outcome of multiple {@link ChannelPromise}
 * objects into a single {@link ChannelPromise} object.
 *
 * This was copied verbatim out of the Netty source because we need it to implement
 * NettyHttpToHttp2Handler but the Netty version isn't visible to us. The code is
 * unmodified.
 */
public class SimpleChannelPromiseAggregator extends DefaultChannelPromise {
    private final ChannelPromise promise;
    private int expectedCount;
    private int successfulCount;
    private int failureCount;
    private boolean doneAllocating;

    SimpleChannelPromiseAggregator(ChannelPromise promise, Channel c, EventExecutor e) {
        super(c, e);
        assert promise != null;
        this.promise = promise;
    }

    /**
     * Allocate a new promise which will be used to aggregate the overall success of this promise aggregator.
     * @return A new promise which will be aggregated.
     * {@code null} if {@link #doneAllocatingPromises()} was previously called.
     */
    public ChannelPromise newPromise() {
        if (this.doneAllocating) {
            throw new IllegalStateException("Done allocating. No more promises can be allocated.");
        }
        ++this.expectedCount;
        return this;
    }

    /**
     * Signify that no more {@link #newPromise()} allocations will be made.
     * The aggregation can not be successful until this method is called.
     * @return The promise that is the aggregation of all promises allocated with {@link #newPromise()}.
     */
    public ChannelPromise doneAllocatingPromises() {
        if (!this.doneAllocating) {
            this.doneAllocating = true;
            if (this.successfulCount == this.expectedCount) {
                this.promise.setSuccess();
                return super.setSuccess(null);
            }
        }
        return this;
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        if (allowFailure()) {
            ++this.failureCount;
            if (this.failureCount == 1) {
                this.promise.tryFailure(cause);
                return super.tryFailure(cause);
            }
            // TODO: We break the interface a bit here.
            // Multiple failure events can be processed without issue because this is an aggregation.
            return true;
        }
        return false;
    }

    /**
     * Fail this object if it has not already been failed.
     * <p>
     * This method will NOT throw an {@link IllegalStateException} if called multiple times
     * because that may be expected.
     */
    @Override
    public ChannelPromise setFailure(Throwable cause) {
        if (allowFailure()) {
            ++this.failureCount;
            if (this.failureCount == 1) {
                this.promise.setFailure(cause);
                return super.setFailure(cause);
            }
        }
        return this;
    }

    private boolean allowFailure() {
        return awaitingPromises() || this.expectedCount == 0;
    }

    private boolean awaitingPromises() {
        return this.successfulCount + this.failureCount < this.expectedCount;
    }

    @Override
    public ChannelPromise setSuccess(Void result) {
        if (awaitingPromises()) {
            ++this.successfulCount;
            if (this.successfulCount == this.expectedCount && this.doneAllocating) {
                this.promise.setSuccess(result);
                return super.setSuccess(result);
            }
        }
        return this;
    }

    @Override
    public boolean trySuccess(Void result) {
        if (awaitingPromises()) {
            ++this.successfulCount;
            if (this.successfulCount == this.expectedCount && this.doneAllocating) {
                this.promise.trySuccess(result);
                return super.trySuccess(result);
            }
            // TODO: We break the interface a bit here.
            // Multiple success events can be processed without issue because this is an aggregation.
            return true;
        }
        return false;
    }
}
