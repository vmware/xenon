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
    private int doneCount;
    private Throwable lastFailure;
    private boolean doneAllocating;

    SimpleChannelPromiseAggregator(ChannelPromise promise, Channel c, EventExecutor e) {
        super(c, e);
        assert promise != null && !promise.isDone();
        this.promise = promise;
    }

    /**
     * Allocate a new promise which will be used to aggregate the overall success of this promise aggregator.
     * @return A new promise which will be aggregated.
     * {@code null} if {@link #doneAllocatingPromises()} was previously called.
     */
    public ChannelPromise newPromise() {
        assert !this.doneAllocating : "Done allocating. No more promises can be allocated.";
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
            if (this.doneCount == this.expectedCount || this.expectedCount == 0) {
                return setPromise();
            }
        }
        return this;
    }

    @Override
    public boolean tryFailure(Throwable cause) {
        if (allowFailure()) {
            ++this.doneCount;
            this.lastFailure = cause;
            if (allPromisesDone()) {
                return tryPromise();
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
            ++this.doneCount;
            this.lastFailure = cause;
            if (allPromisesDone()) {
                return setPromise();
            }
        }
        return this;
    }

    @Override
    public ChannelPromise setSuccess(Void result) {
        if (awaitingPromises()) {
            ++this.doneCount;
            if (allPromisesDone()) {
                setPromise();
            }
        }
        return this;
    }

    @Override
    public boolean trySuccess(Void result) {
        if (awaitingPromises()) {
            ++this.doneCount;
            if (allPromisesDone()) {
                return tryPromise();
            }
            // TODO: We break the interface a bit here.
            // Multiple success events can be processed without issue because this is an aggregation.
            return true;
        }
        return false;
    }

    private boolean allowFailure() {
        return awaitingPromises() || this.expectedCount == 0;
    }

    private boolean awaitingPromises() {
        return this.doneCount < this.expectedCount;
    }

    private boolean allPromisesDone() {
        return this.doneCount == this.expectedCount && this.doneAllocating;
    }

    private ChannelPromise setPromise() {
        if (this.lastFailure == null) {
            this.promise.setSuccess();
            return super.setSuccess(null);
        } else {
            this.promise.setFailure(this.lastFailure);
            return super.setFailure(this.lastFailure);
        }
    }

    private boolean tryPromise() {
        if (this.lastFailure == null) {
            this.promise.trySuccess();
            return super.trySuccess(null);
        } else {
            this.promise.tryFailure(this.lastFailure);
            return super.tryFailure(this.lastFailure);
        }
    }
}
