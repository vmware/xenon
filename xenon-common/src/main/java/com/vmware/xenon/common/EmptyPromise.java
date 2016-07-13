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

/**
 * This is a {@link Promise} which does not hold any value, used only to signal when a task
 * completes or fails.
 */
public final class EmptyPromise extends DefaultPromise<Void> {

    /**
     * Completes the promise.
     * @return
     */
    public boolean complete() {
        return super.complete(null);
    }

    /**
     * Constructs already completed promise.
     * @return
     */
    public static EmptyPromise completed() {
        EmptyPromise promise = new EmptyPromise();
        promise.complete();
        return promise;
    }

    /**
     * Constructs promise signaling failure.
     * @param ex
     * @return
     */
    @SuppressWarnings("unchecked")
    public static EmptyPromise failed(Throwable ex) {
        EmptyPromise promise = new EmptyPromise();
        promise.fail(ex);
        return promise;
    }
}
