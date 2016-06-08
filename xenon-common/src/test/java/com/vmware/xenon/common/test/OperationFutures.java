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

package com.vmware.xenon.common.test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;

/**
 * Utility methods for composing completable futures.
 */
public class OperationFutures {

    /**
     * Converts a native {@link CompletionHandler} to a {@link BiFunction}.
     * @param ch
     * @return
     */
    public static BiFunction<Operation, Throwable, Void> asFunc(CompletionHandler ch) {
        return (Operation completedOp, Throwable failure) -> {
            ch.handle(completedOp, failure);
            return null;
        };
    }

    public static CompletableFuture<List<Operation>> join(
            List<CompletableFuture<Operation>> futures) {
        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture<?>[futures.size()]));

        return allDoneFuture.thenApply(v -> futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
        );
    }
}
