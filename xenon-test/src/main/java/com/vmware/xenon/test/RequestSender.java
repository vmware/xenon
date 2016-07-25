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

package com.vmware.xenon.test;

import java.util.function.Consumer;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.ServiceDocument;

public interface RequestSender {

    @FunctionalInterface
    interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }

    /**
     * Asynchronously perform operation
     */
    void sendAsync(Operation op);

    /**
     * Synchronously perform operation.
     * {@link CompletionHandler} does NOT need explicitly pass/use {@link TestContext}.
     */
    void send(Operation op);

    <T extends ServiceDocument> T sendThenGetBody(Operation op, Class<T> type);

    String sendThenGetSelfLink(Operation post);

    void sendExpectSuccess(Operation op);

    void sendExpectSuccess(Operation op, Consumer<Operation> successCallback);

    void sendThen(Operation op, CompletionHandler handler);

    void sendThen(Operation op, TriConsumer<TestContext, Operation, Throwable> consumer);
}
