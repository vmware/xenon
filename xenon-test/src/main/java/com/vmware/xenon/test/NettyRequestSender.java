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

/**
 * Request sender using Netty.
 */
public class NettyRequestSender implements RequestSender {

    @Override
    public void sendAsync(Operation op) {
        // TODO: impl
    }

    @Override
    public void send(Operation op) {
        // TODO: impl
    }

    @Override
    public <T extends ServiceDocument> T sendThenGetBody(Operation op, Class<T> type) {
        // TODO: impl
        return null;
    }

    @Override
    public String sendThenGetSelfLink(Operation post) {
        // TODO: impl
        return null;
    }

    @Override
    public void sendExpectSuccess(Operation op) {
        // TODO: impl
    }

    @Override
    public void sendExpectSuccess(Operation op, Consumer<Operation> successCallback) {
        // TODO: impl
    }

    @Override
    public void sendThen(Operation op, CompletionHandler handler) {
        // TODO: impl
    }

    @Override
    public void sendThen(Operation op, TriConsumer<TestContext, Operation, Throwable> consumer) {
        // TODO: impl
    }
}
