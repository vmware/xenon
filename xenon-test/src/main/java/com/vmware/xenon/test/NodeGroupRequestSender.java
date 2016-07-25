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
import com.vmware.xenon.common.ServiceHost;

public class NodeGroupRequestSender implements TargetedRequestSender {

    private InProcessNodeGroup nodeGroup;

    public NodeGroupRequestSender(InProcessNodeGroup nodeGroup) {
        this.nodeGroup = nodeGroup;
    }

    public InProcessNodeGroup getNodeGroup() {
        return this.nodeGroup;
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendAsync(Operation op) {
        getSender().sendAsync(op);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void send(Operation op) {
        getSender().send(op);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public <T extends ServiceDocument> T sendThenGetBody(Operation op, Class<T> type) {
        return getSender().sendThenGetBody(op, type);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public String sendThenGetSelfLink(Operation post) {
        return getSender().sendThenGetSelfLink(post);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendExpectSuccess(Operation op) {
        getSender().sendExpectSuccess(op);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendExpectSuccess(Operation op, Consumer<Operation> successCallback) {
        getSender().sendExpectSuccess(op, successCallback);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendThen(Operation op, CompletionHandler handler) {
        getSender().sendThen(op, handler);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendThen(Operation op, TriConsumer<TestContext, Operation, Throwable> consumer) {
        getSender().sendThen(op, consumer);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendGet(String servicePath, Consumer<Operation> consumer) {
        getSender().sendGet(servicePath, consumer);
    }

    /**
     * Send via one of the host randomly chose in the node-group.
     */
    @Override
    public void sendPost(String servicePath, Consumer<Operation> consumer) {
        getSender().sendPost(servicePath, consumer);
    }

    // TODO: sendPut, sendDelete

    @SuppressWarnings("unchecked")
    private <T extends ServiceHost> ServiceHostRequestSender<T> getSender() {
        return new ServiceHostRequestSender<>((T) this.nodeGroup.getHost());
    }

}
