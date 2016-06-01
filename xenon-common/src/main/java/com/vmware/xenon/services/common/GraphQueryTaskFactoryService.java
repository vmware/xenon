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

package com.vmware.xenon.services.common;

import java.util.function.Consumer;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.TaskState;

public class GraphQueryTaskFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.CORE_GRAPH_QUERIES;

    public GraphQueryTaskFactoryService() {
        super(GraphQueryTask.class);
    }

    @Override
    public void handleRequest(Operation op, OperationProcessingStage opProcessingStage) {
        opProcessingStage = OperationProcessingStage.EXECUTING_SERVICE_HANDLER;

        if (op.getAction() != Action.POST) {
            super.handleRequest(op, opProcessingStage);
            return;
        }

        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        GraphQueryTask initState = op.getBody(GraphQueryTask.class);

        if (initState.taskInfo == null || !initState.taskInfo.isDirect) {
            super.handleRequest(op, opProcessingStage);
            return;
        }

        handleDirectTaskPost(op, initState);
    }

    private void handleDirectTaskPost(Operation post, GraphQueryTask initState) {
        // Direct task handling. We want to keep the graph service simple and unaware of the
        // pending POST from the client. This keeps the child task a true finite state machine that
        // can PATCH itself, etc

        Operation clonedPost = post.clone();
        // do not replicate direct queries
        clonedPost.setReplicationDisabled(true);
        // reset task info since the child service does not expect it as initial state
        initState.taskInfo = null;

        clonedPost.setCompletion((o, e) -> {
            if (e != null) {
                post.setStatusCode(o.getStatusCode())
                        .setBodyNoCloning(o.getBodyRaw())
                        .fail(e);
                return;
            }
            subscribeToChildTask(o, post);
        });

        super.handleRequest(clonedPost, OperationProcessingStage.EXECUTING_SERVICE_HANDLER);
    }

    private void subscribeToChildTask(Operation o, Operation post) {
        Operation subscribe = Operation.createPost(o.getUri()).transferRefererFrom(post)
                .setCompletion((so, e) -> {
                    if (e == null) {
                        return;
                    }

                    post.setStatusCode(so.getStatusCode())
                            .setBodyNoCloning(so.getBodyRaw())
                            .fail(e);
                });

        ServiceSubscriber sr = ServiceSubscriber.create(true).setUsePublicUri(true);
        Consumer<Operation> notifyC = (nOp) -> {
            switch (nOp.getAction()) {
            case PUT:
            case PATCH:
                GraphQueryTask task = nOp.getBody(GraphQueryTask.class);
                if (TaskState.isInProgress(task.taskInfo)) {
                    return;
                }
                // task is in final state (failed, or completed), complete original post
                post.setBodyNoCloning(task).complete();
                return;
            case DELETE:
                // the task might have expired and self deleted, fail the client post
                post.setStatusCode(Operation.STATUS_CODE_TIMEOUT)
                        .fail(new IllegalStateException("Task self deleted"));
                return;
            default:
                break;

            }
        };

        ReliableSubscriptionService notificationTarget = ReliableSubscriptionService.create(
                subscribe, sr, notifyC);
        getHost().startSubscriptionService(subscribe, notificationTarget, sr);

    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new GraphQueryTaskService();
    }

}