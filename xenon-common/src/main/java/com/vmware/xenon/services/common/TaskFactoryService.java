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

import java.net.URI;
import java.util.Arrays;
import java.util.function.Consumer;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.TaskService.TaskServiceState;

/**
 * Default implementation of a task factory service that handles indirect to direct task
 * processing. The factory will special case a POST request to create a child task, that has
 * taskInfo.isDirect=true. Using a subscription, it will delay completion of the POST,
 * and only complete it when it receives a notification that the child task has reached a final
 * state
 */
public class TaskFactoryService extends FactoryService {

    public TaskFactoryService(Class<? extends TaskService.TaskServiceState> stateClass) {
        super(stateClass);
    }

    @SuppressWarnings("unchecked")
    public static FactoryService create(Class<? extends Service> childServiceType,
            ServiceOption... options) {
        try {
            Service s = childServiceType.newInstance();
            Class<? extends TaskService.TaskServiceState> childServiceDocumentType =
                    (Class<? extends TaskServiceState>) s.getStateType();
            FactoryService fs = new TaskFactoryService(childServiceDocumentType) {
                @Override
                public Service createServiceInstance() throws Throwable {
                    return childServiceType.newInstance();
                }
            };
            Arrays.stream(options).forEach(option -> fs.toggleOption(option, true));
            return fs;
        } catch (Throwable e) {
            Utils.logWarning("Failure creating factory for %s: %s", childServiceType,
                    Utils.toString(e));
            return null;
        }

    }

    @Override
    public void handleRequest(Operation op, OperationProcessingStage opProcessingStage) {
        opProcessingStage = OperationProcessingStage.EXECUTING_SERVICE_HANDLER;

        boolean isIdempotentPut = (op.getAction() == Action.PUT) &&
                op.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT);

        if (op.getAction() != Action.POST && !isIdempotentPut) {
            super.handleRequest(op, opProcessingStage);
            return;
        }

        if (!op.hasBody()) {
            op.fail(new IllegalArgumentException("body is required"));
            return;
        }

        TaskServiceState initState = (TaskServiceState) op.getBody(super.getStateType());

        if (initState.taskInfo == null || !initState.taskInfo.isDirect) {
            super.handleRequest(op, opProcessingStage);
            return;
        }

        // handle only direct request from a client, not forwarded or replicated requests, to avoid
        // duplicate processing
        if (op.isFromReplication() || op.isForwarded()) {
            super.handleRequest(op, opProcessingStage);
            return;
        }

        handleDirectTaskPost(op, initState);
    }

    private void handleDirectTaskPost(Operation post, TaskServiceState initState) {
        // Direct task handling. We want to keep the task service simple and unaware of the
        // pending POST from the client. This keeps the child task a true finite state machine that
        // can PATCH itself, etc
        if (initState.taskInfo.stage == null) {
            initState.taskInfo.stage = TaskStage.CREATED;
        }
        Operation clonedPost = post.clone();
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
        TaskServiceState initState = (TaskServiceState) o.getBody(super.getStateType());
        Operation subscribe = Operation.createPost(this, initState.documentSelfLink)
                .transferRefererFrom(post)
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
            nOp.complete();
            switch (nOp.getAction()) {
            case PUT:
            case PATCH:
                TaskServiceState task = (TaskServiceState) nOp.getBody(super.getStateType());
                if (task.taskInfo == null || TaskState.isInProgress(task.taskInfo)) {
                    return;
                }
                // task is in final state (failed, or completed), complete original post
                post.setBodyNoCloning(task).complete();
                stopInDirectTaskSubscription(subscribe, nOp.getUri());
                return;
            case DELETE:
                // the task might have expired and self deleted, fail the client post
                post.setStatusCode(Operation.STATUS_CODE_TIMEOUT)
                        .fail(new IllegalStateException("Task self deleted"));
                stopInDirectTaskSubscription(subscribe, nOp.getUri());
                return;
            default:
                break;

            }
        };

        // Only if this is an owner-selected service, we create a reliable subscription.
        // Otherwise for non-replicated services, we just create a normal subscription.
        if (this.hasChildOption(ServiceOption.OWNER_SELECTION)) {
            ReliableSubscriptionService notificationTarget = ReliableSubscriptionService.create(
                    subscribe, sr, notifyC);
            getHost().startSubscriptionService(subscribe, notificationTarget, sr);
        } else {
            getHost().startSubscriptionService(subscribe, notifyC, sr);
        }
    }

    private void stopInDirectTaskSubscription(Operation sub, URI notificationTarget) {
        getHost().stopSubscriptionService(sub.clone().setAction(Action.DELETE),
                notificationTarget);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return null;
    }

}