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

package com.vmware.xenon.common;

import java.net.URI;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TaskService;

public class SynchronizationTaskService
        extends TaskService<SynchronizationTaskService.State> {

    public enum SubStage {
        QUERY_CHILDREN,
        SYNCH_CHILDREN,
        RESTART_SYNCH
    }

    public static final String FACTORY_LINK = ServiceUriPaths.SYNCHRONIZATION_TASKS;

    public static class State extends TaskService.TaskServiceState {
        /**
         * SelfLink of the FactoryService that will be synchronized by this task.
         */
        public String factorySelfLink;

        /**
         * Type of the ServiceDocument used by child services of the factory service.
         */
        public String factoryStateKind;

        /**
         * The node-selector used for replicating child services of this factory.
         */
        public String nodeSelectorLink;

        /**
         * ResultLimit used by the synchronization task when querying for child services
         * for this factory.
         */
        public int queryResultLimit;

        /**
         * The last known membershipUpdateTime that triggered this synchronization event.
         */
        public Long membershipUpdateTimeMicros;

        /**
         * ServiceOptions supported by the child service.
         */
        public EnumSet<ServiceOption> childOptions;

        /**
         * The current SubStage of the synchronization task.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public SubStage subStage;

        /**
         * URI of the completed query-task that contains results.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI queryPageReference;
    }

    public SynchronizationTaskService() {
        super(State.class);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    @Override
    public void handleStart(Operation post) {
        State initialState = validateStartPost(post);
        if (initialState == null) {
            return;
        }

        initializeState(initialState, post);

        // Skip synchronization for ODL services.
        if (initialState.childOptions.contains(ServiceOption.ON_DEMAND_LOAD)) {
            initialState.taskInfo.stage = TaskState.TaskStage.FINISHED;
            post.setBody(initialState)
                    .setStatusCode(Operation.STATUS_CODE_OK)
                    .complete();
            return;
        }

        initialState.taskInfo.stage = TaskState.TaskStage.CREATED;
        post.setBody(initialState)
                .setStatusCode(Operation.STATUS_CODE_ACCEPTED)
                .complete();

        // self patch to start task's state machine
        if (initialState.taskInfo.stage == TaskState.TaskStage.CREATED) {
            sendSelfPatch(initialState, TaskState.TaskStage.STARTED, subStageSetter(SubStage.QUERY_CHILDREN));
        }
    }

    @Override
    protected State validateStartPost(Operation post) {
        State task = super.validateStartPost(post);
        if (task == null) {
            return null;
        }
        if (task.factorySelfLink == null) {
            post.fail(
                    new IllegalArgumentException("factorySelfLink must be set."));
            return null;
        }
        if (task.factoryStateKind == null) {
            post.fail(
                    new IllegalArgumentException("factoryStateKind must be set."));
            return null;
        }
        if (task.nodeSelectorLink == null) {
            post.fail(
                    new IllegalArgumentException("nodeSelectorLink must be set."));
            return null;
        }
        if (task.queryResultLimit <= 0) {
            post.fail(
                    new IllegalArgumentException("queryResultLimit must be set."));
            return null;
        }
        if (task.childOptions == null || task.childOptions.size() == 0) {
            post.fail(
                    new IllegalArgumentException("childService must be set."));
        }
        if (task.taskInfo.stage != null && task.taskInfo.stage != TaskState.TaskStage.CREATED) {
            post.fail(
                    new IllegalArgumentException(
                            "synch-tasks can only be created in CREATED state"));
        }
        if (task.membershipUpdateTimeMicros == null && task.childOptions.contains(ServiceOption.REPLICATION)) {
            post.fail(
                    new IllegalArgumentException(
                            "membershipUpdateTimeMicros must be set for Replicated services."));
        }
        if (task.subStage != null) {
            post.fail(
                    new IllegalArgumentException("subStage must not be set."));
        }
        if (task.queryPageReference != null) {
            post.fail(
                    new IllegalArgumentException("queryPageReference must not be set."));
        }
        return task;
    }

    /**
     * Called for POST requests converted to PUT because of
     * ServiceOption#IDEMPOTENT_POST
     */
    public void handlePut(Operation put) {
        // Verify this request is a POST converted to PUT.
        if (!put.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
            put.fail(new IllegalStateException("Action.PUT not supported for SynchronizationTaskService"));
            return;
        }

        // TODO - Validate PUT just like how we validate POST

        State task = getState(put);
        State body = getBody(put);

        logInfo("HostId %s, IDEMPOTENT_POST Synch for %s, Stage %s, SubStage %s, NewStage %s, NewSubStage %s",
                this.getHost().getId(), task.factorySelfLink, task.taskInfo.stage, task.subStage, body.taskInfo.stage, body.subStage);

        // This POST could be for an older node-group change notification. If so, don't bother
        // restarting synchronization.
        if (task.membershipUpdateTimeMicros != null &&
                task.membershipUpdateTimeMicros > body.membershipUpdateTimeMicros) {
            put.complete();
            return;
        }

        boolean restartStateMachine = false;

        switch (task.taskInfo.stage) {
        case CREATED:
            // Task just got created. Nothing needs to happen.
            break;
        case STARTED:
            // Task was already running. Set the substage
            // of the task to RESTART_SYNCH. This allows us to
            // preempt the running task to restart itself.
            logInfo("Restarting SynchronizationTask");
            task.subStage = SubStage.RESTART_SYNCH;
            break;
        case FAILED:
        case CANCELLED:
        case FINISHED:
            // Task had previously finished processing. Set the
            // taskStage back to STARTED, to restart state-machine.
            task.taskInfo.stage = TaskState.TaskStage.STARTED;
            task.subStage = SubStage.QUERY_CHILDREN;
            restartStateMachine = true;
            break;
        default:
            break;
        }

        // Let's update the node-group membership properties to the latest
        task.membershipUpdateTimeMicros = body.membershipUpdateTimeMicros;

        // Complete the PUT request. The remaining request will be processed
        // asynchronously.
        put.complete();

        // kick-off task's state-machine again if it was not running.
        if (restartStateMachine) {
            handleSubStage(task);
        }
    }

    public void handlePatch(Operation patch) {
        State task = getState(patch);
        State body = getBody(patch);

        logInfo("HostId %s, Running Synch for %s, Stage %s, SubStage %s, NewStage %s, NewSubStage %s",
                this.getHost().getId(), task.factorySelfLink, task.taskInfo.stage, task.subStage, body.taskInfo.stage, body.subStage);

        if (!validateTransition(patch, task, body)) {
            return;
        }

        if (task.subStage != SubStage.RESTART_SYNCH) {
            updateState(task, body);
        } else {
            // The task was preempted. Ignore the patch request
            // and restart synchronization.
            task.taskInfo.stage = TaskState.TaskStage.STARTED;
            task.subStage = SubStage.QUERY_CHILDREN;
        }
        patch.complete();

        switch (task.taskInfo.stage) {
        case STARTED:
            handleSubStage(task);
            break;
        case CANCELLED:
            logInfo("Task canceled: not implemented, ignoring");
            break;
        case FINISHED:
            logFine("Task finished successfully");
            break;
        case FAILED:
            logWarning("Task failed: %s",
                    (task.failureMessage != null ? task.failureMessage : "No reason given"));
            break;
        default:
            break;
        }
    }

    public void handleSubStage(State task) {
        switch (task.subStage) {
        case QUERY_CHILDREN:
            handleQueryChildren(task);
            break;
        case SYNCH_CHILDREN:
            handleSynchronizeChildren(task, true);
            break;
        default:
            logWarning("Unexpected sub stage: %s", task.subStage);
            break;
        }
    }

    private void handleQueryChildren(State task) {
        QueryTask queryTask = buildChildQueryTask(task);
        Operation queryPost = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(queryTask)
                .setCompletion((o, e) -> {
                    if (getHost().isStopping()) {
                        sendSelfCancellationPatch(task, "host is stopping");
                        return;
                    }

                    if (e != null) {
                        if (!getHost().isStopping()) {
                            logWarning("Query failed with %s", e.toString());
                        }
                        sendSelfFailurePatch(task, e.getMessage());
                        return;
                    }

                    ServiceDocumentQueryResult rsp = o.getBody(QueryTask.class).results;

                    // Query returned zero results. Self-patch the task to FINISHED state.
                    if (rsp == null || rsp.nextPageLink == null) {
                        sendSelfFinishedPatch(task);
                        return;
                    }

                    URI queryTaskUri = UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
                    task.queryPageReference = UriUtils.buildUri(queryTaskUri, rsp.nextPageLink);
                    sendSelfPatch(task,
                            TaskState.TaskStage.STARTED, subStageSetter(SubStage.SYNCH_CHILDREN));
                });

        sendRequest(queryPost);
    }

    private QueryTask buildChildQueryTask(State task) {
        QueryTask queryTask = new QueryTask();
        queryTask.querySpec = new QueryTask.QuerySpecification();
        queryTask.taskInfo.isDirect = true;

        // Add clause for documentSelfLink = <FactorySelfLink>/*
        QueryTask.Query uriPrefixClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_SELF_LINK)
                .setTermMatchType(QueryTask.QueryTerm.MatchType.WILDCARD)
                .setTermMatchValue(
                        task.factorySelfLink +
                                UriUtils.URI_PATH_CHAR +
                                UriUtils.URI_WILDCARD_CHAR);
        queryTask.querySpec.query.addBooleanClause(uriPrefixClause);

        // Add clause for documentKind = Factory state kind
        QueryTask.Query kindClause = new QueryTask.Query()
                .setTermPropertyName(ServiceDocument.FIELD_NAME_KIND)
                .setTermMatchValue(task.factoryStateKind);
        queryTask.querySpec.query.addBooleanClause(kindClause);

        // set timeout based on peer synchronization upper limit
        long timeoutMicros = TimeUnit.SECONDS.toMicros(
                getHost().getPeerSynchronizationTimeLimitSeconds());
        timeoutMicros = Math.max(timeoutMicros, getHost().getOperationTimeoutMicros());
        queryTask.documentExpirationTimeMicros = Utils.getNowMicrosUtc() + timeoutMicros;

        // Make this a broadcast query so that we get child services from all peer nodes.
        queryTask.querySpec.options = EnumSet.of(QueryTask.QuerySpecification.QueryOption.BROADCAST);

        // Set the node-selector link.
        queryTask.nodeSelectorLink = task.nodeSelectorLink;

        // process child services in limited numbers, set query result limit
        queryTask.querySpec.resultLimit = task.queryResultLimit;

        return queryTask;
    }

    private void handleSynchronizeChildren(State task, boolean verifyOwnership) {
        if (task.queryPageReference == null) {
            sendSelfFinishedPatch(task);
            return;
        }

        if (getHost().isStopping()) {
            sendSelfCancellationPatch(task, "host is stopping");
            return;
        }

        if (verifyOwnership && verifySynchronizationOwnership(task)) {
            // Verifying ownership will recursively call into handleSynchronizationChildren
            // with verifyOwnership set to false.
            return;
        }

        Operation.CompletionHandler c = (o, e) -> {
            if (e != null) {
                if (!getHost().isStopping()) {
                    logWarning("Failure retrieving query results from %s: %s",
                            task.queryPageReference,
                            e.toString());
                }
                sendSelfFailurePatch(task,
                        "failure retrieving query page results");
                return;
            }

            ServiceDocumentQueryResult rsp = o.getBody(QueryTask.class).results;
            if (rsp.documentCount == 0 || rsp.documentLinks.isEmpty()) {
                sendSelfFinishedPatch(task);
                return;
            }
            synchronizeChildrenInQueryPage(task, rsp);
        };
        sendRequest(Operation.createGet(
                task.queryPageReference).setCompletion(c));

    }

    private void synchronizeChildrenInQueryPage(State task,
                                                ServiceDocumentQueryResult rsp) {
        if (getHost().isStopping()) {
            sendSelfCancellationPatch(task, "host is stopping");
            return;
        }

        // track child service request in parallel, passing a single parent operation
        AtomicInteger pendingStarts = new AtomicInteger(rsp.documentLinks.size());
        Operation.CompletionHandler c = (o, e) -> {
            int r = pendingStarts.decrementAndGet();
            if (e != null && !getHost().isStopping()) {
                logWarning("Restart for children failed: %s", e.getMessage());
            }

            if (getHost().isStopping()) {
                sendSelfCancellationPatch(task, "host is stopping");
                return;
            }

            if (r != 0) {
                return;
            }

            task.queryPageReference = rsp.nextPageLink != null
                    ? UriUtils.buildUri(task.queryPageReference, rsp.nextPageLink)
                    : null;

            if (task.queryPageReference == null) {
                sendSelfFinishedPatch(task);
                return;
            }
            sendSelfPatch(task, TaskState.TaskStage.STARTED, subStageSetter(SubStage.SYNCH_CHILDREN));
        };

        for (String link : rsp.documentLinks) {
            if (getHost().isStopping()) {
                sendSelfCancellationPatch(task, "host is stopping");
                return;
            }

            Operation post = Operation.createPost(this, link)
                    .setCompletion(c)
                    .setReferer(getUri());
            startOrSynchChildService(task, post);
        }
    }

    private boolean verifySynchronizationOwnership(State task) {
        // If this is not a REPLICATED factory, the task is most likely
        // invoked as part of handleStart from the FactoryService. So, let's
        // not bother verifying consensus.
        if (!task.childOptions.contains(ServiceOption.REPLICATION)) {
            return false;
        }

        // Let's firstly check if the local node thinks it's still the owner for
        // this document.
        Operation selectOp = Operation
                .createPost(null)
                .setExpiration(task.documentExpirationTimeMicros)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        sendSelfFailurePatch(task, e.getMessage());
                        return;
                    }

                    NodeSelectorService.SelectOwnerResponse rsp = o.getBody(
                            NodeSelectorService.SelectOwnerResponse.class);

                    if (!rsp.isLocalHostOwner) {
                        sendSelfCancellationPatch(task, "Local node is no longer owner for this factory.");
                        return;
                    }

                    // Recursively call handleSynchronizeChildren without owner verification.
                    handleSynchronizeChildren(task, false);
                });

        getHost().selectOwner(task.nodeSelectorLink, task.factorySelfLink, selectOp);
        return true;
    }

    private void startOrSynchChildService(State task, Operation post) {
        try {
            getHost().startOrSynchService(post, task.factorySelfLink);
        } catch (Throwable e) {
            logSevere(e);
            post.fail(e);
        }
    }

    private Consumer<State> subStageSetter(SubStage subStage) {
        return taskState -> taskState.subStage = subStage;
    }
}
