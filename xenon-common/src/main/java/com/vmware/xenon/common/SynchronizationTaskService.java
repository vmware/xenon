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
import java.util.function.Supplier;

import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.services.common.NodeGroupService;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.ServiceUriPaths;
import com.vmware.xenon.services.common.TaskService;

/**
 * A Task service used to synchronize child Services for a specific FactoryService.
 */
public class SynchronizationTaskService
        extends TaskService<SynchronizationTaskService.State> {

    public static final String FACTORY_LINK = ServiceUriPaths.SYNCHRONIZATION_TASKS;
    public static final String PROPERTY_NAME_SYNCHRONIZATION_LOGGING = Utils.PROPERTY_NAME_PREFIX
            + "SynchronizationTaskService.isDetailedLoggingEnabled";

    public static SynchronizationTaskService create(Supplier<Service> childServiceInstantiator) {
        if (childServiceInstantiator.get() == null) {
            throw new IllegalArgumentException("childServiceInstantiator created null child service");
        }
        SynchronizationTaskService taskService = new SynchronizationTaskService();
        taskService.childServiceInstantiator = childServiceInstantiator;
        return taskService;
    }

    public enum SubStage {
        QUERY, SYNCHRONIZE, RESTART
    }

    public static class State extends TaskService.TaskServiceState {
        /**
         * SelfLink of the FactoryService that will be synchronized by this task.
         * This value is immutable and gets set once in handleStart.
         */
        public String factorySelfLink;

        /**
         * documentKind of childServices created by the FactoryService.
         * This value is immutable and gets set once in handleStart.
         */
        public String factoryStateKind;

        /**
         * The node-selector used linked to the FactoryService.
         * This value is immutable and gets set once in handleStart.
         */
        public String nodeSelectorLink;

        /**
         * ServiceOptions supported by the child service.
         * This value is immutable and gets set once in handleStart.
         */
        public EnumSet<ServiceOption> childOptions;

        /**
         * Upper limit to the number of results per page of the broadcast query task.
         */
        public int queryResultLimit;

        /**
         * The last known membershipUpdateTimeMicros that kicked-off this
         * synchronization task.
         */
        public Long membershipUpdateTimeMicros;

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

    private Supplier<Service> childServiceInstantiator;

    private final boolean isDetailedLoggingEnabled = Boolean
            .getBoolean(PROPERTY_NAME_SYNCHRONIZATION_LOGGING);

    public SynchronizationTaskService() {
        super(State.class);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }

    /**
     * Each synchronization-task gets created once when the FactoryService starts.
     * The FactoryService starts the task through startService since it has to
     * set the instantiator lambda. Because of these details, handleStart only performs
     * some validation and creates a placeholder task without kicking-off the state
     * machine.
     * The state-machine of the synchronization-task actually gets started
     * on following POST requests that are served by handlePut.
     */
    @Override
    public void handleStart(Operation post) {
        State initialState = validateStartPost(post);
        if (initialState == null) {
            return;
        }

        initializeState(initialState, post);

        if (this.isDetailedLoggingEnabled) {
            logInfo("Creating synchronization-task for factory %s",
                    initialState.factorySelfLink);
        }

        post.setBody(initialState)
                .setStatusCode(Operation.STATUS_CODE_ACCEPTED)
                .complete();
    }

    @Override
    protected void initializeState(State initialState, Operation post) {
        // Initializing internal fields only. Note that the task is initially created
        // in the CREATED stage. This is because, handleStart only creates a
        // place-holder task per factoryService without actually kicking-off
        // the state-machine.
        initialState.taskInfo = new TaskState();
        initialState.taskInfo.stage = TaskState.TaskStage.CREATED;
        initialState.childOptions = this.childServiceInstantiator.get().getOptions();
        initialState.documentExpirationTimeMicros = Long.MAX_VALUE;
    }

    @Override
    protected State validateStartPost(Operation post) {
        State task = super.validateStartPost(post);
        if (task == null) {
            return null;
        }
        if (this.childServiceInstantiator == null) {
            post.fail(new IllegalArgumentException("childServiceInstantiator must be set."));
            return null;
        }
        if (task.factorySelfLink == null) {
            post.fail(new IllegalArgumentException("factorySelfLink must be set."));
            return null;
        }
        if (task.factoryStateKind == null) {
            post.fail(new IllegalArgumentException("factoryStateKind must be set."));
            return null;
        }
        if (task.nodeSelectorLink == null) {
            post.fail(new IllegalArgumentException("nodeSelectorLink must be set."));
            return null;
        }
        if (task.queryResultLimit <= 0) {
            post.fail(new IllegalArgumentException("queryResultLimit must be set."));
            return null;
        }
        if (task.taskInfo != null && task.taskInfo.stage != TaskState.TaskStage.CREATED) {
            post.fail(new IllegalArgumentException("taskInfo.stage must be set to CREATED."));
            return null;
        }
        if (task.childOptions != null) {
            post.fail(new IllegalArgumentException("childOptions must not be set."));
            return null;
        }
        if (task.membershipUpdateTimeMicros != null) {
            post.fail(new IllegalArgumentException("membershipUpdateTimeMicros must not be set."));
            return null;
        }
        if (task.subStage != null) {
            post.fail(new IllegalArgumentException("subStage must not be set."));
            return null;
        }
        if (task.queryPageReference != null) {
            post.fail(new IllegalArgumentException("queryPageReference must not be set."));
            return null;
        }
        return task;
    }

    /**
     * Synchronization-Task uses IDEMPOTENT_POST serviceOption. Once the place-holder
     * task is created through handleStart all POST requests get converted to PUT
     * operations and are served by handlePut. handlePut verifies the current state
     * of the task and if appropriate kicks-off the task state-machine.
     */
    @Override
    public void handlePut(Operation put) {
        // Fail the request if this was not a POST converted to PUT.
        if (!put.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_POST_TO_PUT)) {
            put.fail(new IllegalStateException(
                    "PUT not supported for SynchronizationTaskService"));
            return;
        }

        State task = getState(put);

        TaskState.TaskStage currentStage = task.taskInfo.stage;
        SubStage currentSubStage = task.subStage;

        State body = validatePutRequest(task, put);
        if (body == null) {
            return;
        }

        boolean startStateMachine = false;

        switch (task.taskInfo.stage) {
        case CREATED:
            // A synch-task is in CREATED state ONLY
            // if it just got created by the FactoryService
            // at startup time. Since handleStart does not
            // start the task state-machine, we do that here.
            startStateMachine = true;
            break;
        case STARTED:
            // Task is already running. Set the substage
            // of the task to RESTART, so that the executing
            // thread resets the state-machine back to stage 1
            // i.e. QUERY.
            logInfo("Restarting SynchronizationTask");
            task.subStage = SubStage.RESTART;
            break;
        case FAILED:
        case CANCELLED:
        case FINISHED:
            // Task had previously finished processing. Set the
            // taskStage back to STARTED, to restart the state-machine.
            startStateMachine = true;
            break;
        default:
            break;
        }

        // We only set properties that are mutable.
        // See documentation above for State class.
        task.membershipUpdateTimeMicros = body.membershipUpdateTimeMicros;
        task.queryResultLimit = body.queryResultLimit;
        if (startStateMachine) {
            task.taskInfo.stage = TaskState.TaskStage.STARTED;
            task.subStage = SubStage.QUERY;
        }

        if (this.isDetailedLoggingEnabled) {
            logInfo("Transitioning task from %s-%s to %s-%s. Time %d",
                    currentStage, currentSubStage, task.taskInfo.stage,
                    task.subStage, task.membershipUpdateTimeMicros);
        }

        if (startStateMachine) {
            // The synch-task makes sure that at any given time, there
            // is only one active execution of the task per factory.
            // Since this is where the state-machine starts,
            // we set the factory to un-available. This could be
            // redundant since the FactoryService may already have
            // changed the status to un-available, but just for
            // correctness we do it here again.
            setFactoryAvailability(task, false, (o) -> handleSubStage(task), put);
        } else {
            put.complete();
        }
    }

    public State validatePutRequest(State currentTask, Operation put) {
        State putTask = getBody(put);
        if (putTask == null) {
            put.fail(new IllegalArgumentException("Request contains empty body"));
            return null;
        }
        if (putTask.queryResultLimit <= 0) {
            put.fail(new IllegalArgumentException("queryResultLimit must be set."));
            return null;
        }
        boolean isMembershipTimeSet = (putTask.membershipUpdateTimeMicros != null);
        boolean hasReplicationOption = currentTask.childOptions.contains(ServiceOption.REPLICATION);
        if (!isMembershipTimeSet && hasReplicationOption || isMembershipTimeSet && !hasReplicationOption) {
            put.fail(new IllegalArgumentException("membershipUpdateTimeMicros not set correctly"));
            return null;
        }
        if (currentTask.childOptions.contains(ServiceOption.ON_DEMAND_LOAD)) {
            put.fail(new IllegalArgumentException("ON_DEMAND_LOAD services must synchronize on-demand."));
            return null;
        }
        if (currentTask.membershipUpdateTimeMicros != null &&
                currentTask.membershipUpdateTimeMicros > putTask.membershipUpdateTimeMicros) {
            // This request could be for an older node-group change notification.
            // If so, don't bother restarting synchronization.
            String msg = String.format(
                    "Passed membershipUpdateTimeMicros is outdated. Passed %d, Current %d",
                    putTask.membershipUpdateTimeMicros, currentTask.membershipUpdateTimeMicros);
            Exception e = new IllegalArgumentException(msg);

            ServiceErrorResponse rsp = Utils.toServiceErrorResponse(e);
            rsp.setInternalErrorCode(ServiceErrorResponse.ERROR_CODE_OUTDATED_SYNCH_REQUEST);

            // Another corner case, if this was an outdated synch request and the task
            // is not running anymore, we set the factory as Available. If the task
            // was already running then the factory would become Available as soon
            // as the task reached the FINISHED stage.
            if (!TaskState.isInProgress(currentTask.taskInfo)) {
                setFactoryAvailability(currentTask, true,
                        (o) -> put.fail(Operation.STATUS_CODE_BAD_REQUEST, e, rsp), null);
            } else {
                put.fail(Operation.STATUS_CODE_BAD_REQUEST, e, rsp);
            }
            return null;
        }
        return putTask;
    }

    /**
     * Synchronization-Task self-patches as it progress through the
     * state-machine. handlePatch checks for state transitions and
     * invokes the correct behavior given the task's next stage.
     */
    @Override
    public void handlePatch(Operation patch) {
        State task = getState(patch);
        State body = getBody(patch);

        if (!validateTransition(patch, task, body)) {
            return;
        }

        TaskState.TaskStage currentStage = task.taskInfo.stage;
        SubStage currentSubStage = task.subStage;

        if (task.subStage == SubStage.RESTART) {
            // Synchronization-tasks can get preempted because of a newer
            // node-group change event. When this happens, handlePut sets
            // the task's stage to RESTART. In this case, we reset the task
            // back to QUERY stage.
            task.taskInfo.stage = TaskState.TaskStage.STARTED;
            task.subStage = SubStage.QUERY;
        } else {
            updateState(task, body);
        }

        if (this.isDetailedLoggingEnabled) {
            logInfo("Transitioning task from %s-%s to %s-%s",
                    currentStage, currentSubStage, task.taskInfo.stage, task.subStage);
        }

        boolean isTaskFinished = !TaskState.isInProgress(task.taskInfo);
        if (isTaskFinished) {
            // Since the synch-task finished (regardless of failure), we will
            // mark the factory as available here. Complete the patch *after* we set availability
            // to avoid races with other self patches
            setFactoryAvailability(task, true, null, patch);
        } else {
            patch.complete();
        }

        switch (task.taskInfo.stage) {
        case STARTED:
            handleSubStage(task);
            break;
        case CANCELLED:
            logInfo("Task canceled: not implemented, ignoring");
            break;
        case FINISHED:
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
        case QUERY:
            handleQueryStage(task);
            break;
        case SYNCHRONIZE:
            handleSynchronizeStage(task, true);
            break;
        default:
            logWarning("Unexpected sub stage: %s", task.subStage);
            break;
        }
    }

    private void handleQueryStage(State task) {
        QueryTask queryTask = buildChildQueryTask(task);
        Operation queryPost = Operation
                .createPost(this, ServiceUriPaths.CORE_QUERY_TASKS)
                .setBody(queryTask)
                .setConnectionSharing(true)
                .setExpiration(
                        Utils.fromNowMicrosUtc(NodeGroupService.PEER_REQUEST_TIMEOUT_MICROS))
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

                    // Query returned zero results.Self-patch the task
                    // to FINISHED state.
                    if (rsp == null || rsp.nextPageLink == null) {
                        sendSelfFinishedPatch(task);
                        return;
                    }

                    URI queryTaskUri = UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_QUERY_TASKS);
                    task.queryPageReference = UriUtils.buildUri(queryTaskUri, rsp.nextPageLink);

                    sendSelfPatch(task, TaskState.TaskStage.STARTED,
                            subStageSetter(SubStage.SYNCHRONIZE));
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
        queryTask.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(timeoutMicros);

        // Make this a broadcast query so that we get child services from all peer nodes.
        queryTask.querySpec.options = EnumSet.of(
                QueryTask.QuerySpecification.QueryOption.BROADCAST);

        // Set the node-selector link.
        queryTask.nodeSelectorLink = task.nodeSelectorLink;

        // process child services in limited numbers, set query result limit
        queryTask.querySpec.resultLimit = task.queryResultLimit;

        return queryTask;
    }

    private void handleSynchronizeStage(State task, boolean verifyOwnership) {
        if (task.queryPageReference == null) {
            sendSelfFinishedPatch(task);
            return;
        }

        if (getHost().isStopping()) {
            sendSelfCancellationPatch(task, "host is stopping");
            return;
        }

        if (verifyOwnership && verifySynchronizationOwnership(task)) {
            // Verifying ownership will recursively call into
            // handleSynchronizationStage with verifyOwnership set to false.
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

        sendRequest(Operation.createGet(task.queryPageReference)
                .setExpiration(
                        Utils.fromNowMicrosUtc(NodeGroupService.PEER_REQUEST_TIMEOUT_MICROS))
                .setCompletion(c));
    }

    private void synchronizeChildrenInQueryPage(State task, ServiceDocumentQueryResult rsp) {
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
            sendSelfPatch(task, TaskState.TaskStage.STARTED, subStageSetter(SubStage.SYNCHRONIZE));
        };

        for (String link : rsp.documentLinks) {
            if (getHost().isStopping()) {
                sendSelfCancellationPatch(task, "host is stopping");
                return;
            }

            synchronizeService(task, link, c);
        }
    }

    private boolean verifySynchronizationOwnership(State task) {
        // If this is not a REPLICATED factory, we don't
        // bother verifying ownership.
        if (!task.childOptions.contains(ServiceOption.REPLICATION)) {
            return false;
        }

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
                        logWarning("Current node %s is no longer owner for the factory %s. Cancelling synchronization",
                                this.getHost().getId(), task.factorySelfLink);

                        sendSelfCancellationPatch(task, "Local node is no longer owner for this factory.");
                        return;
                    }

                    // Recursively call handleSynchronizeStage
                    // without owner verification.
                    handleSynchronizeStage(task, false);
                });

        getHost().selectOwner(task.nodeSelectorLink, task.factorySelfLink, selectOp);
        return true;
    }

    private void synchronizeService(State task, String link, Operation.CompletionHandler c) {
        // To trigger synchronization of the child-service, we make
        // a SYNCH-OWNER request. The request body is an empty document
        // with just the documentSelfLink property set to the link
        // of the child-service. This is done so that the FactoryService
        // routes the request to the DOCUMENT_OWNER.
        ServiceDocument d = new ServiceDocument();
        d.documentSelfLink = UriUtils.getLastPathSegment(link);

        // Because the synchronization process is kicked-in when the
        // node-group is going through changes, we explicitly set
        // retryCount to 0, to avoid retrying on a node that is actually
        // down. Not doing so will cause un-necessary operation-tracking
        // that gets worse in conditions under heavy load.
        Operation synchRequest = Operation.createPost(this, task.factorySelfLink)
                .setBody(d)
                .setCompletion(c)
                .setReferer(getUri())
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER)
                .setExpiration(
                        Utils.fromNowMicrosUtc(NodeGroupService.PEER_REQUEST_TIMEOUT_MICROS))
                .setRetryCount(0);
        try {
            sendRequest(synchRequest);
        } catch (Throwable e) {
            logSevere(e);
            synchRequest.fail(e);
        }
    }

    private void setFactoryAvailability(
            State task, boolean isAvailable, Consumer<Operation> action, Operation parentOp) {
        ServiceStats.ServiceStat body = new ServiceStats.ServiceStat();
        body.name = Service.STAT_NAME_AVAILABLE;
        body.latestValue = isAvailable ? STAT_VALUE_TRUE : STAT_VALUE_FALSE;

        Operation put = Operation.createPut(
                UriUtils.buildAvailableUri(this.getHost(), task.factorySelfLink))
                .setBody(body)
                .setCompletion((o, e) -> {
                    if (parentOp != null) {
                        parentOp.complete();
                    }
                    if (e != null) {
                        logSevere("Setting factory availability failed with error %s", e.getMessage());
                        sendSelfFailurePatch(task, "Failed to set Factory Availability");
                        return;
                    }
                    if (action != null) {
                        action.accept(o);
                    }
                });
        sendRequest(put);
    }

    private Consumer<State> subStageSetter(SubStage subStage) {
        return taskState -> taskState.subStage = subStage;
    }
}
