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
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.vmware.xenon.common.NodeSelectorService.SelectOwnerResponse;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.config.XenonConfiguration;
import com.vmware.xenon.services.common.CheckpointService;
import com.vmware.xenon.services.common.NodeGroupBroadcastResponse;
import com.vmware.xenon.services.common.QueryTask;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;
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

    public static final String STAT_NAME_CHILD_SYNCH_RETRY_COUNT = "childSynchRetryCount";
    public static final String STAT_NAME_SYNCH_RETRY_COUNT = "synchRetryCount";

    public static final String PROPERTY_NAME_MAX_CHILD_SYNCH_RETRY_COUNT =
            Utils.PROPERTY_NAME_PREFIX + "SynchronizationTaskService.MAX_CHILD_SYNCH_RETRY_COUNT";

    /**
     * Maximum synch-task retry limit.
     * We are using exponential backoff for synchronization retry, that means last synch retry will
     * be tried after 2 ^ 8 * getMaintenanceIntervalMicros(), which is ~4 minutes if maintenance interval is 1 second.
     */
    public static final int MAX_CHILD_SYNCH_RETRY_COUNT = Integer.getInteger(
            PROPERTY_NAME_MAX_CHILD_SYNCH_RETRY_COUNT, 8);


    public static SynchronizationTaskService create(Supplier<Service> childServiceInstantiator) {
        if (childServiceInstantiator.get() == null) {
            throw new IllegalArgumentException("childServiceInstantiator created null child service");
        }
        SynchronizationTaskService taskService = new SynchronizationTaskService();
        taskService.childServiceInstantiator = childServiceInstantiator;
        return taskService;
    }

    public enum SubStage {
        GET_CHECKPOINTS, QUERY, SYNCHRONIZE, RESTART, CHECK_NG_AVAILABILITY
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
         * Document index link used by the child service
         */
        public String childDocumentIndexLink;

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
         * start time of the synchronization state machine
         */
        public Long startTimeMicros;

        /**
         * The last known time that peer synced given factory and node group
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long checkpoint;

        /**
         * The nodes that have responded to a checkpoint GET request.
         * The next checkpoint is going to be broadcasted only to those
         * nodes, as only those nodes are guaranteed to have the documents
         * prior to the existing checkpoint.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Collection<String> checkpointNodes;

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

        /**
         * Number of child services for which synchronization is completed.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public int synchCompletionCount;
    }

    private Supplier<Service> childServiceInstantiator;

    private FactoryService parent;

    private final boolean isDetailedLoggingEnabled = Boolean
            .getBoolean(PROPERTY_NAME_SYNCHRONIZATION_LOGGING);

    private final boolean isCheckpointEnabled = XenonConfiguration.bool(
            SynchronizationTaskService.class,
            "isCheckpointEnabled",
            true
    );

    private final long schedulePeriodSeconds = XenonConfiguration.number(
            SynchronizationTaskService.class,
            "schedulePeriodSeconds",
            TimeUnit.MINUTES.toSeconds(30)
    );

    public SynchronizationTaskService() {
        super(State.class);
        toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        toggleOption(ServiceOption.INSTRUMENTATION, true);
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
        Service childTemplate = this.childServiceInstantiator.get();
        initialState.taskInfo = new TaskState();
        initialState.taskInfo.stage = TaskState.TaskStage.CREATED;
        initialState.childOptions = childTemplate.getOptions();
        initialState.childDocumentIndexLink = childTemplate.getDocumentIndexPath();
        initialState.documentExpirationTimeMicros = Long.MAX_VALUE;
        initialState.checkpoint = 0L;
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
            if (this.parent != null && this.parent.hasChildOption(ServiceOption.PERSISTENCE) &&
                    checkpointEnabled(task)) {
                task.subStage = SubStage.GET_CHECKPOINTS;
            } else {
                task.subStage = SubStage.QUERY;
                task.checkpoint = 0L;
            }
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
            task.startTimeMicros = Utils.getNowMicrosUtc();
            task.synchCompletionCount = 0;
            setStat(STAT_NAME_CHILD_SYNCH_RETRY_COUNT, 0);
            setStat(STAT_NAME_CHILD_SYNCH_FAILURE_COUNT, 0);
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
            put.fail(new IllegalArgumentException("membershipUpdateTimeMicros not set correctly: "
                    + putTask.membershipUpdateTimeMicros));
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
            if (TaskState.isFinished(currentTask.taskInfo)) {
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
     * Validate that the PATCH we got requests reasonable changes to our state.
     */
    @Override
    protected boolean validateTransition(
            Operation patch, SynchronizationTaskService.State currentTask, SynchronizationTaskService.State patchBody) {
        boolean validTransition = super.validateTransition(patch, currentTask, patchBody);
        if (!validTransition) {
            return false;
        }

        if (!TaskState.isInProgress(currentTask.taskInfo) && !TaskState.isInProgress(patchBody.taskInfo)) {
            patch.fail(new IllegalArgumentException("Task stage cannot transitioned to same stopped state"));
            return false;
        }

        return true;
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
            if (this.parent != null && this.parent.hasChildOption(ServiceOption.PERSISTENCE) &&
                    checkpointEnabled(task)) {
                task.subStage = SubStage.GET_CHECKPOINTS;
            } else {
                task.subStage = SubStage.QUERY;
                task.checkpoint = 0L;
            }
            task.synchCompletionCount = 0;
            setStat(STAT_NAME_CHILD_SYNCH_RETRY_COUNT, 0);
            setStat(STAT_NAME_CHILD_SYNCH_FAILURE_COUNT, 0);
        } else {
            updateState(task, body);
        }

        logInfo("Transitioning task from %s-%s to %s-%s, Services synchronized: %d",
                currentStage, currentSubStage, task.taskInfo.stage, task.subStage, task.synchCompletionCount);

        boolean isTaskFinished = TaskState.isFinished(task.taskInfo);
        if (isTaskFinished) {
            // Since the synch-task finished, we will mark the factory
            // as available here. Complete the patch *after* we set availability
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
        case GET_CHECKPOINTS:
            handleCheckpointStage(task);
            break;
        case QUERY:
            handleQueryStage(task);
            break;
        case SYNCHRONIZE:
            handleSynchronizeStage(task, true);
            break;
        case CHECK_NG_AVAILABILITY:
            handleCheckNodeGroupAvailabilityStage(task);
            break;
        default:
            logWarning("Unexpected sub stage: %s", task.subStage);
            break;
        }
    }

    private void handleCheckpointStage(State task) {
        String checkPointServiceLink = UriUtils.buildUriPath(
                CheckpointService.FACTORY_LINK, UriUtils.convertPathCharsFromLink(this.parent.getSelfLink()));;
        Operation get = Operation.createGet(UriUtils.buildUri(this.getHost(), checkPointServiceLink))
                .setReferer(this.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        logInfo("broadcast get checkpoints failed %s, starting synchronization from timestamp 0", e.toString());
                        task.checkpoint = 0L;
                        sendSelfPatch(task, TaskState.TaskStage.STARTED,
                                subStageSetter(SubStage.QUERY));
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody(NodeGroupBroadcastResponse.class);
                    if (!rsp.failures.isEmpty()) {
                        for (Map.Entry<URI, ServiceErrorResponse> failure : rsp.failures.entrySet()) {
                            // 404 may due to checkpoint is not created yet
                            if (failure.getValue().statusCode != Operation.STATUS_CODE_NOT_FOUND) {
                                logInfo("get checkpoint failed with status %d from %s", failure.getValue().errorCode, failure.getKey());
                            }
                        }
                        logInfo("starting synchronization from timestamp 0");
                        task.checkpoint = 0L;
                        sendSelfPatch(task, TaskState.TaskStage.STARTED,
                                subStageSetter(SubStage.QUERY));
                        return;
                    }
                    List<Long> checkPoints =
                            rsp.jsonResponses.values().stream().map(s -> {
                                CheckpointService.CheckpointState checkpointState =
                                        Utils.fromJson(s, CheckpointService.CheckpointState.class);
                                return checkpointState.timestamp;
                            }).collect(Collectors.toList());
                    task.checkpoint = findMinimumCheckpoint(checkPoints);
                    task.checkpointNodes = new HashSet<>(rsp.selectedNodes.keySet());
                    if (task.checkpoint > 0) {
                        logInfo("synch %s from check point %d",
                                task.factorySelfLink, task.checkpoint);
                    }
                    sendSelfPatch(task, TaskState.TaskStage.STARTED,
                            subStageSetter(SubStage.QUERY));
                });
        this.getHost().broadcastRequest(task.nodeSelectorLink, checkPointServiceLink, false, get);
    }

    private long findMinimumCheckpoint(List<Long> checkpoints) {
        long minimumCheckpoint = Long.MAX_VALUE;
        for (Long checkpoint : checkpoints) {
            minimumCheckpoint = Long.min(minimumCheckpoint, checkpoint);
        }
        return minimumCheckpoint;
    }

    private void handleQueryStage(State task) {
        QueryTask queryTask = buildChildQueryTask(task);
        Operation queryPost = Operation
                .createPost(this, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS)
                .setBody(queryTask)
                .setConnectionSharing(true)
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
                        sendSelfPatch(task, TaskState.TaskStage.STARTED,
                                subStageSetter(SubStage.CHECK_NG_AVAILABILITY));
                        return;
                    }

                    URI queryTaskUri = UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
                    task.queryPageReference = UriUtils.buildUri(queryTaskUri, rsp.nextPageLink);

                    sendSelfPatch(task, TaskState.TaskStage.STARTED,
                            subStageSetter(SubStage.SYNCHRONIZE));
                });

        sendRequest(queryPost);
    }

    private QueryTask buildChildQueryTask(State task) {
        QueryTask queryTask = new QueryTask();
        queryTask.querySpec = new QueryTask.QuerySpecification();
        queryTask.indexLink = task.childDocumentIndexLink;
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

        if (this.parent != null && this.parent.hasChildOption(ServiceOption.PERSISTENCE) &&
                checkpointEnabled(task)) {
            if (this.isDetailedLoggingEnabled) {
                logInfo("query %s from checkpoint %d", task.factorySelfLink, task.checkpoint);
            }

            QueryTask.NumericRange<Long> timeRange =
                    QueryTask.NumericRange.createLongRange(task.checkpoint, Long.MAX_VALUE,
                            false, true);

            QueryTask.Query timeClause = new QueryTask.Query()
                    .setTermPropertyName(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                    .setNumericRange(timeRange);
            queryTask.querySpec.query.addBooleanClause(timeClause);
        }

        // set timeout based on peer synchronization upper limit
        long timeoutMicros = TimeUnit.SECONDS.toMicros(
                getHost().getPeerSynchronizationTimeLimitSeconds());
        timeoutMicros = Math.max(timeoutMicros, getHost().getOperationTimeoutMicros());
        queryTask.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(timeoutMicros);

        // Make this a broadcast query so that we get child services from all peer nodes.
        queryTask.querySpec.options = EnumSet.of(QueryOption.BROADCAST, QueryOption.FORWARD_ONLY);

        // Set the node-selector link.
        queryTask.nodeSelectorLink = task.nodeSelectorLink;

        // process child services in limited numbers, set query result limit
        queryTask.querySpec.resultLimit = task.queryResultLimit;

        return queryTask;
    }

    private void handleSynchronizeStage(State task, boolean verifyOwnership) {
        if (task.queryPageReference == null) {
            sendSelfPatch(task, TaskState.TaskStage.STARTED, subStageSetter(SubStage.CHECK_NG_AVAILABILITY));
            return;
        }

        if (getHost().isStopping()) {
            sendSelfCancellationPatch(task, "host is stopping");
            return;
        }

        if (verifyOwnership && task.childOptions.contains(ServiceOption.REPLICATION)) {
            Consumer<SelectOwnerResponse> ownerHandler = (selectOwnerResponse) -> {
                handleSynchronizeStage(task, false);
            };
            Consumer<SelectOwnerResponse> nonOwnerHandler = (selectOwnerResponse) -> {
                logWarning("Current node %s is no longer owner for the factory %s. Cancelling synchronization",
                        this.getHost().getId(), task.factorySelfLink);
                sendSelfCancellationPatch(task, "Local node is no longer owner for this factory.");
            };
            Consumer<Throwable> failureHandler = (e) -> {
                sendSelfFailurePatch(task, e.getMessage());
            };

            verifySynchronizationOwnership(task, ownerHandler, nonOwnerHandler, failureHandler);
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

            // Delete the read page.
            // Since this is a broadcast query, deleting the result page cascade deletes query result pages.
            // Also, there will be no previous pages since FORWARD_ONLY option is enabled.
            Operation.createDelete(task.queryPageReference)
                    .setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION)
                    .setCompletion((op, ex) -> {
                        if (ex != null) {
                            logWarning("Failed to delete query result page %s: %s", rsp.documentSelfLink, Utils.toString(ex));
                        }
                    })
                    .sendWith(this);

            if (rsp.documentCount == 0 || rsp.documentLinks.isEmpty()) {
                sendSelfPatch(task, TaskState.TaskStage.STARTED, subStageSetter(SubStage.CHECK_NG_AVAILABILITY));
                return;
            }
            List<String> list = new ArrayList<>(rsp.documentLinks);
            synchronizeChildrenInQueryPage(task, rsp, list, 0, list.size());
        };

        sendRequest(Operation.createGet(task.queryPageReference)
                .setConnectionSharing(true)
                .setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION)
                .setRetryCount(3)
                .setCompletion(c));
    }

    private void synchronizeChildrenInQueryPage(State task, ServiceDocumentQueryResult rsp, List<String> documentLinks, int retryCount, int totalServiceCount) {
        if (getHost().isStopping()) {
            sendSelfCancellationPatch(task, "host is stopping");
            return;
        }

        // Keep track of failed services.
        List<String> failedServices = new ArrayList<>();

        // Track child service request in parallel, passing a single parent operation
        AtomicInteger pendingStarts = new AtomicInteger(documentLinks.size());

        Operation.CompletionHandler c = (o, e) -> {
            if (e != null && !getHost().isStopping()) {
                logWarning("Synchronization failed for service %s with status code %d, message %s",
                        o.getUri().getPath(), o.getStatusCode(), e.getMessage());
                if (o.getStatusCode() >= Operation.STATUS_CODE_SERVER_FAILURE_THRESHOLD ||
                        o.getStatusCode() == Operation.STATUS_CODE_TIMEOUT) {
                    synchronized (this) {
                        failedServices.add(o.getUri().getPath());
                    }
                }
            }

            // Wait for failedServices to be updated for all failed services before proceeding further.
            // Keeping this decrement statement here makes sure that we do not have race condition with failedServices.
            int r = pendingStarts.decrementAndGet();

            if (getHost().isStopping()) {
                sendSelfCancellationPatch(task, "host is stopping");
                return;
            }

            if (r != 0) {
                return;
            }

            // Retry synchronization for services failed to synch last time.
            // Only retry if failed services are less than the half of the total services and
            // maximum retry count is not reached.

            if (!failedServices.isEmpty()) {
                if (failedServices.size() <= task.queryResultLimit / 2) {
                    if (retryCount < MAX_CHILD_SYNCH_RETRY_COUNT) {
                        synchronized (this) {
                            if (!getHost().isStopping()) {
                                logWarning("Retrying synchronization for %d failed services", failedServices.size());

                                scheduleRetry(
                                        () -> synchronizeChildrenInQueryPage(
                                                task,
                                                rsp,
                                                failedServices,
                                                retryCount + 1,
                                                totalServiceCount),
                                        STAT_NAME_CHILD_SYNCH_RETRY_COUNT);
                                adjustStat(STAT_NAME_SYNCH_RETRY_COUNT, 1);
                            }

                            return;
                        }
                    } else {
                        if (!getHost().isStopping()) {
                            logSevere("Synchronization failed for %d services", failedServices.size());
                        }
                        adjustStat(STAT_NAME_CHILD_SYNCH_FAILURE_COUNT, failedServices.size());
                        task.synchCompletionCount += (totalServiceCount - failedServices.size());
                        sendSelfFailurePatch(task, "Too many retries in synchronizing child services");
                        return;
                    }
                } else {
                    // Just fail the synch-task since we go so many failures
                    adjustStat(STAT_NAME_CHILD_SYNCH_FAILURE_COUNT, failedServices.size());
                    task.synchCompletionCount += (totalServiceCount - failedServices.size());
                    sendSelfFailurePatch(task, "Too many failures in synchronizing child services");
                    return;
                }
            }

            setStat(STAT_NAME_CHILD_SYNCH_RETRY_COUNT, 0);
            task.queryPageReference = rsp.nextPageLink != null
                    ? UriUtils.buildUri(task.queryPageReference, rsp.nextPageLink)
                    : null;

            task.synchCompletionCount += totalServiceCount;

            if (task.queryPageReference == null) {
                sendSelfPatch(task, TaskState.TaskStage.STARTED, subStageSetter(SubStage.CHECK_NG_AVAILABILITY));
                return;
            }
            sendSelfPatch(task, TaskState.TaskStage.STARTED, subStageSetter(SubStage.SYNCHRONIZE));
        };

        for (String link : documentLinks) {
            if (getHost().isStopping()) {
                sendSelfCancellationPatch(task, "host is stopping");
                return;
            }

            synchronizeService(task, link, c);
        }
    }

    private void scheduleRetry(Runnable task, String statNameRetryCount) {
        adjustStat(statNameRetryCount, 1);
        ServiceStats.ServiceStat stat = getStat(statNameRetryCount);

        long retryCounter = 0;
        if (stat != null) {
            retryCounter = (long) stat.latestValue;
        }

        // Use exponential backoff algorithm in retry logic. The idea is to exponentially
        // increase the delay for each retry based on the number of previous retries.
        // This is done to reduce the load of retries on the system by all the tasks
        // at same time, and giving system more time to stabilize
        // in next retry then the previous retry.
        long delay = getExponentialDelay(statNameRetryCount);

        logWarning("%s: Scheduling retry #%d of task (counter:%s) in %d microseconds",
                getSelfLink(),
                retryCounter,
                statNameRetryCount,
                delay);

        getHost().scheduleCore(task, delay, TimeUnit.MICROSECONDS);
    }

    /**
     * Exponential backoff rely on retry count stat. If this stat is not available
     * then we will fall back to constant delay for each retry.
     * To get exponential delay, multiply retry count's power of 2 with constant delay.
     * @param statNameRetryCount
     */
    private long getExponentialDelay(String statNameRetryCount) {
        long delay = getHost().getMaintenanceIntervalMicros();
        ServiceStats.ServiceStat stat = getStat(statNameRetryCount);
        if (stat != null && stat.latestValue > 0) {
            return (1 << ((long)stat.latestValue)) * delay;
        }

        return delay;
    }

    private void verifySynchronizationOwnership(State task, Consumer<SelectOwnerResponse> ownerHandler,
            Consumer<SelectOwnerResponse> nonOwnerHandler, Consumer<Throwable> failureHandler) {
        Operation selectOp = Operation
                .createPost(null)
                .setExpiration(task.documentExpirationTimeMicros)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        if (failureHandler != null) {
                            failureHandler.accept(e);
                        }
                        return;
                    }

                    SelectOwnerResponse rsp = o.getBody(SelectOwnerResponse.class);

                    if (!rsp.isLocalHostOwner) {
                        if (nonOwnerHandler != null) {
                            nonOwnerHandler.accept(rsp);
                        }
                        return;
                    }

                    if (ownerHandler != null) {
                        ownerHandler.accept(rsp);
                    }
                });

        getHost().selectOwner(task.nodeSelectorLink, task.factorySelfLink, selectOp);
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
                .setConnectionSharing(true)
                .setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_SYNCH_OWNER)
                .setRetryCount(0);
        try {
            sendRequest(synchRequest);
        } catch (Exception e) {
            logSevere(e);
            synchRequest.fail(e);
        }
    }

    private void handleCheckNodeGroupAvailabilityStage(State task) {
        // get node selector state
        Operation getNodeSelectorStateOp = Operation
                .createGet(getHost(), task.nodeSelectorLink)
                .setCompletion((o, e) -> {
                    if (e != null || !o.hasBody()) {
                        sendSelfFailurePatch(task, "failed to get node selector state");
                        return;
                    }

                    NodeSelectorState nsState = o.getBody(NodeSelectorState.class);

                    // check for node group availability
                    if (!NodeSelectorState.isAvailable(nsState)) {
                        // node group is not available - failing the task to
                        // prevent factory from being marked available
                        sendSelfFailurePatch(task, "node group is not available");
                        return;
                    }

                    if (this.parent != null && this.parent.hasChildOption(ServiceOption.PERSISTENCE)
                            && checkpointEnabled(task)) {
                        Consumer<SelectOwnerResponse> ownerHandler = (selectOwnerResponse) -> {
                            if (selectOwnerResponse.availableNodeCount > 1) {
                                createCheckpointsAndReschedule(task);
                            } else {
                                sendSelfFinishedPatch(task);
                            }
                        };
                        Consumer<SelectOwnerResponse> nonOwnerHandler = (selectOwnerResponse) -> {
                            sendSelfFinishedPatch(task);
                        };
                        Consumer<Throwable> failureHandler = (ex) -> {
                            sendSelfFinishedPatch(task);
                        };
                        verifySynchronizationOwnership(task, ownerHandler, nonOwnerHandler, failureHandler);
                        return;
                    }

                    sendSelfFinishedPatch(task);
                });
        sendRequest(getNodeSelectorStateOp);
    }

    private void createCheckpointsAndReschedule(State task) {
        CheckpointService.CheckpointState s = new CheckpointService.CheckpointState();
        s.timestamp = task.startTimeMicros;
        s.factoryLink = this.parent.getSelfLink();

        logInfo("Creating checkpoints for factory %s with timestamp: %d", s.factoryLink, s.timestamp);
        Operation post = Operation.createPost(UriUtils.buildUri(this.getHost(), CheckpointService.FACTORY_LINK))
                .setBody(s)
                .setReferer(this.getUri())
                .setCompletion((op, ex) -> {
                    getHost().scheduleCore(() -> {
                        SynchronizationTaskService.State scheduleTask = this.parent.createSynchronizationTaskState(
                                task.membershipUpdateTimeMicros);
                        Operation.createPost(this, ServiceUriPaths.SYNCHRONIZATION_TASKS)
                                .setBody(scheduleTask)
                                .sendWith(this);
                    }, this.schedulePeriodSeconds, TimeUnit.SECONDS);

                    sendSelfFinishedPatch(task);
                });
        this.getHost().broadcastRequest(this.parent.getPeerNodeSelectorPath(), CheckpointService.FACTORY_LINK,
                false, post, task.checkpointNodes);
    }

    private void setFactoryAvailability(
            State task, boolean isAvailable, Consumer<Operation> action, Operation parentOp) {
        ServiceStats.ServiceStat body = new ServiceStats.ServiceStat();
        body.name = Service.STAT_NAME_AVAILABLE;
        body.latestValue = isAvailable ? STAT_VALUE_TRUE : STAT_VALUE_FALSE;

        Operation put = Operation.createPut(
                UriUtils.buildAvailableUri(this.getHost(), task.factorySelfLink))
                .setBody(body)
                .setConnectionSharing(true)
                .setConnectionTag(ServiceClient.CONNECTION_TAG_SYNCHRONIZATION)
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

    public void setParentService(FactoryService factoryService) {
        this.parent = factoryService;
    }

    @Override
    protected void sendSelfPatch(State taskState, TaskState.TaskStage stage, Consumer<State> updateTaskState) {
        taskState.failureMessage = "";
        super.sendSelfPatch(taskState, stage, updateTaskState);
    }

    private Consumer<State> subStageSetter(SubStage subStage) {
        return taskState -> taskState.subStage = subStage;
    }

    private boolean checkpointEnabled(State task) {
        return this.isCheckpointEnabled &&
                !ServiceUriPaths.CORE_IN_MEMORY_DOCUMENT_INDEX.equals(task.childDocumentIndexLink);
    }
}
