/*
 * Copyright (c) 2014-2017 VMware, Inc. All Rights Reserved.
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

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import static com.vmware.xenon.common.TaskState.TaskStage.CANCELLED;
import static com.vmware.xenon.common.TaskState.TaskStage.FAILED;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.CURRENT_BATCH_FINISHED_WITH_SUCCESS;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.MIGRATE_CURRENT_BATCH;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.MOVE_TO_NEXT_BATCH;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.POST_BATCH_COMPLITION;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.PREPARE_CURRENT_BATCH;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.WAITING_BATCH_TO_FINISH;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.MigrationTaskService.MigrationOption;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.MigrationState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.MigrationState.MigrationRequest;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.MigrationState.ResultReport;

/**
 *
 */
public class NodeGroupMigrationTaskService extends TaskService<MigrationState> {

    public static final String FACTORY_LINK = ServiceUriPaths.NODE_GROUP_MIGRATION_TASKS;

    /**
     * {@code TaskStage}s that represent something bad happened and the migration wasn't
     * successful.
     */
    private static final Set<TaskStage> ERROR_STAGES = EnumSet.of(CANCELLED, FAILED);

    private static final long MIGRATION_TASK_DURATION_MICROS = TimeUnit.MINUTES.toMicros(120);
    private static final int MIGRATION_BATCH_SIZE = 25000;
    private static final int MAX_RETRIES = 5;

    private static Predicate<TaskStage> isMigrationTaskInFinalState = taskStage ->
            EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED).contains(taskStage);

    private static Predicate<TaskStage> isMigrationTaskFailed = taskStage ->
            EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED).contains(taskStage);


    /**
     * These substages are for tracking the stages unique to our task service. They are only
     * relevant to the STARTED TaskStage.
     */
    public enum SubStage {

        INITIALIZING,

        PREPARE_CURRENT_BATCH,
        /**
         * Starts one {@link MigrationTaskService} for the current index in {@link State#factoryLinks}.
         */
        MIGRATE_CURRENT_BATCH,

        WAITING_BATCH_TO_FINISH,
        CURRENT_BATCH_FINISHED_WITH_SUCCESS,
        CURRENT_BATCH_FINISHED_WITH_FAILURE,

        /**
         * Retries the migration operation for the current index in {@link
         * State#factoryLinks}. This stage is triggered if any of the migration
         * tasks in the current batch fails.
         */
        RETRY_CURRENT_BATCH,

        /**
         * A migration task has completed. Once this SubStage occurs {@link
         * State#factoryLinks}{@code .size()} times... then we are done!.
         */
        MIGRATION_TASK_COMPLETED,

        /**
         * Cleanup all subscriptions.
         */
        CLEANUP_SUBSCRIPTIONS,
        POST_BATCH_COMPLITION,
        MOVE_TO_NEXT_BATCH,
    }


    // TODO: maybe rename to NodeGroupMigrationState
    public static class MigrationState extends TaskService.TaskServiceState {

        public static class MigrationRequest {
            public String factoryLink;
            public MigrationTaskService.State request;
        }

        /**
         * Values that have calculated while performing the task
         */
        public static class RuntimeContext {
            public List<MigrationTaskService.State> generatedMigrationRequests = new ArrayList<>();

            //        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
            /**
             * Paths of created migration tasks. Index corresponds to the batch list.
             */
            public Set<String> migrationTaskPaths = new HashSet<>();  // TODO: convert to map(destFactoryLink, taskPath)

            public Set<URI> subscriptionUris = new HashSet<>();
            public Map<String, MigrationTaskService.State> resultByFactoryPath = new HashMap<>();

            public int retryCount = 0;

            /**
             * The current batch index we are processing
             */
            public int batchIndex = 0;

            public void reset() {
                this.generatedMigrationRequests.clear();
                this.migrationTaskPaths.clear();
                this.subscriptionUris.clear();
                this.resultByFactoryPath.clear();
                this.retryCount = 0;
                // batchIndex will NOT reset
            }
        }


        public static class ResultReport {
            public int batchIndex;
            public int executionIndex;  // TODO: retryCount
            public MigrationTaskService.State request;
            public MigrationTaskService.State response;
            public TaskStage resultState;
            public String taskPath;
            public String factoryPath;
        }

        /**
         * The current substage.
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public SubStage subStage;

        /**
         * The source node-group URL to use for migrating state from during an app migration.
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI sourceNodeReference;

        public String sourceNodeGroupPath;

        /**
         * The destination (or target) node-group URL to use to migrate state to during an app migration.
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI destinationNodeReference;

        public String destinationNodeGroupPath;


        public String migrationTaskPath = ServiceUriPaths.MIGRATION_TASKS;

        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long operationTimeoutMicros;

        public List<List<MigrationRequest>> batches = new ArrayList<>();


        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public int maxRetry = 0;

        public RuntimeContext runtime = new RuntimeContext();

        // something solved in runtime TODO: move to runtime context

        public List<URI> sourceReferences;
        public List<URI> destinationReferences;


        public List<ResultReport> results = new ArrayList<>();


//
//
//        /** The factories (and their associated transformation service path) to use during an upgrade. */
//        public List<Set<FactoryAndTransform>> factoryLinks;
//
//        /** The current index within the current batch we are processing in {@link #factoryLinks}. */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        public Integer currentFactoryIndex;
//
//
//        /** The number of retry attempts on failure */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SERVICE_USE)
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        public Integer retryCount;
//
//        /** flag to indicate if the current stage failed */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SERVICE_USE)
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        public Boolean stageFailed;
//
//        /**
//         * The factory link that was last finished, as detected by {@link
//         * #subscribeToMigrationTask(String, MigrationTaskService.State)}
//         */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SERVICE_USE)
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        public String migrationTaskFinishedFactoryLink;
//
//        /**
//         * The Migration Task URI that was last finished, as detected by {@link
//         * #subscribeToMigrationTask(String, MigrationTaskService.State)}.
//         */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SERVICE_USE)
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        public String migrationTaskFinishedURI;
//
//        /**
//         * Timestamp of the document that is guaranteed to be migrated for the factory
//         */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SERVICE_USE)
//        public Long migrationStartTimeMicros;
//
        /**
         * Stores the subscription URIs associated with the migration tasks. Used to delete
         * subscriptions after migration was successful.
         */
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
//        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.SERVICE_USE)
//        public Set<URI> subscriptionUris = new HashSet<>();


        public int numOfMigrationFinishedInCurrentBatch = 0;
//
//        /** the max clock skew between xenon nodes */
//        public long allowedClockSkewMillis = TimeUnit.MINUTES.toMillis(5);
//
//        /** Overriden {@code toString()} makes for easier and better log messages. */
//        @Override
//        public String toString() {
//            String taskStage = taskInfo != null ? taskInfo.stage.toString() : null;
//            String subStage = this.subStage != null ? this.subStage.toString() : null;
//            List<Set<FactoryAndTransform>> factoryLinksToMigrate = this.factoryLinks != null ?
//                    this.factoryLinks : Collections.emptyList();
//
//            return String
//                    .format("UpgradeTaskState: [taskStage=%s] [subStage=%s]" +
//                                    "[sourceNodeGroupReference=%s]%n[factoryLinks=%s]%n" +
//                                    "[currentFactoryBatchIndex=%s]%n[currentFactoryIndex=%s]",
//                            taskStage, subStage, this.sourceNodeGroupReference,
//                            factoryLinksToMigrate, currentFactoryBatchIndex, currentFactoryIndex);
//        }
    }

    static class MigrationFinishedNotice extends ServiceDocument {
        static final String KIND = Utils.buildKind(MigrationFinishedNotice.class);
        public String migrationTaskPath;
        public int currentBatchIndex;

        public MigrationFinishedNotice() {
            this.documentKind = KIND;
        }
    }

    public NodeGroupMigrationTaskService() {
        super(MigrationState.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
        super.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
    }


    /**
     * Validates that a new task service has been requested appropriately (and that the request
     * doesn't provide values for internal-only fields).
     *
     * @see TaskService#validateStartPost(Operation)
     */
    @Override
    protected MigrationState validateStartPost(Operation taskOperation) {
        MigrationState task = super.validateStartPost(taskOperation);

        if (task != null) {
//            if (task.subStage != null) {
//                taskOperation.fail(
//                        new IllegalArgumentException("Do not specify subStage: internal use only"));
//                return null;
//            }
//            if (task.currentFactoryIndex != null) {
//                taskOperation.fail(new IllegalArgumentException("Do not specify currentFactoryIndex: internal use only"));
//                return null;
//            }
//            if (task.currentFactoryBatchIndex != null) {
//                taskOperation.fail(new IllegalArgumentException("Do not specify currentFactoryBatchIndex: internal use only"));
//                return null;
//            }
//            if (task.migrationTaskFinishedFactoryLink != null) {
//                taskOperation.fail(new IllegalArgumentException("Do not specify migrationTaskFinishedFactoryLink: internal use only"));
//                return null;
//            }
//            if (task.migrationTaskFinishedURI != null) {
//                taskOperation.fail(new IllegalArgumentException("Do not specify migrationTaskFinishedURI: internal use only"));
//                return null;
//            }
//            if (task.retryCount != null) {
//                taskOperation.fail(new IllegalArgumentException("Do not specify retryCount: internal use only"));
//                return null;
//            }
//            if (task.stageFailed != null) {
//                taskOperation.fail(new IllegalArgumentException("Do not specify stageFailed: internal use only"));
//                return null;
//            }
//            if (task.factoryLinks == null || task.factoryLinks.size() == 0) {
//                taskOperation.fail(new IllegalArgumentException("factoryLinks: cannot be empty"));
//                return null;
//            }
//            if (task.sourceNodeGroupReference == null) {
//                taskOperation.fail(new IllegalArgumentException("sourceNodeGroupReference: cannot be empty"));
//                return null;
//            }
//            if (task.destinationNodeGroupReference == null) {
//                taskOperation.fail(new IllegalArgumentException("destinationNodeGroupReference: cannot be empty"));
//                return null;
//            }
        }

        return task;
    }

    /**
     * Once the task has been validated, we need to initialize the state to valid values.
     *
     * @see TaskService#initializeState(TaskServiceState, Operation)
     */
    @Override
    protected void initializeState(MigrationState task, Operation taskOperation) {
        super.initializeState(task, taskOperation);
        task.subStage = SubStage.INITIALIZING;
//        task.currentFactoryIndex = 0;
//        // when we encounter the first batch the index will be bumped up to 0
//        task.currentFactoryBatchIndex = -1;
//        task.subscriptionUris = new HashSet<>();
    }


    private boolean isMigrationFinishNotice(Operation patch) {
        if (patch.getBodyRaw() instanceof String) {
            MigrationFinishedNotice doc = Utils.fromJson(patch.getBodyRaw(), MigrationFinishedNotice.class);
            return MigrationFinishedNotice.KIND.equals(doc.documentKind);
        }
        return patch.getBodyRaw() instanceof MigrationFinishedNotice;
    }

    @Override
    public void handlePatch(Operation patch) {


        if (isMigrationFinishNotice(patch)) {
            handleMigrationTaskFinishNotice(patch);
            return;
        }


        MigrationState currentTask = getState(patch);
        MigrationState patchBody = getBody(patch);

        if (!validateTransition(patch, currentTask, patchBody)) {
            return;
        }

        // Prevent stale data by only modifying currentFactoryIndex before calling complete()
//        if (patchBody.subStage == SubStage.MIGRATE_CURRENT_BATCH) {
//            patchBody.currentFactoryIndex = 0;
//            patchBody.currentFactoryBatchIndex++;
//            patchBody.retryCount = 0;
//        } else if (patchBody.subStage == SubStage.MIGRATION_TASK_COMPLETED) {
//            patchBody.currentFactoryIndex++;
//        } else if (patchBody.subStage == SubStage.RETRY_CURRENT_BATCH) {
//            patchBody.currentFactoryIndex = 0;
//            patchBody.retryCount++;
//        }

        updateState(currentTask, patchBody);
        currentTask.batches = patchBody.batches;
        currentTask.runtime.generatedMigrationRequests = patchBody.runtime.generatedMigrationRequests;
        currentTask.runtime.migrationTaskPaths = patchBody.runtime.migrationTaskPaths;
        currentTask.runtime.subscriptionUris = patchBody.runtime.subscriptionUris;
        currentTask.runtime.resultByFactoryPath = patchBody.runtime.resultByFactoryPath;
        currentTask.runtime.retryCount = patchBody.runtime.retryCount;
        currentTask.runtime.batchIndex = patchBody.runtime.batchIndex;
        currentTask.results = patchBody.results;


        patch.complete();

        switch (currentTask.taskInfo.stage) {
        case STARTED:
//            logInfo("Task started: [subStage=%s] [currentFactoryIndex=%s] [currentFactoryBatchIndex=%s]",
//                    currentTask.subStage, currentTask.currentFactoryIndex, currentTask.currentFactoryBatchIndex);
            handleSubstage(currentTask);
            break;
        case FINISHED:
            logInfo("Task finished successfully.");
//            calcAndSaveStats(currentTask);
            break;
        case FAILED:
            logWarning("Task failed: %s", (currentTask.failureMessage == null ? "No reason given"
                    : currentTask.failureMessage));
            break;
        default:
            logWarning("Unexpected stage: %s", currentTask.taskInfo.stage);
            break;
        }
    }

    /**
     * Validate that the PATCH we got requests reasonable changes to our state
     *
     * @see TaskService#validateTransition(Operation, TaskServiceState, TaskServiceState)
     */
    @Override
    protected boolean validateTransition(Operation patch,
            MigrationState currentTask,
            MigrationState patchBody) {
        super.validateTransition(patch, currentTask, patchBody);

        if (patchBody.taskInfo.stage == TaskState.TaskStage.STARTED && patchBody.subStage == null) {
            patch.fail(new IllegalArgumentException("Missing substage"));
            return false;
        }

        // currentFactoryIndex should only be updated by our Service logic; not by a PATCH body. This
        // protects us against stale data
//        if (currentTask.currentBatchIndex != null) {
//            patchBody.currentFactoryIndex = currentTask.currentFactoryIndex;
//        }
        return true;
    }

//    private void calcAndSaveStats(State task) {
//        for (Set<FactoryAndTransform> batches : task.factoryLinks) {
//            for (FactoryAndTransform factory : batches) {
//                SymphonyUtils.setStat(this, getMetric(String.format("%s.duration", factory.factoryLink)),
//                        SECONDS, SymphonyUtils.getStatDifferenceSeconds(this, getMetric(factory.factoryLink)));
//            }
//        }
//    }


    protected void sendSelfPatchForSubStage(MigrationState taskState, SubStage subStage) {
        sendSelfPatch(taskState, TaskState.TaskStage.STARTED, state -> {
            taskState.subStage = subStage;
        });
    }

    private void handleSubstage(MigrationState task) {
        switch (task.subStage) {
        case INITIALIZING:
            // TODO: resolve source/dest nodes

            // if no batch is specified, discover all services and populate task.batches
            if (task.batches.isEmpty()) {

                List<String> preliminaryPaths = new ArrayList<>();
                preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_USERS);
                preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
                preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
                preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_ROLES);

                preliminaryPaths.add(ServiceUriPaths.CORE_CREDENTIALS);
                preliminaryPaths.add(TenantService.FACTORY_LINK);
                preliminaryPaths.add(TransactionFactoryService.SELF_LINK);

                // Convert pathes to a sequence migration requests
                // TODO: check existence of those factories
                task.batches.addAll(
                        preliminaryPaths.stream().map(path -> {
                            MigrationRequest migrationRequest = new MigrationRequest();
                            migrationRequest.factoryLink = path;
                            List<MigrationRequest> list = new ArrayList<>();
                            list.add(migrationRequest);
                            return list;
                        }).collect(toList())
                );

                // make a call to root service and get factories
                // do GET to config urls, and filter out CORE services
            }
            // specify starting batch index
            task.runtime.batchIndex = 0;
            sendSelfPatchForSubStage(task, PREPARE_CURRENT_BATCH);
            break;
        case PREPARE_CURRENT_BATCH:

            // populate migration requests
            List<MigrationRequest> requests = task.batches.get(task.runtime.batchIndex);
            task.runtime.generatedMigrationRequests = requests.stream()
                    .map(request -> createMigrationTaskRequest(request, task))
                    .collect(toList());

            sendSelfPatchForSubStage(task, MIGRATE_CURRENT_BATCH);

            break;
        case MIGRATE_CURRENT_BATCH:
            performAndSubscribeMigrations(task.runtime.generatedMigrationRequests, task);
            sendSelfPatchForSubStage(task, WAITING_BATCH_TO_FINISH);
            break;
        case WAITING_BATCH_TO_FINISH:
            // do nothing since subscriptions notify migration task finishes
            break;
        case CURRENT_BATCH_FINISHED_WITH_FAILURE:

            populateResultReports(task);

            // update runtime context for retry

            // failed/cancelled ones
            Set<String> failedTaskDestFactoryPaths = task.runtime.resultByFactoryPath.entrySet().stream()
                    .filter(entry -> isMigrationTaskFailed.test(entry.getValue().taskInfo.stage))
                    .map(Map.Entry::getKey)
                    .collect(toSet());

            // retrieve failed migration requests. reused for retry
            List<MigrationTaskService.State> failedRequests = task.runtime.generatedMigrationRequests.stream()
                    .filter(state -> failedTaskDestFactoryPaths.contains(state.destinationFactoryLink))
                    .collect(toList());

            // keep runtime info before cleanup
            Set<URI> subscriptionUris = new HashSet<>(task.runtime.subscriptionUris);
            int retryCount = task.runtime.retryCount + 1;

            task.runtime.reset();

            // populate for retry
            task.runtime.generatedMigrationRequests.addAll(failedRequests);
            task.runtime.retryCount = retryCount;

            // cleanup subscription and move to next substage
            cleanupSubscriptions(subscriptionUris, () -> {
                if (task.runtime.retryCount >= task.maxRetry) {
                    String message = format("Maximum retry performed. maxRetry=%s", task.maxRetry);
                    sendSelfFailurePatch(task, message);
                } else {
                    sendSelfPatchForSubStage(task, MIGRATE_CURRENT_BATCH);
                }
            });


            break;
        case CURRENT_BATCH_FINISHED_WITH_SUCCESS:
            populateResultReports(task);
            cleanupSubscriptions(task.runtime.subscriptionUris, () -> sendSelfPatchForSubStage(task, MOVE_TO_NEXT_BATCH));

            break;
        case MOVE_TO_NEXT_BATCH:
            // increment current batch index
            task.runtime.batchIndex++;

            // reset runtime data for next batch
            task.runtime.reset();

            // check whether next batch exists or not
            if (task.batches.size() <= task.runtime.batchIndex) {
                // move to finish
                sendSelfPatchForSubStage(task, POST_BATCH_COMPLITION);
            } else {
                // perform next batch
                sendSelfPatchForSubStage(task, MIGRATE_CURRENT_BATCH);
            }
            break;
        case POST_BATCH_COMPLITION:
            sendSelfPatch(task, TaskState.TaskStage.FINISHED, null);
            break;
        default:
            String errMessage = format("Unexpected sub stage: %s", task.subStage);
            logWarning(errMessage);
            sendSelfFailurePatch(task, errMessage);
            break;
        }
    }


    private MigrationTaskService.State createMigrationTaskRequest(MigrationRequest request, MigrationState input) {

        // when migration request is explicitly specified by caller, use it
        if (request.request != null) {
            return request.request;
        }

        // create default migration request
        MigrationTaskService.State migrationTaskState = new MigrationTaskService.State();
        migrationTaskState.sourceFactoryLink = request.factoryLink;
        migrationTaskState.destinationFactoryLink = request.factoryLink;
        migrationTaskState.sourceNodeGroupReference = UriUtils.buildUri(input.sourceNodeReference, input.sourceNodeGroupPath);
        migrationTaskState.destinationNodeGroupReference = UriUtils.buildUri(input.destinationNodeReference, input.destinationNodeGroupPath);
        migrationTaskState.migrationOptions = EnumSet.of(MigrationOption.DELETE_AFTER);
        if (input.operationTimeoutMicros != null) {
            migrationTaskState.documentExpirationTimeMicros = input.operationTimeoutMicros;
        }

        return migrationTaskState;
    }


    private void performAndSubscribeMigrations(List<MigrationTaskService.State> migrationTaskServiceStates, MigrationState state) {

        Set<URI> subscriptionUris = new HashSet<>();

        List<Operation> posts = migrationTaskServiceStates.stream()
                .map(body ->
                                Operation.createPost(UriUtils.buildUri(state.sourceNodeReference, ServiceUriPaths.MIGRATION_TASKS))
                                        .setBody(body)
                                        .setCompletion((op, ex) -> {
                                            // TODO: exception

                                            String taskPath = op.getBody(MigrationTaskService.State.class).documentSelfLink;
                                            URI taskUri = UriUtils.buildUri(state.sourceNodeReference, taskPath);
                                            Operation subscribe = Operation.createPost(taskUri).setReferer(getUri());
                                            Consumer<Operation> callback = getMigrationTaskSubscriber(state.runtime.batchIndex);
                                            URI subscriptionUri = getHost().startSubscriptionService(
                                                    subscribe, callback, ServiceSubscriptionState.ServiceSubscriber.create(true));

//                                    logInfo("Subscribing to [migrationTask=%s] [factoryLink=%s]",
//                                            response.documentSelfLink, response.destinationFactoryLink);

                                            synchronized (subscriptionUris) {
                                                subscriptionUris.add(subscriptionUri);
                                            }
                                        })
                ).collect(toList());


        OperationJoin.create(posts)
                .setCompletion((ops, exs) -> {
                    // TODO: failure
                    state.runtime.subscriptionUris.addAll(subscriptionUris);

                    // populate migration task paths
                    state.runtime.migrationTaskPaths = ops.values().stream()
                            .map(op -> op.getBody(MigrationTaskService.State.class))
                            .map(migrationTaskState -> migrationTaskState.documentSelfLink)
                            .collect(toSet());

                    // update itself. TODO: maybe some race with subscription
                    sendSelfPatch(state);
                })
                .sendWith(this);
    }

    private Consumer<Operation> getMigrationTaskSubscriber(int currentBatchIndex) {
        return update -> {

            // callback logic for migration task notification

            update.complete();
            if (!update.hasBody()) {
                return;
            }

            MigrationTaskService.State taskState = update.getBody(MigrationTaskService.State.class);
            if (taskState.taskInfo == null) {
                return;
            }

            if (isMigrationTaskInFinalState.test(taskState.taskInfo.stage)) {
                // notify the task that migration has finished
                MigrationFinishedNotice body = new MigrationFinishedNotice();
                body.migrationTaskPath = taskState.documentSelfLink;
                body.currentBatchIndex = currentBatchIndex;

                Operation patch = Operation.createPatch(this, getSelfLink()).setBody(body);
                sendRequest(patch);
            }
        };
    }


    private void handleMigrationTaskFinishNotice(Operation patch) {
        // TODO: check status of migration tasks, when all are finished, move to next stage(clean up subscription, retry)

        MigrationState currentTask = getState(patch);
        MigrationFinishedNotice request = patch.getBody(MigrationFinishedNotice.class);

        patch.complete();

        if (currentTask.runtime.batchIndex != request.currentBatchIndex) {
            // out of date finish request. Do nothing.
            return;
        }

        // check status of migration tasks, when all are finished, move to next stage(clean up subscription, retry)

        // TODO: use liveview of migration tasks once implemented
        List<Operation> getOps = currentTask.runtime.migrationTaskPaths.stream()
                .map(path -> Operation.createGet(UriUtils.buildUri(currentTask.sourceNodeReference, path))
                        .setReferer(getSelfLink()))
                .collect(toList());

        OperationJoin.create(getOps)
                .setCompletion((ops, exs) -> {

                    Set<MigrationTaskService.State> results = ops.values().stream()
                            .map(op -> op.getBody(MigrationTaskService.State.class))
                            .collect(toSet());

                    Set<TaskStage> taskStages = results.stream().map(state -> state.taskInfo.stage).collect(toSet());

                    boolean isAllInFinalState = taskStages.stream().allMatch(isMigrationTaskInFinalState);
                    if (!isAllInFinalState) {
                        // still some migration tasks are running. do nothing
                        return;
                    }

                    // results by factory path
                    currentTask.runtime.resultByFactoryPath = results.stream()
                            .collect(toMap(state -> state.destinationFactoryLink, identity()));


                    // TODO: for canceled task??
                    if (taskStages.contains(TaskStage.FAILED)) {
                        // TODO: when there is failure => do retry
                    } else {
                        // current batch migration tasks all finished, move on to next
                        sendSelfPatchForSubStage(currentTask, CURRENT_BATCH_FINISHED_WITH_SUCCESS);
                    }
                })
                .sendWith(this);
    }

    @FunctionalInterface
    private interface OnCompleteSubscriptionDeletion {
        void perform();
    }

    private void cleanupSubscriptions(Set<URI> subscriptionUris, OnCompleteSubscriptionDeletion onSuccessfulDeletion) {
        List<Operation> deletes = subscriptionUris.stream()
                .map(Operation::createDelete)
                .collect(toList());
//    private void cleanupSubscriptions(MigrationState state, OnCompleteSubscriptionDeletion onSuccessfulDeletion) {
//        List<Operation> deletes = state.runtime.subscriptionUris.stream()
//                .map(Operation::createDelete)
//                .collect(toList());

        OperationJoin.create(deletes)
                .setCompletion((ops, exs) -> {
                    if (exs != null) {
//                        logWarning("Failed to cleanup subscriptions: %s", Utils.toString(failures.entrySet().iterator().next().getValue()));
//                        sendSelfFailurePatch(task, "Failed to cleanup subscriptions");
                        // TODO: patch failure
                        return;
                    }
                    onSuccessfulDeletion.perform();
                })
                .sendWith(this);
    }

    private void populateResultReports(MigrationState task) {
        for (MigrationTaskService.State request : task.runtime.generatedMigrationRequests) {
            ResultReport report = new ResultReport();
            report.batchIndex = task.runtime.batchIndex;
            report.executionIndex = task.runtime.retryCount;
            report.factoryPath = request.destinationFactoryLink;
            report.request = request;
            report.response = task.runtime.resultByFactoryPath.get(request.destinationFactoryLink);
            report.resultState = report.response.taskInfo.stage;
            report.taskPath = report.response.documentSelfLink;

            task.results.add(report);
        }
    }
}
