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

import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.CURRENT_BATCH_FINISHED_WITH_FAILURE;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.CURRENT_BATCH_FINISHED_WITH_SUCCESS;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.MIGRATE_CURRENT_BATCH;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.MOVE_TO_NEXT_BATCH;
import static com.vmware.xenon.services.common.NodeGroupMigrationTaskService.SubStage.POST_BATCH_COMPLETION;
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
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceSubscriptionState.ServiceSubscriber;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.MigrationTaskService.MigrationOption;
import com.vmware.xenon.services.common.MigrationTaskService.State;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState.MigrationRequest;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState.ResultReport;

/**
 * Orchestrate nodegroup to nodegroup migration.
 *
 * @see MigrationTaskService
 */
public class NodeGroupMigrationTaskService extends TaskService<NodeGroupMigrationState> {

    public static final String FACTORY_LINK = ServiceUriPaths.NODE_GROUP_MIGRATION_TASKS;

    private static Predicate<TaskStage> isMigrationTaskInFinalState = taskStage ->
            EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED).contains(taskStage);

    private static Predicate<TaskStage> isMigrationTaskFailed = taskStage ->
            EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED).contains(taskStage);

    private static final List<String> AUTO_MIGRATE_PRELIMINARY_PATHS = new ArrayList<>();
    private static final Set<String> AUTO_MIGRATE_EXCLUSION_PATHS = new HashSet<>();

    static {
        // These services need to be migrated in sequence before user services. Order is important.
        AUTO_MIGRATE_PRELIMINARY_PATHS.add(ServiceUriPaths.CORE_AUTHZ_USERS);
        AUTO_MIGRATE_PRELIMINARY_PATHS.add(ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        AUTO_MIGRATE_PRELIMINARY_PATHS.add(ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
        AUTO_MIGRATE_PRELIMINARY_PATHS.add(ServiceUriPaths.CORE_AUTHZ_ROLES);
        AUTO_MIGRATE_PRELIMINARY_PATHS.add(ServiceUriPaths.CORE_CREDENTIALS);
        AUTO_MIGRATE_PRELIMINARY_PATHS.add(TenantService.FACTORY_LINK);

        // following service paths are excluded from auto-resolving migration factories
        AUTO_MIGRATE_EXCLUSION_PATHS.addAll(AUTO_MIGRATE_PRELIMINARY_PATHS);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.CORE_GRAPH_QUERIES);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.NODE_GROUP_FACTORY);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.CORE_QUERY_TASKS);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.SYNCHRONIZATION_TASKS);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(TransactionFactoryService.SELF_LINK);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.MIGRATION_TASKS);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.NODE_GROUP_MIGRATION_TASKS);
        AUTO_MIGRATE_EXCLUSION_PATHS.add(ServiceUriPaths.CHECKPOINTS);
    }


    /**
     * When main task stage is STARTED, go through this sub stages.
     */
    public enum SubStage {

        INITIALIZING,

        /**
         * Prepare to run current batch stage.
         */
        PREPARE_CURRENT_BATCH,

        /**
         * Perform migration on current batch stage.
         */
        MIGRATE_CURRENT_BATCH,

        /**
         * Waiting migration tasks for current batch stage to be finished.
         */
        WAITING_BATCH_TO_FINISH,

        /**
         * Current stage migration tasks finished with all success.
         */
        CURRENT_BATCH_FINISHED_WITH_SUCCESS,

        /**
         * Current stage migration tasks finished but have some/all failures.
         */
        CURRENT_BATCH_FINISHED_WITH_FAILURE,

        /**
         * Cleanup all subscriptions.
         */
        CLEANUP_SUBSCRIPTIONS,

        /**
         * When current batch stage is finished, move to next stage.
         */
        MOVE_TO_NEXT_BATCH,

        /**
         * When all batch stages are finished
         */
        POST_BATCH_COMPLETION,

    }


    public static class NodeGroupMigrationState extends TaskService.TaskServiceState {

        public static class MigrationRequest {
            public String factoryLink;
            public MigrationTaskService.State request;
        }

        /**
         * Values that have calculated while performing the task for current batch
         */
        public static class RuntimeContext {

            /**
             * Keeps migration request for current batch
             */
            public List<MigrationTaskService.State> generatedMigrationRequests = new ArrayList<>();

            /**
             * Reference of created migration tasks from {@link #generatedMigrationRequests}.
             */
            public Set<URI> migrationTaskReferences = new HashSet<>();

            /**
             * Keeps subscription uris for migration task in current batch
             */
            public Set<URI> subscriptionUris = new HashSet<>();

            /**
             * MigrationTask result by destination factory path.
             */
            public Map<String, MigrationTaskService.State> resultByFactoryPath = new HashMap<>();

            /**
             * Retry count for the current batch. Default does not retry.
             */
            public int retryCount = 0;

            /**
             * The current batch index we are processing
             */
            public int batchIndex = 0;

            /**
             * Used as a flag to prevent stale patch request from migration task finished notification.
             * Patch request that contains less than this index value in current state is stale request.
             */
            public int finishedBatchIndex = -1;

            public void reset() {
                this.generatedMigrationRequests.clear();
                this.migrationTaskReferences.clear();
                this.subscriptionUris.clear();
                this.resultByFactoryPath.clear();
                this.retryCount = 0;
                // batchIndex and finishedBatchIndex will NOT reset
            }
        }


        /**
         * Hold result of single migration task
         */
        public static class ResultReport {
            public int batchIndex;
            public int retryCount;
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
         * e.g.: http://source-node.vmware.com
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI sourceNodeReference;

        /**
         * e.g.: /core/node-groups/default
         */
        public String sourceNodeGroupPath = ServiceUriPaths.DEFAULT_NODE_GROUP;

        /**
         * The destination (or target) node-group URL to use to migrate state to during an app migration.
         * e.g.: http://dest-node.vmware.com
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI destinationNodeReference;

        /**
         * e.g.: /core/node-groups/default
         */
        public String destinationNodeGroupPath = ServiceUriPaths.DEFAULT_NODE_GROUP;

        /**
         * Path to migration task
         */
        public String migrationTaskPath = ServiceUriPaths.MIGRATION_TASKS;

        /**
         * When timeout is specified, this value will be set to each migration request.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long operationTimeoutMicros;

        /**
         * Hold migration requests.
         *
         * The outside list performs each element in sequence and inside list performs them in parallel.
         */
        public List<List<MigrationRequest>> batches = new ArrayList<>();


        /**
         * Number of retry when migration task failed. (default is 0)
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public int maxRetry = 0;

        /**
         * Hold runtime information
         */
        public RuntimeContext runtime = new RuntimeContext();

        /**
         * Hold result of each migration request
         */
        public List<ResultReport> results = new ArrayList<>();
    }

    /**
     * Used for notifying migration finish from subscription callback. (Internal use within this class).
     */
    static class MigrationFinishedNotice extends ServiceDocument {
        static final String KIND = Utils.buildKind(MigrationFinishedNotice.class);
        public String migrationTaskPath;
        public int currentBatchIndex;

        public MigrationFinishedNotice() {
            this.documentKind = KIND;
        }
    }

    public NodeGroupMigrationTaskService() {
        super(NodeGroupMigrationState.class);
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
    protected NodeGroupMigrationState validateStartPost(Operation taskOperation) {
        NodeGroupMigrationState task = super.validateStartPost(taskOperation);

        if (task != null) {
            if (task.subStage != null) {
                taskOperation.fail(
                        new IllegalArgumentException("Do not specify subStage: internal use only"));
                return null;
            }
            if (task.sourceNodeReference == null) {
                taskOperation.fail(new IllegalArgumentException("sourceNodeReference cannot be empty"));
                return null;
            }
            if (task.destinationNodeReference == null) {
                taskOperation.fail(new IllegalArgumentException("destinationNodeReference cannot be empty"));
                return null;
            }
        }

        return task;
    }

    /**
     * Once the task has been validated, we need to initialize the state to valid values.
     *
     * @see TaskService#initializeState(TaskServiceState, Operation)
     */
    @Override
    protected void initializeState(NodeGroupMigrationState task, Operation taskOperation) {
        super.initializeState(task, taskOperation);
        task.subStage = SubStage.INITIALIZING;
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

        NodeGroupMigrationState currentTask = getState(patch);
        NodeGroupMigrationState patchBody = getBody(patch);

        if (!validateTransition(patch, currentTask, patchBody)) {
            return;
        }

        boolean staleRequest = patchBody.runtime.finishedBatchIndex < currentTask.runtime.finishedBatchIndex;
        if (staleRequest) {
            String message = "Received stale request. current: batchIndex=%d, finishedIndex=%d, stage=%s " +
                    " patch: batchIndex=%d, finishedIndex=%d, stage=%s";
            logInfo(message, currentTask.runtime.batchIndex, currentTask.runtime.finishedBatchIndex, currentTask.subStage,
                    patchBody.runtime.batchIndex, patchBody.runtime.finishedBatchIndex, patchBody.subStage
            );
            return;
        }

        updateState(currentTask, patchBody);
        currentTask.batches = patchBody.batches;
        currentTask.runtime.generatedMigrationRequests = patchBody.runtime.generatedMigrationRequests;
        currentTask.runtime.migrationTaskReferences = patchBody.runtime.migrationTaskReferences;
        currentTask.runtime.subscriptionUris = patchBody.runtime.subscriptionUris;
        currentTask.runtime.resultByFactoryPath = patchBody.runtime.resultByFactoryPath;
        currentTask.runtime.retryCount = patchBody.runtime.retryCount;
        currentTask.runtime.batchIndex = patchBody.runtime.batchIndex;
        currentTask.runtime.finishedBatchIndex = patchBody.runtime.finishedBatchIndex;
        currentTask.results = patchBody.results;

        patch.complete();

        switch (currentTask.taskInfo.stage) {
        case STARTED:
            handleSubStage(currentTask);
            break;
        case FINISHED:
            logInfo("Task finished successfully.");
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
            NodeGroupMigrationState currentTask,
            NodeGroupMigrationState patchBody) {
        super.validateTransition(patch, currentTask, patchBody);

        if (patchBody.taskInfo.stage == TaskState.TaskStage.STARTED && patchBody.subStage == null) {
            patch.fail(new IllegalArgumentException("Missing substage"));
            return false;
        }

        return true;
    }


    protected void sendSelfPatchForSubStage(NodeGroupMigrationState taskState, SubStage subStage) {
        sendSelfPatch(taskState, TaskState.TaskStage.STARTED, state -> {
            taskState.subStage = subStage;
        });
    }

    private void handleSubStage(NodeGroupMigrationState task) {
        switch (task.subStage) {
        case INITIALIZING:

            // specify starting batch index
            task.runtime.batchIndex = 0;

            // if no batch is specified, discover all services and populate task.batches
            if (task.batches.isEmpty()) {

                // make a call to root service and get factories
                Operation.createGet(task.sourceNodeReference)
                        .setCompletion((op, ex) -> {

                            List<String> factoryPaths = new ArrayList<>();

                            if (ex != null) {
                                // When RootNamespaceService is NOT available, fallback to find factories on localhost
                                // since most likely migration task runs on a destination node.
                                logWarning("RootNamespaceService is not available on destination node. Fallback factory discovery to local node");

                                // find all factory services. this operation happens synchronously.
                                Operation callback = Operation.createGet(null);
                                callback.setCompletion((queryOp, queryEx) -> {
                                    ServiceDocumentQueryResult result = queryOp.getBody(ServiceDocumentQueryResult.class);
                                    factoryPaths.addAll(result.documentLinks);
                                });
                                getHost().queryServiceUris(EnumSet.of(ServiceOption.FACTORY), true, callback);
                            } else {
                                // factories from RootNamespaceService
                                ServiceDocumentQueryResult result = op.getBody(ServiceDocumentQueryResult.class);
                                factoryPaths.addAll(result.documentLinks);
                            }

                            // create sequential migration batch stages for each preliminary factories that exist in dest node
                            AUTO_MIGRATE_PRELIMINARY_PATHS.stream()
                                    .filter(factoryPaths::contains)
                                    .map(path -> {
                                        MigrationRequest migrationRequest = new MigrationRequest();
                                        migrationRequest.factoryLink = path;
                                        List<MigrationRequest> list = new ArrayList<>();
                                        list.add(migrationRequest);
                                        return list;
                                    })
                                    .forEach(task.batches::add);


                            // remove factories in exclusion list
                            factoryPaths.removeAll(AUTO_MIGRATE_EXCLUSION_PATHS);

                            logInfo("Discovered factory services: %s", factoryPaths);

                            // make a single batch for those resolved services. (they will run in parallel)
                            List<MigrationRequest> requests = factoryPaths.stream()
                                    .map(path -> {
                                        MigrationRequest migrationRequest = new MigrationRequest();
                                        migrationRequest.factoryLink = path;
                                        return migrationRequest;
                                    })
                                    .collect(toList());
                            task.batches.add(requests);

                            sendSelfPatchForSubStage(task, PREPARE_CURRENT_BATCH);
                        })
                        .sendWith(this);
                return;
            }

            sendSelfPatchForSubStage(task, PREPARE_CURRENT_BATCH);
            break;
        case PREPARE_CURRENT_BATCH:

            // populate migration requests for this batch
            List<MigrationRequest> requests = task.batches.get(task.runtime.batchIndex);
            task.runtime.generatedMigrationRequests = requests.stream()
                    .map(request -> createMigrationTaskRequest(task.runtime.batchIndex, request, task))
                    .collect(toList());

            sendSelfPatchForSubStage(task, MIGRATE_CURRENT_BATCH);

            break;
        case MIGRATE_CURRENT_BATCH:
            performAndSubscribeMigrations(task);
            sendSelfPatchForSubStage(task, WAITING_BATCH_TO_FINISH);
            break;
        case WAITING_BATCH_TO_FINISH:
            // do nothing since subscriptions notify migration task finishes
            break;
        case CURRENT_BATCH_FINISHED_WITH_FAILURE:

            populateResultReports(task);

            // failed/cancelled factory paths
            Set<String> failedTaskDestFactoryPaths = task.runtime.resultByFactoryPath.entrySet().stream()
                    .filter(entry -> isMigrationTaskFailed.test(entry.getValue().taskInfo.stage))
                    .map(Map.Entry::getKey)
                    .collect(toSet());

            // retrieve failed migration requests. reused for retry
            List<MigrationTaskService.State> failedRequests = task.runtime.generatedMigrationRequests.stream()
                    .filter(state -> failedTaskDestFactoryPaths.contains(state.destinationFactoryLink))
                    .collect(toList());

            // keep runtime info before reset
            Set<URI> subscriptionUris = new HashSet<>(task.runtime.subscriptionUris);
            int retryCount = task.runtime.retryCount;

            task.runtime.reset();

            // update runtime context for retry
            task.runtime.generatedMigrationRequests.addAll(failedRequests);
            task.runtime.retryCount = retryCount + 1;

            // cleanup current subscription and move to next substage
            cleanupSubscriptions(task, subscriptionUris, () -> {
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
            cleanupSubscriptions(task, task.runtime.subscriptionUris, () -> sendSelfPatchForSubStage(task, MOVE_TO_NEXT_BATCH));

            break;
        case MOVE_TO_NEXT_BATCH:
            // reset runtime data for next batch
            task.runtime.reset();

            // increment current batch index (batchIndex was not reset)
            task.runtime.batchIndex++;

            // check whether next batch exists or not
            if (task.batches.size() <= task.runtime.batchIndex) {
                // move to finish
                sendSelfPatchForSubStage(task, POST_BATCH_COMPLETION);
            } else {
                // perform next batch
                sendSelfPatchForSubStage(task, PREPARE_CURRENT_BATCH);
            }
            break;
        case POST_BATCH_COMPLETION:
            sendSelfPatch(task, TaskState.TaskStage.FINISHED, null);
            break;
        default:
            String errMessage = format("Unexpected sub stage: %s", task.subStage);
            logWarning(errMessage);
            sendSelfFailurePatch(task, errMessage);
            break;
        }
    }


    private MigrationTaskService.State createMigrationTaskRequest(int batchIndex, MigrationRequest request, NodeGroupMigrationState input) {

        // when migration request is explicitly specified by caller, use it
        if (request.request != null) {
            logInfo("Migration request is specified. batch=%s, source=%s", batchIndex, request.request.sourceFactoryLink);
            return request.request;
        }

        // create default migration request
        URI sourceNodeGroupReference = UriUtils.buildUri(input.sourceNodeReference, input.sourceNodeGroupPath);
        URI destNodeGroupReference = UriUtils.buildUri(input.destinationNodeReference, input.destinationNodeGroupPath);

        MigrationTaskService.State migrationTaskState = new MigrationTaskService.State();
        migrationTaskState.sourceFactoryLink = request.factoryLink;
        migrationTaskState.destinationFactoryLink = request.factoryLink;
        migrationTaskState.sourceNodeGroupReference = sourceNodeGroupReference;
        migrationTaskState.destinationNodeGroupReference = destNodeGroupReference;
        migrationTaskState.migrationOptions = EnumSet.of(MigrationOption.DELETE_AFTER);
        if (input.operationTimeoutMicros != null) {
            migrationTaskState.documentExpirationTimeMicros = input.operationTimeoutMicros;
        }

        logInfo("Migration request is generated. batch=%s, source=%s", batchIndex, request.factoryLink);

        return migrationTaskState;
    }


    private void performAndSubscribeMigrations(NodeGroupMigrationState task) {

        List<MigrationTaskService.State> migrationTaskServiceStates = task.runtime.generatedMigrationRequests;

        // create post ops to start migration task.
        List<Operation> posts = migrationTaskServiceStates.stream()
                .map(body ->
                        Operation.createPost(UriUtils.buildUri(task.destinationNodeReference, task.migrationTaskPath))
                                .setBody(body)
                ).collect(toList());


        OperationJoin.create(posts)
                .setCompletion((ops, exs) -> {
                    if (exs != null) {
                        String message = format("Failed to create MigrationTasks. exs=%s", Utils.toString(exs));
                        sendSelfFailurePatch(task, message);
                        return;
                    }

                    for (Operation op : ops.values()) {
                        State migrationState = op.getBody(State.class);

                        URI taskReference = UriUtils.buildUri(op.getUri(), migrationState.documentSelfLink);
                        logInfo("MigrationTask created. batch=%s, uri=%s", task.runtime.batchIndex, taskReference);

                        // create subscription for migration task
                        String taskPath = migrationState.documentSelfLink;
                        URI taskUri = UriUtils.buildUri(task.destinationNodeReference, taskPath);
                        Operation subscribe = Operation.createPost(taskUri).setReferer(getUri());
                        Consumer<Operation> callback = getMigrationTaskSubscriber(task.runtime.batchIndex);
                        URI subscriptionUri = getHost().startSubscriptionService(
                                subscribe, callback, ServiceSubscriber.create(true));

                        // populate runtime info
                        task.runtime.subscriptionUris.add(subscriptionUri);
                        task.runtime.migrationTaskReferences.add(taskUri);
                    }

                    // update itself.
                    sendSelfPatch(task);
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

        NodeGroupMigrationState currentTask = getState(patch);
        MigrationFinishedNotice request = patch.getBody(MigrationFinishedNotice.class);

        patch.complete();

        if (currentTask.runtime.batchIndex != request.currentBatchIndex) {
            // out of date finish request. Do nothing.
            return;
        }

        // check status of migration tasks, when all are finished, move to the next stage
        List<Operation> getOps = currentTask.runtime.migrationTaskReferences.stream()
                .map(uri -> Operation.createGet(UriUtils.buildUri(uri)).setReferer(getSelfLink()))
                .collect(toList());

        OperationJoin.create(getOps)
                .setCompletion((ops, exs) -> {

                    if (exs != null) {
                        String message = format("Failed to retrieve MigrationTasks. exs=%s", Utils.toString(exs));
                        sendSelfFailurePatch(currentTask, message);
                        return;
                    }

                    Set<MigrationTaskService.State> results = ops.values().stream()
                            .map(op -> op.getBody(MigrationTaskService.State.class))
                            .collect(toSet());

                    Set<TaskStage> taskStages = results.stream().map(state -> state.taskInfo.stage).collect(toSet());

                    boolean isAllInFinalState = taskStages.stream().allMatch(isMigrationTaskInFinalState);
                    if (!isAllInFinalState) {
                        // still some migration tasks are running. do nothing
                        return;
                    }

                    // Since this GET ops can be invoked asynchronous to notification, this part
                    // may be called multiple times.
                    // (For example, let's say there are two migration tasks. One is finished and
                    // received notification
                    // while the other one is still running. The notification triggers this GETs.
                    // When GET ops reached to tasks, the other migration task finished and all
                    // get requests returns finished status. The notification from second migration
                    // task may arrive after that, and trigger this GET ops again.)
                    //
                    // To prevent double processing the next sub-stage, update finished batch
                    // index. In "handlePatch", it checks current and patch-body
                    // "finishedBatchIndex" that prevents processing stale patch requests.
                    currentTask.runtime.finishedBatchIndex = currentTask.runtime.batchIndex;

                    // results by factory path
                    currentTask.runtime.resultByFactoryPath = results.stream()
                            .collect(toMap(state -> state.destinationFactoryLink, identity()));

                    // count of failed/canceled migration tasks
                    long failedMigrationCount = results.stream()
                            .map(state -> state.taskInfo.stage)
                            .filter(isMigrationTaskFailed)
                            .count();

                    if (failedMigrationCount > 0) {
                        sendSelfPatchForSubStage(currentTask, CURRENT_BATCH_FINISHED_WITH_FAILURE);
                    } else {
                        // current batch migration tasks all finished, move on to next
                        sendSelfPatchForSubStage(currentTask, CURRENT_BATCH_FINISHED_WITH_SUCCESS);
                    }
                })
                .sendWith(this);
    }

    private void cleanupSubscriptions(NodeGroupMigrationState task, Set<URI> subscriptionUris,
            Runnable onSuccessfulDeletion) {

        List<Operation> deletes = subscriptionUris.stream()
                .map(Operation::createDelete)
                .collect(toList());

        OperationJoin.create(deletes)
                .setCompletion((ops, exs) -> {
                    if (exs != null) {
                        List<URI> failedSubscriptionUris = exs.keySet().stream()
                                .map(opId -> ops.get(opId).getUri())
                                .collect(toList());
                        String message = format("Failed to cleanup subscriptions: url=%s, error=%s",
                                failedSubscriptionUris, Utils.toString(exs));
                        logWarning(message);
                        sendSelfFailurePatch(task, message);
                        return;
                    }
                    onSuccessfulDeletion.run();
                })
                .sendWith(this);
    }

    private void populateResultReports(NodeGroupMigrationState task) {
        for (MigrationTaskService.State request : task.runtime.generatedMigrationRequests) {
            ResultReport report = new ResultReport();
            report.batchIndex = task.runtime.batchIndex;
            report.retryCount = task.runtime.retryCount;
            report.factoryPath = request.destinationFactoryLink;
            report.request = request;
            report.response = task.runtime.resultByFactoryPath.get(request.destinationFactoryLink);
            report.resultState = report.response.taskInfo.stage;
            report.taskPath = report.response.documentSelfLink;

            task.results.add(report);
        }
    }
}
