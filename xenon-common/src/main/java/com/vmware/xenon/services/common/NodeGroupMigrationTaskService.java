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
import java.util.Collections;
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
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceSubscriptionState;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.MigrationTaskService.MigrationOption;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState.MigrationRequest;
import com.vmware.xenon.services.common.NodeGroupMigrationTaskService.NodeGroupMigrationState.ResultReport;

/**
 *
 */
public class NodeGroupMigrationTaskService extends TaskService<NodeGroupMigrationState> {

    public static final String FACTORY_LINK = ServiceUriPaths.NODE_GROUP_MIGRATION_TASKS;

    private static final long MIGRATION_TASK_DURATION_MICROS = TimeUnit.MINUTES.toMicros(120);
    private static final int MIGRATION_BATCH_SIZE = 25000;
    private static final int MAX_RETRIES = 5;

    private static Predicate<TaskStage> isMigrationTaskInFinalState = taskStage ->
            EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED, TaskStage.FINISHED).contains(taskStage);

    private static Predicate<TaskStage> isMigrationTaskFailed = taskStage ->
            EnumSet.of(TaskStage.CANCELLED, TaskStage.FAILED).contains(taskStage);

    private static final List<String> AUTO_MIGRATE_PRELIMINARY_PATHS;
    private static final Set<String> AUTO_MIGRATE_EXCLUSION_PATHS;

    static {
        // These services need to be migrated in sequence before user services. Order is important.
        List<String> preliminaryPaths = new ArrayList<>();
        preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_USERS);
        preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_USER_GROUPS);
        preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_RESOURCE_GROUPS);
        preliminaryPaths.add(ServiceUriPaths.CORE_AUTHZ_ROLES);
        preliminaryPaths.add(ServiceUriPaths.CORE_CREDENTIALS);
        preliminaryPaths.add(TenantService.FACTORY_LINK);
        preliminaryPaths.add(TransactionFactoryService.SELF_LINK);

        AUTO_MIGRATE_PRELIMINARY_PATHS = Collections.unmodifiableList(preliminaryPaths);

        // following service paths are excluded from auto-resolving migration factories
        Set<String> exclusionPaths = new HashSet<>();
        exclusionPaths.addAll(AUTO_MIGRATE_PRELIMINARY_PATHS);
        exclusionPaths.add(ServiceUriPaths.CORE_GRAPH_QUERIES);
        exclusionPaths.add(ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        exclusionPaths.add(ServiceUriPaths.NODE_GROUP_FACTORY);
        exclusionPaths.add(ServiceUriPaths.CORE_QUERY_TASKS);
        exclusionPaths.add(ServiceUriPaths.SYNCHRONIZATION_TASKS);
        exclusionPaths.add(ServiceUriPaths.MIGRATION_TASKS);
        exclusionPaths.add(ServiceUriPaths.NODE_GROUP_MIGRATION_TASKS);

        AUTO_MIGRATE_EXCLUSION_PATHS = Collections.unmodifiableSet(exclusionPaths);
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
             * Paths of created migration tasks from {@link #generatedMigrationRequests}.
             */
            public Set<String> migrationTaskPaths = new HashSet<>();  // TODO: convert to map(destFactoryLink, taskPath)

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

            public void reset() {
                this.generatedMigrationRequests.clear();
                this.migrationTaskPaths.clear();
                this.subscriptionUris.clear();
                this.resultByFactoryPath.clear();
                this.retryCount = 0;
                // batchIndex will NOT reset
            }
        }


        /**
         * Hold result of single migration
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
        public String sourceNodeGroupPath;

        /**
         * The destination (or target) node-group URL to use to migrate state to during an app migration.
         * e.g.: http://dest-node.vmware.com
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public URI destinationNodeReference;

        /**
         * e.g.: /core/node-groups/default
         */
        public String destinationNodeGroupPath;

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
         */
        public List<List<MigrationRequest>> batches = new ArrayList<>();


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

    /**
     * Used for notifying migration finish from subscription callback. (Internal use in this class).
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
    protected void initializeState(NodeGroupMigrationState task, Operation taskOperation) {
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


        NodeGroupMigrationState currentTask = getState(patch);
        NodeGroupMigrationState patchBody = getBody(patch);

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

        // currentFactoryIndex should only be updated by our Service logic; not by a PATCH body. This
        // protects us against stale data
//        if (currentTask.currentBatchIndex != null) {
//            patchBody.currentFactoryIndex = currentTask.currentFactoryIndex;
//        }
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

                List<String> paths = new ArrayList<>();
                Operation callback = Operation.createGet(null);
                callback.setCompletion((queryOp, queryEx) -> {
                    ServiceDocumentQueryResult result = queryOp.getBody(ServiceDocumentQueryResult.class);
                    paths.addAll(result.documentLinks);
                });
                getHost().queryServiceUris(EnumSet.of(ServiceOption.FACTORY), true, callback);


                // make a call to root service and get factories
//                Operation.createGet(task.sourceNodeReference)
                Operation.createGet(task.destinationNodeReference)
                        .setCompletion((op, ex) -> {
                            if (ex != null) {

                                // When RootNamespaceService is NOT available, fallback to find factories on localhost
                                // since most likely migration task runs on a destination node.

                                logWarning("RootNamespaceService is not available on destination node. Fallback factory discovery to local node");

//                                List<String>
//                                Operation callback = Operation.createGet(null);
//                                callback.setCompletion((queryOp, queryEx) -> {
//                                    ServiceDocumentQueryResult result = queryOp.getBody(ServiceDocumentQueryResult.class);
//                                    result.documentLinks;
//                                });
//                                getHost().queryServiceUris(EnumSet.of(ServiceOption.FACTORY), true, );

                                String message = "RootNamespaceService is required to discover factories for migration. ex=%s";
                                sendSelfFailurePatch(task, format(message, Utils.toString(ex)));
                                return;
                            }
                            ServiceDocumentQueryResult result = op.getBody(ServiceDocumentQueryResult.class);
                            List<String> factoryPaths = result.documentLinks;


                            paths.removeAll(factoryPaths);

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

            // keep runtime info before reset
            Set<URI> subscriptionUris = new HashSet<>(task.runtime.subscriptionUris);
            int retryCount = task.runtime.retryCount;

            task.runtime.reset();

            // populate for retry
            task.runtime.generatedMigrationRequests.addAll(failedRequests);
            task.runtime.retryCount = retryCount + 1;

            // cleanup current subscription and move to next substage
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


    private MigrationTaskService.State createMigrationTaskRequest(MigrationRequest request, NodeGroupMigrationState input) {

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


    private void performAndSubscribeMigrations(List<MigrationTaskService.State> migrationTaskServiceStates, NodeGroupMigrationState state) {

        Set<URI> subscriptionUris = new HashSet<>();

        List<Operation> posts = migrationTaskServiceStates.stream()
                .map(body ->
                                Operation.createPost(UriUtils.buildUri(state.destinationNodeReference, state.migrationTaskPath))
                                        .setBody(body)
                                        .setCompletion((op, ex) -> {
                                            // TODO: exception

                                            String taskPath = op.getBody(MigrationTaskService.State.class).documentSelfLink;
                                            URI taskUri = UriUtils.buildUri(state.destinationNodeReference, taskPath);
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

        NodeGroupMigrationState currentTask = getState(patch);
        MigrationFinishedNotice request = patch.getBody(MigrationFinishedNotice.class);

        patch.complete();

        if (currentTask.runtime.batchIndex != request.currentBatchIndex) {
            // out of date finish request. Do nothing.
            return;
        }

        // check status of migration tasks, when all are finished, move to the next stage
        List<Operation> getOps = currentTask.runtime.migrationTaskPaths.stream()
                .map(path -> Operation.createGet(UriUtils.buildUri(currentTask.destinationNodeReference, path))
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

    @FunctionalInterface
    private interface OnCompleteSubscriptionDeletion {
        void perform();
    }

    private void cleanupSubscriptions(Set<URI> subscriptionUris, OnCompleteSubscriptionDeletion onSuccessfulDeletion) {
        List<Operation> deletes = subscriptionUris.stream()
                .map(Operation::createDelete)
                .collect(toList());

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
