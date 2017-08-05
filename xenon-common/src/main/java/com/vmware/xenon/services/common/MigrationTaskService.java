/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
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

import static java.util.stream.Collectors.toList;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.FactoryService.FactoryServiceConfiguration;
import com.vmware.xenon.common.NodeSelectorState;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceConfiguration;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceDocumentQueryResult;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.ServiceStatUtils;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats;
import com.vmware.xenon.common.ServiceStats.TimeSeriesStats.AggregationType;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
import com.vmware.xenon.services.common.QueryTask.Query.Builder;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification;
import com.vmware.xenon.services.common.QueryTask.QuerySpecification.QueryOption;

/**
 * This service queries a Xenon node group for all service documents created from a specified factory link.
 * It migrates service documents in three steps:
 *
 * 1. Retrieve service documents from the source node-group
 * 2. Post service documents to the transformation service
 * 3. Post transformed service documents to the destination node-group
 *
 * To retrieve the service documents from the source system by running local paginated queries against each node.
 * We merge these results by selecting only the documents owned by the respective hosts and keeping track of the
 * lastUpdatedTime per host. This will allow us to start a new migration task picking up all documents changed
 * after the lastUpdatedTime.
 *
 * Before we start retrieving document from the source node group we verify that both the source and destination
 * node groups are stable. Once both node group are stable we run a query to obtain the current count of
 * documents matching the query. Since we patch the service document after each page is processed with the number
 * of documents we migrated, we can report the progress of a migration.
 *
 * This task can also run continuously restarting during maintenance intervals if not running. If enabled, the
 * query spec supplied will be modified by adding a numeric constraint to ensuring that only documents modified
 * after latestSourceUpdateTimeMicros.
 *
 * TransformationService expectations:
 * Version 1 of TransformationService:
 *   We post a map of source document to destination factory to the transformation service and expect a map of
 *   String (json of transformed object) to destination factory (can be different than the posted destination
 *   factory) as a response.
 *
 * Version 2 of TransformationService:
 *   We post a TransformRequest to the transformation service. The TransformRequest contains the source document
 *   and destination factory. The TransformResponse contains a map of the transformed json (key) and the factory
 *   to send the transformed json to (can be different than the posted destination factory).
 *
 * Suggested Use:
 *   For each service that needs to be migration start one MigrationTaskService instance. Common scenarios for the
 *   use of this service are:
 *
 * - Warming up new nodes to add to an existing node group to minimize the impact of adding a node to an existing
 * - Upgrade
 */
public class MigrationTaskService extends StatefulService {

    public static final String STAT_NAME_PROCESSED_DOCUMENTS = "processedServiceCount";
    public static final String STAT_NAME_ESTIMATED_TOTAL_SERVICE_COUNT = "estimatedTotalServiceCount";
    public static final String STAT_NAME_FETCHED_DOCUMENT_COUNT = "fetchedDocumentCount";
    public static final String STAT_NAME_OWNER_MISMATCH_COUNT = "ownerMismatchDocumentCount";
    public static final String STAT_NAME_BEFORE_TRANSFORM_COUNT = "beforeTransformDocumentCount";
    public static final String STAT_NAME_AFTER_TRANSFORM_COUNT = "afterTransformDocumentCount";
    public static final String STAT_NAME_COUNT_QUERY_TIME_DURATION_MICRO = "countQueryTimeDurationMicros";
    public static final String STAT_NAME_RETRIEVAL_OPERATIONS_DURATION_MICRO = "retrievalOperationsDurationMicros";
    public static final String STAT_NAME_RETRIEVAL_QUERY_TIME_DURATION_MICRO_FORMAT = "retrievalQueryTimeDurationMicros-%s";
    public static final String FACTORY_LINK = ServiceUriPaths.MIGRATION_TASKS;

    private static final Integer DEFAULT_PAGE_SIZE = 10_000;
    private static final Long DEFAULT_MAINTENANCE_INTERVAL_MILLIS = TimeUnit.MINUTES.toMicros(1);
    private static final Integer DEFAULT_MAXIMUM_CONVERGENCE_CHECKS = 10;

    // used for the result value of DeferredResult in order to workaround findbug warning for passing null by "defered.complete(null)".
    private static final Object DUMMY_OBJECT = new Object();

    public enum MigrationOption {
        /**
         * Enables continuous data migration.
         */
        CONTINUOUS,

        /**
         * Enables delete post upgrade fall back if idempotent post does not work.
         */
        DELETE_AFTER,

        /**
         * Enables v2 of TransformationService contract, which sends an object instead of a map.
         */
        USE_TRANSFORM_REQUEST,

        /**
         * Enable migrating historical document(old document versions).
         * The migrated versions may not have the same document versions in source, but the order of the history is
         * maintained.
         *
         * NOTE:
         * When migrating history with DELETE, destination will only have histories after delete.
         * This is due to the DELETE change in xenon 1.3.7+ that DELETE now purges past histories.
         * In prior versions, POST with PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE after DELETE added new version on top of
         * existing histories.
         */
        ALL_VERSIONS,

        /**
         * When this option is specified, the migration task calculates estimated number of documents to migrate
         * before starting actual migration.
         * The number is available via "estimatedTotalServiceCount" stats and in the log.
         *
         * NOTE:
         * It uses count query to calculate the estimate. If target is not IMMUTABLE documents, count is expensive
         * operation especially when target has a large number of documents. Thus, specifying this option may cause a
         * delay to start actual migration.
         */
        ESTIMATE_COUNT,
    }

    /**
     * Create a default factory service that starts instances of this task service on POST.
     */
    public static Service createFactory() {
        Service fs = FactoryService.create(MigrationTaskService.class, State.class);
        // Set additional factory service option. This can be set in service constructor as well
        // but its really relevant on the factory of a service.
        fs.toggleOption(ServiceOption.IDEMPOTENT_POST, true);
        fs.toggleOption(ServiceOption.INSTRUMENTATION, true);
        return fs;
    }

    public static class State extends ServiceDocument {
        /**
         * URI pointing to the source systems node group. This link takes the form of
         * {protocol}://{address}:{port}/core/node-groups/{nodegroup}.
         *
         * Cannot combine with {@link #sourceReferences}.
         */
        public URI sourceNodeGroupReference;

        /**
         * URIs of source nodes.
         *
         * <em>IMPORTANT</em>
         * Convergence check will NOT be performed on these uri references; Therefore, caller needs to check the convergence
         * before starting the migration.
         * Also, migration task retrieves each document from its owner node. This sourceReferences list needs to include
         * all node uris in source node-group; Otherwise, only partial number of documents will be migrated.
         *
         * Cannot combine with {@link #sourceNodeGroupReference}.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<URI> sourceReferences = new ArrayList<>();

        /**
         * Factory link of the source factory.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String sourceFactoryLink;

        /**
         * URI pointing to the destination system node group. This link takes the form of
         * {protocol}://{address}:{port}/core/node-groups/{nodegroup}.
         *
         * Cannot combine with {@link #destinationReferences}.
         */
        public URI destinationNodeGroupReference;

        /**
         * URIs of destination nodes.
         *
         * Convergence check will NOT be performed on these uris; Therefore, caller needs to check the convergence
         * before starting the migration.
         *
         * Cannot combine with {@link #destinationNodeGroupReference}.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<URI> destinationReferences = new ArrayList<>();

        /**
         * Factory link to post the new data to.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String destinationFactoryLink;

        /**
         * (Optional) Link to the service transforming migrated data on the destination system.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String transformationServiceLink;

        /**
         * (Optional) Additional query terms used when querying the source system.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public QuerySpecification querySpec;

        /**
         * (Optional) Status of the migration task.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public TaskState taskInfo;

        /**
         * (Optional) This time is used to setting the maintenance interval and as scheduling time.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long maintenanceIntervalMicros;

        /**
         * (Optional) Number of checks for node group convergence before failing the task.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Integer maximumConvergenceChecks;

        /**
         * (Optional) Flag enabling continuous data migration (default: false).
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Boolean continuousMigration;

        /**
         * (Optional) Migration options as destribed in {@link MigrationOption}.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public EnumSet<MigrationOption> migrationOptions;

        /**
         * (Optional) Operation timeout value to be applied to operations created by the migration task.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long operationTimeoutMicros;

        // The following attributes are the outputs of the task.
        /**
         * Timestamp of the newest document migrated. This will only be accurate once the migration
         * finished successfully.
         */
        public Long latestSourceUpdateTimeMicros = 0L;

        /**
         * Child options used by the factory being migrated.
         */
        public EnumSet<ServiceOption> factoryChildOptions;

        /**
         * Contains self-links that were selected for migration.
         *
         * This property is not indexed and is only used for book-keeping during
         * the migration process.
         */
        public Set<String> migratedSelfLinks = new ConcurrentSkipListSet<>();

        /**
         * Contains self-links that were not-selected for migration because
         * of documentOwner mismatches.
         *
         * This property is not indexed and is only used for book-keeping during
         * the migration process.
         */
        public Set<String> nonMigratedSelfLinks = new ConcurrentSkipListSet<>();

        @Override
        public String toString() {
            String stage = this.taskInfo != null && this.taskInfo.stage != null ? this.taskInfo.stage.name() : "null";
            return String.format(
                    "MigrationTaskService: [documentSelfLink=%s] [stage=%s] [sourceFactoryLink=%s]",
                    this.documentSelfLink, stage, this.sourceFactoryLink);
        }
    }

    /** The request body that is sent to a transform service as input. */
    public static class TransformRequest {

        /** The JSON-encoded original document from the source node group. */
        public String originalDocument;

        /** The original destination factory, as specified by the migration's {@link State}. */
        public String destinationLink;
    }

    /** The response body that is returned by a transform service as output. */
    public static class TransformResponse {

        /**
         * Key: transformed JSON, and Value: the target destination factory to send it to.
         * We use a map to support the use case where we want to break down one object into
         * multiple objects that need to be POSTed to different factories after they are
         * transformed.
         */
        public Map<String, String> destinationLinks;
    }

    public MigrationTaskService() {
        super(MigrationTaskService.State.class);
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.toggleOption(ServiceOption.INSTRUMENTATION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        State initState = getBody(startPost);
        logInfo("Starting migration with initState: %s", initState);
        initState = initialize(initState);
        if (TaskState.isFinished(initState.taskInfo)) {
            startPost.complete();
            return;
        }
        if (!verifyState(initState, startPost)) {
            return;
        }
        startPost.complete();
        State patchState = new State();
        if (initState.taskInfo == null) {
            patchState.taskInfo = TaskState.create();
        }

        if (initState.continuousMigration) {
            setMaintenanceIntervalMicros(initState.maintenanceIntervalMicros);
        }

        if (initState.taskInfo.stage == TaskStage.CANCELLED) {
            logInfo("In stage %s, will restart on next maintenance interval",
                    initState.taskInfo.stage);
            return;
        }

        Operation.createPatch(getUri())
            .setBody(patchState)
            .sendWith(this);
    }

    private State initialize(State initState) {
        if (initState.querySpec == null) {
            initState.querySpec = new QuerySpecification();
        }
        if (initState.querySpec.resultLimit == null) {
            initState.querySpec.resultLimit = DEFAULT_PAGE_SIZE;
        }
        initState.querySpec.options.add(QueryTask.QuerySpecification.QueryOption.EXPAND_CONTENT);
        if (initState.querySpec.query == null
                || initState.querySpec.query.booleanClauses == null
                || initState.querySpec.query.booleanClauses.isEmpty()) {
            initState.querySpec.query = buildFieldClause(initState);
        } else {
            initState.querySpec.query.addBooleanClause(buildFieldClause(initState));
        }

        if (initState.taskInfo == null) {
            initState.taskInfo = new TaskState();
        }
        if (initState.taskInfo.stage == null) {
            initState.taskInfo.stage = TaskStage.CREATED;
        }
        if (initState.maintenanceIntervalMicros == null) {
            initState.maintenanceIntervalMicros = DEFAULT_MAINTENANCE_INTERVAL_MILLIS;
        }
        if (initState.maximumConvergenceChecks == null) {
            initState.maximumConvergenceChecks = DEFAULT_MAXIMUM_CONVERGENCE_CHECKS;
        }
        if (initState.migrationOptions == null) {
            initState.migrationOptions = EnumSet.noneOf(MigrationOption.class);
        }
        if (initState.continuousMigration == null) {
            initState.continuousMigration = Boolean.FALSE;
        }
        if (initState.continuousMigration) {
            initState.migrationOptions.add(MigrationOption.CONTINUOUS);
        }
        return initState;
    }

    private Query buildFieldClause(State initState) {
        Query query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, addSlash(initState.sourceFactoryLink),
                        QueryTask.QueryTerm.MatchType.PREFIX)
                .build();
        return query;
    }

    private boolean verifyState(State state, Operation operation) {
        List<String> errMsgs = new ArrayList<>();
        if (state.sourceFactoryLink == null) {
            errMsgs.add("sourceFactory cannot be null.");
        }
        if (state.sourceNodeGroupReference == null && state.sourceReferences.isEmpty()) {
            errMsgs.add("sourceNode or sourceUri need to be specified.");
        }
        if (state.sourceNodeGroupReference != null && !state.sourceReferences.isEmpty()) {
            errMsgs.add("cannot specify both sourceNode and sourceReferences.");
        }
        if (state.destinationFactoryLink == null) {
            errMsgs.add("destinationFactory cannot be null.");
        }
        if (state.destinationNodeGroupReference == null && state.destinationReferences.isEmpty()) {
            errMsgs.add("destinationNode or destinationReferences need to be specified.");
        }
        if (state.destinationNodeGroupReference != null && !state.destinationReferences.isEmpty()) {
            errMsgs.add("cannot specify both destinationNode and destinationReferences.");
        }
        if (!errMsgs.isEmpty()) {
            operation.fail(new IllegalArgumentException(String.join(" ", errMsgs)));
        }
        return errMsgs.isEmpty();
    }

    @Override
    public void handlePatch(Operation patchOperation) {
        State patchState = getBody(patchOperation);
        State currentState = getState(patchOperation);

        applyPatch(patchState, currentState);
        if (!verifyState(currentState, patchOperation)
                && !verifyPatchedState(currentState, patchOperation)) {
            return;
        }
        patchOperation.complete();
        logInfo("After PATCH, the latest state is: %s", currentState);
        if (TaskState.isFinished(currentState.taskInfo) ||
                TaskState.isFailed(currentState.taskInfo) ||
                TaskState.isCancelled(currentState.taskInfo)) {
            return;
        }

        if ((patchState.maintenanceIntervalMicros != null || patchState.continuousMigration != null)
                && currentState.continuousMigration) {
            setMaintenanceIntervalMicros(currentState.maintenanceIntervalMicros);
        }

        resolveNodeGroupReferences(currentState);
    }

    private URI extractBaseUri(NodeState state) {
        URI uri = state.groupReference;
        return UriUtils.buildUri(uri.getScheme(), uri.getHost(), uri.getPort(), null, null);
    }

    @Override
    public void handleMaintenance(Operation maintenanceOperation) {
        maintenanceOperation.complete();
        ServiceMaintenanceRequest serviceMaintenanceRequest = maintenanceOperation.getBody(ServiceMaintenanceRequest.class);
        if (!serviceMaintenanceRequest.reasons.contains(MaintenanceReason.PERIODIC_SCHEDULE)) {
            return;
        }

        CompletionHandler c = (o, t) -> {
            if (t != null) {
                logWarning("Error retrieving document %s, %s", getUri(), t);
                return;
            }
            State state = o.getBody(State.class);
            if (!state.continuousMigration) {
                return;
            }
            if (state.taskInfo.stage == TaskStage.STARTED
                    || state.taskInfo.stage == TaskStage.CREATED) {
                return;
            }

            State patch = new State();
            logInfo("Continuous migration enabled, restarting");
            // iff the task finished, it is safe to pick up the latestSourceUpdateTimeMicros
            // otherwise we will use the last used query
            if (state.taskInfo.stage == TaskStage.FINISHED) {
                patch.querySpec = state.querySpec;
                // update or add a the numeric query clause
                Query q = findUpdateTimeMicrosRangeClause(patch.querySpec.query);
                if (q != null) {
                    q.setNumericRange(NumericRange
                            .createGreaterThanOrEqualRange(state.latestSourceUpdateTimeMicros));
                } else {
                    Query timeClause = Query.Builder
                            .create()
                            .addRangeClause(
                                    ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                                    NumericRange
                                            .createGreaterThanOrEqualRange(state.latestSourceUpdateTimeMicros))
                            .build();
                    patch.querySpec.query.addBooleanClause(timeClause);
                }
            }
            // send state update putting service back into started state
            patch.taskInfo = TaskState.createAsStarted();
            Operation.createPatch(getUri()).setBody(patch).sendWith(this);
        };
        Operation.createGet(getUri()).setCompletion(c).sendWith(this);
    }

    private Query findUpdateTimeMicrosRangeClause(Query query) {
        if (query.term != null
                && query.term.propertyName != null
                && query.term.propertyName.equals(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                && query.term.range != null) {
            return query;
        }
        if (query.booleanClauses == null) {
            return null;
        }
        for (Query q : query.booleanClauses) {
            Query match = findUpdateTimeMicrosRangeClause(q);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    /**
     * Resolve source and destination nodes.
     *
     * When sourceReferences or destinationReferences are specified in migration request, those specified uris will be used instead
     * of resolving from node-groups. In that case, convergence check of source/destination node-groups will not be
     * performed. This is because specified URIs may not be resolvable from node-group service. Therefore, caller needs
     * to make sure node-groups are currently converged.
     * When node group reference is specified instead of uris, it will access node-group service and obtain currently
     * AVAILABLE nodes, and perform convergence check.
     */
    private void resolveNodeGroupReferences(State currentState) {
        logInfo("Resolving node group differences. [source=%s] [destination=%s]",
                currentState.sourceNodeGroupReference, currentState.destinationNodeGroupReference);

        // to workaround findbug warning for passing null on complete(), make DeferredResult parameterize with Object
        // and return dummy object. The result of DeferredResult here will not be used.
        DeferredResult<Object> sourceDeferred = new DeferredResult<>();
        if (currentState.sourceReferences.isEmpty()) {

            // resolve source node URIs
            createGet(currentState.sourceNodeGroupReference, currentState)
                    .setCompletion((os, ex) -> {
                        if (ex != null) {
                            sourceDeferred.fail(ex);
                            return;
                        }

                        NodeGroupState sourceGroup = os.getBody(NodeGroupState.class);
                        currentState.sourceReferences = filterAvailableNodeUris(sourceGroup);

                        // when node-group is converged, deferred result will be completed.
                        waitUntilNodeGroupsAreStable(currentState, currentState.sourceNodeGroupReference,
                                currentState.maximumConvergenceChecks, sourceDeferred);
                    })
                    .sendWith(this);
        } else {
            sourceDeferred.complete(DUMMY_OBJECT);
        }

        DeferredResult<Object> destDeferred = new DeferredResult<>();
        if (currentState.destinationReferences.isEmpty()) {
            createGet(currentState.destinationNodeGroupReference, currentState)
                    .setCompletion((os, ex) -> {
                        if (ex != null) {
                            destDeferred.fail(ex);
                            return;
                        }

                        NodeGroupState sourceGroup = os.getBody(NodeGroupState.class);
                        currentState.destinationReferences = filterAvailableNodeUris(sourceGroup);

                        waitUntilNodeGroupsAreStable(currentState, currentState.destinationNodeGroupReference,
                                currentState.maximumConvergenceChecks, destDeferred);
                    })
                    .sendWith(this);
        } else {
            destDeferred.complete(DUMMY_OBJECT);
        }



        DeferredResult.allOf(sourceDeferred, destDeferred)
                .thenCompose(aVoid -> {
                    DeferredResult<Object> nodeSelectorAvailabilityDeferred = new DeferredResult<>();
                    waitNodeSelectorAreStable(
                            currentState, currentState.sourceReferences, currentState.sourceFactoryLink,
                            currentState.maximumConvergenceChecks, nodeSelectorAvailabilityDeferred);
                    return nodeSelectorAvailabilityDeferred;
                })
                .thenCompose(peerNodeSelectorPath -> {
                    DeferredResult<Object> waitFactoryIsAvailableDeferred = new DeferredResult<>();
                    waitFactoryIsAvailable(
                            currentState, currentState.sourceReferences.get(0), (String)peerNodeSelectorPath, currentState.sourceFactoryLink,
                            currentState.maximumConvergenceChecks, waitFactoryIsAvailableDeferred);
                    return waitFactoryIsAvailableDeferred;
                })
                .thenAccept(aVoid -> {
                    computeFirstCurrentPageLinks(currentState, currentState.sourceReferences, currentState.destinationReferences);
                })
                .exceptionally(throwable -> {
                    failTask(throwable);
                    return null;
                });
    }

    private List<URI> filterAvailableNodeUris(NodeGroupState destinationGroup) {
        return destinationGroup.nodes.values().stream()
                .filter(ns -> !NodeState.isUnAvailable(ns))
                .map(this::extractBaseUri)
                .collect(Collectors.toList());
    }

    /**
     * When node-group is converged, given DeferredResult will be marked as complete.
     * Otherwise, it will re-schedule the convergence check until it exceeds allowed convergence check count, then fail the DeferredResult.
     */
    private void waitUntilNodeGroupsAreStable(State currentState, URI nodeGroupReference, int allowedConvergenceChecks, DeferredResult<Object> deferredResult) {
        Operation callbackOp = new Operation()
                .setCompletion((op, ex) -> {
                    if (ex != null) {
                        if (allowedConvergenceChecks <= 0) {
                            String msg = "Nodegroups did not converge after " + currentState.maximumConvergenceChecks + " retries.";
                            deferredResult.fail(new Exception(msg));
                            return;
                        }

                        logInfo("Nodegroups are not convereged scheduling retry.");
                        getHost().schedule(() -> {
                            waitUntilNodeGroupsAreStable(currentState, nodeGroupReference, allowedConvergenceChecks - 1, deferredResult);
                        }, currentState.maintenanceIntervalMicros, TimeUnit.MICROSECONDS);
                        return;
                    }

                    deferredResult.complete(null);
                })
                .setReferer(getUri());
        NodeGroupUtils.checkConvergence(getHost(), nodeGroupReference, callbackOp);
    }

    private void waitFactoryIsAvailable(
            State currentState, URI hostUri, String nodeSelectorPath, String factoryLink, int maxRetry, DeferredResult<Object> deferredResult) {
        if (maxRetry <= 0) {
            String msg = String.format("Failed to verify availability of factory service %s after %s retries",
                    factoryLink, currentState.maximumConvergenceChecks);
            logSevere(msg);
            deferredResult.fail(new Exception(msg));
            return;
        }

        URI service = UriUtils.buildUri(hostUri, factoryLink);
        NodeGroupUtils.checkServiceAvailability((o, e) -> {
            if (e != null) {
                logInfo("Factory service not available yet, scheduling retry #%d. Error message: %s ",
                        maxRetry, e.getMessage());
                getHost().schedule(() -> waitFactoryIsAvailable(
                        currentState, hostUri, nodeSelectorPath, factoryLink, maxRetry - 1, deferredResult),
                        currentState.maintenanceIntervalMicros, TimeUnit.MICROSECONDS);
                return;
            }

            logInfo("Factory service is available.");
            deferredResult.complete(null);
        }, this.getHost(), service, nodeSelectorPath);
    }

    private void waitNodeSelectorAreStable(State currentState, List<URI> sourceURIs, String factoryLink, int maxRetry, DeferredResult<Object> deferredResult) {
        Set<Operation> getOps = sourceURIs.stream()
                .map(sourceUri -> {
                    URI uri = UriUtils.buildUri(sourceUri, factoryLink);
                    return Operation.createGet(UriUtils.buildConfigUri(uri));
                })
                .collect(Collectors.toSet());

        OperationJoin.create(getOps)
                .setCompletion((os, ts) -> {
                    if (ts != null && !ts.isEmpty()) {
                        String msg = "Failed to get factory config from all source nodes";
                        logSevere(msg);
                        deferredResult.fail(new Exception(msg));
                        return;
                    }

                    String peerNodeSelectorPath = os.values().stream()
                            .map(operation -> operation.getBody(ServiceConfiguration.class).peerNodeSelectorPath)
                            .filter(selectorPath -> selectorPath != null)
                            .findFirst().get();

                    waitNodeSelectorAreStableRetry(currentState, sourceURIs, maxRetry, peerNodeSelectorPath, deferredResult);
                })
                .sendWith(this);
    }

    private void waitNodeSelectorAreStableRetry(State currentState, List<URI> sourceURIs, int maxRetry, String peerNodeSelectorPath, DeferredResult<Object> deferredResult) {
        if (maxRetry <= 0) {
            String msg = String.format("Failed to verify availability of all node selector paths after %s retries",
                    currentState.maximumConvergenceChecks);
            logSevere(msg);
            deferredResult.fail(new Exception(msg));
            return;
        }

        Set<Operation> getOps = sourceURIs.stream()
                .map(sourceUri ->
                        Operation.createGet(UriUtils.buildUri(sourceUri, peerNodeSelectorPath)))
                .collect(Collectors.toSet());

        OperationJoin.create(getOps)
                .setCompletion((os, ts) -> {
                    if (ts != null && !ts.isEmpty()) {
                        logInfo("Failed (%s) to get reply from all (%s) Node Selectors, scheduling retry #%d.",
                                ts.size(), os.size(), maxRetry);
                        getHost().schedule(() -> waitNodeSelectorAreStableRetry(
                                currentState, sourceURIs, maxRetry - 1, peerNodeSelectorPath, deferredResult),
                                currentState.maintenanceIntervalMicros, TimeUnit.MICROSECONDS);
                        return;
                    }

                    List<NodeSelectorState.Status> availableNodeSelectors = os.values().stream()
                            .map(operation -> operation.getBody(NodeSelectorState.class).status)
                            .filter(status -> status.equals(NodeSelectorState.Status.AVAILABLE))
                            .collect(Collectors.toList());

                    if (availableNodeSelectors.size() != sourceURIs.size()) {
                        logInfo("Not all (%d) Node Selectors are available (%d) , scheduling retry #%d.",
                                sourceURIs.size(), availableNodeSelectors.size(), maxRetry);
                        getHost().schedule(() -> waitNodeSelectorAreStableRetry(
                                currentState, sourceURIs, maxRetry - 1, peerNodeSelectorPath, deferredResult),
                                currentState.maintenanceIntervalMicros, TimeUnit.MICROSECONDS);
                    } else {
                        logInfo("Node Selectors are available.");
                        deferredResult.complete(peerNodeSelectorPath);
                    }
                })
                .sendWith(this);
    }

    private void computeFirstCurrentPageLinks(State currentState, List<URI> sourceURIs, List<URI> destinationURIs) {
        logInfo("Node groups are stable. Computing pages to be migrated...");
        long documentExpirationTimeMicros = currentState.documentExpirationTimeMicros;

        // 1) request config GET on source factory, and checks whether target docs are immutable or not
        // 2) when "calculateEstimate==true", perform count query and set as estimated total count to migrate
        // 3) start migration

        URI sourceHostUri = selectRandomUri(sourceURIs);
        URI factoryUri = UriUtils.buildUri(sourceHostUri, currentState.sourceFactoryLink);
        URI factoryConfigUri = UriUtils.buildConfigUri(factoryUri);
        Operation configGet = createGet(factoryConfigUri, currentState);
        this.sendWithDeferredResult(configGet)
                .thenCompose(op -> {
                    FactoryServiceConfiguration factoryConfig = op.getBody(FactoryServiceConfiguration.class);
                    currentState.factoryChildOptions = factoryConfig.childOptions;

                    QueryTask countQuery = QueryTask.Builder.createDirectTask()
                            .addOption(QueryOption.COUNT)
                            .setQuery(buildFieldClause(currentState))
                            .build();

                    // When source hosts are older version of xenon, factory config request may not contain childOptions.
                    // To support count query to use "INCLUDE_ALL_VERSIONS" in that case, here also checks user specified
                    // query options.
                    // When retrieval query(user specified) contains INCLUDE_ALL_VERSIONS, or target data is immutable,
                    // add INCLUDE_ALL_VERSIONS to count query
                    if (currentState.querySpec.options.contains(QueryOption.INCLUDE_ALL_VERSIONS)
                            || factoryConfig.childOptions.contains(ServiceOption.IMMUTABLE)) {
                        // Use INCLUDE_ALL_VERSIONS speeds up count query for immutable docs
                        countQuery.querySpec.options.add(QueryOption.INCLUDE_ALL_VERSIONS);
                    }


                    if (currentState.migrationOptions.contains(MigrationOption.ESTIMATE_COUNT)) {
                        countQuery.documentExpirationTimeMicros = documentExpirationTimeMicros;
                        Operation countOp =
                                createPost(UriUtils.buildUri(sourceHostUri, ServiceUriPaths.CORE_QUERY_TASKS),
                                        currentState)
                                        .setBody(countQuery);
                        return this.sendWithDeferredResult(countOp);
                    } else {
                        // populate necessary fields in next step
                        countQuery.results = new ServiceDocumentQueryResult();
                        countQuery.results.documentCount = -1L;
                        countQuery.results.queryTimeMicros = -1L;

                        Operation dummyOp = Operation.createGet(null).setBody(countQuery);
                        return DeferredResult.completed(dummyOp);
                    }
                })
                .thenAccept(countOp -> {
                    QueryTask countQueryTask = countOp.getBody(QueryTask.class);
                    Long estimatedTotalServiceCount = countQueryTask.results.documentCount;
                    long queryTimeMicros = countQueryTask.results.queryTimeMicros;

                    // query time for count query
                    logInfo("[factory=%s] Estimated total service count =%,d calculation took %,d microsec ",
                            currentState.sourceFactoryLink, estimatedTotalServiceCount, queryTimeMicros);
                    setStat(STAT_NAME_COUNT_QUERY_TIME_DURATION_MICRO, queryTimeMicros);

                    // estimated count
                    adjustStat(STAT_NAME_ESTIMATED_TOTAL_SERVICE_COUNT, estimatedTotalServiceCount);


                    QueryTask queryTask = QueryTask.create(currentState.querySpec).setDirect(true);

                    // to speed up query for immutable docs, also include include_all_version option for retrieval
                    if (countQueryTask.querySpec.options.contains(QueryOption.INCLUDE_ALL_VERSIONS)) {
                        queryTask.querySpec.options.add(QueryOption.INCLUDE_ALL_VERSIONS);
                    }

                    queryTask.documentExpirationTimeMicros = documentExpirationTimeMicros;

                    Set<Operation> queryOps = sourceURIs.stream()
                            .map(sourceUri -> {
                                URI uri = UriUtils.buildUri(sourceUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
                                return createPost(uri, currentState).setBody(queryTask);
                            })
                            .collect(Collectors.toSet());


                    OperationJoin.create(queryOps)
                            .setCompletion((os, ts) -> {
                                if (ts != null && !ts.isEmpty()) {
                                    failTask(ts.values());
                                    return;
                                }

                                Set<URI> currentPageLinks = os.values().stream()
                                        .filter(operation -> operation.getBody(QueryTask.class).results.nextPageLink != null)
                                        .map(this::getNextPageLinkUri)
                                        .collect(Collectors.toSet());


                                // if there are no next page links we are done early with migration
                                if (currentPageLinks.isEmpty()) {
                                    patchToFinished(null);
                                } else {
                                    migrate(currentState, currentPageLinks, destinationURIs, new HashMap<>());
                                }
                            })
                            .sendWith(this);
                })
                .exceptionally(throwable -> {
                    failTask(throwable);
                    throw new CompletionException(throwable);
                });
    }

    private void migrate(State currentState, Set<URI> currentPageLinks, List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {

        // This method is recursively called. When a page doesn't have nextPageLink, the recursion
        // will call here with empty currentPageLinks.
        // In that case, this has processed all entries, thus self patch to mark finish, then exit.
        if (currentPageLinks.isEmpty()) {
            if (!currentState.nonMigratedSelfLinks.isEmpty()) {
                logSevere("Some documents were not migrated due to ownership mismatches. SelfLinks: %s",
                        Utils.toJson(currentState.nonMigratedSelfLinks));

                failTask(new IllegalStateException(
                        String.format("%d documents were not migrated due to ownership mismatches.",
                                currentState.nonMigratedSelfLinks.size())));

                // These fields are only used for bookkeeping. They don't need to be
                // persisted, hence setting them to NULL so that they get garbage collected.
                currentState.migratedSelfLinks = null;
                currentState.nonMigratedSelfLinks = null;

                return;
            }

            patchToFinished(lastUpdateTimesPerOwner);
            return;
        }

        // get results
        Collection<Operation> gets = currentPageLinks.stream()
                .map(Operation::createGet)
                .collect(Collectors.toSet());
        logFine("Migrating results using %d GET operations, which came from %d currentPageLinks",
                gets.size(), currentPageLinks.size());

        long start = Utils.getSystemNowMicrosUtc();
        OperationJoin.create(gets)
            .setCompletion((os, ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    failTask(ts.values());
                    return;
                }

                // update how long it took to retrieve page documents
                ServiceStat retrievalOpTimeStat = getSingleBinTimeSeriesStat(STAT_NAME_RETRIEVAL_OPERATIONS_DURATION_MICRO);
                setStat(retrievalOpTimeStat, Utils.getSystemNowMicrosUtc() - start);


                Set<URI> nextPages = os.values().stream()
                        .filter(operation -> operation.getBody(QueryTask.class).results.nextPageLink != null)
                        .map(operation -> getNextPageLinkUri(operation))
                        .collect(Collectors.toSet());

                Collection<Object> results = new ArrayList<>();
                Map<Object, URI> hostUriByResult = new HashMap<>();

                // merging results, only select documents that have the same owner as the query tasks to ensure
                // we get the most up to date version of the document and documents without owner.
                for (Operation op : os.values()) {
                    QueryTask queryTask = op.getBody(QueryTask.class);

                    // actual query time per source host
                    String authority = op.getUri().getAuthority();
                    String queryTimeStatKey = String.format(STAT_NAME_RETRIEVAL_QUERY_TIME_DURATION_MICRO_FORMAT, authority);
                    setStat(getSingleBinTimeSeriesStat(queryTimeStatKey), queryTask.results.queryTimeMicros);

                    Collection<Object> docs = queryTask.results.documents.values();
                    int totalFetched = docs.size();
                    int ownerMissMatched = 0;

                    for (Object doc : docs) {
                        ServiceDocument document = Utils.fromJson(doc, ServiceDocument.class);
                        String documentOwner = document.documentOwner;
                        if (documentOwner == null) {
                            documentOwner = queryTask.results.documentOwner;
                        }

                        if (documentOwner.equals(queryTask.results.documentOwner)) {
                            results.add(doc);

                            // keep last processed document update time(max) in each host
                            lastUpdateTimesPerOwner.compute(documentOwner, (key, val) -> {
                                        if (val == null) {
                                            return document.documentUpdateTimeMicros;
                                        }
                                        return Math.max(val, document.documentUpdateTimeMicros);
                                    }
                            );

                            URI hostUri = getHostUri(op);
                            hostUriByResult.put(doc, hostUri);

                            if (!currentState.factoryChildOptions.contains(ServiceOption.ON_DEMAND_LOAD)) {
                                // save selfLinks that were selected for migration.
                                currentState.nonMigratedSelfLinks.remove(document.documentSelfLink);
                                currentState.migratedSelfLinks.add(document.documentSelfLink);
                            }
                        } else {
                            ownerMissMatched++;

                            // save selfLinks that were not selected due to own mismatch.
                            if (!currentState.factoryChildOptions.contains(ServiceOption.ON_DEMAND_LOAD) &&
                                    !currentState.migratedSelfLinks.contains(document.documentSelfLink)) {
                                currentState.nonMigratedSelfLinks.add(document.documentSelfLink);
                            }
                        }
                    }

                    adjustStat(STAT_NAME_FETCHED_DOCUMENT_COUNT, totalFetched);
                    adjustStat(STAT_NAME_OWNER_MISMATCH_COUNT, ownerMissMatched);
                }

                if (results.isEmpty()) {
                    // The results might be empty if all the local queries returned documents the respective hosts don't own.
                    // In this case we can just move on to the next set of pages.
                    migrate(currentState, nextPages, destinationURIs, lastUpdateTimesPerOwner);
                    return;
                }


                if (currentState.migrationOptions.contains(MigrationOption.ALL_VERSIONS)) {

                    retrieveAllVersions(results, hostUriByResult, nextPages, currentState, destinationURIs,
                            lastUpdateTimesPerOwner);

                } else {

                    transformResults(currentState, results, nextPages, destinationURIs, lastUpdateTimesPerOwner);
                }
            })
            .sendWith(this);
    }

    /**
     * For ALL_VERSIONS option, retrieve all versions of target documents
     */
    private void retrieveAllVersions(Collection<Object> results, Map<Object, URI> hostUriByResult,
            Set<URI> nextPages, State currentState, List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {

        List<DeferredResult<List<Object>>> deferredResults = new ArrayList<>();
        for (Object doc : results) {

            // full host URI where authoritative doc resides
            URI hostUri = hostUriByResult.get(doc);

            ServiceDocument document = Utils.fromJson(doc, ServiceDocument.class);
            String selfLink = document.documentSelfLink;
            URI templateUri = UriUtils.buildUri(hostUri, selfLink, ServiceHost.SERVICE_URI_SUFFIX_TEMPLATE);

            // retrieve retentionLimit from template for the doc
            Operation o = createGet(templateUri, currentState);
            DeferredResult<List<Object>> deferredResult = this.sendWithDeferredResult(o)
                    .thenCompose(op -> {
                        // based on doc desc, create a query op that retrieves all versions
                        ServiceDocument template = op.getBody(ServiceDocument.class);
                        int resultLimit = Long.valueOf(template.documentDescription.versionRetentionLimit).intValue();

                        Query qs = Builder.create()
                                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, selfLink)
                                .build();

                        QueryTask q = QueryTask.Builder.createDirectTask()
                                .addOption(QueryOption.INCLUDE_ALL_VERSIONS)
                                .addOption(QueryOption.EXPAND_CONTENT)
                                .setQuery(qs)
                                .setResultLimit(resultLimit)
                                .orderAscending(ServiceDocument.FIELD_NAME_VERSION, TypeName.LONG)
                                .build();

                        URI postUri = UriUtils.buildUri(hostUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);

                        Operation queryOp = createPost(postUri, currentState).setBody(q);
                        return this.sendWithDeferredResult(queryOp);
                    })
                    .thenCompose(op -> {
                        Operation getNextPageOp = createGet(getNextPageLinkUri(op), currentState);
                        return this.sendWithDeferredResult(getNextPageOp);
                    })
                    .thenApply(op -> {
                        QueryTask queryTask = op.getBody(QueryTask.class);
                        List<Object> docs = queryTask.results.documentLinks.stream()
                                .map(link -> queryTask.results.documents.get(link))
                                .collect(toList());

                        // list of all versions ascending by version
                        return docs;
                    })
                    .exceptionally(ex -> {
                        failTask(ex);
                        // this is terminal stage, returned value will not be used
                        return null;
                    });

            deferredResults.add(deferredResult);
        }

        DeferredResult.allOf(deferredResults)
                .thenAccept(docsList -> {
                    List<Object> allVersions = docsList.stream().flatMap(List::stream).collect(toList());
                    transformResults(currentState, allVersions, nextPages, destinationURIs, lastUpdateTimesPerOwner);
                });
    }

    private void transformUsingMap(State state, Collection<Object> cleanJson, Set<URI> nextPageLinks, List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {
        Collection<Operation> transformations = cleanJson.stream()
                .map(doc -> {
                    return createPost(
                            UriUtils.buildUri(
                                    selectRandomUri(destinationURIs),
                                    state.transformationServiceLink),
                            state)
                            .setBody(Collections.singletonMap(doc, state.destinationFactoryLink));
                })
                .collect(Collectors.toList());

        adjustStat(STAT_NAME_BEFORE_TRANSFORM_COUNT, transformations.size());

        OperationJoin.create(transformations)
                .setCompletion((os, ts) -> {
                    if (ts != null && !ts.isEmpty()) {
                        failTask(ts.values());
                        return;
                    }
                    Map<Object, String> transformedJson = new HashMap<>();
                    for (Operation o : os.values()) {
                        Map<?, ?> m = o.getBody(Map.class);
                        for (Map.Entry<?, ?> entry : m.entrySet()) {
                            transformedJson.put(
                                    entry.getKey(),
                                    Utils.fromJson(entry.getValue(), String.class)
                            );
                        }
                    }
                    adjustStat(STAT_NAME_AFTER_TRANSFORM_COUNT, transformedJson.size());
                    migrateEntities(transformedJson, state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                })
                .sendWith(this);
    }

    private void transformUsingObject(State state, Collection<Object> cleanJson, Set<URI> nextPageLinks, List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {
        Collection<Operation> transformations = cleanJson.stream()
                .map(doc -> {
                    TransformRequest transformRequest = new TransformRequest();
                    transformRequest.originalDocument = Utils.toJson(doc);
                    transformRequest.destinationLink = state.destinationFactoryLink;

                    return createPost(UriUtils.buildUri(
                            selectRandomUri(destinationURIs),
                            state.transformationServiceLink),
                            state)
                            .setBody(transformRequest);
                })
                .collect(Collectors.toList());

        adjustStat(STAT_NAME_BEFORE_TRANSFORM_COUNT, transformations.size());

        OperationJoin.create(transformations)
                .setCompletion((os, ts) -> {
                    if (ts != null && !ts.isEmpty()) {
                        failTask(ts.values());
                        return;
                    }
                    Map<Object, String> transformedJson = new HashMap<>();
                    for (Operation o : os.values()) {
                        TransformResponse response = o.getBody(TransformResponse.class);
                        transformedJson.putAll(response.destinationLinks);
                    }

                    adjustStat(STAT_NAME_AFTER_TRANSFORM_COUNT, transformedJson.size());
                    migrateEntities(transformedJson, state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                })
                .sendWith(this);
    }

    private void transformResults(State state, Collection<Object> results, Set<URI> nextPageLinks, List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {
        // scrub document self links
        Collection<Object> cleanJson = results.stream()
                .map(d -> {
                    return removeFactoryPathFromSelfLink(d, state.sourceFactoryLink);
                }).collect(Collectors.toList());

        // post to transformation service
        if (state.transformationServiceLink != null) {
            logInfo("Transforming results using [migrationOptions=%s] [transformLink=%s]",
                    state.migrationOptions, state.transformationServiceLink);
            if (state.migrationOptions.contains(MigrationOption.USE_TRANSFORM_REQUEST)) {
                transformUsingObject(state, cleanJson, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
            } else {
                transformUsingMap(state, cleanJson, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
            }
        } else {
            Map<Object, String> jsonMap = cleanJson.stream().collect(Collectors.toMap(e -> e, e -> state.destinationFactoryLink));
            migrateEntities(jsonMap, state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
        }
    }

    private void migrateEntities(Map<Object, String> json, State state, Set<URI> nextPageLinks,
            List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {

        if (json.isEmpty()) {
            // no doc to create in destination, move on to the next page
            migrate(state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
            return;
        }
        if (state.migrationOptions.contains(MigrationOption.ALL_VERSIONS)) {
            migrateEntitiesForAllVersions(json, state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
        } else {
            migrateEntitiesForSingleVersion(json, state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
        }
    }

    private void migrateEntitiesForAllVersions(Map<Object, String> json, State state, Set<URI> nextPageLinks,
            List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {

        boolean performRetry = state.migrationOptions.contains(MigrationOption.DELETE_AFTER);

        // map: selflink -> version sorted docs
        Map<String, SortedSet<Object>> docsBySelfLink = new HashMap<>();
        Map<String, String> factoryLinkBySelfLink = new HashMap<>();

        // validate supported actions in old version docs
        for (Object docJson : json.keySet()) {
            ServiceDocument doc = Utils.fromJson(docJson, ServiceDocument.class);
            Action action = Action.valueOf(doc.documentUpdateAction);
            switch (action) {
            case PUT:
            case PATCH:
            case DELETE:
            case POST:
                break;
            default:
                String format = "action=%s is not supported for ALL_VERSIONS migration. selfLink=%s, version=%s";
                String message = String.format(format, action, doc.documentSelfLink, doc.documentVersion);
                failTask(new RuntimeException(message));
                return;
            }
        }

        // populate docsBySelfLink: key=selfLink, value=docs sorted by version
        for (Entry<Object, String> entry : json.entrySet()) {
            Object docJson = entry.getKey();
            String factoryLink = entry.getValue();
            String selfLink = Utils.fromJson(docJson, ServiceDocument.class).documentSelfLink;

            factoryLinkBySelfLink.putIfAbsent(selfLink, factoryLink);
            SortedSet<Object> docs = docsBySelfLink.computeIfAbsent(selfLink, key -> {
                // sort by version ascending
                return new TreeSet<>((left, right) -> {
                    ServiceDocument leftDoc = Utils.fromJson(left, ServiceDocument.class);
                    ServiceDocument rightDoc = Utils.fromJson(right, ServiceDocument.class);
                    return Long.compare(leftDoc.documentVersion, rightDoc.documentVersion);
                });
            });

            docs.add(docJson);
        }

        // keeps failure thrown during migration ops by selfLink
        ConcurrentMap<String, Throwable> failureBySelfLink = new ConcurrentHashMap<>();

        List<DeferredResult<Operation>> deferredResults = new ArrayList<>();

        for (Entry<String, SortedSet<Object>> entry : docsBySelfLink.entrySet()) {
            String selfLink = entry.getKey();
            SortedSet<Object> docs = entry.getValue();
            String factoryLink = factoryLinkBySelfLink.get(selfLink);
            URI destinationUri = selectRandomUri(destinationURIs);

            List<Operation> ops = createMigrateOpsWithAllVersions(state, destinationUri, factoryLink, selfLink, docs);

            // use dummy operation since findbugs complain if we give null.
            DeferredResult<Operation> deferredResult = DeferredResult.completed(new Operation());
            for (Operation op : ops) {
                deferredResult = deferredResult.thenCompose(o -> {
                    logFine(() -> String.format("migrating history. link=%s%s action=%s dest=%s",
                            factoryLink, selfLink, o.getAction(), destinationUri)
                    );
                    return this.sendWithDeferredResult(op);
                });
            }
            deferredResult = deferredResult.exceptionally(throwable -> {
                logWarning("Migrating entity failed. link=%s, ex=%s", selfLink, throwable);
                failureBySelfLink.put(selfLink, throwable);
                // this is terminal stage, returned value will not be used
                return null;
            });

            deferredResults.add(deferredResult);
        }


        int numOfProcessedDoc = json.size();

        DeferredResult.allOf(deferredResults)
                // since exceptionally stage handled thrown throwable, second argument(Throwable) is always null here.
                .whenComplete((operations, ignore) -> {
                    if (failureBySelfLink.isEmpty()) {
                        adjustStat(STAT_NAME_PROCESSED_DOCUMENTS, numOfProcessedDoc);
                        migrate(state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                    } else {
                        if (performRetry) {
                            logInfo("Migration retry start. links=%s", failureBySelfLink.size());
                            retryMigrateEntitiesForAllVersions(failureBySelfLink.keySet(), docsBySelfLink, factoryLinkBySelfLink,
                                    numOfProcessedDoc, state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                        } else {
                            failTask(failureBySelfLink.values());
                        }
                    }
                });
    }

    private void retryMigrateEntitiesForAllVersions(Set<String> failedSelfLinks, Map<String, SortedSet<Object>> docsBySelfLink,
            Map<String, String> factoryLinkBySelfLink, int numOfProcessedDoc, State state, Set<URI> nextPageLinks,
            List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {
        List<DeferredResult<Operation>> retryDeferredResults = new ArrayList<>();

        for (String failedSelfLink : failedSelfLinks) {
            SortedSet<Object> docs = docsBySelfLink.get(failedSelfLink);
            String factoryLink = factoryLinkBySelfLink.get(failedSelfLink);
            URI destinationUri = selectRandomUri(destinationURIs);

            List<Operation> ops = createRetryMigrateOpsWithAllVersions(state, destinationUri, factoryLink,
                    failedSelfLink, docs);

            // start with dummy operation
            DeferredResult<Operation> deferredResult = DeferredResult.completed(new Operation());
            for (Operation op : ops) {
                deferredResult = deferredResult.thenCompose(ignore -> {
                    logFine(() -> String.format("migrating history. link=%s%s action=%s dest=%s",
                            factoryLink, failedSelfLink, op.getAction(), destinationUri)
                    );
                    return this.sendWithDeferredResult(op);
                });
            }

            retryDeferredResults.add(deferredResult);
        }

        DeferredResult.allOf(retryDeferredResults)
                .whenComplete((retryOps, retryEx) -> {
                    if (retryEx != null) {
                        failTask(retryEx);
                        return;
                    }
                    adjustStat(STAT_NAME_PROCESSED_DOCUMENTS, numOfProcessedDoc);
                    migrate(state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                });
    }

    private void migrateEntitiesForSingleVersion(Map<Object, String> json, State state, Set<URI> nextPageLinks,
            List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {

        boolean performRetry = state.migrationOptions.contains(MigrationOption.DELETE_AFTER);

        Map<Operation, Object> posts = json.entrySet().stream()
                .map(d -> {
                    Object docJson = d.getKey();
                    String factoryLink = d.getValue();
                    URI uri = UriUtils.buildUri(selectRandomUri(destinationURIs), factoryLink);
                    Operation op = createPost(uri, state).setBodyNoCloning(docJson);
                    op.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK);
                    return new AbstractMap.SimpleEntry<>(op, docJson);
                })
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        // create objects on destination
        OperationJoin.create(posts.keySet())
                .setCompletion((os, ts) -> {
                    if (ts != null && !ts.isEmpty()) {
                        if (performRetry) {
                            logWarning("Migrating entities failed with exception: %s; Retrying operation.", ts.values().iterator().next());
                            useFallBack(state, posts, ts, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                        } else {
                            failTask(ts.values());
                            return;
                        }
                    } else {
                        logInfo("[source=%s][dest=%s] MigrationTask created %,d entries in destination.",
                                state.sourceFactoryLink, state.destinationFactoryLink, posts.size());
                        adjustStat(STAT_NAME_PROCESSED_DOCUMENTS, posts.size());
                        migrate(state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                    }
                })
                .sendWith(this);
    }

    private List<Operation> createRetryMigrateOpsWithAllVersions(State currentState, URI destinationUri,
            String factoryLink, String selfLink, SortedSet<Object> docs) {

        URI destinationFactoryUri = UriUtils.buildUri(destinationUri, factoryLink);
        URI destinationTargetUri = UriUtils.extendUri(destinationFactoryUri, selfLink);

        Operation delete = createDelete(destinationTargetUri, currentState)
                .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK);

        List<Operation> createOps = createMigrateOpsWithAllVersions(currentState, destinationUri, factoryLink,
                selfLink, docs);

        List<Operation> ops = new ArrayList<>();
        ops.add(delete);
        ops.addAll(createOps);

        return ops;
    }

    private List<Operation> createMigrateOpsWithAllVersions(State currentState, URI destinationUri, String factoryLink,
            String selfLink, SortedSet<Object> sortedDocs) {
        List<Object> docs = new ArrayList<>(sortedDocs);
        Object firstDoc = docs.remove(0);

        URI destinationFactoryUri = UriUtils.buildUri(destinationUri, factoryLink);
        URI destinationTargetUri = UriUtils.extendUri(destinationFactoryUri, selfLink);

        List<Operation> ops = new ArrayList<>();

        // this post is used not only for initial creation in destination, but for creation after DELETE when
        // DELETE_AFTER is enabled. Therefore, PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE is specified.
        Operation post = createPost(destinationFactoryUri, currentState)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK)
                .setBodyNoCloning(firstDoc);
        ops.add(post);

        // append completion handlers to create doc history
        for (Object doc : docs) {
            Action action = Action.valueOf(Utils.fromJson(doc, ServiceDocument.class).documentUpdateAction);

            Operation operation;
            switch (action) {
            case PUT:
                operation = createPut(destinationTargetUri, currentState).setBodyNoCloning(doc);
                break;
            case PATCH:
                operation = createPatch(destinationTargetUri, currentState).setBodyNoCloning(doc);
                break;
            case DELETE:
                operation = createDelete(destinationTargetUri, currentState)
                        .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                                Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);
                break;
            case POST:
                // this means it was deleted then created again with same selflink
                operation = createPost(destinationFactoryUri, currentState)
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                        .setBodyNoCloning(doc);
                break;
            default:
                // action has validated before
                throw new IllegalStateException("Unsupported action type: " + action);
            }

            operation.addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK);
            ops.add(operation);
        }

        return ops;
    }

    /**
     * Patch the task state to finished.
     */
    private void patchToFinished(Map<String, Long> lastUpdateTimesPerOwner) {
        State patch = new State();
        patch.taskInfo = TaskState.createAsFinished();
        if (lastUpdateTimesPerOwner != null) {
            // pick the smallest(min) among the hosts(documentOwner)
            patch.latestSourceUpdateTimeMicros = lastUpdateTimesPerOwner.values().stream().min(Long::compare).orElse(0L);
        }
        Operation.createPatch(getUri()).setBody(patch).sendWith(this);
    }

    private void useFallBack(State state, Map<Operation, Object> posts, Map<Long, Throwable> operationFailures, Set<URI> nextPageLinks, List<URI> destinationURIs, Map<String, Long> lastUpdateTimesPerOwner) {
        Map<URI, Operation> entityDestinationUriTofailedOps = getFailedOperations(posts, operationFailures);
        Collection<Operation> deleteOperations = createDeleteOperations(state,
                entityDestinationUriTofailedOps.keySet());

        OperationJoin.create(deleteOperations)
            .setCompletion((os, ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    failTask(ts.values());
                    return;
                }
                Collection<Operation> postOperations = createPostOperations(state,
                        entityDestinationUriTofailedOps, posts);

                OperationJoin
                    .create(postOperations)
                    .setCompletion((oss, tss) -> {
                        if (tss != null && !tss.isEmpty()) {
                            failTask(tss.values());
                            return;
                        }
                        adjustStat(STAT_NAME_PROCESSED_DOCUMENTS, posts.size());
                        migrate(state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                    })
                    .sendWith(this);
            })
            .sendWith(this);
    }

    private Map<URI, Operation> getFailedOperations(Map<Operation, Object> posts, Map<Long, Throwable> operationFailures) {
        Map<URI, Operation> ops = new HashMap<>();
        for (Map.Entry<Operation, Object> entry : posts.entrySet()) {
            Operation op = entry.getKey();
            if (operationFailures.containsKey(op.getId())) {
                Object jsonObject = entry.getValue();
                String selfLink = Utils.getJsonMapValue(jsonObject, ServiceDocument.FIELD_NAME_SELF_LINK, String.class);
                URI getUri = UriUtils.buildUri(op.getUri(), op.getUri().getPath(), selfLink);
                ops.put(getUri, op);
            }
        }
        return ops;
    }

    private static Operation createDelete(URI uri, State currentState) {
        Operation op = Operation.createDelete(uri);
        return prepareOp(op, currentState);
    }

    private static Operation createGet(URI uri, State currentState) {
        Operation op = Operation.createGet(uri);
        return prepareOp(op, currentState);
    }

    private static Operation createPatch(URI uri, State currentState) {
        Operation op = Operation.createPatch(uri);
        return prepareOp(op, currentState);
    }

    private static Operation createPost(URI uri, State currentState) {
        Operation op = Operation.createPost(uri);
        return prepareOp(op, currentState);
    }

    private static Operation createPut(URI uri, State currentState) {
        Operation op = Operation.createPut(uri);
        return prepareOp(op, currentState);
    }

    private static Operation prepareOp(Operation op, State currentState) {
        if (currentState.operationTimeoutMicros != null) {
            op.setExpiration(Utils.fromNowMicrosUtc(currentState.operationTimeoutMicros));
        }
        return op;
    }

    private Collection<Operation> createDeleteOperations(State currentState, Collection<URI> uris) {
        return uris.stream().map(u -> createDelete(u, currentState)
                .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                        Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK))
                .collect(Collectors.toList());
    }

    private Collection<Operation> createPostOperations(State currentState, Map<URI, Operation> failedOps,
            Map<Operation, Object> posts) {
        return failedOps.values().stream()
                .map(o -> {
                    Object newBody = posts.get(o);
                    return createPost(o.getUri(), currentState)
                            .setBodyNoCloning(newBody)
                            .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK)
                            .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE);

                })
                .collect(Collectors.toList());
    }

    private boolean verifyPatchedState(State state, Operation operation) {
        List<String> errMsgs = new ArrayList<>();
        if (!errMsgs.isEmpty()) {
            operation.fail(new IllegalArgumentException(String.join("\n", errMsgs)));
        }
        return errMsgs.isEmpty();
    }

    private State applyPatch(State patchState, State currentState) {
        Utils.mergeWithState(getStateDescription(), currentState, patchState);
        currentState.latestSourceUpdateTimeMicros = Math.max(
                Optional.ofNullable(currentState.latestSourceUpdateTimeMicros).orElse(0L),
                Optional.ofNullable(patchState.latestSourceUpdateTimeMicros).orElse(0L));
        return currentState;
    }

    private Object removeFactoryPathFromSelfLink(Object jsonObject, String factoryPath) {
        String selfLink = extractId(jsonObject, factoryPath);
        return Utils.toJson(
                Utils.setJsonProperty(jsonObject, ServiceDocument.FIELD_NAME_SELF_LINK, selfLink));
    }

    private String extractId(Object jsonObject, String factoryPath) {
        String selfLink = Utils.getJsonMapValue(jsonObject, ServiceDocument.FIELD_NAME_SELF_LINK,
                String.class);
        if (selfLink.startsWith(factoryPath)) {
            selfLink = selfLink.replaceFirst(factoryPath, "");
        }
        return selfLink;
    }

    private URI selectRandomUri(Collection<URI> uris) {
        int num = (int) (Math.random() * uris.size());
        for (URI uri : uris) {
            if (--num < 0) {
                return uri;
            }
        }
        return null;
    }

    private String addSlash(String string) {
        if (string.endsWith("/")) {
            return string;
        }
        return string + "/";
    }

    private URI getNextPageLinkUri(Operation operation) {
        URI queryUri = operation.getUri();
        return UriUtils.buildUri(
                queryUri.getScheme(),
                queryUri.getHost(),
                queryUri.getPort(),
                operation.getBody(QueryTask.class).results.nextPageLink,
                null);
    }

    private URI getHostUri(Operation operation) {
        URI uri = operation.getUri();
        return UriUtils.buildUri(uri.getScheme(), uri.getHost(), uri.getPort(), null, null);
    }

    private void failTask(Throwable t) {
        State patch = new State();
        patch.taskInfo = TaskState.createAsFailed();
        patch.taskInfo.failure = Utils.toServiceErrorResponse(t);
        Operation.createPatch(getUri())
            .setBody(patch)
            .sendWith(this);
    }

    private void failTask(Collection<Throwable> ts) {
        for (Throwable t : ts) {
            logWarning("%s", t);
        }
        failTask(ts.iterator().next());
    }

    /**
     * Single bin time series with aggregations.
     * This is to capture min, max, avg for same stats over time.
     * Therefore, using single bin which captures all time(Long.Max_VALUE).
     */
    private ServiceStat getSingleBinTimeSeriesStat(String statName) {
        return ServiceStatUtils.getOrCreateTimeSeriesStat(this, statName, () -> new TimeSeriesStats(1, Long.MAX_VALUE,
                EnumSet.of(AggregationType.AVG, AggregationType.MAX, AggregationType.MIN, AggregationType.LATEST)));
    }
}
