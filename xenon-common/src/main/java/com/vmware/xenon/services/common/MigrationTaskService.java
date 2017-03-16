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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.vmware.xenon.common.DeferredResult;
import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.FactoryService.FactoryServiceConfiguration;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.Operation.CompletionHandler;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceDocumentDescription.TypeName;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.ServiceStats.ServiceStat;
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
         */
        public URI sourceNodeGroupReference;

        /**
         * Factory link of the source factory.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String sourceFactoryLink;

        /**
         * URI pointing to the destination system node group. This link takes the form of
         * {protocol}://{address}:{port}/core/node-groups/{nodegroup}.
         */
        public URI destinationNodeGroupReference;

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

        // The following attributes are the outputs of the task.
        /**
         * Timestamp of the newest document migrated. This will only be accurate once the migration
         * finished successfully.
         */
        public Long latestSourceUpdateTimeMicros = 0L;

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

    private static final Integer DEFAULT_PAGE_SIZE = 500;
    private static final Long DEFAULT_MAINTENANCE_INTERVAL_MILLIS = TimeUnit.MINUTES.toMicros(1);
    private static final Integer DEFAULT_MAXIMUM_CONVERGENCE_CHECKS = 10;

    public MigrationTaskService() {
        super(MigrationTaskService.State.class);
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
        if (state.sourceNodeGroupReference == null) {
            errMsgs.add("sourceNode cannot be null.");
        }
        if (state.destinationFactoryLink == null) {
            errMsgs.add("destinationFactory cannot be null.");
        }
        if (state.destinationNodeGroupReference == null) {
            errMsgs.add("destinationNode cannot be null.");
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

    private void resolveNodeGroupReferences(State currentState) {
        logInfo("Resolving node group differences. [source=%s] [destination=%s]",
                currentState.sourceNodeGroupReference, currentState.destinationNodeGroupReference);
        Operation sourceGet = Operation.createGet(currentState.sourceNodeGroupReference);
        Operation destinationGet = Operation.createGet(currentState.destinationNodeGroupReference);

        OperationJoin.create(sourceGet, destinationGet)
                .setCompletion((os, ts) -> {
                    if (ts != null && !ts.isEmpty()) {
                        failTask(ts.values());
                        return;
                    }

                    NodeGroupState sourceGroup = os.get(sourceGet.getId())
                            .getBody(NodeGroupState.class);
                    List<URI> sourceURIs = filterAvailableNodeUris(sourceGroup);

                    NodeGroupState destinationGroup = os.get(destinationGet.getId())
                            .getBody(NodeGroupState.class);
                    List<URI> destinationURIs = filterAvailableNodeUris(destinationGroup);

                    waitUntilNodeGroupsAreStable(
                            currentState,
                            currentState.maximumConvergenceChecks,
                            () -> computeFirstCurrentPageLinks(currentState, sourceURIs,
                                    destinationURIs));
                }).sendWith(this);
    }

    private List<URI> filterAvailableNodeUris(NodeGroupState destinationGroup) {
        return destinationGroup.nodes.values().stream()
                .map(e -> {
                    if (NodeState.isUnAvailable(e)) {
                        return null;
                    }
                    return extractBaseUri(e);
                })
                .filter(uri -> uri != null)
                .collect(Collectors.toList());
    }

    private void waitUntilNodeGroupsAreStable(State currentState, int allowedConvergenceChecks, Runnable onSuccess) {
        Operation.CompletionHandler destinationCheckHandler = (o, t) -> {
            if (t != null) {
                scheduleWaitUntilNodeGroupsAreStable(currentState, allowedConvergenceChecks, onSuccess);
                return;
            }
            onSuccess.run();
        };

        Operation.CompletionHandler sourceCheckHandler = (o, t) -> {
            if (t != null) {
                scheduleWaitUntilNodeGroupsAreStable(currentState, allowedConvergenceChecks, onSuccess);
                return;
            }
            Operation destinationOp = new Operation()
                    .setReferer(getUri())
                    .setCompletion(destinationCheckHandler);
            NodeGroupUtils.checkConvergence(getHost(), currentState.sourceNodeGroupReference, destinationOp);
        };

        Operation sourceOp = new Operation()
                .setCompletion(sourceCheckHandler)
                .setReferer(getUri());
        NodeGroupUtils.checkConvergence(getHost(), currentState.sourceNodeGroupReference, sourceOp);
    }

    private void scheduleWaitUntilNodeGroupsAreStable(State currentState, int allowedConvergenceChecks, Runnable onSuccess) {
        if (allowedConvergenceChecks <= 0) {
            failTask(new Exception("Nodegroups did not converge after " + currentState.maximumConvergenceChecks + " retries."));
            return;
        }
        logInfo("Nodegroups are not convereged scheduling retry.");
        getHost().schedule(() -> {
            waitUntilNodeGroupsAreStable(currentState, allowedConvergenceChecks - 1, onSuccess);
        }, currentState.maintenanceIntervalMicros, TimeUnit.MICROSECONDS);
    }


    private void computeFirstCurrentPageLinks(State currentState, List<URI> sourceURIs, List<URI> destinationURIs) {
        logInfo("Node groups are stable. Computing pages to be migrated...");
        long documentExpirationTimeMicros = currentState.documentExpirationTimeMicros;

        // 1) request config GET on source factory, and checks whether target docs are immutable or not
        // 2) compose count query
        // 3) perform count query and set as estimated total count to migrate
        // 4) start migration

        URI sourceHostUri = selectRandomUri(sourceURIs);
        URI factoryUri = UriUtils.buildUri(sourceHostUri, currentState.destinationFactoryLink);
        URI factoryConfigUri = UriUtils.buildConfigUri(factoryUri);
        Operation configGet = Operation.createGet(factoryConfigUri);
        this.sendWithDeferredResult(configGet)
                .thenCompose(op -> {
                    FactoryServiceConfiguration factoryConfig = op.getBody(FactoryServiceConfiguration.class);

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

                    countQuery.documentExpirationTimeMicros = documentExpirationTimeMicros;
                    Operation countOp = Operation.createPost(UriUtils.buildUri(sourceHostUri, ServiceUriPaths.CORE_QUERY_TASKS))
                            .setBody(countQuery);
                    return this.sendWithDeferredResult(countOp);
                })
                .thenAccept(countOp -> {
                    QueryTask countQueryTask = countOp.getBody(QueryTask.class);
                    Long estimatedTotalServiceCount = countQueryTask.results.documentCount;

                    // query time for count query
                    setStat(STAT_NAME_COUNT_QUERY_TIME_DURATION_MICRO, countQueryTask.results.queryTimeMicros);

                    QueryTask queryTask = QueryTask.create(currentState.querySpec).setDirect(true);

                    // to speed up query for immutable docs, also include include_all_version option for retrieval
                    if (countQueryTask.querySpec.options.contains(QueryOption.INCLUDE_ALL_VERSIONS)) {
                        queryTask.querySpec.options.add(QueryOption.INCLUDE_ALL_VERSIONS);
                    }

                    queryTask.documentExpirationTimeMicros = documentExpirationTimeMicros;

                    Set<Operation> queryOps = sourceURIs.stream()
                            .map(sourceUri -> {
                                URI uri = UriUtils.buildUri(sourceUri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
                                return Operation.createPost(uri).setBody(queryTask);
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

                                adjustStat(STAT_NAME_ESTIMATED_TOTAL_SERVICE_COUNT, estimatedTotalServiceCount);

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
                        } else {
                            ownerMissMatched++;
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
            Operation o = Operation.createGet(templateUri);
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

                        Operation queryOp = Operation.createPost(postUri).setBody(q);
                        return this.sendWithDeferredResult(queryOp);
                    })
                    .thenCompose(op -> {
                        Operation getNextPageOp = Operation.createGet(getNextPageLinkUri(op));
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
                    return Operation.createPost(
                            UriUtils.buildUri(
                                    selectRandomUri(destinationURIs),
                                    state.transformationServiceLink))
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

                    return Operation.createPost(UriUtils.buildUri(
                                    selectRandomUri(destinationURIs),
                                    state.transformationServiceLink))
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

            List<Operation> ops = createMigrateOpsWithAllVersions(destinationUri, factoryLink, selfLink, docs);

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

            List<Operation> ops = createRetryMigrateOpsWithAllVersions(destinationUri, factoryLink, failedSelfLink, docs);

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
                    Operation op = Operation.createPost(uri).setBodyNoCloning(docJson);
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
                        adjustStat(STAT_NAME_PROCESSED_DOCUMENTS, posts.size());
                        migrate(state, nextPageLinks, destinationURIs, lastUpdateTimesPerOwner);
                    }
                })
                .sendWith(this);
    }


    private List<Operation> createRetryMigrateOpsWithAllVersions(URI destinationUri, String factoryLink, String selfLink, SortedSet<Object> docs) {

        URI destinationFactoryUri = UriUtils.buildUri(destinationUri, factoryLink);
        URI destinationTargetUri = UriUtils.extendUri(destinationFactoryUri, selfLink);

        Operation delete = Operation.createDelete(destinationTargetUri)
                    .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);

        List<Operation> createOps = createMigrateOpsWithAllVersions(destinationUri, factoryLink, selfLink, docs);

        List<Operation> ops = new ArrayList<>();
        ops.add(delete);
        ops.addAll(createOps);

        return ops;
    }

    private List<Operation> createMigrateOpsWithAllVersions(URI destinationUri, String factoryLink, String selfLink, SortedSet<Object> sortedDocs) {
        List<Object> docs = new ArrayList<>(sortedDocs);
        Object firstDoc = docs.remove(0);

        URI destinationFactoryUri = UriUtils.buildUri(destinationUri, factoryLink);
        URI destinationTargetUri = UriUtils.extendUri(destinationFactoryUri, selfLink);

        List<Operation> ops = new ArrayList<>();

        // this post is used not only for initial creation in destination, but for creation after DELETE when
        // DELETE_AFTER is enabled. Therefore, PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE is specified.
        Operation post = Operation.createPost(destinationFactoryUri)
                .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                .setBodyNoCloning(firstDoc);
        ops.add(post);

        // append completion handlers to create doc history
        for (Object doc : docs) {
            Action action = Action.valueOf(Utils.fromJson(doc, ServiceDocument.class).documentUpdateAction);

            Operation operation;
            switch (action) {
            case PUT:
                operation = Operation.createPut(destinationTargetUri)
                        .setBodyNoCloning(doc);
                break;
            case PATCH:
                operation = Operation.createPatch(destinationTargetUri)
                        .setBodyNoCloning(doc);
                break;
            case DELETE:
                operation = Operation.createDelete(destinationTargetUri)
                        .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER, Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL);
                break;
            case POST:
                // this means it was deleted then created again with same selflink
                operation = Operation.createPost(destinationFactoryUri)
                        .addPragmaDirective(Operation.PRAGMA_DIRECTIVE_FORCE_INDEX_UPDATE)
                        .setBodyNoCloning(doc);
                break;
            default:
                // action has validated before
                throw new IllegalStateException("Unsupported action type: " + action);
            }


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
        Collection<Operation> deleteOperations = createDeleteOperations(entityDestinationUriTofailedOps.keySet());

        OperationJoin.create(deleteOperations)
            .setCompletion((os ,ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    failTask(ts.values());
                    return;
                }
                Collection<Operation> postOperations = createPostOperations(entityDestinationUriTofailedOps, posts);

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

    private Collection<Operation> createDeleteOperations(Collection<URI> uris) {
        return uris.stream().map(u -> Operation.createDelete(u)
                .addRequestHeader(Operation.REPLICATION_QUORUM_HEADER,
                        Operation.REPLICATION_QUORUM_HEADER_VALUE_ALL))
                .collect(Collectors.toList());
    }

    private Collection<Operation> createPostOperations(Map<URI, Operation> failedOps, Map<Operation, Object> posts) {
        return failedOps.values().stream()
               .map(o -> {
                   Object newBody = posts.get(o);
                   return Operation.createPost(o.getUri())
                           .setBodyNoCloning(newBody)
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
        return getTimeSeriesStat(statName, 1, Long.MAX_VALUE,
                EnumSet.of(AggregationType.AVG, AggregationType.MAX, AggregationType.MIN, AggregationType.LATEST));
    }
}
