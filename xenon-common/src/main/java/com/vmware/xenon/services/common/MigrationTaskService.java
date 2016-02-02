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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.OperationJoin;
import com.vmware.xenon.common.Service;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription.PropertyUsageOption;
import com.vmware.xenon.common.ServiceMaintenanceRequest;
import com.vmware.xenon.common.ServiceMaintenanceRequest.MaintenanceReason;
import com.vmware.xenon.common.StatefulService;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.TaskState.TaskStage;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.NodeGroupService.NodeGroupState;
import com.vmware.xenon.services.common.QueryTask.NumericRange;
import com.vmware.xenon.services.common.QueryTask.Query;
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
 *   We post a map of source document to destination factory to the transformation service and expect a map of
 *   String (json of transformed object) to destination factory (can be different than the posted destination
 *   factory) as a response.
 *
 * Suggested Use:
 *   For each service that needs to be migration start one MigrationTaskService instance. Common scenarios for the
 *   use of this service are:
 *
 * - Warming up new nodes to add to an existing node group to minimize the impact of adding a node to an existing
 * - Upgrade
 */
public class MigrationTaskService extends StatefulService {

    public static final String FACTORY_LINK = ServiceUriPaths.MIGRATION_TASKS;

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

        // The following attributes are state internal to the task.
        /**
         * Current query pages being processed.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Collection<URI> currentPageLinks;

        /**
         * Cached list of source node references.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<URI> resolvedSourceNodeGroupReferences;

        /**
         * Cached list of destination node references.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public List<URI> resolvedDestinationNodeGroupReferences;

        // The following attributes are the outputs of the task.
        /**
         * Number of entities processed thus far.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long processedServiceCount;

        /**
         * Expected number of entities to be processed.
         */
        @UsageOption(option = PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long estimatedTotalServiceCount;

        /**
         * Timestamp of the newest document migrated. This will only be accurate once the migration
         * finished successfully.
         */
        public Long latestSourceUpdateTimeMicros = 0L;
    }

    private static final Integer DEFAULT_PAGE_SIZE = 500;
    private static final Long DEFAULT_MAINTENANCE_INTERVAL_MILLIS = TimeUnit.MINUTES.toMicros(1);
    private static final Integer DEFAULT_MAXIMUM_CONVERGENCE_CHECKS = 10;

    public MigrationTaskService() {
        super(MigrationTaskService.State.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        super.toggleOption(ServiceOption.PERSISTENCE, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        State initState = getBody(startPost);
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

        if (initState.currentPageLinks == null) {
            initState.currentPageLinks = new ArrayList<>();
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
        if (initState.continuousMigration == null) {
            initState.continuousMigration = Boolean.FALSE;
        }
        return initState;
    }

    private Query buildFieldClause(State initState) {
        Query query = Query.Builder.create()
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK, addSlash(initState.sourceFactoryLink) + "*",
                        QueryTask.QueryTerm.MatchType.WILDCARD)
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
        if (TaskState.isFinished(currentState.taskInfo) ||
                TaskState.isFailed(currentState.taskInfo) ||
                TaskState.isCancelled(currentState.taskInfo)) {
            return;
        }

        if ((patchState.maintenanceIntervalMicros != null || patchState.continuousMigration != null)
                && currentState.continuousMigration) {
            setMaintenanceIntervalMicros(currentState.maintenanceIntervalMicros);
        }

        if (currentState.resolvedDestinationNodeGroupReferences == null
                || currentState.resolvedDestinationNodeGroupReferences.isEmpty()
                || currentState.resolvedSourceNodeGroupReferences == null
                || currentState.resolvedSourceNodeGroupReferences.isEmpty()) {
            resolveNodeGroupReferences(currentState);
        } else if (currentState.currentPageLinks.isEmpty()) {
            waitUntilNodeGroupsAreStable(
                    currentState,
                    currentState.maximumConvergenceChecks,
                    () -> computeFirstCurrentPageLinks(currentState));
        } else {
            migrate(currentState);
        }
    }

    @Override
    public void handleMaintenance(Operation maintenanceOperation) {
        maintenanceOperation.complete();
        ServiceMaintenanceRequest serviceMaintenanceRequest = maintenanceOperation.getBody(ServiceMaintenanceRequest.class);
        if (!serviceMaintenanceRequest.reasons.contains(MaintenanceReason.PERIODIC_SCHEDULE)) {
            return;
        }

        Operation.createGet(getUri())
            .setCompletion((o, t) -> {
                if (t != null) {
                    logWarning("Error retrieving document %s, %s", getUri(), t);
                    return;
                }
                State state = o.getBody(State.class);
                if (state.continuousMigration
                        && !(state.taskInfo.stage == TaskStage.STARTED || state.taskInfo.stage == TaskStage.CREATED)) {
                    State patch = new State();
                    // iff the task finished, it is safe to pick up the latestSourceUpdateTimeMicros
                    // otherwise we will use the last used query
                    if (state.taskInfo.stage == TaskStage.FINISHED) {
                        patch.querySpec = state.querySpec;
                        // update or add a the numeric query clause
                        Query q = findUpdateTimeMicrosRangeClause(patch.querySpec.query);
                        if (q != null) {
                            q.setNumericRange(NumericRange.createGreaterThanOrEqualRange(state.latestSourceUpdateTimeMicros));
                        } else {
                            Query timeClause = Query.Builder.create()
                                    .addRangeClause(
                                            ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS,
                                            NumericRange.createGreaterThanOrEqualRange(state.latestSourceUpdateTimeMicros))
                                    .build();
                            patch.querySpec.query.addBooleanClause(timeClause);
                        }
                    }
                    // send state update putting service back into started state
                    patch.taskInfo = TaskState.createAsStarted();
                    Operation.createPatch(getUri())
                        .setBody(patch)
                        .sendWith(this);
                }
            })
            .sendWith(this);;
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

    private void resolveNodeGroupReferences(State initState) {
        Operation sourceGet = Operation.createGet(initState.sourceNodeGroupReference);
        Operation destinationGet = Operation.createGet(initState.destinationNodeGroupReference);

        OperationJoin.create(sourceGet, destinationGet)
            .setCompletion((os, ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    failTask(ts.values());
                    return;
                }
                // resolve the nodegroups to
                NodeGroupState sourceGroup = os.get(sourceGet.getId()).getBody(NodeGroupState.class);
                State patchState = new State();
                patchState.resolvedSourceNodeGroupReferences = sourceGroup.nodes.entrySet().stream()
                        .map(e -> {
                            NodeState state = e.getValue();
                            URI uri = state.groupReference;
                            return UriUtils.buildUri(uri.getScheme(), uri.getHost(), uri.getPort(), null, null);
                        }).collect(Collectors.toList());

                NodeGroupState destinationGroup = os.get(destinationGet.getId()).getBody(NodeGroupState.class);
                patchState.resolvedDestinationNodeGroupReferences = destinationGroup.nodes.entrySet().stream()
                        .map(e -> {
                            NodeState state = e.getValue();
                            URI uri = state.groupReference;
                            return UriUtils.buildUri(uri.getScheme(), uri.getHost(), uri.getPort(), null, null);
                        }).collect(Collectors.toList());

                Operation.createPatch(getUri())
                    .setBody(patchState)
                    .sendWith(this);
            })
            .sendWith(this);
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
            NodeGroupUtils.checkConvergenceOfRemoteNodegroup(getHost(), currentState.sourceNodeGroupReference, destinationOp);
        };

        Operation sourceOp = new Operation()
                .setCompletion(sourceCheckHandler)
                .setReferer(getUri());
        NodeGroupUtils.checkConvergenceOfRemoteNodegroup(getHost(), currentState.sourceNodeGroupReference, sourceOp);
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

    private void computeFirstCurrentPageLinks(State currentState) {
        QueryTask queryTask = QueryTask.create(currentState.querySpec).setDirect(true);
        Collection<Operation> queryOps = currentState.resolvedSourceNodeGroupReferences.stream()
                .map(uri -> {
                    return Operation.createPost(UriUtils.buildUri(uri, ServiceUriPaths.CORE_LOCAL_QUERY_TASKS))
                            .setBody(queryTask);
                })
                .collect(Collectors.toSet());

        QueryTask resultCountQuery = QueryTask.Builder.createDirectTask()
                .addOption(QueryOption.COUNT)
                .setQuery(buildFieldClause(currentState))
                .build();
        Operation resultCountOperation = Operation.createPost(
                UriUtils.buildUri(
                        selectRandomUri(currentState.resolvedSourceNodeGroupReferences),
                        ServiceUriPaths.CORE_QUERY_TASKS))
                .setBody(resultCountQuery);

        queryOps.add(resultCountOperation);

        OperationJoin.create(queryOps)
            .setCompletion((os, ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    failTask(ts.values());
                    return;
                }
                State patch = new State();
                patch.currentPageLinks = os.values().stream()
                        .filter(operation -> operation.getId() != resultCountOperation.getId())
                        .filter(operation -> operation.getBody(QueryTask.class).results.nextPageLink != null)
                        .map(operation -> getNextPageLinkUri(operation))
                        .collect(Collectors.toSet());

                patch.estimatedTotalServiceCount
                    = Optional.ofNullable(currentState.estimatedTotalServiceCount).orElse(0L)
                        + os.get(resultCountOperation.getId()).getBody(QueryTask.class).results.documentCount;

                // if there are no next page links we are done early with migration
                if (patch.currentPageLinks.isEmpty()) {
                    patch.taskInfo = TaskState.createAsFinished();
                    patch.processedServiceCount = 0L;
                }

                Operation.createPatch(getUri())
                    .setBody(patch)
                    .sendWith(this);
            })
            .sendWith(this);
    }

    private void migrate(State currentState) {
        // get results
        Collection<Operation> gets = currentState.currentPageLinks.stream()
                .map(uri -> Operation.createGet(uri))
                .collect(Collectors.toSet());

        OperationJoin.create(gets)
            .setCompletion((os, ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    failTask(ts.values());
                    return;
                }
                Collection<URI> nextPages = os.values().stream()
                        .filter(operation -> operation.getBody(QueryTask.class).results.nextPageLink != null)
                        .map(operation -> getNextPageLinkUri(operation))
                        .collect(Collectors.toSet());
                Collection<Object> results = new ArrayList<>();
                Map<String, Long> perHostLastUpdateTimes = new HashMap<>();
                // merging results, only select documents that have the same owner as the query tasks to ensure
                // we get the most up to date version of the document and documents without owner.
                for (Operation op : os.values()) {
                    QueryTask queryTask = op.getBody(QueryTask.class);
                    for (Object doc : queryTask.results.documents.values()) {
                        ServiceDocument document = Utils.fromJson(doc, ServiceDocument.class);
                        String documentOwner = document.documentOwner;

                        if (documentOwner == null || documentOwner.equals(queryTask.results.documentOwner)) {
                            Long lastUpdateTime = perHostLastUpdateTimes.getOrDefault(documentOwner, 0L);
                            perHostLastUpdateTimes
                                .put(document.documentOwner, Math.max(lastUpdateTime, document.documentUpdateTimeMicros));
                            results.add(doc);
                        }
                    }
                }

                State patch = new State();
                patch.latestSourceUpdateTimeMicros = perHostLastUpdateTimes.values().stream()
                        .mapToLong(l -> l)
                        .min()
                        .orElse(0);
                patch.processedServiceCount = results.size() + Optional.ofNullable(currentState.processedServiceCount).orElse(0L);
                patch.currentPageLinks = nextPages;
                if (nextPages.isEmpty()) {
                    patch.taskInfo = TaskState.createAsFinished();
                }

                if (results.isEmpty()) {
                    Operation.createPatch(getUri()).setBody(patch).sendWith(this);
                } else {
                    transformResults(currentState, results, patch);
                }
            })
            .sendWith(this);
    }

    private void transformResults(State state, Collection<Object> results, State patch) {
        // scrub document self links
        Collection<Object> cleanJson = results.stream()
                .map(d -> {
                    return removeFactoryPathFromSelfLink(d, state.sourceFactoryLink);
                }).collect(Collectors.toList());

        // post to transformation service
        if (state.transformationServiceLink != null) {
            Collection<Operation> transformations = cleanJson.stream()
                    .map(doc -> {
                        return Operation.createPost(
                                UriUtils.buildUri(
                                        selectRandomUri(state.resolvedDestinationNodeGroupReferences),
                                        state.transformationServiceLink))
                                .setBody(Collections.singletonMap(doc, state.destinationFactoryLink));
                    })
                    .collect(Collectors.toList());
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
                    migrateEntities(transformedJson, state, patch);
                })
                .sendWith(this);
        } else {
            Map<Object, String> jsonMap = cleanJson.stream().collect(Collectors.toMap(e -> e, e -> state.destinationFactoryLink));
            migrateEntities(jsonMap, state, patch);
        }
    }

    private void migrateEntities(Map<Object, String> json, State state, State patch) {
        // create objects on destination
        Collection<Operation> posts = json.entrySet().stream()
                .map(d -> {
                    return Operation.createPost(
                            UriUtils.buildUri(
                                    selectRandomUri(state.resolvedDestinationNodeGroupReferences),
                                    d.getValue()))
                            .setBody(d.getKey());
                })
                .collect(Collectors.toList());

        OperationJoin.create(posts)
            .setCompletion((os, ts) -> {
                if (ts != null && !ts.isEmpty()) {
                    patch.taskInfo = TaskState.createAsFailed();
                    patch.taskInfo.failure = Utils.toServiceErrorResponse(ts.values().iterator().next());
                }
                Operation.createPatch(getUri()).setBody(patch).sendWith(this);
            })
            .sendWith(this);
    }

    private boolean verifyPatchedState(State state, Operation operation) {
        List<String> errMsgs = new ArrayList<>();
        if (state.taskInfo.stage == TaskStage.STARTED) {
            if (state.currentPageLinks != null) {
                errMsgs.add("nextPageLinks cannot be null.");
            }
        }
        if (!errMsgs.isEmpty()) {
            operation.fail(new IllegalArgumentException(String.join("\n", errMsgs)));
        }
        return errMsgs.isEmpty();
    }

    private State applyPatch(State patchState, State currentState) {
        Utils.mergeWithState(getDocumentTemplate().documentDescription, currentState, patchState);
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

    private void failTask(Throwable t) {
        State patch = new State();
        patch.taskInfo = TaskState.createAsFailed();
        patch.taskInfo.failure = Utils.toServiceErrorResponse(t);
        Operation.createPatch(getUri())
            .setBody(patch)
            .sendWith(this);
    }

    private void failTask(Collection<Throwable> t) {
        failTask(t.iterator().next());
    }
}
