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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.vmware.xenon.common.FactoryService;
import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.TaskState;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

/**
 * Query synchronization check point and synchronize on demand
 */
public class CheckPointTaskService extends
        TaskService<CheckPointTaskService.State>{

    public enum SubStage {
        GET_CHECK_POINTS, SCAN_CHECK_POINT, PATCH_CHECK_POINTS, SYNCHRONIZE
    }

    /** Time in seconds for the task to live */
    private static final long DEFAULT_EXPIRATION_SECONDS = 60;

    /** Time in milliseconds for check point lag */
    private static final long DEFAULT_CHECK_POINT_LAG_MILLISECONDS = 10;

    private static final int DEFAULT_WINDOW_SIZE_LIMIT = 100;

    public static final String FACTORY_LINK = "/checkpoint-task";

    /**
     * Create a default factory service that starts instances of this task service on POST.
     */
    public static FactoryService createFactory() {
        return TaskFactoryService.create(CheckPointTaskService.class);
    }

    public static class State extends TaskService.TaskServiceState {
        /**
         * Customized boolean logic which specify subset of documents to scan
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public QueryTask.Query query;

        /**
         * Result limit {@link QueryTask.QuerySpecification#resultLimit} of scanning query task
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Integer checkPointScanWindowSize;

        /**
         * Self link of node selector service which place query tasks on selected nodes
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String nodeSelectorLink;

        /**
         * Self link of {@link CheckPointService}
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public String checkPointServiceLink;

        /**
         * Minimum checkPoint {@link CheckPointService.CheckPointState#checkPoint} across peers
         * which serves as lower bound of scanning query task
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long prevMinCheckPoint;

        /**
         * Next check point
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long nextCheckPoint;

        /**
         * Decides the upper bound of scanning query task to avoid late commit document
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Long checkPointLag;

        /**
         * The current substage. See {@link SubStage}
         */
        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public SubStage subStage;

        @UsageOption(option = ServiceDocumentDescription.PropertyUsageOption.AUTO_MERGE_IF_NOT_NULL)
        public Map<String, ServiceDocument> missDocs;

        public boolean onDemandSync;
    }

    public CheckPointTaskService() {
        super(State.class);
        toggleOption(ServiceOption.REPLICATION, true);
        toggleOption(ServiceOption.OWNER_SELECTION, true);
        this.setPeerNodeSelectorPath(ServiceUriPaths.DEFAULT_1X_NODE_SELECTOR);
    }

    /**
     * Validate and initialize task state on creation
     */
    @Override
    public void handleStart(Operation startPost) {
        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("POST body is required"));
            return;
        }
        State initialState = startPost.getBody(State.class);
        if (!validateInitialState(startPost, initialState)) {
            return;
        }
        startPost.complete();
        sendSelfPatch(initialState, TaskState.TaskStage.STARTED, subStageSetter(SubStage.GET_CHECK_POINTS));
    }

    @Override
    public void handlePatch(Operation patch) {
        State currentState = getState(patch);
        State patchBody = getBody(patch);
        if (!validateTransition(patch, currentState, patchBody)) {
            return;
        }
        updateState(currentState, patchBody);
        patch.complete();
        switch (patchBody.taskInfo.stage) {
        case CREATED:
                // Won't happen: validateTransition reports error
            break;
        case STARTED:
            handleSubstage(patchBody);
            break;
        case CANCELLED:
            logInfo("Task canceled: not implemented, ignoring");
            break;
        case FINISHED:
            logFine("Task finished successfully");
            break;
        case FAILED:
            logWarning("Task failed: %s", (patchBody.failureMessage == null ? "No reason given"
                    : patchBody.failureMessage));
            break;
        default:
            logWarning("Unexpected stage: %s", patchBody.taskInfo.stage);
            break;
        }
    }

    private void handleSubstage(State state) {
        switch (state.subStage) {
        case GET_CHECK_POINTS:
            handleGetCheckPoints(state, SubStage.SCAN_CHECK_POINT);
            break;
        case SCAN_CHECK_POINT:
            handleScanCheckPoint(state, SubStage.PATCH_CHECK_POINTS);
            break;
        case PATCH_CHECK_POINTS:
            handlePatchCheckPoints(state, SubStage.SYNCHRONIZE);
            break;
        case SYNCHRONIZE:
            handleSynchronization(state);
            break;
        default:
            logWarning("Unexpected sub stage: %s", state.subStage);
            break;
        }
    }

    private void handleGetCheckPoints(State state, SubStage nextSubStage) {
        Operation get = Operation.createGet(UriUtils.buildUri(this.getHost(), state.checkPointServiceLink))
                .setReferer(this.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        sendSelfFailurePatch(state, e.getMessage());
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody(NodeGroupBroadcastResponse.class);
                    if (!rsp.failures.isEmpty()) {
                        sendSelfFailurePatch(state, String.format("Get check points failed: %s", rsp.failures));
                        return;
                    }
                    List<Long> checkPoints =
                            rsp.jsonResponses.values().stream().map(s -> {
                                CheckPointService.CheckPointState checkPointState =
                                        Utils.fromJson(s, CheckPointService.CheckPointState.class);
                                return checkPointState.checkPoint;
                            }).collect(Collectors.toList());
                    findMinimumCheckPoint(state, checkPoints);
                    sendSelfPatch(state, TaskState.TaskStage.STARTED, subStageSetter(nextSubStage));
                });
        this.getHost().broadcastRequest(state.nodeSelectorLink, state.checkPointServiceLink, false, get);
    }

    private void handleScanCheckPoint(State state, SubStage nextSubStage) {
        QueryTask queryTask = buildScanQueryTask(state);
        URI localQueryTaskFactoryUri = UriUtils.buildUri(getHost(),
                ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        URI forwardingService = UriUtils.buildBroadcastRequestUri(localQueryTaskFactoryUri,
                state.nodeSelectorLink);
        Operation.createPost(forwardingService)
                .setBody(queryTask)
                .setReferer(getHost().getUri())
                .setConnectionSharing(true)
                .setCompletion((o, e) -> {
                    if (e != null) {
                        sendSelfFailurePatch(state, e.getMessage());
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody((NodeGroupBroadcastResponse.class));
                    if (!rsp.failures.isEmpty()) {
                        sendSelfFailurePatch(state, String.format("Query check points failed: %s", rsp.failures));
                        return;
                    }
                    Map<URI, Map<String, ServiceDocument>> results = new HashMap<>();
                    // convert object to ServiceDocument
                    for (URI uri : rsp.jsonResponses.keySet()) {
                        QueryTask qt = Utils.fromJson(rsp.jsonResponses.get(uri), QueryTask.class);
                        if (qt.results.documentCount > 0L) {
                            results.put(uri, qt.results.documents.entrySet().stream()
                                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> Utils.fromJson(entry.getValue(), ServiceDocument.class))));
                        }
                    }
                    if (findNextCheckPoint(state, results)) {
                        sendSelfPatch(state, TaskState.TaskStage.STARTED, subStageSetter(nextSubStage));
                        return;
                    }
                    sendSelfFinishedPatch(state);
                })
                .sendWith(getHost());
    }

    private void handlePatchCheckPoints(State state, SubStage nextSubStage) {
        CheckPointService.CheckPointState s = new CheckPointService.CheckPointState();
        s.checkPoint = state.nextCheckPoint;
        Operation patch = Operation.createPatch(UriUtils.buildUri(this.getHost(), state.checkPointServiceLink))
                .setBody(s)
                .setReferer(this.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        sendSelfFailurePatch(state, e.getMessage());
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody((NodeGroupBroadcastResponse.class));
                    if (!rsp.failures.isEmpty()) {
                        sendSelfFailurePatch(state, String.format("Patch check points failed: %s", rsp.failures));
                        return;
                    }
                    if (state.onDemandSync && state.missDocs.size() > 0) {
                        sendSelfPatch(state, TaskState.TaskStage.STARTED, subStageSetter(nextSubStage));
                        return;
                    }
                    sendSelfFinishedPatch(state);
                });
        this.getHost().broadcastRequest(state.nodeSelectorLink, state.checkPointServiceLink, false, patch);
    }

    private void handleSynchronization(State state) {
        for (String link : state.missDocs.keySet()) {
            // TODO ON DEMAND SYNCRHONIZATION
            logInfo("synchronize %s on demand of check point", link);
        }
        sendSelfFinishedPatch(state);
    }

    private void findMinimumCheckPoint(State state, List<Long> checkPoints) {
        long minimumCheckPoint = Long.MAX_VALUE;
        for (Long checkPoint : checkPoints) {
            minimumCheckPoint = Long.min(minimumCheckPoint, checkPoint);
        }
        state.prevMinCheckPoint =  minimumCheckPoint;
    }

    private boolean findNextCheckPoint(State state, Map<URI, Map<String, ServiceDocument>> results) {
        if (results.isEmpty()) {
            return false;
        }
        Long maxHitTime = null;
        Long minMissTime = null;
        // choose one peer response to compare with
        Map.Entry<URI, Map<String, ServiceDocument>> selectedResult = results.entrySet().iterator().next();
        URI selectedUri = selectedResult.getKey();
        Set<String> hitLinkSet = new HashSet<>();
        Map<String, ServiceDocument> missDocs = new HashMap<>();
        // filter out hit link
        for (Map.Entry<String, ServiceDocument> selectedEntry : selectedResult.getValue().entrySet()) {
            String selectedLink = selectedEntry.getKey();
            boolean hit = true;
            for (Map.Entry<URI, Map<String, ServiceDocument>> entry : results.entrySet()) {
                URI u = entry.getKey();
                if (u == selectedUri) {
                    // skip selected
                    continue;
                }
                Map<String, ServiceDocument> result = entry.getValue();
                ServiceDocument doc = result.get(selectedLink);
                if (doc == null) {
                    hit = false;
                    break;
                }
                ServiceDocument selectedDoc = selectedEntry.getValue();
                if (selectedDoc.documentVersion != doc.documentVersion) {
                    hit = false;
                    break;
                }
            }
            if (hit) {
                ServiceDocument hitDoc = selectedEntry.getValue();
                // update hit time
                long hitTime = hitDoc.documentUpdateTimeMicros;
                maxHitTime = maxHitTime == null ? hitTime : Long.max(maxHitTime, hitTime);
                // for hit links filtering
                hitLinkSet.add(selectedLink);
            }
        }
        // collect miss
        for (Map<String, ServiceDocument> result : results.values()) {
            for (Map.Entry<String, ServiceDocument> entry : result.entrySet()) {
                String link = entry.getKey();
                if (hitLinkSet.contains(link)) {
                    continue;
                }
                ServiceDocument missDoc = entry.getValue();
                // filter out old version
                missDocs.compute(link, (k, v) -> {
                    if (v == null || missDoc.documentVersion > v.documentVersion) {
                        return missDoc;
                    }
                    return v;
                });
            }
        }
        hitLinkSet.clear();
        for (ServiceDocument d : missDocs.values()) {
            minMissTime = minMissTime == null ?
                    d.documentUpdateTimeMicros : Long.min(minMissTime, d.documentUpdateTimeMicros);
        }
        state.missDocs = missDocs;
        if (maxHitTime == null) {
            state.nextCheckPoint = minMissTime - 1;
            return true;
        }
        if (minMissTime == null) {
            state.nextCheckPoint = maxHitTime;
            return true;
        }
        if (maxHitTime < minMissTime) {
            state.nextCheckPoint = maxHitTime;
            return true;
        }
        state.nextCheckPoint = minMissTime - 1;
        return true;
    }

    private QueryTask buildScanQueryTask(State state) {
        QueryTask.NumericRange<Long> r =
                QueryTask.NumericRange.createLongRange(state.prevMinCheckPoint, Utils.getNowMicrosUtc() - state.checkPointLag,
                        false, true);
        QueryTask.Query q = QueryTask.Query.Builder.create()
                        .addRangeClause(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, r).build();
        if (state.query != null) {
            q.addBooleanClause(state.query);
        }
        return QueryTask.Builder.createDirectTask()
                .setQuery(q)
                .setResultLimit(state.checkPointScanWindowSize)
                .orderAscending(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, ServiceDocumentDescription.TypeName.LONG)
                .addSelectTerm(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS)
                .addSelectTerm(ServiceDocument.FIELD_NAME_VERSION)
                .addOption(QueryTask.QuerySpecification.QueryOption.EXPAND_SELECTED_FIELDS)
                .addOption(QueryTask.QuerySpecification.QueryOption.TOP_RESULTS)
                .build();
    }
    /**
     * Ensure that the input task is valid.
     */
    private boolean validateInitialState(Operation startPost, State initialState) {
        List<String> errMsgs = new ArrayList<>();
        if (!validateQuery(initialState.query)) {
            errMsgs.add("invalid query");
        }
        if (initialState.nodeSelectorLink == null) {
            errMsgs.add("nodeSelectorLink is required");
        }
        if (initialState.checkPointServiceLink == null) {
            errMsgs.add("checkPointServiceLink is required");
        }
        if (initialState.checkPointLag != null && !(initialState.checkPointLag > 0L)) {
            errMsgs.add("positive checkPointLag is required");
        }
        if (!errMsgs.isEmpty()) {
            startPost.fail(new IllegalArgumentException(String.join("\n", errMsgs)));
            return false;
        }
        if (initialState.checkPointScanWindowSize == null) {
            initialState.checkPointScanWindowSize = DEFAULT_WINDOW_SIZE_LIMIT;
        }
        if (initialState.checkPointLag == null) {
            initialState.checkPointLag = TimeUnit.MILLISECONDS.toMicros(DEFAULT_CHECK_POINT_LAG_MILLISECONDS);
        }
        if (initialState.documentExpirationTimeMicros == 0) {
            initialState.documentExpirationTimeMicros = Utils.fromNowMicrosUtc(
                    TimeUnit.SECONDS.toMicros(DEFAULT_EXPIRATION_SECONDS));
        }
        return true;
    }

    private boolean validateQuery(QueryTask.Query query) {
        if (query == null) {
            return false;
        }
        if (query.booleanClauses == null || query.booleanClauses.isEmpty()) {
            return query.term != null;
        }
        for (QueryTask.Query q : query.booleanClauses) {
            if (!validateQuery(q)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean validateTransition(Operation patch, State currentTask,
                                         State patchBody) {
        super.validateTransition(patch, currentTask, patchBody);
        if (patchBody.taskInfo.stage == TaskState.TaskStage.STARTED && patchBody.subStage == null) {
            patch.fail(new IllegalArgumentException("Missing substage"));
            return false;
        }
        if (currentTask.taskInfo != null && currentTask.taskInfo.stage != null) {
            if (currentTask.taskInfo.stage == TaskState.TaskStage.STARTED
                    && patchBody.taskInfo.stage == TaskState.TaskStage.STARTED) {
                if (currentTask.subStage.ordinal() > patchBody.subStage.ordinal()) {
                    patch.fail(new IllegalArgumentException("Task substage cannot move backwards"));
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Helper method that returns a lambda that will set SubStage for us
     * @param subStage the SubStage to use
     *
     * @return lambda helper needed for {@link TaskService#sendSelfPatch(TaskServiceState, TaskState.TaskStage, Consumer)}
     */
    private Consumer<State> subStageSetter(SubStage subStage) {
        return state -> state.subStage = subStage;
    }
}
