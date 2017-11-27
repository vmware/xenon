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
import java.util.stream.Collectors;

import com.vmware.xenon.common.Operation;
import com.vmware.xenon.common.ServiceDocument;
import com.vmware.xenon.common.ServiceDocumentDescription;
import com.vmware.xenon.common.StatelessService;
import com.vmware.xenon.common.UriUtils;
import com.vmware.xenon.common.Utils;

public class CheckPointTaskService extends StatelessService {

    public static final String SELF_LINK = ServiceUriPaths.CHECKPOINT_TASKS;

    /** Time in milliseconds for check point lag */
    private static final long DEFAULT_CHECK_POINT_LAG_MILLISECONDS = 10;

    private static final int DEFAULT_WINDOW_SIZE_LIMIT = 100;

    public static class State extends ServiceDocument {
        /**
         * factory link of child services
         */
        public String factoryLink;

        /**
         * documentKind of child service
         */
        public String kind;

        /**
         * Result limit {@link QueryTask.QuerySpecification#resultLimit} of scanning query task
         */
        public Integer checkPointScanWindowSize;

        /**
         * Self link of node selector service which place query tasks on selected nodes
         */
        public String nodeSelectorLink;

        /**
         * Self link of {@link CheckPointService}
         */
        public String checkPointServiceLink;

        /**
         * URI of peer {@link CheckPointService}
         */
        public Set<URI> checkPointServiceUris;

        /**
         * Nodes selected from broadcast GET check points
         */
        public Map<String, URI> selectedNodes;

        /**
         * Minimum checkPoint {@link CheckPointService.CheckPointState#checkPoint} across peers
         * which serves as lower bound of scanning query task
         */
        public Long prevMinCheckPoint;

        /**
         * Next check point
         */
        public Long nextCheckPoint;

        /**
         * Decides the upper bound of scanning query task to avoid late commit document
         */
        public Long checkPointLag;

    }

    @Override
    public void handlePatch(Operation patch) {
        if (!patch.hasBody()) {
            patch.fail(new IllegalArgumentException("POST body is required"));
            return;
        }
        State initialState = patch.getBody(State.class);
        if (!validateInitialState(patch, initialState)) {
            return;
        }
        getCheckPoints(initialState, patch);
    }

    /**
     * Ensure that the input task is valid.
     */
    private boolean validateInitialState(Operation patch, State initialState) {
        List<String> errMsgs = new ArrayList<>();
        if (initialState.factoryLink == null) {
            errMsgs.add("factoryLink is required");
        }
        if (initialState.kind == null) {
            errMsgs.add("kind of document is required");
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
            patch.fail(new IllegalArgumentException(String.join("\n", errMsgs)));
            return false;
        }
        if (initialState.checkPointScanWindowSize == null) {
            initialState.checkPointScanWindowSize = DEFAULT_WINDOW_SIZE_LIMIT;
        }
        if (initialState.checkPointLag == null) {
            initialState.checkPointLag = TimeUnit.MILLISECONDS.toMicros(DEFAULT_CHECK_POINT_LAG_MILLISECONDS);
        }
        initialState.selectedNodes = null;
        initialState.checkPointServiceUris = null;
        return true;
    }

    private void getCheckPoints(State state, Operation parent) {
        Operation get = Operation.createGet(UriUtils.buildUri(this.getHost(), state.checkPointServiceLink))
                .setReferer(this.getUri())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        parent.fail(e);
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody(NodeGroupBroadcastResponse.class);
                    if (!rsp.failures.isEmpty()) {
                        parent.fail(new IllegalArgumentException(String.format("get check points failed: %s", rsp.failures)));
                        return;
                    }
                    if (rsp.nodeCount < rsp.membershipQuorum) {
                        parent.complete();
                        return;
                    }
                    state.selectedNodes = rsp.selectedNodes;
                    state.checkPointServiceUris = rsp.receivers;
                    List<Long> checkPoints =
                            rsp.jsonResponses.values().stream().map(s -> {
                                CheckPointService.CheckPointState checkPointState =
                                        Utils.fromJson(s, CheckPointService.CheckPointState.class);
                                return checkPointState.checkPoint;
                            }).collect(Collectors.toList());
                    findMinimumCheckPoint(state, checkPoints);
                    ;
                });
        this.getHost().broadcastRequest(state.nodeSelectorLink, state.checkPointServiceLink, false, get);
    }

    private void scanCheckPoint(State state, Operation parent) {
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
                        parent.fail(e);
                        return;
                    }
                    NodeGroupBroadcastResponse rsp = o.getBody((NodeGroupBroadcastResponse.class));
                    if (!rsp.failures.isEmpty()) {
                        parent.fail(new IllegalArgumentException(String.format("Query check points failed: %s", rsp.failures)));
                        return;
                    }
                    if (rsp.nodeCount < rsp.membershipQuorum) {
                        parent.complete();
                        return;
                    }
                    if (!validateSelectedNodes(state.selectedNodes, rsp.selectedNodes)) {
                        parent.fail(new IllegalArgumentException(String.format("selectedNodes miss match, expected %s, actual %s",
                                Utils.toJsonHtml(state.selectedNodes), Utils.toJsonHtml(rsp.selectedNodes))));
                        return;
                    }
                    Map<URI, Map<String, ServiceDocument>> results = new HashMap<>();
                    // convert object to ServiceDocument
                    for (URI uri : rsp.jsonResponses.keySet()) {
                        QueryTask qt = Utils.fromJson(rsp.jsonResponses.get(uri), QueryTask.class);
                        if (qt.results.documentCount > 0L) {
                            results.put(uri, qt.results.documents.entrySet().stream()
                                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> Utils.fromJson(entry.getValue(), ServiceDocument.class))));
                        } else {
                            results.put(uri, new HashMap<>());
                        }
                    }
                    if (findNextCheckPoint(state, results)) {
                        // patch
                        return;
                    }
                    parent.complete();
                })
                .sendWith(getHost());
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
        // choose one peer response with least docs to compare with
        Map.Entry<URI, Map<String, ServiceDocument>> selectedResult = null;
        for (Map.Entry<URI, Map<String, ServiceDocument>> result : results.entrySet()) {
            if (selectedResult == null) {
                selectedResult = result;
                continue;
            }
            if (selectedResult.getValue().size() < result.getValue().size()) {
                continue;
            }
            selectedResult = result;
        }
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
                // filter out old version, otherwise, check point won't
                // make any progress since old version is not synced by default
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
        if (maxHitTime == null && minMissTime == null) {
            return false;
        }
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
                .addFieldClause(ServiceDocument.FIELD_NAME_SELF_LINK,
                        state.factoryLink + UriUtils.URI_PATH_CHAR + UriUtils.URI_WILDCARD_CHAR, QueryTask.QueryTerm.MatchType.WILDCARD)
                .addFieldClause(ServiceDocument.FIELD_NAME_KIND, state.kind)
                .addRangeClause(ServiceDocument.FIELD_NAME_UPDATE_TIME_MICROS, r).build();

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

    private boolean validateSelectedNodes(Map<String, URI> expected, Map<String, URI> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (Map.Entry<String, URI> expectedEntry : expected.entrySet()) {
            String nodeId = expectedEntry.getKey();
            if (!actual.containsKey(nodeId)) {
                return false;
            }
            URI expectedUri = expectedEntry.getValue();
            URI actualUri = actual.get(nodeId);
            if (!expectedUri.equals(actualUri)) {
                return false;
            }
        }
        return true;
    }
}
