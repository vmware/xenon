/*
 * Copyright (c) 2015 VMware, Inc. All Rights Reserved.
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

package com.vmware.dcp.services.common;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.vmware.dcp.common.Operation;
import com.vmware.dcp.common.Operation.CompletionHandler;
import com.vmware.dcp.common.ServiceDocument;
import com.vmware.dcp.common.ServiceDocumentDescription;
import com.vmware.dcp.common.ServiceDocumentQueryResult;
import com.vmware.dcp.common.StatefulService;
import com.vmware.dcp.common.TaskState;
import com.vmware.dcp.common.TaskState.TaskStage;
import com.vmware.dcp.common.UriUtils;
import com.vmware.dcp.common.Utils;
import com.vmware.dcp.services.common.ExampleService.ExampleServiceState;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification;
import com.vmware.dcp.services.common.QueryTask.QuerySpecification.QueryOption;
import com.vmware.dcp.services.common.QueryTask.QueryTerm.MatchType;

public class LuceneQueryTaskService extends StatefulService {
    private static final long DEFAULT_EXPIRATION_SECONDS = 600;
    private ServiceDocumentQueryResult results;

    public LuceneQueryTaskService() {
        super(QueryTask.class);
        super.toggleOption(ServiceOption.REPLICATION, true);
        super.toggleOption(ServiceOption.OWNER_SELECTION, true);
    }

    @Override
    public void handleStart(Operation startPost) {
        if (!startPost.hasBody()) {
            startPost.fail(new IllegalArgumentException("Body is required"));
            return;
        }

        QueryTask initState = startPost.getBody(QueryTask.class);
        if (initState.taskInfo == null) {
            initState.taskInfo = new TaskState();
        } else if (TaskState.isFinished(initState.taskInfo)) {
            startPost.complete();
            return;
        }

        if (!validateState(initState, startPost)) {
            return;
        }

        // If the request has BROADCAST option, a forwarding service needs to be created,
        // and all nodes needs to be requested.
        if (initState.querySpec.options != null && initState.querySpec.options.contains(QueryOption
                .BROADCAST)) {
            createAndSendBroadcastQuery(initState, startPost);
        } else {
            if (initState.documentExpirationTimeMicros == 0) {
                // always set expiration so we do not accumulate tasks
                initState.documentExpirationTimeMicros = Utils.getNowMicrosUtc()
                        + TimeUnit.SECONDS.toMicros(DEFAULT_EXPIRATION_SECONDS);
            }
            initState.taskInfo.stage = TaskStage.CREATED;

            if (!initState.taskInfo.isDirect) {
                // complete POST immediately
                startPost.setStatusCode(Operation.STATUS_CODE_ACCEPTED).complete();
                // kick off query processing by patching self to STARTED
                QueryTask patchBody = new QueryTask();
                patchBody.taskInfo = new TaskState();
                patchBody.taskInfo.stage = TaskStage.STARTED;
                sendRequest(Operation.createPatch(getUri()).setBody(patchBody));
            } else {
                // Complete POST when we have results
                this.convertAndForwardToLucene(initState, startPost);
            }
        }
    }

    private boolean validateState(QueryTask initState, Operation startPost) {
        if (initState.querySpec == null) {
            startPost.fail(new IllegalArgumentException("specification is required"));
            return false;
        }

        if (initState.querySpec.query == null) {
            startPost.fail(new IllegalArgumentException("specification.query is required"));
            return false;
        }

        if (initState.taskInfo.isDirect
                && initState.querySpec.options != null
                && initState.querySpec.options.contains(QueryOption.CONTINUOUS)) {
            startPost.fail(new IllegalArgumentException("direct query task is not compatible with "
                    + QueryOption.CONTINUOUS));
            return false;
        }

        if (initState.querySpec.options != null
                && initState.querySpec.options.contains(QueryOption.BROADCAST)
                && initState.querySpec.options.contains(QueryOption.SORT)
                && initState.querySpec.sortTerm != null
                && initState.querySpec.sortTerm.propertyName != ServiceDocument.FIELD_NAME_SELF_LINK) {
            startPost.fail(new IllegalArgumentException("broadcasted query only supports sorting on " +
                    "[documentSelfLink]"));
            return false;
        }
        return true;
    }

    private void createAndSendBroadcastQuery(QueryTask queryTask, Operation origOperation) {
        queryTask.querySpec.options.remove(QueryOption.BROADCAST);

        if (!queryTask.querySpec.options.contains(QueryOption.SORT)) {
            queryTask.querySpec.options.add(QueryOption.SORT);
            queryTask.querySpec.sortTerm = new QueryTask.QueryTerm();
            queryTask.querySpec.sortTerm.propertyType = ServiceDocumentDescription.TypeName.STRING;
            queryTask.querySpec.sortTerm.propertyName = ServiceDocument.FIELD_NAME_SELF_LINK;
        }

        URI localQueryTaskFactoryUri = UriUtils.buildUri(this.getHost(), ServiceUriPaths.CORE_LOCAL_QUERY_TASKS);
        URI forwardingService = UriUtils.buildBroadcastRequestUri(localQueryTaskFactoryUri,
                ServiceUriPaths.DEFAULT_NODE_SELECTOR);

        Operation op = Operation
                .createPost(forwardingService)
                .setBody(queryTask)
                .setReferer(origOperation.getReferer())
                .setCompletion((o, e) -> {
                    if (e != null) {
                        failTask(e, o, null);
                        return;
                    }

                    NodeGroupBroadcastResponse rsp = o.getBody((NodeGroupBroadcastResponse.class));
                    if (!rsp.failures.isEmpty()) {
                        failTask(new IllegalStateException("Failures received: " + Utils.toJsonHtml(rsp)), o, null);
                        return;
                    }

                    collectBroadcastQueryResults(rsp.jsonResponses, queryTask);

                    handleQueryCompletion(queryTask, null, origOperation);
                });
        this.getHost().sendRequest(op);
    }

    private void collectBroadcastQueryResults(Map<URI, String> jsonResponses, QueryTask queryTask) {
        List<ServiceDocumentQueryResult> queryResults = new ArrayList<>();
        for (Map.Entry<URI, String> entry : jsonResponses.entrySet()) {
            QueryTask rsp = Utils.fromJson(entry.getValue(), QueryTask.class);
            queryResults.add(rsp.results);
        }

        long startTime = System.currentTimeMillis();
        ServiceDocumentQueryResult mergeResult = mergeSortedList(queryResults);
        long timeElapsed = (System.currentTimeMillis() - startTime) * 1000;

        queryTask.results = mergeResult;
        queryTask.taskInfo.stage = TaskStage.FINISHED;
        queryTask.taskInfo.durationMicros = timeElapsed + Collections.max(queryResults.stream()
                .map(r -> r.queryTimeMicros)
                .collect(Collectors.toList()));
    }

    private ServiceDocumentQueryResult mergeSortedList(List<ServiceDocumentQueryResult> dataSources) {
        ServiceDocumentQueryResult result = new ServiceDocumentQueryResult();
        result.documents = new HashMap<>();
        result.documentCount = new Long(0);

        int[] indices = new int[dataSources.size()];
        while (true) {
            String documentLinkPicked = null;
            List<Integer>sourcesPicked = new ArrayList<>();
            for (int i = 0; i < dataSources.size(); i++) {
                if (indices[i] < dataSources.get(i).documentCount) {
                    String documentLink = dataSources.get(i).documentLinks.get(indices[i]);
                    if (documentLinkPicked == null) {
                        documentLinkPicked = documentLink;
                        sourcesPicked.add(i);
                    } else {
                        if (documentLink.compareTo(documentLinkPicked) < 0) {
                            documentLinkPicked = documentLink;
                            sourcesPicked.clear();
                            sourcesPicked.add(i);
                        } else if (documentLink.equals(documentLinkPicked)) {
                            documentLinkPicked = documentLink;
                            sourcesPicked.add(i);
                        }
                    }
                }
            }

            if (documentLinkPicked != null) {
                result.documentLinks.add(documentLinkPicked);
                result.documents.put(documentLinkPicked,
                        dataSources.get(sourcesPicked.get(0)).documents.get(documentLinkPicked));
                result.documentCount++;

                for (int i : sourcesPicked) {
                    indices[i]++;
                }
            } else {
                break;
            }
        }

        return result;
    }

    @Override
    public void handleGet(Operation get) {
        QueryTask currentState = Utils.clone(getState(get));
        ServiceDocumentQueryResult r = this.results;
        if (r == null || currentState == null) {
            get.setBodyNoCloning(currentState).complete();
            return;
        }

        // Infrastructure special case, do not cut and paste in services:
        // the results might contain non clonable JSON serialization artifacts so we go through
        // all these steps to use cached results, avoid cloning, etc This is NOT what services
        // should be doing but due to a unfortunate combination of KRYO and GSON, we cant
        // use results as the body, since it will not clone properly
        currentState.results = new ServiceDocumentQueryResult();
        r.copyTo(this.results);

        currentState.results.documentCount = r.documentCount;
        currentState.results.nextPageLink = r.nextPageLink;
        currentState.results.prevPageLink = r.prevPageLink;

        if (r.documentLinks != null) {
            currentState.results.documentLinks = new ArrayList<>(r.documentLinks);
        }
        if (r.documents != null) {
            currentState.results.documents = new HashMap<>(r.documents);
        }

        get.setBodyNoCloning(currentState).complete();
    }

    @Override
    public void handlePatch(Operation patch) {
        if (patch.isFromReplication()) {
            patch.complete();
            return;
        }

        QueryTask state = getState(patch);

        if (state == null) {
            // service has likely expired
            patch.fail(new IllegalStateException("service state missing"));
            return;
        }

        QueryTask patchBody = patch.getBody(QueryTask.class);
        TaskState newTaskState = patchBody.taskInfo;

        if (newTaskState == null) {
            patch.fail(new IllegalArgumentException("taskInfo is required"));
            return;
        }

        if (newTaskState.stage == null) {
            patch.fail(new IllegalArgumentException("stage is required"));
            return;
        }

        if (state.querySpec.options.contains(QueryOption.CONTINUOUS)) {
            if (handlePatchForContinuousQuery(state, patchBody, patch)) {
                return;
            }
        }

        if (newTaskState.stage.ordinal() <= state.taskInfo.stage.ordinal()) {
            patch.fail(new IllegalArgumentException(
                    "new stage must be greater than current"));
            return;
        }

        state.taskInfo = newTaskState;
        if (newTaskState.stage == TaskStage.STARTED) {
            patch.setStatusCode(Operation.STATUS_CODE_ACCEPTED);
        } else if (newTaskState.stage == TaskStage.FAILED
                || newTaskState.stage == TaskStage.CANCELLED) {
            if (newTaskState.failure == null) {
                patch.fail(new IllegalArgumentException(
                        "failure must be specified"));
                return;
            }
            logWarning("query failed: %s", newTaskState.failure.message);
        }

        patch.complete();

        if (newTaskState.stage == TaskStage.STARTED) {
            convertAndForwardToLucene(state, null);
        }
    }

    private boolean handlePatchForContinuousQuery(QueryTask state, QueryTask patchBody,
            Operation patch) {
        switch (state.taskInfo.stage) {
        case STARTED:
            // handled below
            break;
        default:
            return false;
        }

        // handle transitions from the STARTED stage
        switch (patchBody.taskInfo.stage) {
        case CREATED:
            return false;
        case STARTED:
            // if the new state is STARTED, and we are in STARTED, this is just a update notification
            // from the index that either the initial query completed, or a new update passed the
            // query filter. Subscribers can subscribe to this task and see what changed.
            break;

        case CANCELLED:
        case FAILED:
        case FINISHED:
            cancelContinuousQueryOnIndex(state);
            break;
        default:
            break;
        }

        patch.complete();
        return true;
    }

    private void convertAndForwardToLucene(QueryTask task, Operation directOp) {
        try {
            org.apache.lucene.search.Query q =
                    LuceneQueryConverter.convertToLuceneQuery(task.querySpec.query);

            task.querySpec.context.nativeQuery = q;

            org.apache.lucene.search.Sort sort = null;
            if (task.querySpec.options != null
                    && task.querySpec.options.contains(QuerySpecification.QueryOption.SORT)) {
                sort = LuceneQueryConverter.convertToLuceneSort(task.querySpec);
            }

            task.querySpec.context.nativeSort = sort;

            if (task.querySpec.resultLimit == null) {
                task.querySpec.resultLimit = Integer.MAX_VALUE;
            }

            Operation localPatch = Operation
                    .createPatch(this, task.indexLink)
                    .setBodyNoCloning(task)
                    .setCompletion((o, e) -> {
                        if (e == null) {
                            task.results = (ServiceDocumentQueryResult) o.getBodyRaw();
                        }

                        handleQueryCompletion(task, e, directOp);
                    });

            sendRequest(localPatch);
        } catch (Throwable e) {
            handleQueryCompletion(task, e, directOp);
        }
    }

    private void scheduleTaskExpiration(QueryTask task) {
        if (task.taskInfo.isDirect) {
            getHost().stopService(this);
            return;
        }

        if (getHost().isStopping()) {
            return;
        }

        Operation delete = Operation.createDelete(getUri()).setBody(new ServiceDocument());
        long delta = task.documentExpirationTimeMicros - Utils.getNowMicrosUtc();
        delta = Math.max(1, delta);
        getHost().schedule(() -> {
            if (task.querySpec.options.contains(QueryOption.CONTINUOUS)) {
                cancelContinuousQueryOnIndex(task);
            }
            sendRequest(delete);
        }, delta, TimeUnit.MICROSECONDS);
    }

    private void cancelContinuousQueryOnIndex(QueryTask task) {
        QueryTask body = new QueryTask();
        body.documentSelfLink = task.documentSelfLink;
        body.taskInfo.stage = TaskStage.CANCELLED;
        body.querySpec = task.querySpec;
        body.documentKind = task.documentKind;
        Operation cancelActiveQueryPatch = Operation
                .createPatch(this, task.indexLink)
                .setBodyNoCloning(body);
        sendRequest(cancelActiveQueryPatch);
    }

    private void failTask(Throwable e, Operation directOp, CompletionHandler c) {
        QueryTask t = new QueryTask();
        // self patch to failure
        t.taskInfo.stage = TaskStage.FAILED;
        t.taskInfo.failure = Utils.toServiceErrorResponse(e);
        if (directOp != null) {
            directOp.setBody(t).fail(e);
            return;
        }

        sendRequest(Operation.createPatch(getUri()).setBody(t).setCompletion(c));
    }

    private boolean handleQueryRetry(QueryTask task, Operation directOp) {
        if (task.querySpec.expectedResultCount == null) {
            return false;
        }

        if (task.results.documentCount >= task.querySpec.expectedResultCount) {
            return false;
        }

        // Fail the task now if we would expire within the next maint interval.
        // Otherwise self patch can fail if the document has expired and clients
        // need a chance to GET the FAILED state.
        long exp = task.documentExpirationTimeMicros - getHost().getMaintenanceIntervalMicros();
        if (exp < Utils.getNowMicrosUtc()) {
            failTask(new TimeoutException(), directOp, (o, e) -> {
                scheduleTaskExpiration(task);
            });
            return true;
        }

        getHost().schedule(() -> {
            convertAndForwardToLucene(task, directOp);
        }, getMaintenanceIntervalMicros(), TimeUnit.MICROSECONDS);

        return true;
    }

    private void handleQueryCompletion(QueryTask task, Throwable e, Operation directOp) {
        boolean scheduleExpiration = true;

        try {
            task.querySpec.context.nativeQuery = null;
            if (task.postProcessingSpec != null) {
                e = new IllegalArgumentException(
                        "Post processing is not currently supported");
            }

            if (e != null) {
                failTask(e, directOp, null);
                return;
            }

            if (handleQueryRetry(task, directOp)) {
                scheduleExpiration = false;
                return;
            }

            if (task.querySpec.options.contains(QueryOption.CONTINUOUS)) {
                // A continuous query does not cache results: since it receive updates
                // at any time, a GET on the query will cause the query to be re-computed. This is
                // costly, so it should be avoided.
                task.taskInfo.stage = TaskStage.STARTED;
            } else {
                this.results = task.results;
                task.taskInfo.stage = TaskStage.FINISHED;
                task.taskInfo.durationMicros = task.results.queryTimeMicros;
            }

            if (task.documentOwner == null) {
                task.documentOwner = getHost().getId();
            }

            if (directOp != null) {
                directOp.setBodyNoCloning(task).complete();
            } else {
                // PATCH self to finished
                // we do not clone our state since we already cloned before the query
                // started
                sendRequest(Operation.createPatch(getUri()).setBodyNoCloning(task));
            }
        } finally {
            if (scheduleExpiration) {
                scheduleTaskExpiration(task);
            }
        }
    }

    @Override
    public ServiceDocument getDocumentTemplate() {
        ServiceDocument td = super.getDocumentTemplate();
        QueryTask template = (QueryTask) td;
        QuerySpecification q = new QueryTask.QuerySpecification();

        QueryTask.Query kindClause = new QueryTask.Query().setTermPropertyName(
                ServiceDocument.FIELD_NAME_KIND).setTermMatchValue(
                Utils.buildKind(ExampleServiceState.class));

        QueryTask.Query nameClause = new QueryTask.Query();
        nameClause.setTermPropertyName("name")
                .setTermMatchValue("query-target")
                .setTermMatchType(MatchType.PHRASE);

        q.query.addBooleanClause(kindClause).addBooleanClause(nameClause);
        template.querySpec = q;

        QueryTask exampleTask = new QueryTask();
        template.indexLink = exampleTask.indexLink;
        return template;
    }
}
